use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex as SyncMutex;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::api::{CandidateInfo, RaftLeadership, RaftStateView, Router};
use crate::error::Error;
use crate::ids::NodeId;
use crate::leader::jobs::membership::change::LivenessQuery;

/// The live set and its change notification in one primitive.
///
/// The set *is* the event: consumers only ever ask "who is live now", so a
/// stream of transition events was a longer way of saying the same thing — and
/// a lossy one, since removals mutated the set without emitting anything and
/// were noticed only on the consumer's next poll tick.
#[derive(Clone)]
pub(in crate::leader) struct LivenessState {
    /// A **receiver**, deliberately: the only sender belongs to the worker, so
    /// a consumer observes the channel closing when the worker dies. Holding a
    /// sender here would make that unobservable, and a consumer would go on
    /// deciding evictions from a live set that has stopped being updated —
    /// which is exactly the hazard `leader:membership` stops for.
    live: watch::Receiver<BTreeSet<NodeId>>,
    rtts: Arc<SyncMutex<HashMap<NodeId, Duration>>>,
    /// Test-only: keeps a fixed set's sender alive, since there is no worker
    /// behind it to own one.
    #[cfg(test)]
    _fixed: Option<Arc<watch::Sender<BTreeSet<NodeId>>>>,
}

impl LivenessState {
    #[must_use]
    pub(in crate::leader) fn live_nodes(&self) -> BTreeSet<NodeId> {
        self.live.borrow().clone()
    }

    /// Wakes on every change to the live set, and reports the worker's death by
    /// closing.
    #[must_use]
    pub(in crate::leader) fn subscribe(&self) -> watch::Receiver<BTreeSet<NodeId>> {
        self.live.clone()
    }

    /// A state with a fixed live set and no worker behind it, for tests that
    /// drive a job which *requires* a `LivenessState` but is not exercising
    /// liveness itself.
    #[cfg(test)]
    #[must_use]
    pub(in crate::leader) fn fixed_for_test(live: BTreeSet<NodeId>) -> Self {
        let (tx, rx) = watch::channel(live);
        Self {
            live: rx,
            rtts: Arc::new(SyncMutex::new(HashMap::new())),
            _fixed: Some(Arc::new(tx)),
        }
    }
}

impl LivenessQuery for LivenessState {
    fn candidates(&self) -> BTreeMap<NodeId, CandidateInfo> {
        let live = self.live.borrow().clone();
        let rtts = self.rtts.lock();
        live.into_iter()
            .map(|node_id| {
                (
                    node_id,
                    CandidateInfo {
                        node_id,
                        rtt: rtts.get(&node_id).copied(),
                    },
                )
            })
            .collect()
    }
}

pub(in crate::leader) struct LivenessWorkerInputs {
    pub(in crate::leader) self_node_id: NodeId,
    pub(in crate::leader) raft_state: Arc<dyn RaftStateView>,
    pub(in crate::leader) leadership: Arc<dyn RaftLeadership>,
    pub(in crate::leader) initial_live: BTreeSet<NodeId>,
    pub(in crate::leader) router: Arc<dyn Router>,
    pub(in crate::leader) interval: Duration,
    /// Upper bound on a single probe; an over-deadline probe counts as a
    /// failure.
    pub(in crate::leader) probe_timeout: Duration,
    pub(in crate::leader) max_failures: u32,
}

pub(in crate::leader) struct LivenessWorker {
    self_node_id: NodeId,
    raft_state: Arc<dyn RaftStateView>,
    leadership: Arc<dyn RaftLeadership>,
    router: Arc<dyn Router>,
    interval: Duration,
    probe_timeout: Duration,
    max_failures: u32,
    cancel: CancellationToken,
    failures: SyncMutex<HashMap<NodeId, u32>>,
    live: Arc<watch::Sender<BTreeSet<NodeId>>>,
    rtts: Arc<SyncMutex<HashMap<NodeId, Duration>>>,
}

impl LivenessWorker {
    #[must_use]
    pub(in crate::leader) fn new(
        inputs: LivenessWorkerInputs,
        cancel: CancellationToken,
    ) -> (Self, LivenessState) {
        let (live, live_rx) = watch::channel(inputs.initial_live);
        let live = Arc::new(live);
        let rtts = Arc::new(SyncMutex::new(HashMap::new()));
        let state = LivenessState {
            live: live_rx,
            rtts: rtts.clone(),
            #[cfg(test)]
            _fixed: None,
        };
        let worker = Self {
            self_node_id: inputs.self_node_id,
            raft_state: inputs.raft_state,
            leadership: inputs.leadership,
            router: inputs.router,
            interval: inputs.interval,
            probe_timeout: inputs.probe_timeout,
            max_failures: inputs.max_failures,
            cancel,
            failures: SyncMutex::new(HashMap::new()),
            live,
            rtts,
        };
        (worker, state)
    }

    pub(in crate::leader) async fn run(self) -> Result<(), Error> {
        info!(self_node_id = ?self.self_node_id, "liveness worker: starting");
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = self.cancel.cancelled() => {
                    info!(self_node_id = ?self.self_node_id, "liveness worker: shutdown");
                    return Ok(());
                }
                _ = ticker.tick() => {}
            }

            let snapshot = self.raft_state.snapshot();
            let voter_ids = self.leadership.voter_ids();
            let peers: BTreeSet<NodeId> = crate::quorum::all_targets(&snapshot.replica)
                .into_iter()
                .chain(voter_ids)
                .filter(|&id| id != self.self_node_id)
                .collect();

            // Drop anyone we no longer probe. `live` is otherwise only ever
            // mutated from a probe result, so a node that leaves both the
            // replica set and the voter set would stay live forever — and a
            // phantom live node is not a cosmetic error: `compute_target`
            // picks it as a spare, and `needs_retarget` (`next ⊆ live`) then
            // reports no retarget is needed, so the resulting joint config
            // waits for a generation quorum from a node that is gone. There is
            // no write-point fence for this: the config is legal, it just
            // never completes.
            {
                self.live.send_if_modified(|live| {
                    let before = live.len();
                    live.retain(|id| peers.contains(id) || *id == self.self_node_id);
                    live.len() != before
                });
                // These two are keyed on the probe set, not on `live`: a peer
                // that is failing but has not yet hit `max_failures` is absent
                // from `live` while still being probed, and wiping its counter
                // every round would stop it ever reaching the threshold.
                self.failures.lock().retain(|id, _| peers.contains(id));
                self.rtts.lock().retain(|id, _| peers.contains(id));
            }

            // Probe all peers concurrently: one wedged peer must not stall
            // the probing of the others. Each probe is additionally bounded
            // by `probe_timeout` so a single round always terminates; an
            // over-deadline probe counts as a failure.
            let probes = futures::future::join_all(peers.into_iter().map(|peer| {
                let router = self.router.clone();
                let probe_timeout = self.probe_timeout;
                async move {
                    // Per-peer client from the router; its `probe` targets that peer.
                    let client = router.remote(peer);
                    let t0 = tokio::time::Instant::now();
                    let result = tokio::time::timeout(probe_timeout, client.probe())
                        .await
                        .unwrap_or_else(|_| {
                            Err(Error::Rpc(format!(
                                "liveness probe to {peer:?} timed out after {probe_timeout:?}"
                            )))
                        });
                    (peer, result, t0.elapsed())
                }
            }));
            let results = tokio::select! {
                () = self.cancel.cancelled() => return Ok(()),
                r = probes => r,
            };
            for (peer, probe_result, rtt) in results {
                match probe_result {
                    Ok(()) => {
                        self.failures.lock().remove(&peer);
                        self.rtts.lock().insert(peer, rtt);
                        crate::metrics::node_last_seen(peer);
                        crate::metrics::node_live(peer, true);
                        // Notifies only on the not-live → live transition:
                        // `send_if_modified` publishes exactly when the set
                        // actually changed.
                        self.live.send_if_modified(|live| live.insert(peer));
                        debug!(?peer, ?rtt, "liveness probe ok");
                    }
                    Err(Error::UnexpectedNodeId(ids)) => {
                        let crate::error::UnexpectedNodeId { expected, actual } = *ids;
                        debug!(
                            ?peer,
                            ?expected,
                            ?actual,
                            "liveness probe: peer replaced (unexpected node_id); triggering immediate Raft eviction",
                        );
                        self.live.send_if_modified(|live| live.remove(&peer));
                        self.rtts.lock().remove(&peer);
                        crate::metrics::node_live(peer, false);
                    }
                    Err(Error::UnknownPeer { .. }) => {
                        debug!(
                            ?peer,
                            "liveness probe: peer evicted from registry; removing from live set"
                        );
                        self.failures.lock().remove(&peer);
                        self.live.send_if_modified(|live| live.remove(&peer));
                        self.rtts.lock().remove(&peer);
                        crate::metrics::node_live(peer, false);
                    }
                    Err(e) => {
                        let consecutive = {
                            let mut map = self.failures.lock();
                            let counter = map.entry(peer).or_insert(0);
                            *counter = counter.saturating_add(1);
                            *counter
                        };
                        warn!(?peer, ?consecutive, error = %e, "liveness probe failed");
                        // Notifies only on the live → not-live transition.
                        if consecutive >= self.max_failures
                            && self.live.send_if_modified(|live| live.remove(&peer))
                        {
                            self.rtts.lock().remove(&peer);
                            crate::metrics::node_live(peer, false);
                        }
                    }
                }
            }
        }
    }
}

/// Liveness probing configuration.
#[derive(Debug, Clone)]
pub(crate) struct LivenessConfig {
    pub(crate) probe_interval: Duration,
    pub(crate) probe_timeout: Duration,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_millis(500),
            probe_timeout: Duration::from_secs(1),
        }
    }
}

impl LivenessConfig {
    pub(crate) fn validate(&self) -> Result<(), crate::error::Error> {
        if self.probe_timeout.is_zero() {
            return Err(crate::error::Error::InvalidConfiguration(
                "liveness_probe_timeout must be > 0".to_owned(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::api::LivenessProbe;
    use crate::ids::ServerName;
    use crate::meta::{RaftState, ReplicaMembership};
    use crate::raft::RaftStateHandle;
    use crate::shutdown::Shutdown;
    use async_trait::async_trait;

    struct NoVoters;
    #[async_trait::async_trait]
    impl RaftLeadership for NoVoters {
        fn current_leader(&self) -> Option<NodeId> {
            None
        }
        fn voter_ids(&self) -> BTreeSet<NodeId> {
            BTreeSet::new()
        }
        fn voter_server_names(&self) -> Vec<(NodeId, ServerName)> {
            Vec::new()
        }
        async fn remove_member(&self, _node_id: NodeId) -> Result<(), crate::Error> {
            Ok(())
        }
    }

    use crate::tests::nid;

    fn raft_state_with_peers(peers: &[NodeId]) -> Arc<dyn RaftStateView> {
        let state = RaftStateHandle::new();
        state.replace(RaftState {
            replica: ReplicaMembership::single(peers.iter().copied().collect()),
            ..RaftState::empty()
        });
        Arc::new(state)
    }

    struct OkProbe {
        calls: Arc<AtomicU32>,
    }

    #[async_trait]
    impl LivenessProbe for OkProbe {
        async fn probe(&self) -> Result<(), Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FailProbe;

    #[async_trait]
    impl LivenessProbe for FailProbe {
        async fn probe(&self) -> Result<(), Error> {
            Err(crate::error::Error::Rpc("simulated".into()))
        }
    }

    /// The worker owns the **only** sender, so its death is observable.
    ///
    /// `leader:membership` stops when this channel closes, because a live set
    /// that has stopped being updated would have it evicting on stale
    /// information. Parking a sender anywhere a consumer can reach — in
    /// `LivenessState`, say — would silently turn that guard into dead code and
    /// leave the job deciding from a frozen set.
    #[tokio::test]
    async fn dropping_the_worker_closes_the_live_set_channel() {
        let self_id = nid(0);
        let peer = nid(1);
        let shutdown = Shutdown::new();
        let (worker, state) = LivenessWorker::new(
            LivenessWorkerInputs {
                self_node_id: self_id,
                raft_state: raft_state_with_peers(&[peer]),
                leadership: Arc::new(NoVoters),
                initial_live: BTreeSet::new(),
                router: crate::tests::probe_router(Arc::new(FailProbe)),
                interval: Duration::from_millis(10),
                probe_timeout: Duration::from_secs(5),
                max_failures: 1,
            },
            shutdown.token(),
        );

        let mut live = state.subscribe();
        drop(worker);
        // Bounded: a regression here does not make `changed()` return the wrong
        // answer, it makes it never return at all.
        let closed = tokio::time::timeout(Duration::from_secs(5), live.changed())
            .await
            .expect("a sender is still parked somewhere a consumer can reach");
        assert!(
            closed.is_err(),
            "a consumer must be able to tell the worker is gone"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn worker_emits_healthy_for_responding_peer() {
        let self_id = nid(0);
        let peer = nid(1);
        let raft_state = raft_state_with_peers(&[peer]);
        let calls = Arc::new(AtomicU32::new(0));
        let mut shutdown = Shutdown::new();
        let probe = Arc::new(OkProbe {
            calls: calls.clone(),
        });
        let interval = Duration::from_millis(10);
        // Peer starts outside the live set so the first healthy probe is a
        // genuine not-live → live transition (events fire per transition).
        let (worker, state) = LivenessWorker::new(
            LivenessWorkerInputs {
                self_node_id: self_id,
                raft_state,
                leadership: Arc::new(NoVoters),
                initial_live: BTreeSet::new(),
                router: crate::tests::probe_router(probe),
                interval,
                probe_timeout: Duration::from_secs(5),
                max_failures: 5,
            },
            shutdown.token(),
        );
        shutdown.spawn("test", worker.run());

        tokio::time::advance(Duration::from_millis(20)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            state.live_nodes().contains(&peer),
            "a responding peer joins the live set"
        );
        assert!(calls.load(Ordering::SeqCst) >= 1);
        shutdown.token().cancel();
        shutdown
            .run(Duration::from_millis(50))
            .await
            .expect("clean shutdown");
    }

    /// Steady-state healthy probes after the first transition emit no
    /// further events — consumers are woken per transition, not per probe.
    #[tokio::test(start_paused = true)]
    async fn steady_state_healthy_probes_emit_single_transition() {
        let self_id = nid(0);
        let peer = nid(1);
        let raft_state = raft_state_with_peers(&[peer]);
        let calls = Arc::new(AtomicU32::new(0));
        let mut shutdown = Shutdown::new();
        let probe = Arc::new(OkProbe {
            calls: calls.clone(),
        });
        let (worker, state) = LivenessWorker::new(
            LivenessWorkerInputs {
                self_node_id: self_id,
                raft_state,
                leadership: Arc::new(NoVoters),
                initial_live: BTreeSet::new(),
                router: crate::tests::probe_router(probe),
                interval: Duration::from_millis(10),
                probe_timeout: Duration::from_secs(5),
                max_failures: 5,
            },
            shutdown.token(),
        );
        // Subscribed before the worker runs, so the transition below is a
        // change this receiver has not seen.
        let mut rx = state.subscribe();
        shutdown.spawn("test", worker.run());

        // Many probe rounds.
        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(10)).await;
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
        }
        assert!(
            calls.load(Ordering::SeqCst) >= 5,
            "expected several probe rounds, got {}",
            calls.load(Ordering::SeqCst)
        );
        assert!(
            rx.has_changed().expect("sender alive"),
            "the not-live -> live transition notifies"
        );
        assert_eq!(*rx.borrow_and_update(), BTreeSet::from([peer]));
        assert!(
            !rx.has_changed().expect("sender alive"),
            "a steady-state probe that changes nothing notifies nothing"
        );
        shutdown.token().cancel();
        shutdown
            .run(Duration::from_millis(50))
            .await
            .expect("clean shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn worker_emits_unhealthy_after_threshold_failures() {
        let self_id = nid(0);
        let peer = nid(1);
        let raft_state = raft_state_with_peers(&[peer]);
        let mut shutdown = Shutdown::new();
        let probe = Arc::new(FailProbe);
        let interval = Duration::from_millis(10);
        let (worker, state) = LivenessWorker::new(
            LivenessWorkerInputs {
                self_node_id: self_id,
                raft_state,
                leadership: Arc::new(NoVoters),
                initial_live: BTreeSet::from([peer]),
                router: crate::tests::probe_router(probe),
                interval,
                probe_timeout: Duration::from_secs(5),
                max_failures: 1,
            },
            shutdown.token(),
        );
        shutdown.spawn("test", worker.run());

        tokio::time::advance(Duration::from_millis(20)).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            !state.live_nodes().contains(&peer),
            "a peer past the failure threshold leaves the live set"
        );
        shutdown.token().cancel();
        shutdown
            .run(Duration::from_millis(50))
            .await
            .expect("clean shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn worker_skips_self_node_id() {
        let self_id = nid(0);
        let raft_state = raft_state_with_peers(&[self_id]);
        let calls = Arc::new(AtomicU32::new(0));
        let mut shutdown = Shutdown::new();
        let probe = Arc::new(OkProbe {
            calls: calls.clone(),
        });
        let interval = Duration::from_millis(10);
        let (worker, _state) = LivenessWorker::new(
            LivenessWorkerInputs {
                self_node_id: self_id,
                raft_state,
                leadership: Arc::new(NoVoters),
                initial_live: BTreeSet::from([self_id]),
                router: crate::tests::probe_router(probe),
                interval,
                probe_timeout: Duration::from_secs(5),
                max_failures: 5,
            },
            shutdown.token(),
        );
        shutdown.spawn("test", worker.run());

        tokio::time::advance(Duration::from_millis(20)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0, "self must not be probed");
        shutdown.token().cancel();
        shutdown
            .run(Duration::from_millis(50))
            .await
            .expect("clean shutdown");
    }

    /// A node that leaves both the replica set and the voter set stops being
    /// probed, so nothing in the probe-result arms can ever evict it. It must
    /// be pruned against the probe set instead, or `compute_target` keeps
    /// offering it as a spare and `needs_retarget` keeps reporting that the
    /// resulting joint config is fully live.
    #[tokio::test(start_paused = true)]
    async fn a_node_that_left_the_cluster_leaves_the_live_set() {
        let self_id = nid(0);
        let peer = nid(1);
        let departed = nid(2);
        let raft_state = raft_state_with_peers(&[peer]);
        let mut shutdown = Shutdown::new();
        let probe = Arc::new(OkProbe {
            calls: Arc::new(AtomicU32::new(0)),
        });
        // `departed` is in neither the replica set nor the voter set, so it is
        // never probed — exactly the state left behind by a voter eviction.
        let (worker, state) = LivenessWorker::new(
            LivenessWorkerInputs {
                self_node_id: self_id,
                raft_state,
                leadership: Arc::new(NoVoters),
                initial_live: BTreeSet::from([self_id, peer, departed]),
                router: crate::tests::probe_router(probe),
                interval: Duration::from_millis(10),
                probe_timeout: Duration::from_secs(5),
                max_failures: 5,
            },
            shutdown.token(),
        );
        shutdown.spawn("test", worker.run());

        tokio::time::advance(Duration::from_millis(20)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        let live = state.candidates();
        assert!(
            !live.contains_key(&departed),
            "a node that is neither a replica nor a voter must not stay live",
        );
        assert!(live.contains_key(&peer), "a probed peer stays live");
        assert!(
            live.contains_key(&self_id),
            "self is never probed but is a legitimate replica candidate",
        );
        shutdown.token().cancel();
        shutdown
            .run(Duration::from_millis(50))
            .await
            .expect("clean shutdown");
    }

    /// One slow peer must not stall the probing of the others (F-29): probes
    /// for one tick run concurrently. With three peers whose probes all park
    /// on a sleep, sequential probing would never have more than one probe
    /// in flight; concurrent probing has all three.
    #[tokio::test(start_paused = true)]
    async fn probes_run_concurrently_within_one_tick() {
        struct SlowProbe {
            in_flight: Arc<AtomicU32>,
            peak: Arc<AtomicU32>,
        }
        #[async_trait]
        impl LivenessProbe for SlowProbe {
            async fn probe(&self) -> Result<(), Error> {
                let now = self
                    .in_flight
                    .fetch_add(1, Ordering::SeqCst)
                    .saturating_add(1);
                self.peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let self_id = nid(0);
        let peers = [nid(1), nid(2), nid(3)];
        let raft_state = raft_state_with_peers(&peers);
        let in_flight = Arc::new(AtomicU32::new(0));
        let peak = Arc::new(AtomicU32::new(0));
        let mut shutdown = Shutdown::new();
        let probe = Arc::new(SlowProbe {
            in_flight: in_flight.clone(),
            peak: peak.clone(),
        });
        let (worker, _state) = LivenessWorker::new(
            LivenessWorkerInputs {
                self_node_id: self_id,
                raft_state,
                leadership: Arc::new(NoVoters),
                initial_live: peers.iter().copied().collect(),
                router: crate::tests::probe_router(probe),
                interval: Duration::from_millis(10),
                probe_timeout: Duration::from_secs(5),
                max_failures: 5,
            },
            shutdown.token(),
        );
        shutdown.spawn("test", worker.run());

        tokio::time::advance(Duration::from_millis(100)).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            peak.load(Ordering::SeqCst) >= 3,
            "all three probes must be in flight at once (peak = {})",
            peak.load(Ordering::SeqCst)
        );
        shutdown.token().cancel();
        shutdown
            .run(Duration::from_millis(50))
            .await
            .expect("clean shutdown");
    }
    /// A probe that never returns must be cut off by `probe_timeout` and
    /// counted as a failure — eventually marking the peer unhealthy instead
    /// of wedging the probe round forever.
    #[tokio::test(start_paused = true)]
    async fn wedged_probe_times_out_and_marks_peer_unhealthy() {
        struct WedgedProbe;
        #[async_trait]
        impl LivenessProbe for WedgedProbe {
            async fn probe(&self) -> Result<(), Error> {
                std::future::pending().await
            }
        }

        let self_id = nid(0);
        let peer = nid(1);
        let raft_state = raft_state_with_peers(&[peer]);
        let mut shutdown = Shutdown::new();
        let (worker, state) = LivenessWorker::new(
            LivenessWorkerInputs {
                self_node_id: self_id,
                raft_state,
                leadership: Arc::new(NoVoters),
                initial_live: BTreeSet::from([peer]),
                router: crate::tests::probe_router(Arc::new(WedgedProbe)),
                interval: Duration::from_millis(10),
                probe_timeout: Duration::from_millis(50),
                max_failures: 1,
            },
            shutdown.token(),
        );
        shutdown.spawn("test", worker.run());

        // Step time forward so the probe-timeout timer (created only once
        // the probe is in flight) actually fires.
        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(30)).await;
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
        }
        assert!(
            !state.live_nodes().contains(&peer),
            "timed-out probe must count as a failure and drop the peer"
        );
        assert!(
            !state.live_nodes().contains(&peer),
            "wedged peer must leave the live set"
        );
        shutdown.token().cancel();
        shutdown
            .run(Duration::from_millis(50))
            .await
            .expect("clean shutdown");
    }
}
