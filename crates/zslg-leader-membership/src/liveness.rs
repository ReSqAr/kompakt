use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use zslg_consensus::CandidateInfo;
use zslg_consensus::ConsensusError;
use zslg_consensus::{RaftLeadership, RaftStateView};
use zslg_lifecycle_types::ConfigError;
use zslg_net::NetError;
use zslg_net::Router;

use parking_lot::Mutex as SyncMutex;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::Error;
use crate::change::LivenessQuery;
use zslg_consensus::NodeId;

/// The live set and its change notification in one primitive.
///
/// The set *is* the event: consumers only ever ask "who is live now", so a
/// stream of transition events was a longer way of saying the same thing — and
/// a lossy one, since removals mutated the set without emitting anything and
/// were noticed only on the consumer's next poll tick.
#[derive(Clone)]
pub struct LivenessState {
    /// A **receiver**, deliberately: the only sender belongs to the worker, so
    /// a consumer observes the channel closing when the worker dies. Holding a
    /// sender here would make that unobservable, and a consumer would go on
    /// deciding evictions from a live set that has stopped being updated —
    /// which is exactly the hazard `leader:membership` stops for.
    live: watch::Receiver<BTreeSet<NodeId>>,
    rtts: Arc<SyncMutex<HashMap<NodeId, Duration>>>,
    /// Test-only: keeps a fixed set's sender alive, since there is no worker
    /// behind it to own one.
    #[cfg(any(test, feature = "testing"))]
    _fixed: Option<Arc<watch::Sender<BTreeSet<NodeId>>>>,
}

impl LivenessState {
    #[must_use]
    pub fn live_nodes(&self) -> BTreeSet<NodeId> {
        self.live.borrow().clone()
    }

    /// Wakes on every change to the live set, and reports the worker's death by
    /// closing.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<BTreeSet<NodeId>> {
        self.live.clone()
    }

    /// A state with a fixed live set and no worker behind it, for tests that
    /// drive a job which *requires* a `LivenessState` but is not exercising
    /// liveness itself.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn fixed_for_test(live: BTreeSet<NodeId>) -> Self {
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

pub(crate) struct LivenessWorkerInputs {
    pub self_node_id: NodeId,
    pub raft_state: Arc<dyn RaftStateView>,
    pub leadership: Arc<dyn RaftLeadership>,
    pub initial_live: BTreeSet<NodeId>,
    pub router: Arc<dyn Router>,
    pub interval: Duration,
    /// Upper bound on a single probe; an over-deadline probe counts as a
    /// failure.
    pub probe_timeout: Duration,
    pub max_failures: u32,
}

pub(crate) struct LivenessWorker {
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
    pub(crate) fn new(
        inputs: LivenessWorkerInputs,
        cancel: CancellationToken,
    ) -> (Self, LivenessState) {
        let (live, live_rx) = watch::channel(inputs.initial_live);
        let live = Arc::new(live);
        let rtts = Arc::new(SyncMutex::new(HashMap::new()));
        let state = LivenessState {
            live: live_rx,
            rtts: rtts.clone(),
            #[cfg(any(test, feature = "testing"))]
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

    pub(crate) async fn run(self) -> Result<(), Error> {
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
            let peers: BTreeSet<NodeId> = zslg_consensus::all_targets(&snapshot.replica)
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
                            Err(zslg_net::Error::Net(NetError::Rpc(format!(
                                "liveness probe to {peer:?} timed out after {probe_timeout:?}"
                            ))))
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
                    Err(zslg_net::Error::Consensus(ConsensusError::UnexpectedNodeId(ids))) => {
                        let zslg_consensus::UnexpectedNodeId { expected, actual } = *ids;
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
                    Err(zslg_net::Error::Consensus(ConsensusError::UnknownPeer { .. })) => {
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
pub struct LivenessConfig {
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
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
    pub fn validate(&self) -> Result<(), crate::Error> {
        if self.probe_timeout.is_zero() {
            return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                "liveness_probe_timeout must be > 0".to_owned(),
            )));
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
mod tests;
