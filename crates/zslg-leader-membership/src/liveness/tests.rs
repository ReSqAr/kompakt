//! Tests for `liveness`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use super::*;

use async_trait::async_trait;
use shutdown::Shutdown;
use zslg_consensus::RaftStateHandle;
use zslg_consensus::{RaftState, ReplicaMembership};
use zslg_net::ServerName;

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
    async fn remove_member(
        &self,
        _node_id: zslg_consensus::NodeId,
    ) -> Result<(), zslg_consensus::Error> {
        Ok(())
    }
}

use zslg_consensus_fake::nid;

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
impl zslg_net::LivenessProbe for OkProbe {
    async fn probe(&self) -> Result<(), zslg_net::Error> {
        async {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        }
        .await
        .map_err(Into::into)
    }
}

struct FailProbe;

#[async_trait]
impl zslg_net::LivenessProbe for FailProbe {
    async fn probe(&self) -> Result<(), zslg_net::Error> {
        async { Err(crate::Error::Net(NetError::Rpc("simulated".into()))) }
            .await
            .map_err(Into::into)
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
    let shutdown = Shutdown::<crate::Error>::new();
    let (worker, state) = LivenessWorker::new(
        LivenessWorkerInputs {
            self_node_id: self_id,
            raft_state: raft_state_with_peers(&[peer]),
            leadership: Arc::new(NoVoters),
            initial_live: BTreeSet::new(),
            router: zslg_net_fake::probe_router(Arc::new(FailProbe)),
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
            router: zslg_net_fake::probe_router(probe),
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
            router: zslg_net_fake::probe_router(probe),
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
            router: zslg_net_fake::probe_router(probe),
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
            router: zslg_net_fake::probe_router(probe),
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
            router: zslg_net_fake::probe_router(probe),
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
    impl zslg_net::LivenessProbe for SlowProbe {
        async fn probe(&self) -> Result<(), zslg_net::Error> {
            async {
                let now = self
                    .in_flight
                    .fetch_add(1, Ordering::SeqCst)
                    .saturating_add(1);
                self.peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, Error>(())
            }
            .await
            .map_err(Into::into)
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
            router: zslg_net_fake::probe_router(probe),
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
    impl zslg_net::LivenessProbe for WedgedProbe {
        async fn probe(&self) -> Result<(), zslg_net::Error> {
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
            router: zslg_net_fake::probe_router(Arc::new(WedgedProbe)),
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
