use std::sync::Arc;
use std::time::Duration;
use zslg_log::LogError;

use crate::error::Error;
use crate::replica::ReplicaLogStore;
use crate::replica::ReplicaService;
use zslg_consensus::NodeId;
use zslg_consensus::RaftStateHandle;
use zslg_consensus::{RaftState, ReplicaMembership};
use zslg_hlc_clock::HlcClock;
use zslg_hlc_gate::HlcGate;
use zslg_net::PeerAddr;

// ── Clock-skew injection ───────────────────────────────────────────────

pub(crate) use zslg_net_fake::{DuplicateReplica, FaultyReplica, MapPeerRouter};
pub(crate) use zslg_time_fake::SharedClockController;

/// An `HlcClock` whose wall-clock source is controlled by a
/// [`SharedClockController`]. The controller can apply a constant offset
/// (fast/slow), freeze, or unfreeze time.
///
/// The inner `Arc<HlcClock>` is reachable via [`hlc_clock()`] for wiring
/// into `ReplicaService::new()` or `RpcClient`.
pub(crate) struct SkewedClock {
    inner: Arc<HlcClock>,
    _controller: SharedClockController,
}

impl SkewedClock {
    pub(crate) fn new(skew_limit: Duration, controller: SharedClockController) -> Self {
        let ctl = controller.clone();
        let time_source: Box<dyn Fn() -> u64 + Send + Sync> = Box::new(move || ctl.wall_now_ms());
        Self {
            inner: Arc::new(HlcClock::with_time_source(skew_limit, time_source)),
            _controller: controller,
        }
    }

    /// The inner clock, suitable for passing to `ReplicaService::new()`.
    #[must_use]
    pub(crate) fn hlc_clock(&self) -> &Arc<HlcClock> {
        &self.inner
    }
}

/// Create a skewed clock + controller pair with the given skew limit.
pub(crate) fn skewed_clock_pair(skew_limit: Duration) -> (SkewedClock, SharedClockController) {
    let ctl = SharedClockController::new();
    let clock = SkewedClock::new(skew_limit, ctl.clone());
    (clock, ctl)
}

// ── ReplicaNode harness for integration tests ──────────────────────────

/// Holds a [`ReplicaService`] and its backing resources for fault-injection
/// tests that need a real replica with a configurable clock.
pub(crate) struct ReplicaServiceHarness {
    pub(crate) replica: Arc<ReplicaService>,
    pub(crate) raft_state: RaftStateHandle,
    pub(crate) hlc_gate: Arc<HlcGate>,
    /// Publisher for the replica's effective-cutoff watch. Held so tests can
    /// drive cutoff advances (and so the watch isn't closed from the start).
    pub(crate) cutoff_tx: tokio::sync::watch::Sender<Option<zslg_time::Hlc>>,
    _dir: tempfile::TempDir,
    _flush: tokio::task::JoinHandle<Result<(), zslg_replica_log::Error>>,
}

impl ReplicaServiceHarness {
    /// Create a `ReplicaService` with the given clock (from `SkewedClock`) and
    /// replica-set membership. The node writes to a temp store.
    pub(crate) async fn new(
        self_id: NodeId,
        replicas: &[NodeId],
        clock: &Arc<HlcClock>,
    ) -> Result<Self, Error> {
        Self::new_with_peers(self_id, replicas, clock, Vec::new()).await
    }

    /// Like [`Self::new`], but with a configured peer list — needed for tests
    /// that observe `fetch_status().peer_names`, which `new`'s empty list
    /// can't exercise non-vacuously.
    pub(crate) async fn new_with_peers(
        self_id: NodeId,
        replicas: &[NodeId],
        clock: &Arc<HlcClock>,
        peer_addrs: Vec<PeerAddr>,
    ) -> Result<Self, Error> {
        let dir = tempfile::tempdir().map_err(|e| {
            Error::Log(LogError::Storage(format!(
                "tempdir for ReplicaServiceHarness: {e}"
            )))
        })?;
        let (store, flush) = ReplicaLogStore::open_with_flush(
            crate::database::open_for_test(&dir.path().join("replica-log"))?,
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
        let raft_state = RaftStateHandle::new();
        raft_state.replace(RaftState {
            replica: ReplicaMembership::single(replicas.iter().copied().collect()),
            ..RaftState::empty()
        });
        let hlc_gate = Arc::new(HlcGate::new());
        let (cutoff_tx, rx) = tokio::sync::watch::channel(None);
        let replica = Arc::new(ReplicaService::new(
            store,
            hlc_gate.clone(),
            self_id,
            clock.clone(),
            Arc::new(raft_state.clone()),
            rx,
            bytesize::nonzero(2048),
            zslg_peers::static_peers(peer_addrs),
        ));
        Ok(Self {
            replica,
            raft_state,
            hlc_gate,
            cutoff_tx,
            _dir: dir,
            _flush: flush,
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;
    use zslg_net::Router as _;

    use bytes::Bytes;

    use crate::tests::nid;
    use zslg_consensus::NodeId;
    use zslg_log::DataId;
    use zslg_log::Replica;
    use zslg_time::Hlc;

    use super::{DuplicateReplica, MapPeerRouter, ReplicaServiceHarness};
    use zslg_net_fake::ReadyReplica;

    fn nids() -> [NodeId; 3] {
        [NodeId::new_v4(), NodeId::new_v4(), NodeId::new_v4()]
    }

    fn dummy_raft_state() -> zslg_consensus::RaftStateHandle {
        let h = zslg_consensus::RaftStateHandle::new();
        h.replace(zslg_consensus::RaftState {
            replica: zslg_consensus::ReplicaMembership::single(Default::default()),
            ..zslg_consensus::RaftState::empty()
        });
        h
    }

    #[tokio::test]
    async fn quorum_write_succeeds_despite_one_failing_write() {
        let ids = nids();
        let [n1, n2, _n3] = ids;

        let mut replicas: BTreeMap<NodeId, Arc<dyn zslg_log::Replica>> = BTreeMap::new();
        for &id in &ids {
            replicas.insert(
                id,
                Arc::new(ReadyReplica {
                    raft_state: dummy_raft_state(),
                }),
            );
        }
        let (router, handles) = MapPeerRouter::wrap_all_faulty(replicas);

        // Make n2 fail its next write.
        handles[&n2].fail_next(1);

        // Verify n2's transport fails the first call.
        let transport = router.remote(n2);
        let err = crate::tests::common::replica_write(
            &*transport,
            zslg_log::Data {
                data_id: DataId::from_parts(Hlc(0), n1, 0),
                blob: Bytes::from_static(b"x"),
            },
        )
        .await
        .expect_err("fail_next(1) should inject error");
        assert!(
            err.to_string().contains("injected fault"),
            "expected injected fault, got {err:?}"
        );

        // After the budget is exhausted, the replica works again.
        transport
            .fetch_status()
            .await
            .expect("should recover after fail budget exhausted");
    }

    #[tokio::test]
    async fn wedged_replica_unblocks_after_unwedge() {
        let ids = nids();
        let [n1, _n2, _n3] = ids;

        let mut replicas: BTreeMap<NodeId, Arc<dyn zslg_log::Replica>> = BTreeMap::new();
        for &id in &ids {
            replicas.insert(
                id,
                Arc::new(ReadyReplica {
                    raft_state: dummy_raft_state(),
                }),
            );
        }
        let (router, handles) = MapPeerRouter::wrap_all_faulty(replicas);

        // Wedge n1.
        handles[&n1].wedge();

        let transport = router.remote(n1);
        let call = tokio::spawn(async move { transport.fetch_status().await });

        // The call should hang while wedged.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!call.is_finished(), "wedged call must block");

        // Unwedge — call completes.
        handles[&n1].unwedge();
        let result = tokio::time::timeout(Duration::from_secs(1), call)
            .await
            .expect("call did not complete after unwedge")
            .expect("task join");
        assert!(result.is_ok(), "unwedged call should succeed: {result:?}");
    }

    #[tokio::test]
    async fn duplicate_write_is_idempotent() {
        let me = nid(1);
        let max_skew = Duration::from_secs(10);
        let ctl = super::SharedClockController::new();
        let clock = super::SkewedClock::new(max_skew, ctl);

        let harness = ReplicaServiceHarness::new(me, &[me], clock.hlc_clock())
            .await
            .expect("harness");

        let (dup_replica, dup_handle) =
            DuplicateReplica::new(harness.replica.clone() as Arc<dyn zslg_log::Replica>);

        // Next write is sent twice, concurrently.
        dup_handle.duplicate_next(1, 2);

        let data = zslg_log::Data {
            data_id: DataId::from_parts(clock.hlc_clock().now(), me, 0),
            blob: Bytes::from_static(b"duplicate-write"),
        };
        // The wrapper returns the first Ok; the duplicate's DataIdReused is discarded.
        let state = crate::tests::common::replica_write(&*dup_replica, data)
            .await
            .expect("duplicate write succeeds");
        assert!(state.replica.current().contains(&me));

        // Data is stored exactly once (the single write got index 0).
        let mut ch = harness
            .replica
            .subscribe_from(Some(zslg_log::LogIndex::ZERO));
        let first = ch.recv();
        assert!(
            first.is_ok_and(|r| r.is_ok()),
            "data must be present after write"
        );
    }
}
