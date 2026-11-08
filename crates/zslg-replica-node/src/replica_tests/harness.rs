//! A real `ReplicaService` over a temp store with a controllable clock.

use std::sync::Arc;
use std::time::Duration;

use zslg_consensus::{NodeId, RaftState, RaftStateHandle, ReplicaMembership};
use zslg_error::Error;
use zslg_hlc_clock::HlcClock;
use zslg_hlc_gate::HlcGate;
use zslg_log::LogError;
use zslg_net::PeerAddr;
use zslg_replica_log::{REPLICA_LOG_CF, ReplicaLogStore};
use zslg_replica_service::ReplicaService;
use zslg_time_fake::SharedClockController;

/// A storage root carrying the replica-log column family.
pub(super) fn open_storage(root: &std::path::Path) -> Result<Arc<rocksdb_kit::Storage>, Error> {
    let cfs = [rocksdb_kit::CfSpec {
        cf: REPLICA_LOG_CF,
        compress: false,
    }];
    Ok(rocksdb_kit::Storage::open(
        root,
        rocksdb_kit::StorageConfig::new(bytesize::ByteSize::from_mib(64)),
        &cfs,
    )?)
}

/// An `HlcClock` whose wall-clock source is controlled by a
/// [`SharedClockController`]. The controller can apply a constant offset
/// (fast/slow), freeze, or unfreeze time.
///
/// The inner `Arc<HlcClock>` is reachable via [`hlc_clock()`] for wiring
/// into `ReplicaService::new()` or `RpcClient`.
pub(super) struct SkewedClock {
    inner: Arc<HlcClock>,
    _controller: SharedClockController,
}

impl SkewedClock {
    pub(super) fn new(skew_limit: Duration, controller: SharedClockController) -> Self {
        let ctl = controller.clone();
        let time_source: Box<dyn Fn() -> u64 + Send + Sync> = Box::new(move || ctl.wall_now_ms());
        Self {
            inner: Arc::new(HlcClock::with_time_source(skew_limit, time_source)),
            _controller: controller,
        }
    }

    /// The inner clock, suitable for passing to `ReplicaService::new()`.
    #[must_use]
    pub(super) fn hlc_clock(&self) -> &Arc<HlcClock> {
        &self.inner
    }
}

/// Create a skewed clock + controller pair with the given skew limit.
pub(super) fn skewed_clock_pair(skew_limit: Duration) -> (SkewedClock, SharedClockController) {
    let ctl = SharedClockController::new();
    let clock = SkewedClock::new(skew_limit, ctl.clone());
    (clock, ctl)
}

// ── ReplicaNode harness for integration tests ──────────────────────────

/// Holds a [`ReplicaService`] and its backing resources for fault-injection
/// tests that need a real replica with a configurable clock.
pub(super) struct ReplicaServiceHarness {
    pub(super) replica: Arc<ReplicaService>,
    pub(super) raft_state: RaftStateHandle,
    pub(super) _hlc_gate: Arc<HlcGate>,
    /// Publisher for the replica's effective-cutoff watch. Held so tests can
    /// drive cutoff advances (and so the watch isn't closed from the start).
    pub(super) cutoff_tx: tokio::sync::watch::Sender<Option<zslg_time::Hlc>>,
    _dir: tempfile::TempDir,
    _flush: tokio::task::JoinHandle<Result<(), zslg_replica_log::Error>>,
}

impl ReplicaServiceHarness {
    /// Create a `ReplicaService` with the given clock (from `SkewedClock`) and
    /// replica-set membership. The node writes to a temp store.
    pub(super) async fn new(
        self_id: NodeId,
        replicas: &[NodeId],
        clock: &Arc<HlcClock>,
    ) -> Result<Self, Error> {
        Self::new_with_peers(self_id, replicas, clock, Vec::new()).await
    }

    /// Like [`Self::new`], but with a configured peer list — needed for tests
    /// that observe `fetch_status().peer_names`, which `new`'s empty list
    /// can't exercise non-vacuously.
    pub(super) async fn new_with_peers(
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
            open_storage(&dir.path().join("replica-log"))?,
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
            _hlc_gate: hlc_gate,
            cutoff_tx,
            _dir: dir,
            _flush: flush,
        })
    }
}
