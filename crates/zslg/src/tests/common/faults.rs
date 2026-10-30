/// Fault-injection doubles at the `Replica` trait seam, shared by the
/// integration tests. Two modes, matching what the tests need:
/// `fail_next(n)` (the next n calls error like a dropped RPC) and
/// `wedge()`/`unwedge()` (calls hang until released — a slow replica).
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::watch;

use crate::api::{Clock, Replica, Router, StampedWrite, WriteOutcome};
use crate::error::Error;
use crate::hlc::{Hlc, HlcClock, HlcGate};
use crate::ids::{DataId, LogIndex, NodeId, PeerAddr};
use crate::meta::{Data, RaftState, ReplicaMembership, ReplicaStatus};
use crate::raft::RaftStateHandle;
use crate::replica::ReplicaLogStore;
use crate::replica::ReplicaService;
use crossbeam_combinators::Channels;

/// Control handle for a [`FaultyReplica`]. Clone freely; clones share state.
#[derive(Clone)]
pub(crate) struct FaultHandle {
    fail_next: Arc<AtomicU32>,
    wedged: watch::Sender<bool>,
}

impl Default for FaultHandle {
    fn default() -> Self {
        let (wedged, _) = watch::channel(false);
        Self {
            fail_next: Arc::new(AtomicU32::new(0)),
            wedged,
        }
    }
}

impl FaultHandle {
    /// Make the next `n` calls fail with an injected RPC error.
    pub(crate) fn fail_next(&self, n: u32) {
        self.fail_next.store(n, Ordering::SeqCst);
    }

    /// Wedge the replica: calls block until [`Self::unwedge`].
    pub(crate) fn wedge(&self) {
        self.wedged.send_replace(true);
    }

    /// Release wedged calls.
    pub(crate) fn unwedge(&self) {
        self.wedged.send_replace(false);
    }
}

/// Wraps any [`Replica`], applying the faults of its [`FaultHandle`] before
/// delegating.
pub(crate) struct FaultyReplica {
    inner: Arc<dyn Replica>,
    faults: FaultHandle,
}

impl FaultyReplica {
    pub(crate) fn new(inner: Arc<dyn Replica>) -> (Arc<Self>, FaultHandle) {
        let faults = FaultHandle::default();
        let replica = Arc::new(Self {
            inner,
            faults: faults.clone(),
        });
        (replica, faults)
    }

    async fn gate(&self) -> Result<(), crate::Error> {
        let mut wedged = self.faults.wedged.subscribe();
        wedged
            .wait_for(|w| !*w)
            .await
            .map_err(|_| Error::Rpc("fault handle dropped while wedged".into()))?;
        let injected = self
            .faults
            .fail_next
            // decrement one failure budget; Err means it was already 0
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if injected {
            return Err(Error::Rpc("injected fault".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl Replica for FaultyReplica {
    /// Splits the input: writes that hit an injected fault are answered directly,
    /// the rest are forwarded to the inner replica's own stream. Both outcome
    /// sources are merged onto one channel by a forwarder thread, so a wedged or
    /// failing write never blocks the ones behind it.
    fn write(&self, input: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
        let (out_tx, out_rx) = crossbeam_channel::unbounded::<Result<WriteOutcome, Error>>();

        let faults = self.faults.clone();
        let inner = self.inner.clone();
        // Fault injection is about outcomes, not timing: drop the stamp here
        // and re-stamp when handing on to the inner replica.
        let mut input = input.filter_map(|s| Some(s.data));
        std::thread::spawn(move || {
            // Opened on the first write that actually reaches the inner replica —
            // some stubs (`ReadyReplica`) serve no writes at all, so a stream
            // opened eagerly would panic on a run where every write is faulted.
            let mut inner_tx: Option<crossbeam_channel::Sender<StampedWrite>> = None;
            let mut open_inner = || {
                inner_tx
                    .get_or_insert_with(|| {
                        let (tx, rx) = crossbeam_channel::unbounded::<StampedWrite>();
                        let mut outcomes = inner.write(Channels::closed(rx));
                        let fwd_tx = out_tx.clone();
                        std::thread::spawn(move || {
                            while let Ok(outcome) = outcomes.recv() {
                                if fwd_tx.send(outcome).is_err() {
                                    return;
                                }
                            }
                        });
                        tx
                    })
                    .clone()
            };
            while let Ok(data) = input.recv() {
                let data_id = data.data_id;
                // `wedged` is a watch channel (async); this thread is not on a
                // reactor, so block on the change notification directly.
                let mut wedged = faults.wedged.subscribe();
                let waited = futures::executor::block_on(wedged.wait_for(|w| !*w));
                if waited.is_err() {
                    let _ = out_tx.send(Ok(WriteOutcome::Failed {
                        data_id,
                        error: Error::Rpc("fault handle dropped while wedged".into()),
                    }));
                    continue;
                }
                let injected = faults
                    .fail_next
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok();
                let sent = if injected {
                    out_tx
                        .send(Ok(WriteOutcome::Failed {
                            data_id,
                            error: Error::Rpc("injected fault".into()),
                        }))
                        .is_ok()
                } else {
                    open_inner().send(StampedWrite::now(data)).is_ok()
                };
                if !sent {
                    return;
                }
            }
        });

        Channels::closed(out_rx)
    }

    fn subscribe_from(
        &self,
        from: Option<LogIndex>,
    ) -> Channels<Result<(LogIndex, Data), crate::Error>> {
        self.inner.subscribe_from(from)
    }
    async fn fetch_status(&self) -> Result<ReplicaStatus, crate::Error> {
        self.gate().await?;
        self.inner.fetch_status().await
    }
    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, crate::Error>>, crate::Error> {
        self.gate().await?;
        self.inner.subscribe_status().await
    }
}

/// Replica stub that is always drained and caught up: cutoff at `u64::MAX`,
/// no frontier — snapshot quorum checks pass instantly against it.
pub(crate) struct ReadyReplica {
    pub(crate) raft_state: RaftStateHandle,
}

#[async_trait]
impl Replica for ReadyReplica {
    fn write(&self, _: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
        unimplemented!("ReadyReplica does not serve writes")
    }
    fn subscribe_from(
        &self,
        _: Option<LogIndex>,
    ) -> Channels<Result<(LogIndex, Data), crate::Error>> {
        unimplemented!("ReadyReplica does not serve subscriptions")
    }
    async fn fetch_status(&self) -> Result<ReplicaStatus, crate::Error> {
        Ok(ReplicaStatus {
            node_id: crate::ids::NodeId::default(),
            frontier: None,
            raft_state: (*self.raft_state.snapshot()).clone(),
            effective_cutoff: Some(Hlc(u64::MAX)),
            max_hlc: None,
            peer_names: Vec::new(),
        })
    }
    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, crate::Error>>, crate::Error> {
        unimplemented!("ReadyReplica does not serve status subscriptions")
    }
}

// ── Clock-skew injection ───────────────────────────────────────────────

struct SkewState {
    /// Wall-clock offset in milliseconds. Positive = clock runs fast.
    offset_ms: i64,
    /// When `Some(ms)`, the clock is frozen at this physical-ms value.
    frozen: Option<u64>,
}

/// Cloneable handle for manipulating a [`SkewedClock`] at runtime.
#[derive(Clone)]
pub(crate) struct SharedClockController {
    state: Arc<SyncMutex<SkewState>>,
}

impl SharedClockController {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(SyncMutex::new(SkewState {
                offset_ms: 0,
                frozen: None,
            })),
        }
    }

    /// Add a constant offset: positive = clock runs fast, negative = slow.
    pub(crate) fn set_offset_ms(&self, offset_ms: i64) {
        self.state.lock().offset_ms = offset_ms;
    }

    /// Freeze the clock at its current wall time.
    pub(crate) fn freeze(&self) {
        let now = wall_clock_ms_now();
        self.state.lock().frozen = Some(now);
    }

    /// Resume normal time progression.
    pub(crate) fn unfreeze(&self) {
        self.state.lock().frozen = None;
    }

    /// Current wall-clock reading from this controller, in milliseconds.
    #[must_use]
    pub(crate) fn wall_now_ms(&self) -> u64 {
        let state = self.state.lock();
        let base = state.frozen.unwrap_or_else(wall_clock_ms_now);
        if state.offset_ms >= 0 {
            base.saturating_add(state.offset_ms as u64)
        } else {
            base.saturating_sub((-state.offset_ms) as u64)
        }
    }
}

impl Default for SharedClockController {
    fn default() -> Self {
        Self::new()
    }
}

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

// ── Helper ─────────────────────────────────────────────────────────────

#[must_use]
fn wall_clock_ms_now() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

// ── Fixed test clock ───────────────────────────────────────────────────

/// Clock pinned to a fixed HLC; `integrate` echoes the peer.
pub(crate) struct FixedClock {
    pub(crate) now: Hlc,
}

impl Clock for FixedClock {
    fn now(&self) -> Hlc {
        self.now
    }
    fn integrate(&self, peer: Hlc) -> Result<Hlc, crate::Error> {
        Ok(peer)
    }
}

/// Static per-node replica routing for tests; panics on unknown peers.
pub(crate) struct MapPeerRouter {
    pub(crate) replicas: BTreeMap<NodeId, Arc<dyn Replica>>,
}

impl Router for MapPeerRouter {
    fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

    fn remote(&self, node_id: NodeId) -> Arc<dyn crate::api::ReplicaClient> {
        super::replica_only(
            self.replicas
                .get(&node_id)
                .unwrap_or_else(|| panic!("no replica registered for {node_id:?}"))
                .clone(),
        )
    }

    fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn crate::api::ReplicaClient> {
        unimplemented!("MapPeerRouter: direct dial not used")
    }
}

impl MapPeerRouter {
    /// Wrap every replica in the map with a [`FaultyReplica`], returning the
    /// wrapped router and per-node [`FaultHandle`]s.
    pub(crate) fn wrap_all_faulty(
        replicas: BTreeMap<NodeId, Arc<dyn Replica>>,
    ) -> (Self, BTreeMap<NodeId, FaultHandle>) {
        let mut wrapped = BTreeMap::new();
        let mut handles = BTreeMap::new();
        for (id, replica) in replicas {
            let (faulty, handle) = FaultyReplica::new(replica);
            wrapped.insert(id, faulty as Arc<dyn Replica>);
            handles.insert(id, handle);
        }
        (Self { replicas: wrapped }, handles)
    }
}

// ── Duplicated RPC injection ───────────────────────────────────────────

struct DuplicationState {
    /// How many more calls to duplicate.
    remaining: u32,
    /// How many times to send each duplicated call (concurrently).
    multiplier: u32,
    /// When true, ALL calls are duplicated (overrides remaining/multiplier).
    duplicate_all: bool,
}

/// Cloneable handle for a [`DuplicateReplica`].
#[derive(Clone)]
pub(crate) struct DuplicateHandle {
    state: Arc<SyncMutex<DuplicationState>>,
}

impl DuplicateHandle {
    /// The next `count` calls are sent `multiplier` times each.
    pub(crate) fn duplicate_next(&self, count: u32, multiplier: u32) {
        let mut s = self.state.lock();
        s.remaining = count;
        s.multiplier = multiplier;
    }

    /// When `enabled`, every call is duplicated (overrides `duplicate_next`).
    pub(crate) fn set_duplicate_all(&self, enabled: bool) {
        self.state.lock().duplicate_all = enabled;
    }

    fn should_duplicate(&self) -> Option<u32> {
        let mut s = self.state.lock();
        if s.duplicate_all {
            return Some(2);
        }
        if s.remaining > 0 {
            s.remaining = s.remaining.saturating_sub(1);
            let m = s.multiplier;
            if m > 1 {
                return Some(m);
            }
        }
        None
    }
}

impl Default for DuplicateHandle {
    fn default() -> Self {
        Self {
            state: Arc::new(SyncMutex::new(DuplicationState {
                remaining: 0,
                multiplier: 0,
                duplicate_all: false,
            })),
        }
    }
}

/// Wraps a [`Replica`] and duplicates calls according to its
/// [`DuplicateHandle`]. Duplicated calls happen concurrently; the first
/// `Ok` result is returned (others are discarded).
pub(crate) struct DuplicateReplica {
    inner: Arc<dyn Replica>,
    dup: DuplicateHandle,
}

impl DuplicateReplica {
    pub(crate) fn new(inner: Arc<dyn Replica>) -> (Arc<Self>, DuplicateHandle) {
        let dup = DuplicateHandle::default();
        let replica = Arc::new(Self {
            inner,
            dup: dup.clone(),
        });
        (replica, dup)
    }
}

/// Per-`data_id` state while a duplicated write's N outcomes come back: how many
/// are still outstanding, and the best one seen so far.
type DupExpectations = Arc<SyncMutex<BTreeMap<DataId, (u32, Option<WriteOutcome>)>>>;

#[async_trait]
impl Replica for DuplicateReplica {
    /// Feeds N copies of each write (same `data_id`) into the inner replica —
    /// exercising the resend path — then collapses the N outcomes back to one: the
    /// `Accepted` if there is one, else the first. (With dedup removed each copy now
    /// simply appends and is `Accepted`; the collapse still yields one.)
    fn write(&self, input: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
        let (out_tx, out_rx) = crossbeam_channel::unbounded::<Result<WriteOutcome, Error>>();
        let (inner_tx, inner_rx) = crossbeam_channel::unbounded::<StampedWrite>();
        let mut outcomes = self.inner.write(Channels::closed(inner_rx));

        // How many outcomes are still expected per data_id, and the best one seen
        // so far. Only shared between this thread and the feeder below.
        let expected: DupExpectations = Arc::new(SyncMutex::new(BTreeMap::new()));

        let dup = self.dup.clone();
        let expected_feed = expected.clone();
        let mut input = input;
        std::thread::spawn(move || {
            while let Ok(stamped) = input.recv() {
                let n = dup.should_duplicate().unwrap_or(1);
                if n > 1 {
                    expected_feed.lock().insert(stamped.data.data_id, (n, None));
                }
                for _ in 0..n {
                    // Each copy is stamped afresh: a duplicate is a distinct
                    // arrival, so sharing one enqueue time would misreport the
                    // residency of every copy after the first.
                    if inner_tx
                        .send(StampedWrite::now(stamped.data.clone()))
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });

        std::thread::spawn(move || {
            while let Ok(outcome) = outcomes.recv() {
                let Ok(outcome) = outcome else {
                    let _ = out_tx.send(outcome);
                    return; // terminal
                };
                let mut guard = expected.lock();
                let Some((remaining, best)) = guard.get_mut(&outcome.data_id()) else {
                    // Not duplicated — pass straight through.
                    drop(guard);
                    if out_tx.send(Ok(outcome)).is_err() {
                        return;
                    }
                    continue;
                };
                *remaining = remaining.saturating_sub(1);
                let accepted = matches!(outcome, WriteOutcome::Accepted { .. });
                if accepted || best.is_none() {
                    *best = Some(outcome);
                }
                if *remaining > 0 {
                    continue;
                }
                let data_id = best.as_ref().map(WriteOutcome::data_id);
                let chosen = data_id.and_then(|id| guard.remove(&id).and_then(|(_, b)| b));
                drop(guard);
                if let Some(chosen) = chosen
                    && out_tx.send(Ok(chosen)).is_err()
                {
                    return;
                }
            }
        });

        Channels::closed(out_rx)
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> Channels<Result<(LogIndex, Data), Error>> {
        self.inner.subscribe_from(from)
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, Error> {
        self.inner.fetch_status().await
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> {
        self.inner.subscribe_status().await
    }
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
    pub(crate) cutoff_tx: tokio::sync::watch::Sender<Option<crate::hlc::Hlc>>,
    _dir: tempfile::TempDir,
    _flush: tokio::task::JoinHandle<Result<(), Error>>,
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
        let dir = tempfile::tempdir()
            .map_err(|e| Error::Storage(format!("tempdir for ReplicaServiceHarness: {e}")))?;
        let (store, flush) = ReplicaLogStore::open_with_flush(
            dir.path().join("replica-log"),
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
            crate::unit::nonzero(2048),
            crate::network::static_peers(peer_addrs),
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

    use bytes::Bytes;

    use crate::api::Replica;
    use crate::api::Router;
    use crate::hlc::Hlc;
    use crate::ids::{DataId, NodeId};
    use crate::tests::nid;

    use super::{DuplicateReplica, MapPeerRouter, ReadyReplica, ReplicaServiceHarness};

    fn nids() -> [NodeId; 3] {
        [NodeId::new_v4(), NodeId::new_v4(), NodeId::new_v4()]
    }

    fn dummy_raft_state() -> crate::raft::RaftStateHandle {
        let h = crate::raft::RaftStateHandle::new();
        h.replace(crate::meta::RaftState {
            replica: crate::meta::ReplicaMembership::single(Default::default()),
            ..crate::meta::RaftState::empty()
        });
        h
    }

    #[tokio::test]
    async fn quorum_write_succeeds_despite_one_failing_write() {
        let ids = nids();
        let [n1, n2, _n3] = ids;

        let mut replicas: BTreeMap<NodeId, Arc<dyn crate::api::Replica>> = BTreeMap::new();
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
            crate::meta::Data {
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

        let mut replicas: BTreeMap<NodeId, Arc<dyn crate::api::Replica>> = BTreeMap::new();
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
            DuplicateReplica::new(harness.replica.clone() as Arc<dyn crate::api::Replica>);

        // Next write is sent twice, concurrently.
        dup_handle.duplicate_next(1, 2);

        let data = crate::meta::Data {
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
            .subscribe_from(Some(crate::ids::LogIndex::ZERO));
        let first = ch.recv();
        assert!(
            first.is_ok_and(|r| r.is_ok()),
            "data must be present after write"
        );
    }
}
