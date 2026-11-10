//! The snapshot job's pacing floor: a window that never advances must not spin,
//! and the floor paces the chunk loop without gating it on an event.
//!
//! Everything here runs on `start_paused = true`. That matters for more than
//! speed: with the clock paused, tokio only advances virtual time when *every*
//! task is idle, so a job that spins in a tight loop consumes **zero** virtual
//! time. "Did it spin?" therefore becomes an exact question — measure how much
//! virtual time N iterations took — instead of a flaky wall-clock one.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crossbeam_combinators::{Channel, Finite};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{SNAPSHOT_LOOP_FLOOR, SnapshotJob};
use zslg_consensus::{
    IntentOutcome, IntentSubmitter, NodeId, RaftIntent, RaftLeadership, RaftManifestState,
    RaftState, RaftStateHandle, ReplicaMembership,
};
use zslg_log::{Data, LocalStoreView, LogError, LogIndex, SnapshotEntry, StableLogSnapshot};
use zslg_net::ServerName;
use zslg_snapshot::{CleanupReport, CompressionLevel, SnapshotId, SnapshotStore, SnapshotWrite};
use zslg_time::Hlc;

struct SelfLeader(NodeId);

#[async_trait::async_trait]
impl RaftLeadership for SelfLeader {
    fn current_leader(&self) -> Option<NodeId> {
        Some(self.0)
    }
    fn voter_ids(&self) -> BTreeSet<NodeId> {
        [self.0].into_iter().collect()
    }
    fn voter_server_names(&self) -> Vec<(NodeId, ServerName)> {
        Vec::new()
    }
    async fn remove_member(&self, _node_id: NodeId) -> Result<(), zslg_consensus::Error> {
        unimplemented!("pacing tests do not remove members")
    }
}

fn seeded_state(node: NodeId, cutoff: Option<Hlc>) -> RaftState {
    let mut state = RaftState::empty();
    state.leader = Some(node);
    state.replica = ReplicaMembership::single([node].into_iter().collect());
    state.manifest_state = RaftManifestState {
        manifest: Vec::new(),
        manifest_watermark: None,
        hlc_cutoff: cutoff,
    };
    state
}

/// A `LocalStoreView` whose window read always fails, so every build returns
/// **without** advancing the manifest watermark — the shape of the `Skipped` /
/// failed-build / `Superseded` paths that leave `pending_window()` open.
struct FailingWindowStore {
    calls: Arc<AtomicUsize>,
    done: watch::Sender<usize>,
    /// Kept alive so `subscribe_integrated` receivers never see `Closed`.
    integrated: watch::Sender<()>,
}

#[async_trait::async_trait]
impl LocalStoreView for FailingWindowStore {
    fn stream_above_hlc(&self, _threshold: Option<Hlc>) -> zslg_log::LocalStoreStream {
        unimplemented!("pacing tests do not stream above an hlc")
    }

    fn stream_window(&self, _above: Option<Hlc>, _upto: Hlc) -> zslg_log::LocalStoreStream {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.done.send(n);
        Channel::once(Err(zslg_log::Error::Log(LogError::Storage(
            "pacing test: window read".into(),
        ))))
        .ordered(None, "pacing test window")
    }

    fn subscribe_integrated(&self) -> watch::Receiver<()> {
        self.integrated.subscribe()
    }

    fn subscribe_unstable(
        &self,
    ) -> crossbeam_combinators::Channel<zslg_log::Data, crossbeam_combinators::Infinite> {
        crossbeam_combinators::Channel::from_infinite(crossbeam_channel::never(), "stub unstable")
    }

    async fn subscription_cursor(
        &self,
        _upstream: NodeId,
    ) -> Result<Option<LogIndex>, zslg_log::Error> {
        Ok(None)
    }
}

struct EmptyStableLog;

#[async_trait::async_trait]
impl StableLogSnapshot for EmptyStableLog {
    fn end_index(&self) -> Option<LogIndex> {
        None
    }
    fn end_hlc(&self) -> Option<Hlc> {
        None
    }
    async fn append(
        &self,
        _entries: Channel<
            Result<zslg_log::SnapshotEntry, zslg_log::Error>,
            crossbeam_combinators::Finite,
        >,
    ) -> Result<usize, zslg_log::Error> {
        unimplemented!("pacing tests never append")
    }
    async fn record_end_hlc(&self, _end_hlc: Hlc) -> Result<(), zslg_log::Error> {
        unimplemented!("pacing tests never record an end hlc")
    }
    fn subscribe_events(
        &self,
    ) -> Channel<zslg_log::StableLogEvent, crossbeam_combinators::Infinite> {
        unimplemented!("pacing tests never subscribe to stable-log events")
    }
    fn watermark(&self) -> crossbeam_combinators::Level<Option<Hlc>> {
        unimplemented!("pacing tests never watch the stable-log watermark")
    }
    fn range(
        &self,
        _after: Option<LogIndex>,
        _until: LogIndex,
    ) -> Channel<Result<(LogIndex, Data), zslg_log::Error>, crossbeam_combinators::Finite> {
        unimplemented!("pacing tests never range the stable log")
    }
    fn set_purge_threshold(&self, _hlc_threshold: Hlc) -> Result<(), zslg_log::Error> {
        unimplemented!("pacing tests never purge")
    }
}

/// Unreachable in these tests: every build fails in the window read, before any
/// object is written.
struct UnusedSnapshotStore(CancellationToken);

#[async_trait::async_trait]
impl SnapshotStore for UnusedSnapshotStore {
    async fn write_snapshot(
        &self,
        _id: SnapshotId,
        _entries: Channel<Result<SnapshotEntry, zslg_snapshot::Error>, Finite>,
        _compression: CompressionLevel,
        _reason: zslg_snapshot::ObjectStoreReason,
    ) -> Result<SnapshotWrite, zslg_snapshot::Error> {
        unimplemented!("pacing tests never reach the object write")
    }
    async fn read_snapshot(
        &self,
        _id: SnapshotId,
        _reason: zslg_snapshot::ObjectStoreReason,
    ) -> Result<Channel<Result<SnapshotEntry, zslg_snapshot::Error>, Finite>, zslg_snapshot::Error>
    {
        unimplemented!("pacing tests never read a snapshot")
    }
    fn shutdown_token(&self) -> CancellationToken {
        self.0.clone()
    }
    async fn has_bootstrap_fence(&self) -> Result<bool, zslg_snapshot::Error> {
        Ok(true)
    }
    async fn write_bootstrap_fence(&self) -> Result<(), zslg_snapshot::Error> {
        unimplemented!("pacing tests never write a fence")
    }
    async fn cleanup_snapshots(
        &self,
        _live_snapshots: BTreeSet<SnapshotId>,
        _cleanup_retention: Duration,
    ) -> Result<CleanupReport, zslg_snapshot::Error> {
        unimplemented!("pacing tests never clean up")
    }
}

struct SnapshotHarness {
    builds: Arc<AtomicUsize>,
    reached: watch::Receiver<usize>,
    cancel: CancellationToken,
}

/// A `SnapshotJob` whose window stays open forever and whose every build fails.
/// Nothing ever publishes a Raft-state event, so the *only* thing that can drive
/// a second iteration is the loop's immediate re-evaluation — which is exactly
/// the chunk loop.
fn spawn_always_failing_snapshot_job() -> SnapshotHarness {
    let node = NodeId::new_v4();
    let raft_state = Arc::new(RaftStateHandle::new());
    // cutoff set, watermark None ⇒ `pending_window()` is open and stays open.
    raft_state.replace(seeded_state(node, Some(Hlc(1_000))));

    let builds = Arc::new(AtomicUsize::new(0));
    let (done, reached) = watch::channel(0usize);
    let (integrated, _) = watch::channel(());
    let local_store = Arc::new(FailingWindowStore {
        calls: builds.clone(),
        done,
        integrated,
    });

    // A replica that is always drained and caught up, so the quorum wait costs
    // nothing and the build reaches the window read every time.
    let ready: Arc<dyn zslg_log::Replica> = Arc::new(zslg_net_fake::ReadyReplica {
        raft_state: (*raft_state).clone(),
    });
    let router = zslg_net_fake::fn_remote(move |_| zslg_net_fake::replica_only(ready.clone()));

    let handles = crate::SnapshotHandles {
        self_node_id: node,
        local_store,
        stable_log: Arc::new(EmptyStableLog),
        snapshot_store: Arc::new(UnusedSnapshotStore(CancellationToken::new())),
        raft_state,
        router,
        leadership: Arc::new(SelfLeader(node)),
        intents: Arc::new(UnreachableIntents),
    };

    let job = SnapshotJob {
        handles,
        config: crate::SnapshotJobConfig {
            quorum_poll_interval: Duration::from_millis(10),
            quorum_wait_timeout: Duration::from_secs(30),
            compression: CompressionLevel::fast(),
            max_snapshot_entries: std::num::NonZeroUsize::MAX,
        },
        build_warn: warn_throttle::ThrottledWarn::new(std::time::Duration::from_secs(5)),
    };
    let cancel = CancellationToken::new();
    tokio::spawn(job.run(cancel.clone()));

    SnapshotHarness {
        builds,
        reached,
        cancel,
    }
}

/// Never reached: every build fails in the window read, before any intent.
struct UnreachableIntents;

#[async_trait::async_trait]
impl IntentSubmitter for UnreachableIntents {
    async fn submit(&self, _intent: RaftIntent) -> Result<IntentOutcome, zslg_consensus::Error> {
        unimplemented!("pacing tests never reach a proposal")
    }
}

/// Comfortably above the paced expectation (9 × `SNAPSHOT_LOOP_FLOOR` for the
/// ten builds these tests wait on), and free: it is virtual time.
const BUILD_WAIT_BOUND: Duration = Duration::from_secs(30);

/// Wait for `n` builds, **bounded**.
///
/// The bound is not belt-and-braces. Under an event-gated implementation — the
/// regression this whole task exists to prevent — the job parks on
/// `events.recv()` and this waiter parks on the watch, leaving no pending timer
/// for paused time to advance to: the test would deadlock instead of asserting,
/// and CLAUDE.md §17 exists because a hanging test blocks everything. The
/// `timeout` is itself the pending timer that turns that hang into a failure
/// naming its own cause.
async fn wait_for_builds(h: &mut SnapshotHarness, n: usize) {
    let wait = async {
        while *h.reached.borrow_and_update() < n {
            h.reached.changed().await.expect("harness sender alive");
        }
    };
    tokio::time::timeout(BUILD_WAIT_BOUND, wait).await.expect(
        "chunk loop stalled before reaching the expected build count — \
         is the pacing floor gating on a broadcast event instead of pacing the \
         immediate `continue`?",
    );
}

/// **The spin guard.** A window that stays open while every build returns
/// without advancing the watermark must not burn a core. Under paused time a
/// hot loop consumes zero virtual time, so the assertion is exact.
#[tokio::test(start_paused = true)]
async fn snapshot_job_paces_a_window_that_never_advances() {
    let mut h = spawn_always_failing_snapshot_job();
    let t0 = Instant::now();
    let iterations = 10usize;
    wait_for_builds(&mut h, iterations).await;
    let elapsed = t0.elapsed();
    h.cancel.cancel();

    // The first iteration is free; the other nine are each floored.
    let expected = SNAPSHOT_LOOP_FLOOR * 9;
    assert!(
        elapsed >= expected,
        "{iterations} non-advancing iterations took {elapsed:?}, \
         expected at least {expected:?} — the loop is spinning",
    );
}

/// **The Task 8 interaction.** The floor paces the loop; it must not gate it on
/// an external event. No Raft-state event is ever published in this harness, so
/// reaching a tenth build proves the immediate `continue` — the chunk loop — is
/// still what drives iteration N+1. A floor implemented as "wait for the next
/// broadcast event" would stall at one.
#[tokio::test(start_paused = true)]
async fn snapshot_job_keeps_iterating_without_any_broadcast_event() {
    let mut h = spawn_always_failing_snapshot_job();
    wait_for_builds(&mut h, 10).await;
    h.cancel.cancel();
    assert!(h.builds.load(Ordering::SeqCst) >= 10);
}
