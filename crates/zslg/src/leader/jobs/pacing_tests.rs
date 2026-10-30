//! Pacing floors for the two jobs whose loops are not already rate-limited by
//! an `interval` (see [`super`]).
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

use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::api::{LocalStoreView, RaftLeadership, SnapshotStore, StableLogSnapshot, WindowItem};
use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::{LogIndex, NodeId, ServerName, SnapshotId};
use crate::leader::integrator::Integrator;
use crate::leader::jobs::LeaderHandles;
use crate::leader::jobs::Pace;
use crate::leader::jobs::membership::liveness::LivenessState;
use crate::leader::jobs::membership::{MEMBERSHIP_STEP_FLOOR, MembershipJob};
use crate::leader::jobs::membership::{MembershipDriver, TickOutcome};
use crate::leader::jobs::snapshot::{SNAPSHOT_LOOP_FLOOR, SnapshotJob};
use crate::meta::{Data, RaftManifestState, RaftState, ReplicaMembership};
use crate::object_store::CompressionLevel;
use crate::raft::RaftStateHandle;
use crossbeam_combinators::Channels;

// ── the pure pacing primitive ────────────────────────────────────────────

const FLOOR: Duration = Duration::from_millis(100);

#[tokio::test(start_paused = true)]
async fn pace_does_not_delay_the_first_iteration() {
    let mut pace = Pace::new(FLOOR);
    let t0 = Instant::now();
    pace.wait().await;
    assert_eq!(t0.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn pace_spaces_back_to_back_iterations_by_the_floor() {
    let mut pace = Pace::new(FLOOR);
    let t0 = Instant::now();
    for _ in 0..5 {
        pace.wait().await;
    }
    // 5 iterations ⇒ 4 gaps. Exact, because virtual time only moves for timers.
    assert_eq!(t0.elapsed(), FLOOR * 4);
}

/// The property the snapshot chunk loop depends on: the floor is start-to-start,
/// so an iteration that already took longer than the floor is not delayed *at
/// all*. A floor implemented as "sleep(floor) after each iteration" would add
/// 100 ms to every chunk of a large backlog; this one adds nothing.
#[tokio::test(start_paused = true)]
async fn pace_adds_nothing_to_an_iteration_slower_than_the_floor() {
    let mut pace = Pace::new(FLOOR);
    pace.wait().await;
    let work = FLOOR * 5;
    tokio::time::sleep(work).await;

    let before = Instant::now();
    pace.wait().await;
    assert_eq!(before.elapsed(), Duration::ZERO);
}

// ── MembershipJob: one step per burst, not one step per event ────────────

struct CountingDriver {
    ticks: Arc<AtomicUsize>,
    /// Fires on every tick, so the test can await progress instead of sleeping.
    bump: watch::Sender<usize>,
}

#[async_trait::async_trait]
impl MembershipDriver for CountingDriver {
    async fn maybe_initiate(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn tick(&self) -> Result<TickOutcome, Error> {
        let n = self.ticks.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.bump.send(n);
        Ok(TickOutcome::Idle)
    }
}

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
    async fn remove_member(&self, _node_id: NodeId) -> Result<(), Error> {
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

/// Advance the manifest watermark, which is both a `subscribe()` event and a
/// change `MembershipRaftState` actually tracks — so the job's `act` predicate
/// is true and a naive implementation really would step once per bump.
fn bump_watermark(handle: &RaftStateHandle, n: u64) {
    let mut state = (*handle.snapshot()).clone();
    state.manifest_state.manifest_watermark = Some(Hlc(n));
    handle.replace(state);
}

struct MembershipHarness {
    ticks: Arc<AtomicUsize>,
    raft_state: Arc<RaftStateHandle>,
    /// Kept alive so the job's liveness receiver never sees the channel close.
    _liveness: LivenessState,
    cancel: CancellationToken,
}

/// A poll interval far above any window these tests measure, so the ticker arm
/// cannot be what drives the steps being counted.
const SLOW_POLL: Duration = Duration::from_secs(3600);

fn spawn_membership_job(poll_interval: Duration) -> MembershipHarness {
    let node = NodeId::new_v4();
    let raft_state = Arc::new(RaftStateHandle::new());
    raft_state.replace(seeded_state(node, None));

    let ticks = Arc::new(AtomicUsize::new(0));
    let (bump, _seen) = watch::channel(0usize);
    let liveness_state = LivenessState::fixed_for_test(BTreeSet::new());

    let job = MembershipJob {
        self_node_id: node,
        leadership: Arc::new(SelfLeader(node)),
        raft_state: raft_state.clone(),
        driver: Arc::new(CountingDriver {
            ticks: ticks.clone(),
            bump,
        }),
        liveness_state: liveness_state.clone(),
        liveness_events: liveness_state.subscribe(),
        poll_interval,
    };
    let cancel = CancellationToken::new();
    tokio::spawn(job.run(cancel.clone()));

    MembershipHarness {
        ticks,
        raft_state,
        _liveness: liveness_state,
        cancel,
    }
}

/// A burst of Raft-state changes must not produce one `step()` — and so one
/// peer fan-out — per change.
///
/// Each bump is a *distinct* state, so the job's own "did anything I care about
/// change?" re-read cannot coalesce them: without the floor this is 50 steps.
/// No virtual time passes during the burst (nothing awaits a timer), so the
/// whole burst lands inside a single floor window.
#[tokio::test(start_paused = true)]
async fn membership_job_debounces_a_burst_of_events() {
    let h = spawn_membership_job(SLOW_POLL);
    // Let the job take its initial `ticker.tick()` before the burst, so that
    // free first step is not counted against the burst.
    tokio::task::yield_now().await;
    let baseline = h.ticks.load(Ordering::SeqCst);

    let burst = 50u64;
    for n in 1..=burst {
        bump_watermark(&h.raft_state, n);
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;

    let ran = h.ticks.load(Ordering::SeqCst) - baseline;
    assert!(
        ran < 5,
        "a {burst}-change burst inside one floor window must debounce; got {ran} steps",
    );
    h.cancel.cancel();
}

/// The floor must not stop the job converging: repeated wake-ups keep producing
/// steps, just at a bounded rate.
#[tokio::test(start_paused = true)]
async fn membership_job_keeps_stepping_across_the_floor() {
    let h = spawn_membership_job(SLOW_POLL);
    tokio::task::yield_now().await;

    for n in 1..=10u64 {
        bump_watermark(&h.raft_state, n);
        tokio::time::sleep(MEMBERSHIP_STEP_FLOOR + Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
    }

    let ran = h.ticks.load(Ordering::SeqCst);
    assert!(ran >= 5, "steps kept happening across the floor; got {ran}");
    h.cancel.cancel();
}

/// **The floor must not override configuration.** An operator who configures a
/// `poll_interval` shorter than `MEMBERSHIP_STEP_FLOOR` asked for faster
/// membership convergence; silently pacing at 100 ms would be ignoring that, and
/// config validation cannot warn about a constant it does not know exists. The
/// effective floor is therefore `min(MEMBERSHIP_STEP_FLOOR, poll_interval)`.
///
/// Measured as a *rate*: over one `MEMBERSHIP_STEP_FLOOR` of virtual time with a
/// 20 ms poll interval, a job honouring the configuration steps roughly
/// `100/20 = 5` times. A hardcoded 100 ms floor caps it at one or two.
#[tokio::test(start_paused = true)]
async fn membership_job_does_not_pace_slower_than_a_short_configured_interval() {
    let poll = Duration::from_millis(20);
    let h = spawn_membership_job(poll);
    tokio::task::yield_now().await;
    let baseline = h.ticks.load(Ordering::SeqCst);

    // Keep genuine work arriving, so the rate is what is under test rather than
    // whether there was anything to do.
    for n in 1..=20u64 {
        bump_watermark(&h.raft_state, n);
        tokio::time::sleep(poll / 2).await;
    }
    tokio::task::yield_now().await;

    let ran = h.ticks.load(Ordering::SeqCst) - baseline;
    let floored = MEMBERSHIP_STEP_FLOOR.as_millis() / poll.as_millis();
    assert!(
        ran >= 4,
        "a {poll:?} poll interval must not be paced at {MEMBERSHIP_STEP_FLOOR:?}: \
         got {ran} steps over {floored} floor-widths of virtual time",
    );
    h.cancel.cancel();
}

// ── SnapshotJob: the chunk loop, paced but not gated ─────────────────────

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
    fn stream_above_hlc(&self, _threshold: Option<Hlc>) -> Channels<Result<Data, Error>> {
        unimplemented!("pacing tests do not stream above an hlc")
    }

    fn stream_window_ordered(
        &self,
        _above: Option<Hlc>,
        _upto: Hlc,
        _max_entries: Option<std::num::NonZeroUsize>,
    ) -> Channels<Result<WindowItem, Error>> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.done.send(n);
        Channels::from_vec(vec![Err(Error::Storage("pacing test: window read".into()))])
    }

    fn subscribe_integrated(&self) -> watch::Receiver<()> {
        self.integrated.subscribe()
    }

    fn subscribe_unstable(&self) -> crossbeam_combinators::Channels<crate::meta::Data> {
        crossbeam_combinators::Channels::from_infinite(crossbeam_channel::never(), "stub unstable")
    }

    async fn subscription_cursor(&self, _upstream: NodeId) -> Result<Option<LogIndex>, Error> {
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
        _entries: Channels<Result<crate::meta::SnapshotEntry, Error>>,
    ) -> Result<usize, Error> {
        unimplemented!("pacing tests never append")
    }
    async fn record_end_hlc(&self, _end_hlc: Hlc) -> Result<(), Error> {
        unimplemented!("pacing tests never record an end hlc")
    }
    fn watermark_moves(&self) -> Channels<()> {
        Channels::from_infinite(crossbeam_channel::never(), "stub watermark moves")
    }
    fn subscribe_events(&self) -> Channels<crate::meta::StableLogEvent> {
        unimplemented!("pacing tests never subscribe to stable-log events")
    }
    fn watch_watermark(&self) -> tokio::sync::watch::Receiver<Option<Hlc>> {
        unimplemented!("pacing tests never watch the stable-log watermark")
    }
    fn range(
        &self,
        _from: LogIndex,
        _until: LogIndex,
    ) -> Channels<Result<(LogIndex, Data), Error>> {
        unimplemented!("pacing tests never range the stable log")
    }
    fn set_purge_threshold(&self, _hlc_threshold: Hlc) -> Result<(), Error> {
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
        _entries: Channels<Result<crate::meta::SnapshotEntry, Error>>,
        _compression: CompressionLevel,
        _reason: crate::metrics::ObjectStoreReason,
    ) -> Result<usize, Error> {
        unimplemented!("pacing tests never reach the object write")
    }
    async fn read_snapshot(
        &self,
        _id: SnapshotId,
        _reason: crate::metrics::ObjectStoreReason,
    ) -> Result<Channels<Result<crate::meta::SnapshotEntry, Error>>, Error> {
        unimplemented!("pacing tests never read a snapshot")
    }
    fn shutdown_token(&self) -> CancellationToken {
        self.0.clone()
    }
    async fn has_bootstrap_fence(&self) -> Result<bool, Error> {
        Ok(true)
    }
    async fn write_bootstrap_fence(&self) -> Result<(), Error> {
        unimplemented!("pacing tests never write a fence")
    }
    async fn cleanup_snapshots(
        &self,
        _live_snapshots: BTreeSet<SnapshotId>,
        _cleanup_retention: Duration,
    ) -> Result<crate::api::CleanupReport, Error> {
        unimplemented!("pacing tests never clean up")
    }
}

struct SnapshotHarness {
    builds: Arc<AtomicUsize>,
    reached: watch::Receiver<usize>,
    cancel: CancellationToken,
    _integrator_cancel: CancellationToken,
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
    let ready: Arc<dyn crate::api::Replica> = Arc::new(crate::tests::ReadyReplica {
        raft_state: (*raft_state).clone(),
    });
    let router =
        crate::tests::common::fn_remote(move |_| crate::tests::common::replica_only(ready.clone()));

    // A real integrator, so `submit` cannot be the thing that stalls the loop.
    // It is never reached here (every build fails first), but wiring the real
    // one keeps the failure mode honest.
    let (intents, integrator_cancel) =
        Integrator::spawn_for_test(Arc::new(NoopReplicator), raft_state.clone());

    let handles = LeaderHandles {
        clock: Arc::new(crate::tests::FixedClock { now: Hlc(1) }),
        local_store,
        stable_log: Arc::new(EmptyStableLog),
        snapshot_store: Arc::new(UnusedSnapshotStore(CancellationToken::new())),
        leadership: Arc::new(SelfLeader(node)),
        raft_state,
        router,
        self_node_id: node,
        intents: intents.clone(),
    };

    let job = SnapshotJob {
        self_node_id: node,
        handles,
        intents,
        quorum_poll_interval: Duration::from_millis(10),
        quorum_wait_timeout: Duration::from_secs(30),
        compression: CompressionLevel::fast(),
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        build_warn: crate::warn_throttle::ThrottledWarn::new(std::time::Duration::from_secs(5)),
    };
    let cancel = CancellationToken::new();
    tokio::spawn(job.run(cancel.clone()));

    SnapshotHarness {
        builds,
        reached,
        cancel,
        _integrator_cancel: integrator_cancel,
    }
}

struct NoopReplicator;

#[async_trait::async_trait]
impl crate::api::StateReplicator for NoopReplicator {
    async fn replicate_state(
        &self,
        _state: crate::api::ReplicatedRaftState,
        _expected_generation: crate::ids::RaftGeneration,
        _context: &'static str,
    ) -> Result<(), Error> {
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
