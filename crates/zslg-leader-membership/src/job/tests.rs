//! The membership job's pacing floor: one step per burst of Raft-state
//! changes, not one step per change.
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
use tokio_util::sync::CancellationToken;

use crate::{LivenessState, MEMBERSHIP_STEP_FLOOR, MembershipDriver, MembershipJob, TickOutcome};
use zslg_consensus::{
    NodeId, RaftLeadership, RaftManifestState, RaftState, RaftStateHandle, ReplicaMembership,
};
use zslg_net::ServerName;
use zslg_time::Hlc;

struct CountingDriver {
    ticks: Arc<AtomicUsize>,
    /// Fires on every tick, so the test can await progress instead of sleeping.
    bump: watch::Sender<usize>,
}

#[async_trait::async_trait]
impl MembershipDriver for CountingDriver {
    async fn maybe_initiate(&self) -> Result<(), crate::Error> {
        Ok(())
    }

    async fn tick(&self) -> Result<TickOutcome, crate::Error> {
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
