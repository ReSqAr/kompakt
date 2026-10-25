//! `leader:membership` — drive replica-set changes to completion.
//!
//! Owns the whole subsystem: the liveness prober whose live set feeds target
//! selection ([`liveness`]), and the state machine that walks the change
//! ([`change`]).
//!
//! ```text
//!   Single ──> Joint ──> JointDraining{exit} ──> Single
//!            step 1     step 2 (gen quorum)     step 4
//!                       step 2b (arm bound)     once watermark >= exit
//! ```
//!
//! Three interchangeable triggers, none load-bearing alone: the Raft-state
//! broadcast (a step committed, or the watermark advanced — which is what
//! completes step 3, hence this job must not queue behind snapshot creation),
//! a liveness change (retarget), and a slow poll as a backstop. Every wake-up
//! re-reads current state, so a missed event costs only latency.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::api::{RaftStateView, Router};
use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::{NodeId, RaftGeneration};
use crate::leader::jobs::Pace;
use crate::leader::jobs::is_local_leader;
use crate::leader::jobs::membership::liveness::LivenessState;
use crate::meta::{RaftState, ReplicaMembership, ReplicaStatus};
use crate::metrics::{self, LeaderJob, OpState};

pub(in crate::leader) mod change;
pub(in crate::leader) mod liveness;

/// Spawn the liveness prober and the job that turns its live set into
/// replica-set changes. Liveness is wired here because this job is its only
/// consumer, so nothing about it is nameable outside this module.
pub(in crate::leader) fn spawn(
    shutdown: &mut crate::shutdown::Shutdown,
    handles: &crate::leader::jobs::LeaderHandles,
    config: &crate::raft::MembershipConfig,
    liveness_config: &liveness::LivenessConfig,
) {
    let (worker, liveness_state) = liveness::LivenessWorker::new(
        liveness::LivenessWorkerInputs {
            self_node_id: handles.self_node_id,
            raft_state: handles.raft_state.clone(),
            leadership: handles.leadership.clone(),
            initial_live: initial_live_set(
                handles.raft_state.as_ref(),
                handles.leadership.as_ref(),
                handles.self_node_id,
            ),
            router: handles.router.clone(),
            interval: liveness_config.probe_interval,
            probe_timeout: liveness_config.probe_timeout,
            max_failures: config.replica_node_max_failures,
        },
        shutdown.token(),
    );
    shutdown.spawn("leader:liveness", worker.run());

    let driver: Arc<dyn MembershipDriver> = Arc::new(change::MembershipChangeManager::new(
        change::MembershipChangeInputs {
            self_node_id: handles.self_node_id,
            intents: handles.intents.clone(),
            leadership: handles.leadership.clone(),
            raft_state: handles.raft_state.clone(),
            observer: Arc::new(NetworkMembershipObserver::new(handles.router.clone())),
            replica_node_count: config.replica_node_count,
            spare_selection: config.spare_selection,
            live_nodes: Arc::new(liveness_state.clone()),
        },
    ));

    let job = MembershipJob {
        self_node_id: handles.self_node_id,
        leadership: handles.leadership.clone(),
        raft_state: handles.raft_state.clone(),
        driver,
        liveness_events: liveness_state.subscribe(),
        liveness_state,
        poll_interval: config.poll_interval,
    };
    shutdown.spawn("leader:membership", job.run(shutdown.token()));
}

/// What one `tick()` of the membership state machine achieved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::leader) enum TickOutcome {
    Idle,
    Pending,
    Complete,
}

#[async_trait::async_trait]
pub(in crate::leader) trait MembershipObserver: Send + Sync + 'static {
    /// One `fetch_status` round trip. Returns the whole [`ReplicaStatus`] so
    /// step 2's generation and step 2b's `max_hlc` come from one observation.
    async fn observe(&self, peer: NodeId) -> Result<ReplicaStatus, crate::Error>;
}

/// The membership state machine, behind a trait so the job's loop can be
/// driven by a counting fake in `jobs::pacing_tests`.
#[async_trait::async_trait]
pub(in crate::leader) trait MembershipDriver: Send + Sync + 'static {
    async fn maybe_initiate(&self) -> Result<(), crate::Error>;

    async fn tick(&self) -> Result<TickOutcome, crate::Error>;
}

#[cfg(test)]
mod change_tests;

/// The only production [`MembershipObserver`]; the trait exists so tests can
/// answer in-process.
pub(in crate::leader) struct NetworkMembershipObserver {
    router: Arc<dyn Router>,
}

impl NetworkMembershipObserver {
    #[must_use]
    pub(in crate::leader) fn new(router: Arc<dyn Router>) -> Self {
        Self { router }
    }
}

#[async_trait::async_trait]
impl MembershipObserver for NetworkMembershipObserver {
    async fn observe(&self, peer: NodeId) -> Result<ReplicaStatus, Error> {
        self.router
            .remote(peer)
            .fetch_status()
            .await
            .map_err(|e| Error::Rpc(format!("ReplicaFetchStatus from {peer:?}: {e}")))
    }
}

/// Caps the *event-driven* step rate; effective floor is
/// `MEMBERSHIP_STEP_FLOOR.min(poll_interval)`.
///
/// `poll_interval` is one `select!` arm among three, so it floors how often a
/// step happens with nothing else waking the job — it does not ceiling it. A
/// burst on either event channel would otherwise cost one peer fan-out each.
/// The `min` keeps a configured interval authoritative. Debouncing is free
/// here: every wake-up re-reads current state, so events arriving during the
/// wait fold into the pass that follows.
pub(in crate::leader) const MEMBERSHIP_STEP_FLOOR: Duration = Duration::from_millis(100);

pub(in crate::leader) struct MembershipJob {
    pub(in crate::leader) self_node_id: NodeId,
    pub(in crate::leader) leadership: Arc<dyn crate::api::RaftLeadership>,
    pub(in crate::leader) raft_state: Arc<dyn RaftStateView>,
    pub(in crate::leader) driver: Arc<dyn MembershipDriver>,
    pub(in crate::leader) liveness_state: LivenessState,
    /// Wakes on every change to the live set — which is the set itself, so
    /// there is nothing to drain and nothing to miss.
    pub(in crate::leader) liveness_events: tokio::sync::watch::Receiver<BTreeSet<NodeId>>,
    pub(in crate::leader) poll_interval: Duration,
}

impl MembershipJob {
    pub(in crate::leader) async fn run(mut self, cancel: CancellationToken) -> Result<(), Error> {
        info!("leader:membership: starting");
        let mut ticker = interval(self.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut raft_events = self.raft_state.subscribe();

        // Tracked even while a follower, so a freshly promoted leader does not
        // replay a stale diff.
        let mut last_raft = MembershipRaftState::from(&*self.raft_state.snapshot());
        let mut last_live = self.liveness_state.live_nodes();

        let mut pace = Pace::new(MEMBERSHIP_STEP_FLOOR.min(self.poll_interval));

        loop {
            let ticked = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => true,
                changed = raft_events.changed() => match changed {
                    Ok(()) => false,
                    Err(_) => break,
                },
                changed = self.liveness_events.changed() => match changed {
                    Ok(()) => false,
                    // A dead worker means a frozen live set, which would evict
                    // on stale information. Stopping is the safe response.
                    Err(_) => {
                        warn!("liveness channel closed; stopping leader:membership");
                        break;
                    }
                },
            };

            let raft = MembershipRaftState::from(&*self.raft_state.snapshot());
            let live = self.liveness_state.live_nodes();
            let changed = raft != last_raft || live != last_live;
            last_raft = raft;
            last_live = live;

            if ticked || changed {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    () = pace.wait() => {}
                }
                let started = tokio::time::Instant::now();
                self.step().await;
                metrics::leader_job_ran(LeaderJob::Membership, started.elapsed());
            }
        }
        info!("leader:membership: shutdown");
        Ok(())
    }

    /// Start a change if warranted, then advance whichever step is committed.
    /// Both are idempotent, so an extra call is only wasted work.
    async fn step(&self) {
        if !is_local_leader(self.leadership.as_ref(), self.self_node_id) {
            return;
        }
        if let Err(e) = self.driver.maybe_initiate().await {
            metrics::membership_change(OpState::Failure);
            warn!(error = %e, "membership change initiate failed; will retry");
            return;
        }
        match self.driver.tick().await {
            Ok(TickOutcome::Idle) => {}
            Ok(TickOutcome::Pending) => {
                let state = self.raft_state.snapshot();
                debug!(
                    generation = ?state.generation,
                    exit_hlc = ?state.replica.exit(),
                    watermark = ?state.manifest_state.manifest_watermark,
                    "membership change in progress",
                );
            }
            Ok(TickOutcome::Complete) => {
                metrics::membership_change(OpState::Success);
                info!("membership change complete");
            }
            Err(e) => {
                metrics::membership_change(OpState::Failure);
                warn!(error = %e, "membership change tick failed");
            }
        }
    }
}

/// The initial live set: everyone the leader might need to talk to before the
/// first liveness round has run.
#[must_use]
pub(in crate::leader) fn initial_live_set(
    raft_state: &dyn RaftStateView,
    leadership: &dyn crate::api::RaftLeadership,
    self_node_id: NodeId,
) -> BTreeSet<NodeId> {
    crate::quorum::all_targets(&raft_state.snapshot().replica)
        .into_iter()
        .chain(leadership.voter_ids())
        .chain(std::iter::once(self_node_id))
        .collect()
}

/// The subset of [`RaftState`] the membership job reacts to, so a broadcast
/// carrying only a leader/term change costs no peer fan-out.
///
/// Our own last-handled view only — never per-replica generation ACKs, which
/// must be read fresh from each response (CLAUDE.md §12).
#[derive(Debug, PartialEq, Eq)]
pub(in crate::leader) struct MembershipRaftState {
    generation: RaftGeneration,
    /// Watermark advances matter because step 3 of a membership change waits on
    /// them (CLAUDE.md §8; the reasoning is on `meta::drain_satisfied`).
    manifest_watermark: Option<Hlc>,
    replica: ReplicaMembership,
}

impl From<&RaftState> for MembershipRaftState {
    fn from(state: &RaftState) -> Self {
        Self {
            generation: state.generation,
            manifest_watermark: state.manifest_state.manifest_watermark,
            replica: state.replica.clone(),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod debounce_tests {
    use super::*;
    use crate::meta::Exit;
    use crate::tests::nid;

    #[test]
    fn membership_view_ignores_leader_and_term_changes() {
        let base = RaftState::empty();

        let mut leader_changed = base.clone();
        leader_changed.leader = Some(nid(1));
        leader_changed.raft_term = 7;
        assert_eq!(
            MembershipRaftState::from(&base),
            MembershipRaftState::from(&leader_changed),
            "leader/term-only change must not wake a membership step"
        );
    }

    #[test]
    fn membership_view_reacts_to_watermark_generation_and_replica_sets() {
        let base = RaftState::empty();

        // The watermark is load-bearing: step 3 of a membership change waits on
        // it (CLAUDE.md §8; the reasoning is on `meta::drain_satisfied`), so an advance must wake the job.
        let mut watermark = base.clone();
        watermark.manifest_state.manifest_watermark = Some(Hlc(42));
        assert_ne!(
            MembershipRaftState::from(&base),
            MembershipRaftState::from(&watermark),
        );

        let mut generation = watermark.clone();
        generation.generation = RaftGeneration(3);
        assert_ne!(
            MembershipRaftState::from(&watermark),
            MembershipRaftState::from(&generation),
        );

        let mut joint = generation.clone();
        joint.replica = ReplicaMembership::joint(
            joint.replica.current().clone(),
            [nid(2)].into_iter().collect(),
        );
        assert_ne!(
            MembershipRaftState::from(&generation),
            MembershipRaftState::from(&joint),
        );

        // Arming the exit condition keeps both sets but adds the bound: the drain
        // wait starts here, so it must compare unequal.
        let mut armed = joint.clone();
        armed.replica = ReplicaMembership::JointDraining {
            old: joint.replica.current().clone(),
            next: [nid(2)].into_iter().collect(),
            exit: Exit::WatermarkAbove(Hlc(99)),
        };
        assert_ne!(
            MembershipRaftState::from(&joint),
            MembershipRaftState::from(&armed),
            "arming the drain bound must wake the job"
        );

        let mut cleared = armed.clone();
        cleared.replica = ReplicaMembership::single([nid(2)].into_iter().collect());
        assert_ne!(
            MembershipRaftState::from(&armed),
            MembershipRaftState::from(&cleared),
        );
    }
}
