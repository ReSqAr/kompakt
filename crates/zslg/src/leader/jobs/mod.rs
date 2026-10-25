//! The leader's background jobs — one tokio task each, all concurrent.
//!
//! ```text
//!   cutoff       timer                    ─┐
//!   snapshot     cutoff > watermark        │
//!   compaction   timer                     ├─ intents ──> integrator ──> raft
//!   cleanup      timer                     │              (single writer)
//!   membership   raft state + liveness     │
//!   voters       timer + peer config      ─┘
//! ```
//!
//! Three properties are why there is no planner and no lock:
//!
//! 1. **A job never overlaps itself** — one task, one serial loop, no
//!    per-iteration spawn. While working it is not watching its trigger.
//! 2. **Triggers are predicates over current state, not deltas** — every wake-up
//!    re-reads current state. The trigger channels are `watch`es, so that is
//!    enforced rather than merely intended: a wake-up carries the level, never a
//!    queue of edges to drain, and one missed while busy is not lost.
//! 3. **Jobs never write Raft state** — they submit intents to
//!    [`crate::leader::integrator`], where the invariants are enforced.
//!
//! Every job gates on [`is_local_leader`] and stops acting on demotion, so
//! leadership can change without a task being spawned or stopped.
//!
//! Pacing: the timer-driven jobs rate-limit themselves via `MissedTickBehavior::
//! Skip`. `snapshot` and `membership` do not — they re-evaluate immediately, and
//! carry an explicit [`Pace`] floor. Each states its own reason at its constant;
//! the reasons differ.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::api::{
    Clock, LocalStoreView, RaftLeadership, RaftStateView, Router, SnapshotStore, StableLogSnapshot,
};
use crate::ids::NodeId;
use crate::leader::IntentSubmitter;

pub(in crate::leader) mod cleanup;
pub(in crate::leader) mod compaction;
pub(in crate::leader) mod cutoff;
pub(in crate::leader) mod membership;
pub(in crate::leader) mod snapshot;
pub(in crate::leader) mod voters;

#[cfg(test)]
mod pacing_tests;

/// A floor on a loop's iteration *rate*, measured start-to-start.
///
/// Never an added delay: an iteration that already outran the floor waits for
/// nothing. Load-bearing for [`snapshot::SnapshotJob`], whose immediate
/// loop-back is how a chunked snapshot drains a backlog.
pub(in crate::leader) struct Pace {
    floor: Duration,
    last: Option<Instant>,
}

impl Pace {
    #[must_use]
    pub(in crate::leader) fn new(floor: Duration) -> Self {
        Self { floor, last: None }
    }

    /// Wait out the remainder of the floor, then mark this iteration started.
    pub(in crate::leader) async fn wait(&mut self) {
        if let Some(last) = self.last {
            // Overflow falls back to "now", i.e. no wait: it can never stall a
            // loop, only fail to slow one on a clock about to wrap.
            let earliest = last.checked_add(self.floor).unwrap_or_else(Instant::now);
            tokio::time::sleep_until(earliest).await;
        }
        self.last = Some(Instant::now());
    }
}

/// Everything a job needs to run. All `Arc`, so cloning per job is cheap.
#[derive(Clone)]
pub(in crate::leader) struct LeaderHandles {
    pub(in crate::leader) clock: Arc<dyn Clock>,
    pub(in crate::leader) local_store: Arc<dyn LocalStoreView>,
    pub(in crate::leader) stable_log: Arc<dyn StableLogSnapshot>,
    pub(in crate::leader) snapshot_store: Arc<dyn SnapshotStore>,
    pub(in crate::leader) leadership: Arc<dyn RaftLeadership>,
    pub(in crate::leader) raft_state: Arc<dyn RaftStateView>,
    pub(in crate::leader) router: Arc<dyn Router>,
    pub(in crate::leader) self_node_id: NodeId,
    /// Unused by `cleanup` and `voters`, which write no replicated state.
    pub(in crate::leader) intents: IntentSubmitter,
}

/// Every job's first gate.
#[must_use]
pub(in crate::leader) fn is_local_leader(
    leadership: &dyn RaftLeadership,
    self_node_id: NodeId,
) -> bool {
    leadership.current_leader() == Some(self_node_id)
}
