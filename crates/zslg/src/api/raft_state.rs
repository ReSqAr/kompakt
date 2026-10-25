//! Every node's view of the cluster's committed Raft state, including the nodes
//! that run no Raft at all.
//!
//! A writer or learner never joins consensus, but it must still know the
//! current generation and replica set to detect a quorum change (CLAUDE.md §9).
//! It learns them the only way a non-member can: `RaftStateTracker::integrate`
//! folds in the `RaftState` that rides along on every reply from a replica.
//!
//! Hence the split. A *reader* of the state takes `RaftStateView`; only the
//! component that owns the fold takes `RaftStateTracker`, so no consumer can
//! push a state nobody vouched for.
//!
//! `integrate` is monotonic in generation — an older observation is dropped, not
//! applied — which is what makes it safe to feed from replies that arrive out of
//! order.

use std::sync::Arc;

use tokio::sync::watch;

use crate::meta::RaftState;

pub trait RaftStateView: Send + Sync + 'static {
    #[must_use]
    fn snapshot(&self) -> Arc<RaftState>;

    #[must_use]
    /// The published state and every change to it.
    ///
    /// Level-triggered: a consumer that was busy sees the latest state on its
    /// next poll, never a lag it must remember to treat as a wake-up. Reading
    /// the state *is* receiving it, so there is no separate snapshot call to
    /// keep in step (`raft::state::state_handle`).
    fn subscribe(&self) -> watch::Receiver<Arc<RaftState>>;
}

pub(crate) trait RaftStateTracker: RaftStateView {
    fn integrate(&self, observed: &RaftState);
}
