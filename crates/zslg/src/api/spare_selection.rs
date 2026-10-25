//! Which spare gets promoted when the replica set is short.
//!
//! The seam exists because the *policy* is expected to grow (catch-up distance,
//! failure domains) while the mechanism must not: the caller
//! promotes a prefix of the returned order and nothing else.
//!
//! Returning a permutation — every candidate exactly once, none invented — is
//! the contract `compute_target` depends on. A strategy that filtered instead of
//! ordered could leave the set short of its target with candidates available.

use std::time::Duration;

use crate::ids::NodeId;

/// Per-candidate signals gathered for spare selection.
#[derive(Debug, Clone)]
pub(crate) struct CandidateInfo {
    pub(crate) node_id: NodeId,
    pub(crate) rtt: Option<Duration>,
}

/// Orders spare (live, non-member) nodes for promotion. The impl is
/// [`SpareSelectionStrategy`](crate::raft::SpareSelectionStrategy), chosen via
/// `MembershipConfig`.
pub(crate) trait SpareSelector: Send + Sync {
    /// A permutation of `candidates` — best first.
    #[must_use]
    fn rank(&self, candidates: &[CandidateInfo]) -> Vec<NodeId>;
}
