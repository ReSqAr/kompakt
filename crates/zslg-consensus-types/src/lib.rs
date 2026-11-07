//! The cluster's vocabulary: who is in it, what state it replicates, the rules
//! that state must obey, and the errors of breaking them.

mod error;
mod handle;
mod intent;
mod meta;
mod types;
mod validators;

pub use error::{
    ConsensusError, InvalidRaftState, StaleStateWrite, SubscriptionEnded, UnexpectedNodeId,
};
pub use handle::RaftStateHandle;
pub use intent::{IntentOutcome, RaftIntent};
pub use meta::{
    Exit, RaftManifestState, RaftState, ReplicaMembership, ReplicaStatus, drain_satisfied,
};
pub use types::{
    CandidateInfo, EvictionStatus, RaftGeneration, RaftLogIndex, RaftStateError,
    ReplicatedRaftState, SpareSelectionStrategy, all_targets, is_quorum,
};
pub use validators::{validate_cutoff_advance, validate_replicated_state, validate_transition};

/// Adapt any displayable error into [`ConsensusError::Raft`], prefixed with what was being done.
pub trait ToRaft<T> {
    /// # Errors
    /// The original error, rendered into a [`ConsensusError::Raft`] with `context`.
    fn to_raft(self, context: &str) -> Result<T, ConsensusError>;
}

impl<T, E: std::fmt::Display> ToRaft<T> for Result<T, E> {
    fn to_raft(self, context: &str) -> Result<T, ConsensusError> {
        self.map_err(|e| ConsensusError::Raft(format!("{context}: {e}")))
    }
}
