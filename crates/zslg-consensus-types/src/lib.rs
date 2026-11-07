//! The cluster's vocabulary: who is in it, what state it replicates, the rules
//! that state must obey, and the errors of breaking them.

mod error;
mod handle;
mod meta;
mod types;
mod validators;

pub use error::{
    ConsensusError, InvalidRaftState, StaleStateWrite, SubscriptionEnded, UnexpectedNodeId,
};
pub use handle::RaftStateHandle;
pub use meta::{
    Exit, RaftManifestState, RaftState, ReplicaMembership, ReplicaStatus, drain_satisfied,
};
pub use types::{
    CandidateInfo, EvictionStatus, RaftGeneration, RaftLogIndex, RaftStateError,
    ReplicatedRaftState, SpareSelectionStrategy, all_targets, is_quorum,
};
pub use validators::{validate_cutoff_advance, validate_replicated_state, validate_transition};
