//! The consensus group's ports: who leads, who may join, how replicated state
//! is proposed and observed. Re-exports the group's types.

use error_union::error_union;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log_types::LogError;
use zslg_net_types::NetError;
use zslg_snapshot_types::SnapshotError;
use zslg_time_types::ClockError;

mod intent_submitter;
mod membership_gate;
mod quorum_check;
mod raft_leadership;
mod raft_membership;
mod raft_state;
mod state_replicator;

pub use intent_submitter::IntentSubmitter;
pub use membership_gate::MembershipGate;
pub use quorum_check::QuorumCheck;
pub use raft_leadership::RaftLeadership;
pub use raft_membership::{AddMemberOutcome, RaftMembership};
pub use raft_state::{RaftStateTracker, RaftStateView};
pub use state_replicator::StateReplicator;
pub use zslg_consensus_types::metrics;
pub use zslg_consensus_types::{
    CandidateInfo, ConsensusError, EvictionStatus, Exit, IntentOutcome, InvalidRaftState,
    RaftGeneration, RaftIntent, RaftLogIndex, RaftManifestState, RaftState, RaftStateError,
    RaftStateHandle, ReplicaMembership, ReplicaStatus, ReplicatedRaftState, SpareSelectionStrategy,
    StaleStateWrite, SubscriptionEnded, ToRaft, UnexpectedNodeId, all_targets, drain_satisfied,
    is_quorum, validate_cutoff_advance, validate_replicated_state, validate_transition,
};
pub use zslg_node_types::NodeId;

error_union! {
    /// What the ports of this crate can fail with. Holds every leaf error for
    /// now; narrowed to the leaves its implementations actually raise once they
    /// have moved out of `zslg`.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(LifecycleError),
        Config(ConfigError),
        Clock(ClockError),
        Log(LogError),
        Snapshot(SnapshotError),
        Net(NetError),
        Consensus(ConsensusError),
    }
}
