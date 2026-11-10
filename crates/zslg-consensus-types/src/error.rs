use crate::{RaftGeneration, RaftStateError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zslg_node_types::NodeId;

/// Raft and the cluster around it: leadership, membership, quorum, replicated state.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum ConsensusError {
    /// RPC received by a node that does not match the caller's expected target.
    #[error(transparent)]
    UnexpectedNodeId(Box<UnexpectedNodeId>),

    /// No quorum could be reached against the current Raft generation.
    #[error("quorum unreachable")]
    QuorumUnreachable,

    #[error(transparent)]
    StaleStateWrite(Box<StaleStateWrite>),

    /// A fenced state write carried a manifest that failed the apply-fence shape
    /// check. Rejected as a *value* at the fence (never a fatal storage fault);
    /// the proposing leader maps that rejection to this error. `source` carries
    /// the structured reason so it reconstructs exactly across the RPC boundary.
    /// A fenced state write was structurally invalid — a malformed manifest, or
    /// a `manifest_watermark > hlc_cutoff` regression (a cutoff at or below
    /// committed snapshot coverage, which no future snapshot window could ever
    /// cover). Caught pre-flight at the propose site and rejected as a *value* at
    /// the apply fence, so it never rides the fatal apply path. `source` carries
    /// the structured reason so it reconstructs exactly across the RPC boundary.
    #[error(transparent)]
    InvalidRaftState(Box<InvalidRaftState>),

    /// Invariant violation: an applied state's generation would move backwards.
    /// Unreachable through the fenced Raft apply path; surfaced as an error
    /// (instead of a process-killing assert) so the host stays in control.
    #[error("raft state generation went backwards: current {current:?}, attempted {attempted:?}")]
    StateRegression {
        current: RaftGeneration,
        attempted: RaftGeneration,
    },

    /// Peer is not the current Raft leader.
    #[error("not leader (hint: {hint:?})")]
    NotLeader { hint: Option<NodeId> },

    /// RPC to a peer not in the routing table.
    #[error("unknown peer: {0}")]
    UnknownPeer(NodeId),

    /// openraft library failure.
    #[error("raft error: {0}")]
    Raft(String),

    /// Write rejected: this node is not in the current replica set.
    #[error("not a replica: {node_id:?}")]
    NotAReplica { node_id: NodeId },

    /// The leader refused to admit (or keep) this node because its `server_name`
    /// is not in the cluster's `config.peers`. The node has been evicted on
    /// purpose (scale-down) or is misconfigured; either way it must crash and let
    /// the orchestrator reap/restart it. No local data is deleted — that is a
    /// human decision. Crosses the wire as a structured `Error` variant (the
    /// whole error type is serializable), so the join client recognises it by
    /// variant rather than by string.
    #[error("raft membership denied: node {node_id:?} is not in config.peers")]
    MembershipDenied { node_id: NodeId },

    #[error(transparent)]
    SubscriptionEnded(Box<SubscriptionEnded>),
}

impl ConsensusError {
    /// A stable, low-cardinality label naming the variant — for metric labels
    /// and structured logging. Keep these strings stable; they are a metric
    /// label dimension.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::UnexpectedNodeId { .. } => "unexpected_node_id",
            Self::QuorumUnreachable => "quorum_unreachable",
            Self::StaleStateWrite { .. } => "stale_state_write",
            Self::InvalidRaftState { .. } => "invalid_raft_state",
            Self::StateRegression { .. } => "state_regression",
            Self::NotLeader { .. } => "not_leader",
            Self::UnknownPeer(_) => "unknown_peer",
            Self::SubscriptionEnded { .. } => "subscription_ended",
            Self::Raft(_) => "raft",
            Self::NotAReplica { .. } => "not_a_replica",
            Self::MembershipDenied { .. } => "membership_denied",
        }
    }
}

/// Payload of `Error::UnexpectedNodeId`.
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("unexpected node id: expected={expected:?} actual={actual:?}")]
pub struct UnexpectedNodeId {
    pub expected: NodeId,
    pub actual: NodeId,
}

/// Payload of `Error::StaleStateWrite`.
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("stale state write ({context}): expected generation {expected:?}, current {current:?}")]
pub struct StaleStateWrite {
    pub context: String,
    pub expected: RaftGeneration,
    pub current: RaftGeneration,
}

/// Payload of `Error::InvalidRaftState`.
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("invalid raft state rejected by fence ({context}): {source}")]
pub struct InvalidRaftState {
    pub context: String,
    #[source]
    pub source: RaftStateError,
}

/// Payload of `Error::SubscriptionEnded`.
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("subscription to {peer_id} ended: {reason}")]
pub struct SubscriptionEnded {
    pub peer_id: NodeId,
    pub reason: String,
}

impl ConsensusError {
    /// Build [`ConsensusError::StaleStateWrite`].
    #[must_use]
    pub fn stale_state_write(
        context: impl Into<String>,
        expected: crate::RaftGeneration,
        current: crate::RaftGeneration,
    ) -> Self {
        Self::StaleStateWrite(Box::new(StaleStateWrite {
            context: context.into(),
            expected,
            current,
        }))
    }

    /// Build [`ConsensusError::InvalidRaftState`].
    #[must_use]
    pub fn invalid_raft_state(context: impl Into<String>, source: crate::RaftStateError) -> Self {
        Self::InvalidRaftState(Box::new(InvalidRaftState {
            context: context.into(),
            source,
        }))
    }

    /// Build [`ConsensusError::SubscriptionEnded`].
    #[must_use]
    pub fn subscription_ended(peer_id: zslg_node_types::NodeId, reason: impl Into<String>) -> Self {
        Self::SubscriptionEnded(Box::new(SubscriptionEnded {
            peer_id,
            reason: reason.into(),
        }))
    }
}
