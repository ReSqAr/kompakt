//! The joining node's side of cluster join: ask a peer to add me.
//!
//! Join is a search for the leader, so the trait's shape is "ask, and learn
//! where to ask next" rather than "ask and fail":
//!
//! ```text
//!   Err                    transport/timeout        → retry same peer
//!   Ok(Rejected{state})    not the leader           → retry state.leader
//!   Err(MembershipDenied)  gate said no (§join)     → fatal, stop
//!   Ok(Accepted{state})    added (or already in)    → done
//! ```
//!
//! Every response carries the peer's `RaftState`, so even a rejection advances
//! the joiner's leader hint. Distinct from
//! `RaftTransport` (steady-state Raft RPCs) and
//! `RaftConsensus` (the server applying the change).

use zslg_consensus_types::RaftState;
use zslg_net_types::PeerAddr;
use zslg_node_types::NodeId;

/// Outcome of a `RaftAddMember` proposal (see [`RaftMembership::add_member`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddMemberOutcome {
    /// Accepted (or idempotently already a member).
    Accepted { state: RaftState },
    /// Not applied now (peer is not the leader); `state.leader` gives the hint.
    Rejected { state: RaftState },
}

/// Implemented by the per-peer transport (`TcpPeerClient`), usually over a
/// direct-by-address connection — during join the target's `NodeId` is not yet
/// known.
#[async_trait::async_trait]
pub trait RaftMembership: Send + Sync + 'static {
    /// Propose adding `node_id` to the cluster via `RaftAddMember`.
    async fn add_member(
        &self,
        node_id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, crate::Error>;
}
