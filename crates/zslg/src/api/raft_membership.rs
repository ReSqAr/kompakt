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
//! [`RaftTransport`](crate::api::RaftTransport) (steady-state Raft RPCs) and
//! [`RaftConsensus`](crate::api::RaftConsensus) (the server applying the change).

use crate::ids::{NodeId, PeerAddr};
use crate::meta::RaftState;

/// Outcome of a `RaftAddMember` proposal (see [`RaftMembership::add_member`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AddMemberOutcome {
    /// Accepted (or idempotently already a member).
    Accepted { state: RaftState },
    /// Not applied now (peer is not the leader); `state.leader` gives the hint.
    Rejected { state: RaftState },
}

/// Implemented by the per-peer transport (`TcpPeerClient`), usually over a
/// direct-by-address connection — during join the target's `NodeId` is not yet
/// known.
#[async_trait::async_trait]
pub(crate) trait RaftMembership: Send + Sync + 'static {
    /// Propose adding `node_id` to the cluster via `RaftAddMember`.
    async fn add_member(
        &self,
        node_id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, crate::Error>;
}

/// The self/loopback path: applying the change locally
/// ([`RaftConsensus`](crate::api::RaftConsensus)) plus reading local state
/// ([`RaftStateView`](crate::api::RaftStateView)) is already everything a
/// `RaftMembership` does, so no wrapper type is needed. No overlap with
/// `TcpPeerClient`'s direct impl: it is neither building block.
#[async_trait::async_trait]
impl<T: crate::api::RaftConsensus + crate::api::RaftStateView + ?Sized> RaftMembership for T {
    async fn add_member(
        &self,
        node_id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, crate::Error> {
        let state = (*crate::api::RaftStateView::snapshot(self)).clone();
        match crate::api::RaftConsensus::add_member(self, node_id, peer_addr).await {
            Ok(()) => Ok(AddMemberOutcome::Accepted { state }),
            // Denied is the one answer no retry can improve: everything else
            // becomes Rejected, which the joiner retries elsewhere.
            Err(crate::Error::MembershipDenied { .. }) => {
                Err(crate::Error::MembershipDenied { node_id })
            }
            Err(_) => Ok(AddMemberOutcome::Rejected { state }),
        }
    }
}
