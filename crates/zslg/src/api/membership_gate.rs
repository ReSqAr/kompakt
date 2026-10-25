//! Who may join the cluster.
//!
//! The check sits at the *last* step before openraft, inside
//! [`RaftNode::add_member`](crate::raft::RaftNode) rather than in the RPC
//! handler, so there is one enforcement point covering the network and loopback
//! paths alike — a second gate in the handler would be a second thing to keep in
//! sync. Bootstrap does not pass through it at all: `openraft::Raft::initialize`
//! never calls `add_member`, so the founding set is admitted by construction.

use crate::ids::{NodeId, PeerAddr};

pub(crate) trait MembershipGate: Send + Sync + 'static {
    /// `Ok(())` if `node_id` at `peer_addr` may join, else
    /// `Err(Error::MembershipDenied)` — which callers treat as fatal for that
    /// joiner, not as a retryable rejection.
    fn admit(&self, node_id: NodeId, peer_addr: &PeerAddr) -> Result<(), crate::Error>;
}
