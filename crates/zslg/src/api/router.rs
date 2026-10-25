//! Address resolution and connection reuse: the one place that turns a
//! `NodeId` into something you can call.
//!
//! ```text
//!   PeerRouter ── self ──> LocalReplicaClient   in-process, no socket
//!        │
//!        └──── RemoteRouter ──> TcpPeerClient   owns the pooled RpcClient
//!
//!   raft holds the RemoteRouter;  writer / learner / leader hold the PeerRouter
//! ```
//!
//! Raft gets the bare `RemoteRouter` because the loopback client does not exist
//! yet when Raft starts: it composes the replica and the Raft node themselves
//! (`replica_node::start`), so a node's Raft RPCs to itself go over TCP.
//!
//! Two ways to name a target, because join is a chicken-and-egg: a joining node
//! has an address but no `NodeId` yet, and learns its own only once admitted.

use std::sync::Arc;

use crate::api::ReplicaClient;
use crate::ids::{NodeId, PeerAddr};

pub(crate) trait Router: Send + Sync + 'static {
    /// Register a peer learned from Raft membership into the routing table so it
    /// can later be addressed by `NodeId`. Idempotent.
    fn register_peer(&self, node_id: NodeId, peer_addr: PeerAddr);

    /// Client for a node addressed by `NodeId` (in-process for `self` on a
    /// `PeerRouter`, otherwise remote).
    #[must_use]
    fn remote(&self, node_id: NodeId) -> Arc<dyn ReplicaClient>;

    /// Client for a peer dialed directly by address — used during cluster join
    /// before the target's `NodeId` is known.
    #[must_use]
    fn remote_direct(&self, peer_addr: PeerAddr) -> Arc<dyn ReplicaClient>;
}
