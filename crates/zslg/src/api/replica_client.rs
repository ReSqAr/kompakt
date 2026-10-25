//! Everything one node can ask another, in one object-safe alias.
//!
//! ```text
//!   ReplicaClient = Replica          writes, subscriptions, status
//!                 + RaftTransport    steady-state raft RPCs
//!                 + RaftMembership   cluster join
//!                 + LivenessProbe    echo
//! ```
//!
//! Existing purely so [`Router::remote`](crate::api::Router::remote) can return
//! one `Arc<dyn _>`: a caller cannot name four traits in a single trait object.
//! Consumers should still take the narrowest facet they use — this alias is for
//! the router's return type, not a default.
//!
//! The blanket impl is what makes it free: `TcpPeerClient` (remote) and the
//! local composite each implement the four facets and become a `ReplicaClient`
//! with nothing further written.

use crate::api::{LivenessProbe, RaftMembership, RaftTransport, Replica};

pub(crate) trait ReplicaClient:
    Replica + RaftTransport + RaftMembership + LivenessProbe
{
}

impl<T: Replica + RaftTransport + RaftMembership + LivenessProbe + ?Sized> ReplicaClient for T {}
