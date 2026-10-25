//! Per-node client routing: hand out a [`ReplicaClient`](crate::api::ReplicaClient)
//! for any node.
//!
//! - [`remote`] — [`RemoteRouter`]: every node dialed remotely over TCP (owns the
//!   [`RpcClient`](crate::network::client::RpcClient)).
//! - [`peer`] — [`PeerRouter`]: wraps a [`RemoteRouter`] and adds an in-process
//!   fast-path for `self`.
//! - [`local`] — [`LocalReplicaClient`]: the in-process `self` client the
//!   [`PeerRouter`] fast-path returns.

mod local;
mod peer;
mod remote;

pub(crate) use local::LocalReplicaClient;
pub(crate) use peer::PeerRouter;
pub(crate) use remote::RemoteRouter;
