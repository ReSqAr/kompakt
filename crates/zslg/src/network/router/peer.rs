//! Local-vs-remote transport routing.
//!
//! [`PeerRouter`] is the single entry point for per-node clients.
//! Callers obtain a per-node [`ReplicaClient`] via
//! [`remote`](crate::api::Router::remote) — the local/self call stays in-process,
//! remote calls go through the inner [`RemoteRouter`](crate::network::RemoteRouter)
//! (TCP) — and use the returned handle normally.
//!
//! # Architecture
//!
//! | Layer | Purpose | Location |
//! |---|---|---|
//! | 1 | Connection / framing | `rpc::client`, `rpc::server` |
//! | 2 | Wire-protocol mapping | `rpc::transport::peer` |
//! | 3 | Remote-node connections | `RemoteRouter` (`router::remote`) |
//! | 4 | Routing (this module) | `PeerRouter` |

use crate::api::{
    AddMemberOutcome, LivenessProbe, RaftMembership, RaftTransport, Replica, ReplicaClient, Router,
    StampedWrite, WriteOutcome,
};
use crate::ids::{LogIndex, NodeId, PeerAddr};
use crate::meta::ReplicaStatus;
use crate::raft::TypeConfig;
use crate::{Data, Error};
use crossbeam_combinators::Channels;
use futures::stream::BoxStream;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::sync::{Arc, Weak};

/// A [`Router`] with an in-process fast-path for `self`: `remote(self_id)` returns
/// the local composite client; every other node is delegated to an inner
/// [`Router`] (the `RemoteRouter`).
pub(crate) struct PeerRouter {
    self_node_id: NodeId,
    local: Weak<dyn ReplicaClient>,
    remote: Arc<dyn Router>,
}

impl PeerRouter {
    #[must_use]
    pub(crate) fn new(
        self_node_id: NodeId,
        local: Weak<dyn ReplicaClient>,
        remote: Arc<dyn Router>,
    ) -> Self {
        Self {
            self_node_id,
            local,
            remote,
        }
    }
}

impl Router for PeerRouter {
    fn register_peer(&self, node_id: NodeId, peer_addr: PeerAddr) {
        self.remote.register_peer(node_id, peer_addr);
    }

    fn remote(&self, node_id: NodeId) -> Arc<dyn ReplicaClient> {
        if node_id == self.self_node_id {
            self.local
                .upgrade()
                .unwrap_or_else(|| Arc::new(ErrorTransport))
        } else {
            self.remote.remote(node_id)
        }
    }

    fn remote_direct(&self, peer_addr: PeerAddr) -> Arc<dyn ReplicaClient> {
        self.remote.remote_direct(peer_addr)
    }
}

struct ErrorTransport;

#[async_trait::async_trait]
impl Replica for ErrorTransport {
    fn write(&self, input: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
        // Node is shutting down: fail every write. As a pull-time map — no thread.
        input.map(|stamped| {
            Ok(WriteOutcome::Failed {
                data_id: stamped.data.data_id,
                error: Error::Shutdown,
            })
        })
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> crate::ordered::ReplicaStream {
        let (tx, rx) = crossbeam_channel::bounded::<Result<(LogIndex, Data), Error>>(1);
        drop(tx);
        // Node is shutting down: a deliberately empty stream, so close it.
        let (ch, closer) = Channels::from(rx, "shutdown stub");
        closer.close();
        crate::ordered::replica_stream(ch, from)
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, crate::Error> {
        Err(Error::Shutdown)
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, crate::Error>>, crate::Error> {
        Err(Error::Shutdown)
    }
}

// The remaining `ReplicaClient` facets only matter when the local node is mid
// shutdown (the self `Arc` already dropped); every call errors with `Shutdown`.
#[async_trait::async_trait]
impl RaftTransport for ErrorTransport {
    async fn append_entries(
        &self,
        _rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, Error> {
        Err(Error::Shutdown)
    }

    async fn vote(
        &self,
        _rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, Error> {
        Err(Error::Shutdown)
    }

    async fn install_snapshot(
        &self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        Error,
    > {
        Err(Error::Shutdown)
    }
}

#[async_trait::async_trait]
impl RaftMembership for ErrorTransport {
    async fn add_member(
        &self,
        _node_id: NodeId,
        _peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, Error> {
        Err(Error::Shutdown)
    }
}

#[async_trait::async_trait]
impl LivenessProbe for ErrorTransport {
    async fn probe(&self) -> Result<(), Error> {
        Err(Error::Shutdown)
    }
}
