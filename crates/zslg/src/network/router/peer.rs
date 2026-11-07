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

use crate::Error;
use crate::api::{LivenessProbe, RaftTransport, ReplicaClient, Router};
use crossbeam_combinators::Channel;
use futures::stream::BoxStream;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::sync::{Arc, Weak};
use zslg_consensus::{AddMemberOutcome, RaftMembership};
use zslg_consensus_types::ReplicaStatus;
use zslg_lifecycle_types::LifecycleError;
use zslg_log::{Replica, StampedWrite, WriteOutcome};
use zslg_log_types::LogIndex;
use zslg_net_types::PeerAddr;
use zslg_node_types::NodeId;
use zslg_raft_types::TypeConfig;

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
    fn write(
        &self,
        input: Channel<StampedWrite, crossbeam_combinators::Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, crossbeam_combinators::Infinite> {
        let run = || -> Channel<Result<WriteOutcome, Error>, crossbeam_combinators::Infinite> {
            // Node is shutting down: fail every write (a pull-time map, no thread),
            // then end the outcome stream as the shutdown it is — never cleanly.
            input
                .map(|stamped| {
                    Ok(WriteOutcome::Failed {
                        data_id: stamped.data.data_id,
                        error: Error::Lifecycle(LifecycleError::Shutdown).into(),
                    })
                })
                .chain(Channel::aborted("router shutting down"))
        };
        let result: Channel<Result<WriteOutcome, Error>, crossbeam_combinators::Infinite> = run();
        result.err_into()
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> zslg_log::ReplicaStream {
        // Node is shutting down: an empty subscription that ends aborted.
        Channel::aborted("router shutting down").ordered(from, "replica subscription")
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        let result: Result<ReplicaStatus, crate::Error> =
            async { Err(Error::Lifecycle(LifecycleError::Shutdown)) }.await;
        result.map_err(Into::into)
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
        let result: Result<BoxStream<'static, Result<ReplicaStatus, crate::Error>>, crate::Error> =
            async { Err(Error::Lifecycle(LifecycleError::Shutdown)) }.await;
        result
            .map(|s| {
                futures::StreamExt::boxed(futures::StreamExt::map(s, |r| r.map_err(Into::into)))
            })
            .map_err(Into::into)
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
        Err(Error::Lifecycle(LifecycleError::Shutdown))
    }

    async fn vote(
        &self,
        _rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, Error> {
        Err(Error::Lifecycle(LifecycleError::Shutdown))
    }

    async fn install_snapshot(
        &self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        Error,
    > {
        Err(Error::Lifecycle(LifecycleError::Shutdown))
    }
}

impl ErrorTransport {
    pub(crate) async fn add_member_in(
        &self,
        _node_id: NodeId,
        _peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, Error> {
        Err(Error::Lifecycle(LifecycleError::Shutdown))
    }
}

#[async_trait::async_trait]
impl RaftMembership for ErrorTransport {
    async fn add_member(
        &self,
        node_id: zslg_node_types::NodeId,
        peer_addr: zslg_net_types::PeerAddr,
    ) -> Result<zslg_consensus::AddMemberOutcome, zslg_consensus::Error> {
        self.add_member_in(node_id, peer_addr)
            .await
            .map_err(Into::into)
    }
}

#[async_trait::async_trait]
impl LivenessProbe for ErrorTransport {
    async fn probe(&self) -> Result<(), Error> {
        Err(Error::Lifecycle(LifecycleError::Shutdown))
    }
}
