//! openraft's RPC transport seam: [`NetworkFactory`] maps openraft's
//! `new_client(target, node)` onto the crate's `Router` — registering the
//! peer's route lazily, which is how followers learn new members — and
//! [`Network`] adapts each RPC's transport errors into openraft's `RPCError`.
use std::sync::Arc;

use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;
use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

use crate::api::{RaftTransport, Router};
use crate::ids::NodeId;
use crate::raft::{NodeInfo, TypeConfig};

#[derive(Debug)]
struct UnreachableMsg(String);

impl std::fmt::Display for UnreachableMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UnreachableMsg {}

#[derive(Clone)]
pub(in crate::raft) struct NetworkFactory {
    net: Arc<dyn Router>,
}

impl NetworkFactory {
    #[must_use]
    pub(in crate::raft) fn new(net: Arc<dyn Router>) -> Self {
        Self { net }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: NodeId, node: &NodeInfo) -> Self::Network {
        // Lazy registration: ensure the peer is in the RPC routing table.
        // This is how followers learn about new members — Raft membership
        // carries PeerAddr { addr, server_name }, and new_client is called
        // before the first RPC to each peer.
        self.net.register_peer(target, (**node).clone());
        Network {
            transport: self.net.remote(target),
            target,
        }
    }
}

pub(in crate::raft) struct Network {
    transport: Arc<dyn RaftTransport>,
    target: NodeId,
}

impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        match self.transport.append_entries(rpc).await {
            Err(e) => Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                e.to_string(),
            )))),
            Ok(Err(e)) => Err(RPCError::RemoteError(RemoteError::new(self.target, e))),
            Ok(Ok(resp)) => Ok(resp),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeInfo, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self.transport.install_snapshot(rpc).await {
            Err(e) => Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                e.to_string(),
            )))),
            Ok(Err(e)) => Err(RPCError::RemoteError(RemoteError::new(self.target, e))),
            Ok(Ok(resp)) => Ok(resp),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        match self.transport.vote(rpc).await {
            Err(e) => Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                e.to_string(),
            )))),
            Ok(Err(e)) => Err(RPCError::RemoteError(RemoteError::new(self.target, e))),
            Ok(Ok(resp)) => Ok(resp),
        }
    }
}
