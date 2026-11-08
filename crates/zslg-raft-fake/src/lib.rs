//! An in-process Raft network: nodes reach each other through shared mailboxes
//! and links can be cut and healed pairwise. No sockets, so a test controls
//! exactly which messages get through.

#![allow(
    clippy::unimplemented,
    clippy::expect_used,
    clippy::result_large_err,
    reason = "fakes refuse the facets they do not answer"
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use dashmap::DashMap;
use raft_kit::RaftNetwork;
use raft_kit::RaftNetworkFactory;
use raft_kit::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use raft_kit::network::RPCOption;
use raft_kit::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use zslg_consensus::NodeId;
use zslg_net::PeerAddr;
use zslg_raft::{NodeInfo, Raft, TypeConfig};

// ── fault injection: pairwise partitions ─────────────────────────────

/// Shared handle that cuts and heals in-process network links. Cloned into
/// every node's `MemRouter`; cutting a pair makes both directions of raft
/// traffic between those nodes fail with `Unreachable` until healed.
#[derive(Clone, Default)]
pub struct Partition {
    cut: Arc<DashMap<(NodeId, NodeId), ()>>,
}

impl Partition {
    /// Cut the link between `a` and `b` (both directions).
    pub fn cut(&self, a: NodeId, b: NodeId) {
        self.cut.insert((a, b), ());
        self.cut.insert((b, a), ());
    }

    /// Restore the link between `a` and `b`.
    pub fn heal(&self, a: NodeId, b: NodeId) {
        self.cut.remove(&(a, b));
        self.cut.remove(&(b, a));
    }

    /// Cut `node` off from every node in `others`.
    pub fn isolate(&self, node: NodeId, others: &[NodeId]) {
        for &other in others {
            if other != node {
                self.cut(node, other);
            }
        }
    }

    /// Restore all links.
    pub fn heal_all(&self) {
        self.cut.clear();
    }

    fn is_cut(&self, from: NodeId, to: NodeId) -> bool {
        self.cut.contains_key(&(from, to))
    }
}

// ── in-memory Raft transport ──────────────────────────────────────────

#[derive(Clone)]
pub struct MemRouter {
    nodes: Arc<DashMap<NodeId, Raft>>,
    source: NodeId,
    partition: Partition,
}

/// The shared medium of one in-process cluster: the nodes' mailboxes and the
/// partition state. Hand each node a [`MemFabric::router`] for itself and
/// [`register`](MemFabric::register) the node once it is built.
#[derive(Clone, Default)]
pub struct MemFabric {
    nodes: Arc<DashMap<NodeId, Raft>>,
    partition: Partition,
}

impl MemFabric {
    #[must_use]
    pub fn router(&self, source: NodeId) -> MemRouter {
        MemRouter {
            nodes: self.nodes.clone(),
            source,
            partition: self.partition.clone(),
        }
    }

    pub fn register(&self, id: NodeId, raft: Raft) {
        self.nodes.insert(id, raft);
    }

    #[must_use]
    pub fn partition(&self) -> Partition {
        self.partition.clone()
    }
}

impl RaftNetworkFactory<TypeConfig> for MemRouter {
    type Network = MemNetwork;
    async fn new_client(&mut self, target: NodeId, _node: &NodeInfo) -> Self::Network {
        MemNetwork {
            router: self.nodes.clone(),
            source: self.source,
            target,
            partition: self.partition.clone(),
        }
    }
}

pub struct MemNetwork {
    router: Arc<DashMap<NodeId, Raft>>,
    source: NodeId,
    target: NodeId,
    partition: Partition,
}

impl MemNetwork {
    // mirrors the RaftNetwork method signatures, whose error type is openraft's
    fn check_link<E: std::error::Error>(&self) -> Result<(), RPCError<NodeId, NodeInfo, E>> {
        if self.partition.is_cut(self.source, self.target) {
            return Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                "partitioned".into(),
            ))));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct UnreachableMsg(String);
impl std::fmt::Display for UnreachableMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for UnreachableMsg {}

impl RaftNetwork<TypeConfig> for MemNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        self.check_link()?;
        match self.router.get(&self.target) {
            Some(raft) => raft
                .append_entries(rpc)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e))),
            None => Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                "not registered".into(),
            )))),
        }
    }
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeInfo, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.check_link()?;
        match self.router.get(&self.target) {
            Some(raft) => raft
                .install_snapshot(rpc)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e))),
            None => Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                "not registered".into(),
            )))),
        }
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        self.check_link()?;
        match self.router.get(&self.target) {
            Some(raft) => raft
                .vote(rpc)
                .await
                .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e))),
            None => Err(RPCError::Unreachable(Unreachable::new(&UnreachableMsg(
                "not registered".into(),
            )))),
        }
    }
}

#[must_use]
pub fn test_membership_map(ids: &[NodeId]) -> BTreeMap<NodeId, NodeInfo> {
    ids.iter()
        .map(|id| {
            (
                *id,
                NodeInfo(Some(PeerAddr {
                    addr: "127.0.0.1:0".into(),
                    server_name: "localhost".into(),
                })),
            )
        })
        .collect()
}
