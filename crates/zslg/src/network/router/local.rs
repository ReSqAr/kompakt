//! Talking to yourself without a socket: the client
//! [`PeerRouter`](crate::network::PeerRouter) hands back for `remote(self_id)`.
//!
//! It implements only the *building-block* traits over the in-process services;
//! the composite `RaftTransport` / `RaftMembership` / `ReplicaClient` surfaces
//! come free from the blanket impls in `api`, which is what lets a caller treat
//! self and a remote peer identically.
//!
//! In practice only the [`Replica`] facet is exercised — openraft handles self
//! internally, cluster join dials peers directly, and a node never
//! liveness-probes itself — but the full surface has to exist for the router's
//! return type to be one `dyn ReplicaClient`.

use std::sync::Arc;

use tokio::sync::watch;

use crate::Error;
use crate::api::{
    LivenessProbe, RaftConsensus, RaftStateView, Replica, StampedWrite, WriteOutcome,
};
use crate::channel::Channels;
use crate::ids::{LogIndex, NodeId, PeerAddr};
use crate::meta::{Data, RaftState, ReplicaStatus};
use crate::raft::TypeConfig;
use futures::stream::BoxStream;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

pub(crate) struct LocalReplicaClient {
    replica: Arc<dyn Replica>,
    raft: Arc<dyn RaftConsensus>,
    raft_state: Arc<dyn RaftStateView>,
}

impl LocalReplicaClient {
    #[must_use]
    pub(crate) fn new(
        replica: Arc<dyn Replica>,
        raft: Arc<dyn RaftConsensus>,
        raft_state: Arc<dyn RaftStateView>,
    ) -> Self {
        Self {
            replica,
            raft,
            raft_state,
        }
    }
}

#[async_trait::async_trait]
impl Replica for LocalReplicaClient {
    fn write(&self, input: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
        // Straight through, timestamp intact, so the replica service measures
        // the server→raft dispatch wait for the leader's own (self) append too.
        self.replica.write(input)
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> Channels<Result<(LogIndex, Data), Error>> {
        self.replica.subscribe_from(from)
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, crate::Error> {
        self.replica.fetch_status().await
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, crate::Error>>, crate::Error> {
        self.replica.subscribe_status().await
    }
}

#[async_trait::async_trait]
impl RaftConsensus for LocalReplicaClient {
    async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> {
        self.raft.append_entries(rpc).await
    }

    async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RaftError<NodeId>> {
        self.raft.vote(rpc).await
    }

    async fn install_snapshot(
        &self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> {
        self.raft.install_snapshot(rpc).await
    }

    async fn add_member(
        &self,
        new_node_id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<(), crate::Error> {
        self.raft.add_member(new_node_id, peer_addr).await
    }
}

impl RaftStateView for LocalReplicaClient {
    fn snapshot(&self) -> Arc<RaftState> {
        self.raft_state.snapshot()
    }

    fn subscribe(&self) -> watch::Receiver<Arc<RaftState>> {
        self.raft_state.subscribe()
    }
}

#[async_trait::async_trait]
impl LivenessProbe for LocalReplicaClient {
    async fn probe(&self) -> Result<(), crate::Error> {
        // A node is trivially alive to itself.
        Ok(())
    }
}

// The composite surfaces come from the blanket impls in `crate::api` (§ header).
