//! Talking to yourself without a socket: the client
//! [`PeerRouter`](crate::network::PeerRouter) hands back for `remote(self_id)`.
//!
//! It implements only the *building-block* traits over the in-process services;
//! the composite `RaftTransport` / `RaftMembership` / `ReplicaClient` surfaces
//! come free from the blanket impls in `api`, which is what lets a caller treat
//! self and a remote peer identically.
//!
//! `RaftMembership` is written out here: its former blanket impl over
//! `RaftConsensus + RaftStateView` cannot live in a crate that defines neither.
//!
//! In practice only the [`Replica`] facet is exercised — openraft handles self
//! internally, cluster join dials peers directly, and a node never
//! liveness-probes itself — but the full surface has to exist for the router's
//! return type to be one `dyn ReplicaClient`.

use std::sync::Arc;
use zslg_consensus::RaftStateView;
use zslg_net::LivenessProbe;
use zslg_raft::RaftConsensus;

use tokio::sync::watch;

use crossbeam_combinators::Channel;
use futures::stream::BoxStream;
use raft_kit::error::{InstallSnapshotError, RaftError};
use raft_kit::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use zslg_consensus::NodeId;
use zslg_consensus::{AddMemberOutcome, RaftMembership};
use zslg_consensus::{ConsensusError, RaftState, ReplicaStatus};
use zslg_log::LogIndex;
use zslg_log::{Replica, StampedWrite, WriteOutcome};
use zslg_net::PeerAddr;
use zslg_raft::TypeConfig;

pub struct LocalReplicaClient {
    replica: Arc<dyn Replica>,
    raft: Arc<dyn RaftConsensus>,
    raft_state: Arc<dyn RaftStateView>,
}

impl LocalReplicaClient {
    #[must_use]
    pub fn new(
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
    fn write(
        &self,
        input: Channel<StampedWrite, crossbeam_combinators::Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, crossbeam_combinators::Infinite> {
        // Straight through, timestamp intact, so the replica service measures
        // the server→raft dispatch wait for the leader's own (self) append too.
        self.replica.write(input)
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> zslg_log::ReplicaStream {
        self.replica.subscribe_from(from)
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        self.replica.fetch_status().await
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
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
    ) -> Result<(), zslg_raft::Error> {
        self.raft.add_member(new_node_id, peer_addr).await
    }
}

#[async_trait::async_trait]
impl RaftMembership for LocalReplicaClient {
    async fn add_member(
        &self,
        node_id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, zslg_consensus::Error> {
        let state = (*self.raft_state.snapshot()).clone();
        match self.raft.add_member(node_id, peer_addr).await {
            Ok(()) => Ok(AddMemberOutcome::Accepted { state }),
            // Denied is the one answer no retry can improve: everything else
            // becomes Rejected, which the joiner retries elsewhere.
            Err(zslg_raft::Error::Consensus(ConsensusError::MembershipDenied { .. })) => Err(
                zslg_consensus::Error::Consensus(ConsensusError::MembershipDenied { node_id }),
            ),
            Err(_) => Ok(AddMemberOutcome::Rejected { state }),
        }
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
    async fn probe(&self) -> Result<(), zslg_net::Error> {
        // A node is trivially alive to itself.
        Ok(())
    }
}

// The composite surfaces come from the blanket impls in `zslg_net` (§ header).
