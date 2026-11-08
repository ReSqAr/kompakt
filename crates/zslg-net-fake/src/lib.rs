#![allow(
    clippy::unimplemented,
    reason = "fakes refuse the facets they do not answer"
)]
//! In-memory stand-ins for the net ports: clients that answer one facet and
//! refuse the rest, and routers that hand them out.

use std::sync::Arc;

use crossbeam_combinators::{Channel, Finite, Infinite};
use futures::stream::BoxStream;
use raft_kit::error::{InstallSnapshotError, RaftError};
use raft_kit::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use zslg_consensus::{AddMemberOutcome, NodeId, RaftMembership, ReplicaStatus};
use zslg_log::{LogIndex, Replica, StampedWrite, WriteOutcome};
use zslg_net::{LivenessProbe, PeerAddr, ReplicaClient, Router};
use zslg_raft::{RaftTransport, TypeConfig};

// ── test transport helpers ───────────────────────────────────────────

/// Wrap a [`Replica`] as a full [`ReplicaClient`]. The non-replica facets are
/// `unimplemented!()` because the consumers that route through `Router::remote`
/// only invoke `Replica` methods.
pub fn replica_only(replica: Arc<dyn Replica>) -> Arc<dyn ReplicaClient> {
    Arc::new(ReplicaOnly(replica))
}

struct ReplicaOnly(Arc<dyn Replica>);

#[async_trait::async_trait]
impl Replica for ReplicaOnly {
    fn write(
        &self,
        input: Channel<StampedWrite, Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, Infinite> {
        self.0.write(input)
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> zslg_log::ReplicaStream {
        self.0.subscribe_from(from)
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        self.0.fetch_status().await
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
        self.0.subscribe_status().await
    }
}

impl ReplicaOnly {
    pub(crate) async fn append_entries_in(
        &self,
        _rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, Error> {
        unimplemented!("ReplicaOnly: raft transport not used")
    }

    pub(crate) async fn vote_in(
        &self,
        _rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, Error> {
        unimplemented!("ReplicaOnly: raft transport not used")
    }

    pub(crate) async fn install_snapshot_in(
        &self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        Error,
    > {
        unimplemented!("ReplicaOnly: raft transport not used")
    }
}

#[async_trait::async_trait]
impl RaftTransport for ReplicaOnly {
    async fn append_entries(
        &self,
        rpc: raft_kit::raft::AppendEntriesRequest<zslg_raft::TypeConfig>,
    ) -> Result<
        Result<
            raft_kit::raft::AppendEntriesResponse<zslg_consensus::NodeId>,
            raft_kit::error::RaftError<zslg_consensus::NodeId>,
        >,
        zslg_raft::Error,
    > {
        self.append_entries_in(rpc).await.map_err(Into::into)
    }
    async fn vote(
        &self,
        rpc: raft_kit::raft::VoteRequest<zslg_consensus::NodeId>,
    ) -> Result<
        Result<
            raft_kit::raft::VoteResponse<zslg_consensus::NodeId>,
            raft_kit::error::RaftError<zslg_consensus::NodeId>,
        >,
        zslg_raft::Error,
    > {
        self.vote_in(rpc).await.map_err(Into::into)
    }
    async fn install_snapshot(
        &self,
        rpc: raft_kit::raft::InstallSnapshotRequest<zslg_raft::TypeConfig>,
    ) -> Result<
        Result<
            raft_kit::raft::InstallSnapshotResponse<zslg_consensus::NodeId>,
            raft_kit::error::RaftError<
                zslg_consensus::NodeId,
                raft_kit::error::InstallSnapshotError,
            >,
        >,
        zslg_raft::Error,
    > {
        self.install_snapshot_in(rpc).await.map_err(Into::into)
    }
}

impl ReplicaOnly {
    pub(crate) async fn add_member_in(
        &self,
        _node_id: NodeId,
        _peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, Error> {
        unimplemented!("ReplicaOnly: membership not used")
    }
}

#[async_trait::async_trait]
impl RaftMembership for ReplicaOnly {
    async fn add_member(
        &self,
        node_id: zslg_consensus::NodeId,
        peer_addr: zslg_net::PeerAddr,
    ) -> Result<zslg_consensus::AddMemberOutcome, zslg_consensus::Error> {
        self.add_member_in(node_id, peer_addr)
            .await
            .map_err(Into::into)
    }
}

impl ReplicaOnly {
    pub(crate) async fn probe_in(&self) -> Result<(), Error> {
        unimplemented!("ReplicaOnly: liveness not used")
    }
}

#[async_trait::async_trait]
impl LivenessProbe for ReplicaOnly {
    async fn probe(&self) -> Result<(), zslg_net::Error> {
        self.probe_in().await.map_err(Into::into)
    }
}

/// A [`Router`] whose `remote(id)` defers to a closure (wrap
/// `Replica` fakes with [`replica_only`]). The direct-dial and liveness paths
/// are `unimplemented!()` — unused by the tests that build this.
pub fn fn_remote(
    f: impl Fn(NodeId) -> Arc<dyn ReplicaClient> + Send + Sync + 'static,
) -> Arc<dyn Router> {
    Arc::new(FnRemote(Box::new(f)))
}

struct FnRemote(Box<dyn Fn(NodeId) -> Arc<dyn ReplicaClient> + Send + Sync>);

impl Router for FnRemote {
    fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

    fn remote(&self, node_id: NodeId) -> Arc<dyn ReplicaClient> {
        (self.0)(node_id)
    }

    fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn ReplicaClient> {
        unimplemented!("FnRemote: direct dial not used")
    }
}

/// A [`Router`] whose `remote(_)` returns a probe-only [`ReplicaClient`] backed by
/// `probe` for any peer — for the leader liveness-worker tests. Only the
/// `LivenessProbe` facet is real; the rest is `unimplemented!()`.
pub fn probe_router(probe: Arc<dyn LivenessProbe>) -> Arc<dyn Router> {
    Arc::new(ProbeRouter(probe))
}

struct ProbeRouter(Arc<dyn LivenessProbe>);

impl Router for ProbeRouter {
    fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

    fn remote(&self, _node_id: NodeId) -> Arc<dyn ReplicaClient> {
        Arc::new(ProbeOnly(self.0.clone()))
    }

    fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn ReplicaClient> {
        unimplemented!("ProbeRouter: direct dial not used")
    }
}

/// A [`ReplicaClient`] that delegates only `probe`; the other facets are
/// `unimplemented!()` (the liveness worker calls nothing else).
struct ProbeOnly(Arc<dyn LivenessProbe>);

impl ProbeOnly {
    pub(crate) async fn probe_in(&self) -> Result<(), Error> {
        self.0.probe().await.map_err(Error::from)
    }
}

#[async_trait::async_trait]
impl LivenessProbe for ProbeOnly {
    async fn probe(&self) -> Result<(), zslg_net::Error> {
        self.probe_in().await.map_err(Into::into)
    }
}

#[async_trait::async_trait]
impl Replica for ProbeOnly {
    fn write(
        &self,
        _: Channel<StampedWrite, Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, Infinite> {
        let run = || -> Channel<Result<WriteOutcome, Error>, Infinite> {
            unimplemented!("ProbeOnly: replica not used")
        };
        let result: Channel<Result<WriteOutcome, Error>, Infinite> = run();
        result.err_into()
    }

    fn subscribe_from(&self, _from: Option<LogIndex>) -> zslg_log::ReplicaStream {
        unimplemented!("ProbeOnly: replica not used")
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        let result: Result<ReplicaStatus, Error> =
            async { unimplemented!("ProbeOnly: replica not used") }.await;
        result.map_err(Into::into)
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
        let result: Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> =
            async { unimplemented!("ProbeOnly: replica not used") }.await;
        result
            .map(|s| {
                futures::StreamExt::boxed(futures::StreamExt::map(s, |r| r.map_err(Into::into)))
            })
            .map_err(Into::into)
    }
}

impl ProbeOnly {
    pub(crate) async fn append_entries_in(
        &self,
        _rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, Error> {
        unimplemented!("ProbeOnly: raft transport not used")
    }

    pub(crate) async fn vote_in(
        &self,
        _rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, Error> {
        unimplemented!("ProbeOnly: raft transport not used")
    }

    pub(crate) async fn install_snapshot_in(
        &self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        Error,
    > {
        unimplemented!("ProbeOnly: raft transport not used")
    }
}

#[async_trait::async_trait]
impl RaftTransport for ProbeOnly {
    async fn append_entries(
        &self,
        rpc: raft_kit::raft::AppendEntriesRequest<zslg_raft::TypeConfig>,
    ) -> Result<
        Result<
            raft_kit::raft::AppendEntriesResponse<zslg_consensus::NodeId>,
            raft_kit::error::RaftError<zslg_consensus::NodeId>,
        >,
        zslg_raft::Error,
    > {
        self.append_entries_in(rpc).await.map_err(Into::into)
    }
    async fn vote(
        &self,
        rpc: raft_kit::raft::VoteRequest<zslg_consensus::NodeId>,
    ) -> Result<
        Result<
            raft_kit::raft::VoteResponse<zslg_consensus::NodeId>,
            raft_kit::error::RaftError<zslg_consensus::NodeId>,
        >,
        zslg_raft::Error,
    > {
        self.vote_in(rpc).await.map_err(Into::into)
    }
    async fn install_snapshot(
        &self,
        rpc: raft_kit::raft::InstallSnapshotRequest<zslg_raft::TypeConfig>,
    ) -> Result<
        Result<
            raft_kit::raft::InstallSnapshotResponse<zslg_consensus::NodeId>,
            raft_kit::error::RaftError<
                zslg_consensus::NodeId,
                raft_kit::error::InstallSnapshotError,
            >,
        >,
        zslg_raft::Error,
    > {
        self.install_snapshot_in(rpc).await.map_err(Into::into)
    }
}

impl ProbeOnly {
    pub(crate) async fn add_member_in(
        &self,
        _node_id: NodeId,
        _peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, Error> {
        unimplemented!("ProbeOnly: membership not used")
    }
}

#[async_trait::async_trait]
impl RaftMembership for ProbeOnly {
    async fn add_member(
        &self,
        node_id: zslg_consensus::NodeId,
        peer_addr: zslg_net::PeerAddr,
    ) -> Result<zslg_consensus::AddMemberOutcome, zslg_consensus::Error> {
        self.add_member_in(node_id, peer_addr)
            .await
            .map_err(Into::into)
    }
}

error_union::error_union! {
    /// What the fakes can fail with before it is narrowed to a port's error.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(zslg_lifecycle_types::LifecycleError),
        Config(zslg_lifecycle_types::ConfigError),
        Clock(zslg_time::ClockError),
        Log(zslg_log::LogError),
        Snapshot(zslg_snapshot::SnapshotError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

error_union::error_bridge!(Error => zslg_log::Error, zslg_net::Error, zslg_raft::Error, zslg_consensus::Error; Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus);

/// Replica stub that is always drained and caught up: cutoff at `u64::MAX`,
/// no frontier — snapshot quorum checks pass instantly against it.
pub struct ReadyReplica {
    pub raft_state: zslg_consensus::RaftStateHandle,
}

#[async_trait::async_trait]
impl Replica for ReadyReplica {
    fn write(
        &self,
        _: Channel<StampedWrite, Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, Infinite> {
        unimplemented!("ReadyReplica does not serve writes")
    }

    fn subscribe_from(&self, _: Option<LogIndex>) -> zslg_log::ReplicaStream {
        unimplemented!("ReadyReplica does not serve subscriptions")
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        Ok(ReplicaStatus {
            node_id: NodeId::default(),
            frontier: None,
            raft_state: (*self.raft_state.snapshot()).clone(),
            effective_cutoff: Some(zslg_time::Hlc(u64::MAX)),
            max_hlc: None,
            peer_names: Vec::new(),
        })
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
        unimplemented!("ReadyReplica does not serve status subscriptions")
    }
}
