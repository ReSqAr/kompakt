//! Where the `api` traits become wire calls: one struct implementing the facets
//! of [`ReplicaClient`](zslg_net::ReplicaClient) by mapping each method onto an
//! `Rpc*Request` and its answer back out.
//!
//! Purely a translation layer, deliberately. Retries, pooling and timeouts belong
//! to `client`; policy belongs to the caller. What this file owns is the 1:1
//! correspondence between the trait surface and the message enums — an
//! unexpected frame here is a protocol violation, so it becomes an error rather
//! than something to interpret.
//!
//! A `Direct` (join-time) client is the one asymmetry: dialed before the peer's
//! `NodeId` is known, it can carry membership and status but not writes.

use std::sync::Arc;
use zslg_consensus::{AddMemberOutcome, RaftMembership};
use zslg_net::LivenessProbe;
use zslg_net::NetError;
use zslg_raft::RaftTransport;

use crate::Error;
use crate::client::{RpcClient, Target};
use async_trait::async_trait;
use crossbeam_combinators::Channel;
use futures::StreamExt;
use futures::stream::BoxStream;
use raft_kit::error::{InstallSnapshotError, RaftError};
use raft_kit::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use zslg_consensus::NodeId;
use zslg_consensus::ReplicaStatus;
use zslg_log::LogIndex;
use zslg_log::{Replica, StampedWrite, WriteOutcome};
use zslg_net::PeerAddr;
use zslg_net_wire::messages::{
    RpcFrame, RpcRaftFrame, RpcRaftRequest, RpcReplicaFrame, RpcReplicaRequest, RpcRequest,
};
use zslg_raft::TypeConfig;
/// Per-peer typed transport over [`RpcClient`]. The peer is addressed either by
/// a registered `NodeId` ([`TcpPeerClient::new`]) or directly by
/// `(addr, server_name)` ([`TcpPeerClient::direct`]); both expose the full
/// [`ReplicaClient`](zslg_net::ReplicaClient) surface. Built only inside
/// [`RemoteRouter`](crate::network::RemoteRouter).
pub struct TcpPeerClient {
    rpc_client: Arc<RpcClient>,
    target: Target,
}

impl TcpPeerClient {
    /// Transport to a peer already registered in the routing table.
    #[must_use]
    pub fn new(rpc_client: Arc<RpcClient>, node_id: NodeId) -> Self {
        Self {
            rpc_client,
            target: Target::Registered(node_id),
        }
    }

    /// Transport to an anonymous peer dialed directly by address — used during
    /// cluster join before the target's `NodeId` is known.
    #[must_use]
    pub fn direct(rpc_client: Arc<RpcClient>, peer_addr: PeerAddr) -> Self {
        Self {
            rpc_client,
            target: Target::Direct(peer_addr),
        }
    }

    /// `NodeId` for diagnostics in error variants that require one. Direct-mode
    /// clients have no known identity; they are only used for join
    /// (`add_member`) and status fetches, never the `write` reject path, so the
    /// nil fallback is never surfaced in practice.
    fn target_node_id(&self) -> NodeId {
        match &self.target {
            Target::Registered(id) => *id,
            Target::Direct(_) => NodeId::default(),
        }
    }
}

impl TcpPeerClient {
    pub async fn add_member_in(
        &self,
        node_id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<AddMemberOutcome, Error> {
        match self
            .rpc_client
            .call_raft(
                &self.target,
                RpcRaftRequest::AddMember { node_id, peer_addr },
            )
            .await
        {
            Ok(RpcRaftFrame::AddMember { added: true, state }) => {
                Ok(AddMemberOutcome::Accepted { state })
            }
            Ok(RpcRaftFrame::AddMember {
                added: false,
                state,
            }) => Ok(AddMemberOutcome::Rejected { state }),
            Ok(other) => Err(Error::Net(NetError::Rpc(format!(
                "unexpected raft frame on add_member to {:?}: {other:?}",
                self.target,
            )))),
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl RaftMembership for TcpPeerClient {
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

impl TcpPeerClient {
    pub async fn probe_in(&self) -> Result<(), Error> {
        self.rpc_client.echo(self.target_node_id()).await
    }
}

#[async_trait]
impl LivenessProbe for TcpPeerClient {
    async fn probe(&self) -> Result<(), zslg_net::Error> {
        self.probe_in().await.map_err(Into::into)
    }
}

#[async_trait]
impl Replica for TcpPeerClient {
    /// Persistent write connection to this peer, opened on first call and held
    /// for as long as the caller drives it. Only a *registered* peer can be
    /// written to: `Target::Direct` exists for cluster join (membership + status
    /// only), before the peer's `NodeId` is even known.
    fn write(
        &self,
        input: Channel<StampedWrite, crossbeam_combinators::Finite>,
    ) -> Channel<Result<WriteOutcome, zslg_log::Error>, crossbeam_combinators::Infinite> {
        let run = || -> Channel<Result<WriteOutcome, Error>, crossbeam_combinators::Infinite> {
            match &self.target {
                Target::Registered(node_id) => self.rpc_client.write(*node_id, input),
                Target::Direct(addr) => {
                    // A direct-dialed peer (join-time transport, before its NodeId is
                    // known) never accepts writes: yield one terminal error.
                    Channel::once(Err(Error::Net(NetError::Rpc(format!(
                        "direct-dialed peer {addr:?} does not accept writes (join-time transport)"
                    )))))
                    .chain(Channel::aborted("direct-dialed peer"))
                }
            }
        };
        let result: Channel<Result<WriteOutcome, Error>, crossbeam_combinators::Infinite> = run();
        result.err_into()
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> zslg_log::ReplicaStream {
        match &self.target {
            Target::Registered(node_id) => self.rpc_client.subscribe_from(*node_id, from),
            Target::Direct(_) => {
                Channel::aborted("direct-dialed peer").ordered(from, "replica subscription")
            }
        }
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
        let result: Result<ReplicaStatus, Error> = async {
            let frame = self
                .rpc_client
                .call_replica(&self.target, RpcReplicaRequest::FetchStatus)
                .await?;
            match frame {
                RpcReplicaFrame::FetchStatus { status } => Ok(status),
                other => Err(Error::Net(NetError::Rpc(format!(
                    "unexpected frame on FetchStatus from {:?}: {other:?}",
                    self.target,
                )))),
            }
        }
        .await;
        result.map_err(Into::into)
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error> {
        let result: Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> = async {
            let rpc_client = self.rpc_client.clone();

            let frame_stream = rpc_client
                .open(
                    &self.target,
                    RpcRequest::Replica(RpcReplicaRequest::SubscribeStatus),
                )
                .await?;

            let stream =
                futures::stream::unfold((frame_stream, false), move |(mut fs, done)| async move {
                    if done {
                        return None;
                    }
                    match fs.next_frame::<RpcFrame>().await {
                        None => None,
                        Some(Ok(RpcFrame::Replica(RpcReplicaFrame::StatusItem { status }))) => {
                            Some((Ok(status), (fs, false)))
                        }
                        Some(Ok(RpcFrame::Error(e))) => Some((Err(e.into()), (fs, true))),
                        Some(Ok(other)) => Some((
                            Err(Error::Net(NetError::Rpc(format!(
                                "unexpected frame in subscribe_status: {other:?}"
                            )))),
                            (fs, true),
                        )),
                        Some(Err(e)) => Some((Err(e.into()), (fs, true))),
                    }
                });

            Ok(stream.boxed())
        }
        .await;
        result
            .map(|s| {
                futures::StreamExt::boxed(futures::StreamExt::map(s, |r| r.map_err(Into::into)))
            })
            .map_err(Into::into)
    }
}

impl TcpPeerClient {
    pub async fn append_entries_in(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, Error> {
        let frame = self
            .rpc_client
            .call_raft(&self.target, RpcRaftRequest::AppendEntries { request: rpc })
            .await?;
        match frame {
            RpcRaftFrame::AppendEntries { response } => Ok(response),
            other => Err(Error::Net(NetError::Rpc(format!(
                "unexpected frame on append_entries to {:?}: {other:?}",
                self.target,
            )))),
        }
    }

    pub async fn vote_in(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, Error> {
        let frame = self
            .rpc_client
            .call_raft(&self.target, RpcRaftRequest::Vote { request: rpc })
            .await?;
        match frame {
            RpcRaftFrame::Vote { response } => Ok(response),
            other => Err(Error::Net(NetError::Rpc(format!(
                "unexpected frame on vote to {:?}: {other:?}",
                self.target,
            )))),
        }
    }

    pub async fn install_snapshot_in(
        &self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        Error,
    > {
        let frame = self
            .rpc_client
            .call_raft(
                &self.target,
                RpcRaftRequest::InstallSnapshot { request: rpc },
            )
            .await?;
        match frame {
            RpcRaftFrame::InstallSnapshot { response } => Ok(response),
            other => Err(Error::Net(NetError::Rpc(format!(
                "unexpected frame on install_snapshot to {:?}: {other:?}",
                self.target,
            )))),
        }
    }
}

#[async_trait]
impl RaftTransport for TcpPeerClient {
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
