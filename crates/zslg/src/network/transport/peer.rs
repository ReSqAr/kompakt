//! Where the `api` traits become wire calls: one struct implementing the facets
//! of [`ReplicaClient`](crate::api::ReplicaClient) by mapping each method onto an
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

use crate::api::{
    AddMemberOutcome, LivenessProbe, RaftMembership, RaftTransport, Replica, StampedWrite,
    WriteOutcome,
};
use crate::error::Error;
use crate::ids::{LogIndex, NodeId, PeerAddr};
use crate::meta::ReplicaStatus;
use crate::network::client::{RpcClient, Target};
use crate::network::messages::{
    RpcFrame, RpcRaftFrame, RpcRaftRequest, RpcReplicaFrame, RpcReplicaRequest, RpcRequest,
};
use crate::raft::TypeConfig;
use async_trait::async_trait;
use crossbeam_combinators::Channels;
use futures::StreamExt;
use futures::stream::BoxStream;
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
/// Per-peer typed transport over [`RpcClient`]. The peer is addressed either by
/// a registered `NodeId` ([`TcpPeerClient::new`]) or directly by
/// `(addr, server_name)` ([`TcpPeerClient::direct`]); both expose the full
/// [`ReplicaClient`](crate::api::ReplicaClient) surface. Built only inside
/// [`RemoteRouter`](crate::network::RemoteRouter).
pub(in crate::network) struct TcpPeerClient {
    rpc_client: Arc<RpcClient>,
    target: Target,
}

impl TcpPeerClient {
    /// Transport to a peer already registered in the routing table.
    #[must_use]
    pub(in crate::network) fn new(rpc_client: Arc<RpcClient>, node_id: NodeId) -> Self {
        Self {
            rpc_client,
            target: Target::Registered(node_id),
        }
    }

    /// Transport to an anonymous peer dialed directly by address — used during
    /// cluster join before the target's `NodeId` is known.
    #[must_use]
    pub(in crate::network) fn direct(rpc_client: Arc<RpcClient>, peer_addr: PeerAddr) -> Self {
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

#[async_trait]
impl RaftMembership for TcpPeerClient {
    async fn add_member(
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
            Ok(other) => Err(Error::Rpc(format!(
                "unexpected raft frame on add_member to {:?}: {other:?}",
                self.target,
            ))),
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl LivenessProbe for TcpPeerClient {
    async fn probe(&self) -> Result<(), Error> {
        self.rpc_client.echo(self.target_node_id()).await
    }
}

#[async_trait]
impl Replica for TcpPeerClient {
    /// Persistent write connection to this peer, opened on first call and held
    /// for as long as the caller drives it. Only a *registered* peer can be
    /// written to: `Target::Direct` exists for cluster join (membership + status
    /// only), before the peer's `NodeId` is even known.
    fn write(&self, input: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
        match &self.target {
            Target::Registered(node_id) => self.rpc_client.write(*node_id, input),
            Target::Direct(addr) => {
                // A direct-dialed peer (join-time transport, before its NodeId is
                // known) never accepts writes: yield one terminal error.
                Channels::from_vec(vec![Err(Error::Rpc(format!(
                    "direct-dialed peer {addr:?} does not accept writes (join-time transport)"
                )))])
            }
        }
    }

    fn subscribe_from(&self, from: Option<LogIndex>) -> crate::ordered::ReplicaStream {
        match &self.target {
            Target::Registered(node_id) => self.rpc_client.subscribe_from(*node_id, from),
            Target::Direct(_) => crate::ordered::replica_stream(Channels::from_vec(vec![]), from),
        }
    }

    async fn fetch_status(&self) -> Result<ReplicaStatus, Error> {
        let frame = self
            .rpc_client
            .call_replica(&self.target, RpcReplicaRequest::FetchStatus)
            .await?;
        match frame {
            RpcReplicaFrame::FetchStatus { status } => Ok(status),
            other => Err(Error::Rpc(format!(
                "unexpected frame on FetchStatus from {:?}: {other:?}",
                self.target,
            ))),
        }
    }

    async fn subscribe_status(
        &self,
    ) -> Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> {
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
                    Some(Ok(RpcFrame::Error(e))) => Some((Err(e), (fs, true))),
                    Some(Ok(other)) => Some((
                        Err(Error::Rpc(format!(
                            "unexpected frame in subscribe_status: {other:?}"
                        ))),
                        (fs, true),
                    )),
                    Some(Err(e)) => Some((Err(e), (fs, true))),
                }
            });

        Ok(stream.boxed())
    }
}

#[async_trait]
impl RaftTransport for TcpPeerClient {
    async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, Error> {
        let frame = self
            .rpc_client
            .call_raft(&self.target, RpcRaftRequest::AppendEntries { request: rpc })
            .await?;
        match frame {
            RpcRaftFrame::AppendEntries { response } => Ok(response),
            other => Err(Error::Rpc(format!(
                "unexpected frame on append_entries to {:?}: {other:?}",
                self.target,
            ))),
        }
    }

    async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, Error> {
        let frame = self
            .rpc_client
            .call_raft(&self.target, RpcRaftRequest::Vote { request: rpc })
            .await?;
        match frame {
            RpcRaftFrame::Vote { response } => Ok(response),
            other => Err(Error::Rpc(format!(
                "unexpected frame on vote to {:?}: {other:?}",
                self.target,
            ))),
        }
    }

    async fn install_snapshot(
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
            other => Err(Error::Rpc(format!(
                "unexpected frame on install_snapshot to {:?}: {other:?}",
                self.target,
            ))),
        }
    }
}
