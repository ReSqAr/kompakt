//! Where a decoded request meets the local node: every arm turns an
//! `Rpc*Request` into a call on the local [`ReplicaClient`] and its answer into
//! a frame. No policy lives here — a handler that decided anything would be a
//! second copy of a rule the local implementation already owns.
//!
//! The `.boxed()` calls in `dispatch` are load-bearing, not style: it funnels
//! every arm through one await so the whole match does not sum into a single
//! poll frame and trip `large_stack_frames` (CLAUDE.md §2).

use std::sync::Arc;

use async_trait::async_trait;
use futures::FutureExt as _;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use crate::Error;
use crate::api::{AddMemberOutcome, ReplicaClient};
use crate::network::framing::{AsyncFrameSink, AsyncFrameStream};
use crate::network::messages::{
    RpcFrame, RpcRaftFrame, RpcRaftRequest, RpcReplicaFrame, RpcReplicaRequest, RpcRequest,
    RpcStreamKind,
};
use crate::network::server::{run_subscribe, run_write};

#[async_trait]
pub(in crate::network) trait RpcHandlers: Send + Sync + 'static {
    /// Handle one unary request, writing its response to `sink`.
    async fn dispatch(&self, request: RpcRequest, sink: &mut AsyncFrameSink) -> Result<(), Error>;

    /// Take over a persistent bidi stream after its `RpcStreamKind` handshake.
    /// Owns the connection end-to-end — it returns only when the stream is
    /// finished.
    async fn dispatch_stream(
        &self,
        kind: RpcStreamKind,
        frame_recv: AsyncFrameStream,
        sink: AsyncFrameSink,
        cancel: CancellationToken,
    );
}

#[derive(Clone)]
pub(in crate::network) struct DispatchHandler {
    client: Arc<dyn ReplicaClient>,
}

impl DispatchHandler {
    #[must_use]
    pub(in crate::network) fn new(client: Arc<dyn ReplicaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RpcHandlers for DispatchHandler {
    async fn dispatch(&self, request: RpcRequest, sink: &mut AsyncFrameSink) -> Result<(), Error> {
        match request {
            RpcRequest::Echo => {
                sink.send(RpcFrame::Echo).await?;
                Ok(())
            }
            RpcRequest::Replica(req) => self.dispatch_replica(req, sink).boxed().await,
            RpcRequest::Raft(req) => self.dispatch_raft(*req, sink).boxed().await,
            RpcRequest::Reserved => Err(Error::NotYetImplemented("Reserved".to_owned())),
            RpcRequest::Stream(_) => Err(Error::Rpc(
                "Stream handshake on unary dispatch path".to_owned(),
            )),
        }
    }

    async fn dispatch_stream(
        &self,
        kind: RpcStreamKind,
        frame_recv: AsyncFrameStream,
        sink: AsyncFrameSink,
        cancel: CancellationToken,
    ) {
        let sink = sink;
        match kind {
            RpcStreamKind::ReplicaWrite => {
                run_write(self.client.clone(), frame_recv, sink, cancel).await;
            }
            RpcStreamKind::ReplicaSubscribe { from } => {
                run_subscribe(self.client.clone(), from, frame_recv, sink, cancel).await;
            }
        }
    }
}

impl DispatchHandler {
    async fn dispatch_replica(
        &self,
        req: RpcReplicaRequest,
        sink: &mut AsyncFrameSink,
    ) -> Result<(), Error> {
        let frame = match req {
            // Writes travel only on the persistent `ReplicaWrite` stream
            // (`stream_handler::run_write`) — there is no unary write path.
            RpcReplicaRequest::Write { data } => {
                return Err(Error::Rpc(format!(
                    "unary write for {:?} rejected: writes must use the ReplicaWrite stream",
                    data.data_id
                )));
            }
            RpcReplicaRequest::FetchStatus => {
                let status = self.client.fetch_status().await?;
                RpcFrame::Replica(RpcReplicaFrame::FetchStatus { status })
            }
            RpcReplicaRequest::SubscribeStatus => {
                let mut stream = self.client.subscribe_status().await?;
                while let Some(item) = stream.next().await {
                    let status = item?;
                    sink.send(RpcFrame::Replica(RpcReplicaFrame::StatusItem { status }))
                        .await?;
                }
                return Ok(());
            }
        };
        sink.send(frame).await
    }

    async fn dispatch_raft(
        &self,
        req: RpcRaftRequest,
        sink: &mut AsyncFrameSink,
    ) -> Result<(), Error> {
        let frame = match req {
            RpcRaftRequest::AppendEntries { request } => {
                let response = self.client.append_entries(request).await?;
                RpcFrame::Raft(RpcRaftFrame::AppendEntries { response })
            }
            RpcRaftRequest::Vote { request } => {
                let response = self.client.vote(request).await?;
                RpcFrame::Raft(RpcRaftFrame::Vote { response })
            }
            RpcRaftRequest::InstallSnapshot { request } => {
                let response = self.client.install_snapshot(request).await?;
                RpcFrame::Raft(RpcRaftFrame::InstallSnapshot { response })
            }
            RpcRaftRequest::AddMember { node_id, peer_addr } => {
                let outcome = self.client.add_member(node_id, peer_addr).await?;
                match outcome {
                    AddMemberOutcome::Accepted { state } => {
                        RpcFrame::Raft(RpcRaftFrame::AddMember { state, added: true })
                    }
                    AddMemberOutcome::Rejected { state } => {
                        tracing::debug!(
                            ?node_id,
                            leader = ?state.leader,
                            "AddMember rejected",
                        );
                        RpcFrame::Raft(RpcRaftFrame::AddMember {
                            state,
                            added: false,
                        })
                    }
                }
            }
        };
        sink.send(frame).await
    }
}
