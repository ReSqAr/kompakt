//! The server side of Raft: applying an RPC to *this* node's consensus state.
//!
//! The pairing to keep straight — same three RPC names, opposite directions:
//!
//! ```text
//!   RaftTransport   caller ──> peer     Result<Result<Resp, RaftError>, Error>
//!                                       outer Err = transport failed
//!   RaftConsensus   peer ──> this node  Result<Resp, RaftError>
//!                                       no transport, so no outer Result
//! ```
//!
//! A local `RaftConsensus` becomes a `RaftTransport` by a blanket impl
//! (`api::raft_peer`), which is how the self/loopback client presents the same
//! surface as a remote one.

use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::ids::{NodeId, PeerAddr};
use crate::raft::TypeConfig;

#[async_trait::async_trait]
pub(crate) trait RaftConsensus: Send + Sync + 'static {
    async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>;

    async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RaftError<NodeId>>;

    async fn install_snapshot(
        &self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>;

    async fn add_member(
        &self,
        new_node_id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<(), crate::Error>;
}
