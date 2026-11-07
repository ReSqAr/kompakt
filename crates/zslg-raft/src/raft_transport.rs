//! Steady-state Raft RPCs to *another* node, plus the blanket impl that lets a
//! local node stand in for one.
//!
//! The nested `Result` is the whole point of the trait: the outer is ours ("the
//! message did not get there"), the inner is openraft's ("it got there and Raft
//! said no"). Collapsing them would make a network partition indistinguishable
//! from a rejected vote, and openraft's own retry logic depends on the
//! difference. See `RaftConsensus` for the
//! server-side counterpart.

use raft_kit::error::{InstallSnapshotError, RaftError};
use raft_kit::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use zslg_node_types::NodeId;
use zslg_raft_types::TypeConfig;

#[async_trait::async_trait]
pub trait RaftTransport: Send + Sync + 'static {
    async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, crate::Error>;

    async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, crate::Error>;

    async fn install_snapshot(
        &self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        crate::Error,
    >;
}

/// Any local `RaftConsensus` is a `RaftTransport`:
/// an in-process call has no transport layer, so the outer `Result` is always
/// `Ok`. Lets the self/loopback client expose the same client surface as the
/// remote `TcpPeerClient` without a hand-written wrapper. (Coheres with
/// `TcpPeerClient`'s direct impl because it is not a `RaftConsensus`.)
#[async_trait::async_trait]
impl<T: crate::RaftConsensus + ?Sized> RaftTransport for T {
    async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, crate::Error> {
        Ok(crate::RaftConsensus::append_entries(self, rpc).await)
    }
    async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, crate::Error> {
        Ok(crate::RaftConsensus::vote(self, rpc).await)
    }
    async fn install_snapshot(
        &self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        crate::Error,
    > {
        Ok(crate::RaftConsensus::install_snapshot(self, rpc).await)
    }
}
