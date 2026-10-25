//! Steady-state Raft RPCs to *another* node, plus the blanket impl that lets a
//! local node stand in for one.
//!
//! The nested `Result` is the whole point of the trait: the outer is ours ("the
//! message did not get there"), the inner is openraft's ("it got there and Raft
//! said no"). Collapsing them would make a network partition indistinguishable
//! from a rejected vote, and openraft's own retry logic depends on the
//! difference. See [`RaftConsensus`](crate::api::RaftConsensus) for the
//! server-side counterpart.

use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::ids::NodeId;
use crate::raft::TypeConfig;

#[async_trait::async_trait]
pub(crate) trait RaftTransport: Send + Sync + 'static {
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

/// Any local [`RaftConsensus`](crate::api::RaftConsensus) is a `RaftTransport`:
/// an in-process call has no transport layer, so the outer `Result` is always
/// `Ok`. Lets the self/loopback client expose the same client surface as the
/// remote `TcpPeerClient` without a hand-written wrapper. (Coheres with
/// `TcpPeerClient`'s direct impl because it is not a `RaftConsensus`.)
#[async_trait::async_trait]
impl<T: crate::api::RaftConsensus + ?Sized> RaftTransport for T {
    async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>, crate::Error> {
        Ok(crate::api::RaftConsensus::append_entries(self, rpc).await)
    }
    async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<Result<VoteResponse<NodeId>, RaftError<NodeId>>, crate::Error> {
        Ok(crate::api::RaftConsensus::vote(self, rpc).await)
    }
    async fn install_snapshot(
        &self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
        crate::Error,
    > {
        Ok(crate::api::RaftConsensus::install_snapshot(self, rpc).await)
    }
}
