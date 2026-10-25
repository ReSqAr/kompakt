//! The RPC vocabulary: what one node can say to another, and what comes back.
//!
//! ```text
//!   RpcRequest ─┬─ Echo                    ─> RpcFrame::Echo
//!               ├─ Replica(..)             ─> RpcFrame::Replica(..)
//!               ├─ Raft(..)                ─> RpcFrame::Raft(..)
//!               └─ Stream(RpcStreamKind)   ─> the rest of the connection
//!                                              belongs to that handler
//!   any of the above, on failure          ─> RpcFrame::Error(crate::Error)
//! ```
//!
//! `Stream` is the pivot: it is a *handshake*, not a request. After it the
//! server stops treating frames as unary requests and hands the socket to the
//! named handler, whose own request and response types take over
//! (`WriteStreamResponse` on a write stream, `SubscribeStreamItem` on a
//! subscribe). That is why a write stream's response type is a struct rather
//! than a variant of `RpcFrame` — see [`WriteStreamResponse`].
//!
//! Errors travel as `crate::Error` itself, not a flattened string, so the caller
//! reconstructs the exact variant it would have seen locally and can match on it
//! (a writer distinguishes `HlcCutoff` from a transport failure and reacts
//! differently).
//!
//! Every frame is wrapped in an [`RpcEnvelope`] carrying the sender's HLC and
//! the intended recipient's `NodeId` — clock exchange and peer identity ride on
//! every message rather than on a separate handshake.
//!
//! **Enums here stay externally tagged, and variants are append-only.** postcard
//! is binary and positional: the variant tag is one byte and the body follows,
//! with no field names on the wire and no `deserialize_any`. So
//! `#[serde(tag = "…")]` cannot round-trip — it fails at runtime, and the
//! symptom is an empty stream rather than a decode error, because the peer
//! cannot encode its own reply. Reordering variants silently reinterprets every
//! message that crosses a version boundary, so new variants go on the end.

use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use crate::hlc::Hlc;
use crate::ids::{DataId, LogIndex, NodeId, PeerAddr};
use crate::meta::{Data, RaftState, ReplicaStatus};
use crate::raft::TypeConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::network) struct RpcEnvelope<T> {
    pub(in crate::network) hlc: Hlc,
    /// Node ID of the intended recipient. `None` is used when the
    /// caller does not yet know the target's identity (e.g. initial
    /// join via a direct-by-address connect); the receiver skips
    /// verification in that case.
    pub(in crate::network) node_id: Option<NodeId>,
    pub(in crate::network) inner: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::network) enum RpcReplicaRequest {
    Write { data: Box<Data> },
    FetchStatus,
    SubscribeStatus,
}

impl RpcReplicaRequest {
    #[must_use]
    pub(in crate::network) fn kind(&self) -> &'static str {
        match self {
            Self::Write { .. } => "ReplicaWrite",
            Self::FetchStatus => "ReplicaFetchStatus",
            Self::SubscribeStatus => "ReplicaSubscribeStatus",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::network) enum RpcRaftRequest {
    AppendEntries {
        request: AppendEntriesRequest<TypeConfig>,
    },
    Vote {
        request: VoteRequest<NodeId>,
    },
    InstallSnapshot {
        request: InstallSnapshotRequest<TypeConfig>,
    },
    AddMember {
        node_id: NodeId,
        peer_addr: PeerAddr,
    },
}

impl RpcRaftRequest {
    #[must_use]
    pub(in crate::network) fn kind(&self) -> &'static str {
        match self {
            Self::AppendEntries { .. } => "RaftAppendEntries",
            Self::Vote { .. } => "RaftVote",
            Self::InstallSnapshot { .. } => "RaftInstallSnapshot",
            Self::AddMember { .. } => "RaftAddMember",
        }
    }
}

/// Which persistent handler a `Stream` handshake selects. Always the first frame
/// on a long-lived connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::network) enum RpcStreamKind {
    /// The persistent per-peer write stream (`client::write_stream` /
    /// `server::stream_handler`).
    ReplicaWrite,
    ReplicaSubscribe {
        from: Option<LogIndex>,
    },
}

impl RpcStreamKind {
    #[must_use]
    pub(in crate::network) fn kind(&self) -> &'static str {
        match self {
            Self::ReplicaWrite => "ReplicaWriteStream",
            Self::ReplicaSubscribe { .. } => "ReplicaSubscribeStream",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::network) enum RpcRequest {
    #[doc(hidden)]
    Reserved,

    Echo,

    Replica(RpcReplicaRequest),

    Raft(Box<RpcRaftRequest>),

    Stream(RpcStreamKind),
}

impl RpcRequest {
    #[must_use]
    pub(in crate::network) fn kind(&self) -> &'static str {
        match self {
            Self::Reserved => "Reserved",
            Self::Echo => "Echo",
            Self::Replica(r) => r.kind(),
            Self::Raft(r) => r.kind(),
            Self::Stream(k) => k.kind(),
        }
    }
}

/// Outcome of one replica write. The wire form of
/// [`api::WriteOutcome`](crate::api::WriteOutcome), mapped 1:1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::network) enum WriteOutcome {
    /// Accepted by this replica, with its Raft state at accept time.
    Accepted { raft_state: RaftState },
    /// Rejected, with the state the writer needs for generation tracking
    /// (CLAUDE.md §9).
    Rejected {
        error: crate::error::Error,
        raft_state: RaftState,
    },
    /// Rejected *and* `fetch_status` failed too, so there is no state to report
    /// — rare, since `fetch_status` is a local call. The fan-out treats it as
    /// unreachable and takes the fresh generation from another replica.
    Failed { error: crate::error::Error },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::network) enum RpcReplicaFrame {
    FetchStatus { status: ReplicaStatus },
    StatusItem { status: ReplicaStatus },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::network) struct SubscribeStreamItem {
    pub(in crate::network) source_index: LogIndex,
    pub(in crate::network) record: Data,
}

/// One response on a write stream. Its own type rather than an [`RpcFrame`]
/// variant so that *every* value it can hold carries a [`DataId`]: the client
/// correlates by id, and there is no representable frame it would have to treat
/// as a protocol break.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::network) struct WriteStreamResponse {
    pub(in crate::network) data_id: DataId,
    pub(in crate::network) outcome: WriteOutcome,
}

#[derive(Debug, Serialize, Deserialize)]
pub(in crate::network) enum RpcRaftFrame {
    AppendEntries {
        response: Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>,
    },
    Vote {
        response: Result<VoteResponse<NodeId>, RaftError<NodeId>>,
    },
    InstallSnapshot {
        response: Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    },
    AddMember {
        state: RaftState,
        added: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(in crate::network) enum RpcFrame {
    #[doc(hidden)]
    Reserved,

    /// A failed RPC. The crate error type travels the wire directly and is
    /// reconstructed exactly on the caller side (no lossy translation layer).
    Error(crate::error::Error),

    Echo,

    Replica(RpcReplicaFrame),

    Raft(RpcRaftFrame),
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use super::*;

    // ── kind() strings ──────────────────────────────────────────────────

    #[test]
    fn replica_request_kind_strings() {
        assert_eq!(RpcReplicaRequest::FetchStatus.kind(), "ReplicaFetchStatus");
    }

    #[test]
    fn raft_request_kind_strings() {
        assert_eq!(
            RpcRaftRequest::AddMember {
                node_id: NodeId::new_v4(),
                peer_addr: PeerAddr {
                    addr: "127.0.0.1:0".into(),
                    server_name: "s".into(),
                },
            }
            .kind(),
            "RaftAddMember"
        );
    }

    #[test]
    fn rpc_request_kind_delegates() {
        assert_eq!(RpcRequest::Echo.kind(), "Echo");
        assert_eq!(RpcRequest::Reserved.kind(), "Reserved");
        assert_eq!(
            RpcRequest::Replica(RpcReplicaRequest::FetchStatus).kind(),
            "ReplicaFetchStatus"
        );
    }
}
