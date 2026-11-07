//! The raft log's wire vocabulary: the [`FencedStateWrite`] entries, their
//! [`RaftResponse`]s, and the structural [`RaftStateError`] reasons — bound
//! into openraft by [`TypeConfig`].
//!
//! A rejected write is a *value*, never an `Err`: the verdicts travel through
//! openraft's response channel, while an `Err` from the apply fence would be a
//! fatal storage fault on an already-committed entry (see `raft_storage`'s
//! header). The proposing leader maps a rejection to a normal `Err`; followers
//! and the recovery replay skip the entry without applying it.
use std::io::Cursor;

use openraft::Entry;
use openraft::TokioRuntime;
use serde::{Deserialize, Serialize};
use zslg_consensus_types::{RaftGeneration, RaftStateError, ReplicatedRaftState};
use zslg_net_types::PeerAddr;
use zslg_node_types::NodeId;

/// One raft-log entry's payload: the replicated state plus the generation it
/// must follow (the fence). An entry whose fence is stale applies as a no-op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FencedStateWrite {
    pub expected_generation: RaftGeneration,
    pub state: ReplicatedRaftState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftResponse {
    Applied,
    RejectedStaleGeneration {
        expected: RaftGeneration,
        current: RaftGeneration,
    },
    /// The proposed state was structurally invalid (a malformed manifest, or a
    /// `manifest_watermark > hlc_cutoff` regression) — rejected as a *value*,
    /// never an `Err` (header); the leader maps it to a normal `Err`.
    RejectedInvalidState {
        reason: RaftStateError,
    },
}

/// Thin wrapper around [`PeerAddr`] for use as the openraft `Node` type.
/// Uses `Option` so the default is simply `None` — no garbage address.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeInfo(pub Option<PeerAddr>);

impl std::ops::Deref for NodeInfo {
    type Target = PeerAddr;

    fn deref(&self) -> &PeerAddr {
        #[allow(
            clippy::expect_used,
            reason = "Default is only used by openraft internals"
        )]
        self.0
            .as_ref()
            .expect("NodeInfo always contains a PeerAddr")
    }
}

impl From<PeerAddr> for NodeInfo {
    fn from(peer_addr: PeerAddr) -> Self {
        Self(Some(peer_addr))
    }
}

impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(pa) => std::fmt::Display::fmt(pa, f),
            None => f.write_str("(none)"),
        }
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D            = FencedStateWrite,
        R            = RaftResponse,
        NodeId       = NodeId,
        Node         = NodeInfo,
        Entry        = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

pub type Raft = openraft::Raft<TypeConfig>;
