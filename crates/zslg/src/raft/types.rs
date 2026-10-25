//! The raft log's wire vocabulary: the [`FencedStateWrite`] entries, their
//! [`RaftResponse`]s, and the structural [`RaftStateError`] reasons — bound
//! into openraft by [`TypeConfig`].
//!
//! A rejected write is a *value*, never an `Err`: the verdicts travel through
//! openraft's response channel, while an `Err` from the apply fence would be a
//! fatal storage fault on an already-committed entry (see `raft_storage`'s
//! header). The proposing leader maps a rejection to a normal `Err`; followers
//! and the recovery replay skip the entry without applying it.
use std::collections::BTreeSet;
use std::io::Cursor;

use crate::api::{ReplicatedRaftState, StateReplicator};
use crate::hlc::Hlc;
use crate::ids::{NodeId, PeerAddr, RaftGeneration, SnapshotId};
use openraft::Entry;
use openraft::TokioRuntime;
use serde::{Deserialize, Serialize};

/// Index into the raft log: openraft's `u64` boundary representation, wrapped
/// so raft-internal code never mixes it with the crate's data-log
/// [`crate::ids::LogIndex`] or a raw position. Numerically the same space as
/// `LogIndex` (the stable log mirrors the applied raft log) and
/// `RaftGeneration` (the applied generation *is* the applied entry's index);
/// conversion happens only where this type meets openraft's `u64` API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct RaftLogIndex(u64);

impl RaftLogIndex {
    pub(crate) const ZERO: Self = Self(0);

    /// The index after this one — saturating; `u64::MAX` is unreachable in
    /// practice (2^64 raft entries).
    #[must_use]
    pub(crate) fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// The index after this one, or `None` at `u64::MAX`.
    #[must_use]
    pub(crate) fn checked_next(self) -> Option<Self> {
        Some(Self(self.0.checked_add(1)?))
    }

    #[must_use]
    pub(crate) fn raw(self) -> u64 {
        self.0
    }
}

impl From<u64> for RaftLogIndex {
    fn from(index: u64) -> Self {
        Self(index)
    }
}

impl From<RaftLogIndex> for u64 {
    fn from(index: RaftLogIndex) -> Self {
        index.0
    }
}

impl std::fmt::Display for RaftLogIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// One raft-log entry's payload: the replicated state plus the generation it
/// must follow (the fence). An entry whose fence is stale applies as a no-op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FencedStateWrite {
    pub(crate) expected_generation: RaftGeneration,
    pub(crate) state: ReplicatedRaftState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RaftResponse {
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

/// Structured reason a proposed raft state failed a structural invariant at the
/// apply fence (`raft_storage::validate_replicated_state`). Carried as a
/// *value* in [`RaftResponse::RejectedInvalidState`] and rendered by
/// [`crate::Error`] — no pre-flattened string (CLAUDE.md §5). Every field is
/// `Eq + Serialize`, so it satisfies `RaftResponse`'s derives and reconstructs
/// exactly across the RPC boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum RaftStateError {
    /// A manifest entry does not start where the previous one ended + 1
    /// (unsorted, has a gap, overlaps, or the first entry does not start at 0).
    #[error(
        "snapshot {snapshot_id:?} starts at index {start} but {expected} was expected — the \
         manifest must be sorted, contiguous, and cover the index space from 0"
    )]
    ManifestNonContiguous {
        snapshot_id: SnapshotId,
        start: u64,
        expected: u64,
    },
    #[error("snapshot {snapshot_id:?} ends at index {end} before its start index {start}")]
    ManifestEndBeforeStart {
        snapshot_id: SnapshotId,
        start: u64,
        end: u64,
    },
    /// `end_index + 1` would overflow `u64` — an absurd (malformed) manifest.
    #[error("manifest index overflow at snapshot {snapshot_id:?}")]
    ManifestIndexOverflow { snapshot_id: SnapshotId },
    /// `manifest_watermark > hlc_cutoff`: a cutoff at or below committed snapshot
    /// coverage, which no future snapshot window (strictly above the watermark)
    /// could ever cover — enforced as an invariant instead of floored at read
    /// time on each replica.
    #[error("manifest watermark {watermark:?} is above the hlc cutoff {cutoff:?}")]
    WatermarkAboveCutoff { watermark: Hlc, cutoff: Hlc },
    /// The committed HLC cutoff moved backwards (CLAUDE.md §8: it may never,
    /// per replica, ever). The leader's integrator already declines a
    /// non-advancing `AdvanceCutoff`, so this fires only if that single writer
    /// is wrong — a bug, lost leadership mid-change, or a stale in-flight
    /// proposal. Distinct from a *proposed* cutoff below the previous leader's,
    /// which is expected (CLAUDE.md §12) and is either skipped before proposal
    /// or absorbed as a no-op by each replica's `HlcGate`; this is about the
    /// committed value.
    #[error("committed hlc cutoff would regress from {previous:?} to {proposed:?}")]
    CutoffRegressed { previous: Hlc, proposed: Hlc },
    /// A joint → joint retarget changed the outgoing set. The doc requires the
    /// `old` set to be identical across a retarget: the drain bound is sampled
    /// from `old`, and every quorum in the episode is a quorum of `old`, so
    /// swapping it would silently invalidate both.
    #[error(
        "joint retarget changed the outgoing replica set (was {previous:?}, proposed {proposed:?})"
    )]
    JointOldSetChanged {
        previous: BTreeSet<NodeId>,
        proposed: BTreeSet<NodeId>,
    },
    /// A single (non-joint) config changed its replica set directly. Every
    /// legitimate replica-set change goes `Single -> Joint -> JointDraining ->
    /// Single`, so a direct swap here would drop the outgoing set without ever
    /// arming or satisfying a drain bound — the exact loss this fence exists to
    /// prevent. The only legal `Single -> Single` is a pass-through: every
    /// snapshot/compaction/cutoff write carries the membership along unchanged.
    #[error(
        "single replica set changed without a joint config (was {previous:?}, proposed {proposed:?})"
    )]
    SingleSetChangedWithoutJoint {
        previous: BTreeSet<NodeId>,
        proposed: BTreeSet<NodeId>,
    },
    /// The armed drain bound went backwards within one joint episode. It is a
    /// bound on data the outgoing set may hold alone, so lowering it could admit
    /// an exit that drops un-snapshotted writes.
    #[error("joint exit bound regressed from {previous:?} to {proposed:?}")]
    ExitHlcRegressed {
        previous: crate::meta::Exit,
        proposed: crate::meta::Exit,
    },
    /// **The membership durability invariant.** A joint config may only collapse
    /// once the stable prefix covers its exit bound; otherwise the writes the
    /// outgoing set holds alone are lost when those nodes are dropped.
    #[error(
        "refusing to leave joint config: exit bound {exit:?} is not yet covered by the \
         manifest watermark {watermark:?}"
    )]
    JointExitTooEarly {
        exit: crate::meta::Exit,
        watermark: Option<Hlc>,
    },
    /// A joint config tried to collapse onto its incoming set before an exit
    /// bound was ever armed — i.e. before the generation quorum of step 2. There
    /// is no evidence yet about what the outgoing set holds alone.
    #[error("refusing to leave joint config: no exit bound has been armed (step 2b not complete)")]
    JointExitNotArmed,
    #[error(
        "joint config collapsed to {proposed:?}, which is neither the incoming set {next:?} nor \
         the outgoing set {old:?}"
    )]
    JointExitWrongSet {
        proposed: BTreeSet<NodeId>,
        old: BTreeSet<NodeId>,
        next: BTreeSet<NodeId>,
    },
    #[error("joint config entered with outgoing set {proposed:?}, but {previous:?} was in force")]
    JointEntryWrongOldSet {
        proposed: BTreeSet<NodeId>,
        previous: BTreeSet<NodeId>,
    },
    #[error(
        "joint config arming changed the incoming set (was {previous:?}, proposed {proposed:?})"
    )]
    JointArmingChangedNextSet {
        previous: BTreeSet<NodeId>,
        proposed: BTreeSet<NodeId>,
    },
}

/// Thin wrapper around [`PeerAddr`] for use as the openraft `Node` type.
/// Uses `Option` so the default is simply `None` — no garbage address.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct NodeInfo(pub(crate) Option<PeerAddr>);

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
    pub(crate) TypeConfig:
        D            = FencedStateWrite,
        R            = RaftResponse,
        NodeId       = NodeId,
        Node         = NodeInfo,
        Entry        = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

pub(crate) type Raft = openraft::Raft<TypeConfig>;

#[async_trait::async_trait]
impl StateReplicator for Raft {
    async fn replicate_state(
        &self,
        state: ReplicatedRaftState,
        expected_generation: RaftGeneration,
        context: &'static str,
    ) -> Result<(), crate::Error> {
        use crate::error::ToRaft as _;
        // See `StateReplicator::replicate_state` for the pre-flight contract.
        crate::raft::raft_storage::validate_replicated_state(&state)
            .map_err(|source| crate::error::Error::invalid_raft_state(context, source))?;
        let response = self
            .client_write(FencedStateWrite {
                expected_generation,
                state,
            })
            .await
            .to_raft(context)?;
        match response.data {
            RaftResponse::Applied => Ok(()),
            RaftResponse::RejectedStaleGeneration { expected, current } => {
                crate::metrics::raft_fenced_write();
                Err(crate::error::Error::stale_state_write(
                    context, expected, current,
                ))
            }
            RaftResponse::RejectedInvalidState { reason } => {
                crate::metrics::raft_fenced_write();
                Err(crate::error::Error::invalid_raft_state(context, reason))
            }
        }
    }
}
