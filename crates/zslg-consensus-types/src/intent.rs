//! What a leader job may ask the integrator to change in the replicated Raft
//! state.

use std::collections::BTreeSet;

use zslg_log_types::LogIndex;
use zslg_snapshot_types::{SnapshotId, SnapshotMetadata};
use zslg_time_types::Hlc;

use crate::{RaftGeneration, ReplicaMembership};

/// A requested change to the replicated Raft state.
///
/// Each variant carries the premise it was computed under, so the integrator can
/// tell "this is still applicable" from "the world moved on".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftIntent {
    /// Publish a new HLC cutoff (snapshot protocol step 1). Idempotent: a cutoff
    /// at or below the committed one is already satisfied.
    AdvanceCutoff { cutoff: Hlc },

    /// Append `entry` to the manifest (absent for an empty window) and advance
    /// the watermark. Premise: the manifest it was built against — its index
    /// assignment continues from `prev_end_index`, so it cannot be rebased onto
    /// a manifest that has grown since.
    CommitSnapshot {
        entry: Option<SnapshotMetadata>,
        new_watermark: Hlc,
        prev_watermark: Option<Hlc>,
        prev_end_index: Option<LogIndex>,
    },

    /// Splice `replacement` over `replaced`, leaving the rest of the manifest
    /// alone. A splice rather than a whole manifest so a long compaction
    /// survives snapshot commits that land while it runs.
    CompactManifest {
        replaced: BTreeSet<SnapshotId>,
        replacement: Vec<SnapshotMetadata>,
    },

    /// A replica-membership transition, validated by the same function the
    /// Raft apply fence uses.
    SetMembership { replica: ReplicaMembership },
}

impl RaftIntent {
    #[must_use]
    pub fn describe(&self) -> &'static str {
        match self {
            Self::AdvanceCutoff { .. } => "AdvanceCutoff",
            Self::CommitSnapshot { .. } => "CommitSnapshot",
            Self::CompactManifest { .. } => "CompactManifest",
            Self::SetMembership { .. } => "SetMembership",
        }
    }
}

/// Distinct from `Err`: the integrator worked, the intent no longer applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentOutcome {
    /// Folded into a proposal that committed.
    Applied { generation: RaftGeneration },
    /// Already true in the committed state; nothing to propose.
    AlreadySatisfied,
    /// The premise no longer holds; recompute from current state.
    Superseded { reason: String },
}
