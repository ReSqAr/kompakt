//! The single write point for replicated cluster state — the one seam through
//! which the leader's decisions become committed Raft entries.
//!
//! Everything the leader agrees on travels as one value, because these fields
//! constrain each other and a partial update could commit a combination no
//! invariant check would have accepted (a watermark above the cutoff, a joint
//! config collapsing before its drain). `expected_generation` makes the write a
//! compare-and-set on leadership: a proposal built from state this node no
//! longer owns is refused rather than applied late.
//!
//! `crate::leader::integrator` is the only caller — it folds every job's intent
//! into one proposal — and `raft::RaftNode` the only implementor, which
//! validates again at apply (CLAUDE.md §12).

use crate::Hlc;
use crate::ids::RaftGeneration;
use crate::meta::{ReplicaMembership, SnapshotMetadata};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct ReplicatedRaftState {
    pub(crate) replica: ReplicaMembership,
    pub(crate) manifest: Vec<SnapshotMetadata>,
    pub(crate) manifest_watermark: Option<Hlc>,
    pub(crate) hlc_cutoff: Option<Hlc>,
}

#[async_trait::async_trait]
pub(crate) trait StateReplicator: Send + Sync + 'static {
    /// Submit `state` as the next committed state. The impl refuses to even
    /// propose a structurally invalid state, so the caller gets an immediate
    /// `Err` and no known-bad entry ever enters the raft log; the apply fence
    /// runs the same check again as a value rejection (defence-in-depth for
    /// followers and the recovery replay).
    async fn replicate_state(
        &self,
        state: ReplicatedRaftState,
        expected_generation: RaftGeneration,
        context: &'static str,
    ) -> Result<(), crate::Error>;
}
