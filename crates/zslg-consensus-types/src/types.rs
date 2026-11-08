//! The cluster's vocabulary: who is in it, what state it replicates, and the rules that state must obey.

// carved from zslg
use crate::meta::ReplicaMembership;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use zslg_node_types::NodeId;
use zslg_snapshot_types::{SnapshotId, SnapshotMetadata};
use zslg_time_types::Hlc;

/// Monotonic Raft membership generation (quorum trusts the highest).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct RaftGeneration(pub u64);

impl RaftGeneration {
    pub const ZERO: Self = Self(0);

    /// The raw counter. Public because `RaftState::generation` is: a caller
    /// that can read the generation must be able to compare or print it.
    #[must_use]
    pub fn raw(self) -> u64 {
        self.0
    }
}

/// Index into the raft log: openraft's `u64` boundary representation, wrapped
/// so raft-internal code never mixes it with the crate's data-log
/// [`zslg_log_types::LogIndex`] or a raw position. Numerically the same space as
/// `LogIndex` (the stable log mirrors the applied raft log) and
/// `RaftGeneration` (the applied generation *is* the applied entry's index);
/// conversion happens only where this type meets openraft's `u64` API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RaftLogIndex(u64);

impl RaftLogIndex {
    pub const ZERO: Self = Self(0);

    /// The index after this one — saturating; `u64::MAX` is unreachable in
    /// practice (2^64 raft entries).
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// The index after this one, or `None` at `u64::MAX`.
    #[must_use]
    pub fn checked_next(self) -> Option<Self> {
        Some(Self(self.0.checked_add(1)?))
    }

    #[must_use]
    pub fn raw(self) -> u64 {
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

/// Structured reason a proposed raft state failed a structural invariant at the
/// apply fence (`raft_storage::validate_replicated_state`). Carried as a
/// *value* in `RaftResponse::RejectedInvalidState` and rendered by
/// `crate::Error` — no pre-flattened string (CLAUDE.md §5). Every field is
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicatedRaftState {
    pub replica: ReplicaMembership,
    pub manifest: Vec<SnapshotMetadata>,
    pub manifest_watermark: Option<Hlc>,
    pub hlc_cutoff: Option<Hlc>,
}

/// Whether a reachable peer has **affirmatively refused** this node's rejoin
/// attempt (`JoinAttempt::Denied`) — i.e. this node has been removed from the
/// group's `config.peers`.
///
/// Published by `RejoinMonitor`, consulted by
/// `learner::read::can_serve_clients`. Deliberately narrower than "not
/// settled": a partition yields `JoinAttempt::Retry` (nothing answered), never
/// `Denied`, and must NOT set this — affirmative evidence only, silence is not
/// evidence (the same principle the design doc applies to leader
/// self-eviction). A node that cannot reach anyone keeps serving reads; a node
/// a reachable peer has explicitly turned away does not.
///
/// Cheap to clone (one `Arc`); reads/writes are `Relaxed` — this gates a
/// coarse "can this node serve clients" check, not a linearizable fact, and
/// every writer/reader is on the same field with no other invariant to
/// preserve ordering against.
#[derive(Clone)]
pub struct EvictionStatus(Arc<AtomicBool>);

impl Default for EvictionStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl EvictionStatus {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// True once a reachable peer has denied this node's rejoin attempt.
    #[must_use]
    pub fn is_denied(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// A status a peer has already denied — the evicted node, for read-gate
    /// tests outside this module.
    #[doc(hidden)]
    #[must_use]
    pub fn denied_for_test() -> Self {
        let status = Self::new();
        status.set(true);
        status
    }

    pub fn set(&self, denied: bool) {
        self.0.store(denied, Ordering::Relaxed);
    }
}

/// Strategy for choosing which spare (live, non-member) nodes are promoted to
/// fill a replica-set deficit during a membership change. Future signal-aware strategies
/// (most-caught-up, lowest-latency, failure-domain) add variants here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SpareSelectionStrategy {
    /// Pick uniformly at random among the available live spares.
    #[default]
    Random,
    /// Prefer the spare with the lowest probe RTT to this (leader) node;
    /// not-yet-probed spares (`rtt == None`) sort last.
    LowestLatency,
}

/// What the selection knows about one live spare.
#[derive(Debug, Clone)]
pub struct CandidateInfo {
    pub node_id: NodeId,
    /// Probe round-trip time to the leader; `None` until probed.
    pub rtt: Option<std::time::Duration>,
}

impl SpareSelectionStrategy {
    /// `candidates` in the order they should be promoted.
    #[must_use]
    pub fn rank(&self, candidates: &[CandidateInfo]) -> Vec<NodeId> {
        match self {
            Self::Random => {
                let mut v: Vec<NodeId> = candidates.iter().map(|c| c.node_id).collect();
                rand::seq::SliceRandom::shuffle(v.as_mut_slice(), &mut rand::rng());
                v
            }
            Self::LowestLatency => {
                let mut v = candidates.to_vec();
                // None RTT sorts last (Duration::MAX); NodeId breaks ties for a
                // total order.
                v.sort_by_key(|c| (c.rtt.unwrap_or(std::time::Duration::MAX), c.node_id));
                v.into_iter().map(|c| c.node_id).collect()
            }
        }
    }
}

#[must_use]
#[allow(
    clippy::expect_used,
    reason = "checked_add(1) on a set size cannot overflow usize"
)]
fn quorum_threshold(n: NonZeroUsize) -> usize {
    (n.get() / 2)
        .checked_add(1)
        .expect("set size / 2 + 1 cannot overflow usize")
}

#[must_use]
fn is_quorum_of(set: &BTreeSet<NodeId>, successful: &BTreeSet<NodeId>) -> bool {
    let Some(n) = NonZeroUsize::new(set.len()) else {
        debug_assert!(false, "is_quorum_of called with an empty set");
        return false;
    };
    set.intersection(successful).count() >= quorum_threshold(n)
}

#[must_use]
pub fn is_quorum(replica: &ReplicaMembership, successful: &BTreeSet<NodeId>) -> bool {
    let current = is_quorum_of(replica.current(), successful);
    match replica.next() {
        None => current,
        Some(next) => current && is_quorum_of(next, successful),
    }
}

#[must_use]
pub fn all_targets(replica: &ReplicaMembership) -> BTreeSet<NodeId> {
    let mut out = replica.current().clone();
    if let Some(next) = replica.next() {
        out.extend(next.iter().copied());
    }
    out
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
    use proptest::prelude::*;

    /// Node ids are drawn from a small pool so that generated sets actually
    /// overlap — with a wide pool almost every pair would be disjoint and the
    /// property would pass vacuously.
    const POOL: std::ops::Range<u8> = 0..12;

    fn nid(seed: u8) -> NodeId {
        NodeId::from_uuid(uuid::Uuid::from_u128(u128::from(seed) + 1))
    }

    // ── RaftGeneration ──────────────────────────────────────────────────

    #[test]
    fn raft_generation_zero_and_raw() {
        assert_eq!(RaftGeneration::ZERO, RaftGeneration(0));
        assert_eq!(RaftGeneration::ZERO.raw(), 0);
        assert_eq!(RaftGeneration(9).raw(), 9);
    }

    #[test]
    fn raft_generation_ordering() {
        assert!(RaftGeneration(1) < RaftGeneration(2));
    }

    fn ids_from(seeds: &[u8]) -> BTreeSet<NodeId> {
        seeds.iter().map(|&b| nid(b)).collect()
    }

    fn single(ids: &[u8]) -> ReplicaMembership {
        ReplicaMembership::single(ids_from(ids))
    }

    fn joint(old: &[u8], new: &[u8]) -> ReplicaMembership {
        ReplicaMembership::joint(ids_from(old), ids_from(new))
    }

    fn draining(old: &[u8], new: &[u8]) -> ReplicaMembership {
        ReplicaMembership::JointDraining {
            old: ids_from(old),
            next: ids_from(new),
            exit: crate::meta::Exit::WatermarkAbove(zslg_time_types::Hlc(1)),
        }
    }

    // ── quorum_threshold ────────────────────────────────────────────────

    #[test]
    fn quorum_threshold_values() {
        let n = |v: usize| NonZeroUsize::new(v).expect("non-zero");
        assert_eq!(quorum_threshold(n(1)), 1);
        assert_eq!(quorum_threshold(n(2)), 2);
        assert_eq!(quorum_threshold(n(3)), 2);
        assert_eq!(quorum_threshold(n(4)), 3);
        assert_eq!(quorum_threshold(n(5)), 3);
        assert_eq!(quorum_threshold(n(100)), 51);
    }

    // ── is_quorum_of ────────────────────────────────────────────────────

    #[test]
    fn is_quorum_of_simple_majority() {
        // 3-node set: 2 acks is a quorum, 1 is not.
        let s = ids_from(&[1, 2, 3]);
        assert!(is_quorum_of(&s, &ids_from(&[1, 2])));
        assert!(!is_quorum_of(&s, &ids_from(&[1])));
    }

    #[test]
    fn is_quorum_of_exact_boundary() {
        // 5-node set: exactly 3 is the threshold.
        let s = ids_from(&[1, 2, 3, 4, 5]);
        assert!(!is_quorum_of(&s, &ids_from(&[1, 2])));
        assert!(is_quorum_of(&s, &ids_from(&[1, 2, 3])));
    }

    #[test]
    fn is_quorum_of_ignores_acks_from_outside_the_set() {
        // Acks from nodes not in the set must not count.
        let s = ids_from(&[1, 2, 3]);
        assert!(!is_quorum_of(&s, &ids_from(&[4, 5])));
        assert!(is_quorum_of(&s, &ids_from(&[1, 4, 2])));
    }

    // ── is_quorum: single config ────────────────────────────────────────

    #[test]
    fn is_quorum_single_config() {
        let r = single(&[1, 2, 3]);
        assert!(is_quorum(&r, &ids_from(&[1, 2])));
        assert!(!is_quorum(&r, &ids_from(&[1])));
    }

    // ── is_quorum: joint config ─────────────────────────────────────────

    #[test]
    fn is_quorum_joint_requires_both() {
        // old = {1,2,3}, new = {3,4,5}. Need a quorum in BOTH.
        let r = joint(&[1, 2, 3], &[3, 4, 5]);
        // Quorum in old only → not enough.
        assert!(!is_quorum(&r, &ids_from(&[1, 2])));
        // Quorum in new only → not enough.
        assert!(!is_quorum(&r, &ids_from(&[4, 5])));
        // Quorum in both (3 is shared) → success.
        assert!(is_quorum(&r, &ids_from(&[2, 3, 4])));
    }

    #[test]
    fn is_quorum_draining_still_requires_both() {
        // An armed exit bound does not relax the quorum requirement: until the
        // config actually collapses, both sets are still load-bearing.
        let r = draining(&[1, 2, 3], &[3, 4, 5]);
        assert!(!is_quorum(&r, &ids_from(&[1, 2])));
        assert!(!is_quorum(&r, &ids_from(&[4, 5])));
        assert!(is_quorum(&r, &ids_from(&[2, 3, 4])));
    }

    #[test]
    fn all_targets_draining_is_union() {
        assert_eq!(
            all_targets(&draining(&[1, 2, 3], &[3, 4, 5])),
            ids_from(&[1, 2, 3, 4, 5])
        );
    }

    #[test]
    fn is_quorum_joint_shared_node_counts_for_both() {
        // old = {1,2,3}, new = {1,2,3} (same set). Acks {1,2} give quorum in both.
        let r = joint(&[1, 2, 3], &[1, 2, 3]);
        assert!(is_quorum(&r, &ids_from(&[1, 2])));
    }

    // ── all_targets ─────────────────────────────────────────────────────

    #[test]
    fn all_targets_single_config() {
        assert_eq!(all_targets(&single(&[1, 2, 3])), ids_from(&[1, 2, 3]));
    }

    #[test]
    fn all_targets_joint_is_union() {
        assert_eq!(
            all_targets(&joint(&[1, 2, 3], &[3, 4, 5])),
            ids_from(&[1, 2, 3, 4, 5])
        );
    }

    fn id_set(max: usize) -> impl Strategy<Value = BTreeSet<NodeId>> {
        proptest::collection::btree_set(POOL, 1..=max)
            .prop_map(|seeds| seeds.iter().map(|&b| nid(b)).collect())
    }

    fn ack_set() -> impl Strategy<Value = BTreeSet<NodeId>> {
        proptest::collection::btree_set(POOL, 0..12)
            .prop_map(|seeds| seeds.iter().map(|&b| nid(b)).collect())
    }

    fn membership() -> impl Strategy<Value = ReplicaMembership> {
        prop_oneof![
            id_set(7).prop_map(ReplicaMembership::single),
            (id_set(7), id_set(7)).prop_map(|(old, next)| ReplicaMembership::joint(old, next)),
            (id_set(7), id_set(7)).prop_map(|(old, next)| ReplicaMembership::JointDraining {
                old,
                next,
                exit: crate::meta::Exit::WatermarkAbove(zslg_time_types::Hlc(1)),
            }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// **The safety property the whole scheme rests on.** Two acknowledgement
        /// sets that each constitute a quorum must share at least one node that
        /// actually counts — otherwise two disjoint groups could both believe
        /// they committed, and a write acknowledged by one would be invisible to
        /// the other.
        ///
        /// It is a statement about *pairs* of runs, which is why the
        /// example-based tests beside it cannot express it: they would have to
        /// pick pairs by hand and would prove nothing about the rest.
        #[test]
        fn invariant_any_two_quorums_intersect(
            m in membership(),
            a in ack_set(),
            b in ack_set(),
        ) {
            if is_quorum(&m, &a) && is_quorum(&m, &b) {
                if let Some(next) = m.next() {
                    // Joint config: quorum requires a majority of `old` and a
                    // majority of `next` independently, so the two quorums
                    // must share a node inside each set, not merely somewhere
                    // in their union.
                    let old = m.current();
                    let shared_old = a.intersection(&b).filter(|id| old.contains(*id)).count();
                    prop_assert!(
                        shared_old > 0,
                        "two quorums of {m:?} were disjoint in `old`: {a:?} and {b:?}"
                    );
                    let shared_next = a.intersection(&b).filter(|id| next.contains(*id)).count();
                    prop_assert!(
                        shared_next > 0,
                        "two quorums of {m:?} were disjoint in `next`: {a:?} and {b:?}"
                    );
                } else {
                    let targets = all_targets(&m);
                    let shared = a
                        .intersection(&b)
                        .filter(|id| targets.contains(*id))
                        .count();
                    prop_assert!(
                        shared > 0,
                        "two quorums of {m:?} were disjoint: {a:?} and {b:?}"
                    );
                }
            }
        }
    }
}
