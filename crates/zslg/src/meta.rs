//! The replicated records — what Raft agrees on, and what goes on the wire.
//!
//! Everything here is `Serialize`/`Deserialize` and outlives the process, so a
//! change to any of it is a format change. Two consequences worth knowing before
//! editing:
//!
//! - **Variant order can be load-bearing.** [`Exit`] derives `Ord`, and the
//!   membership drain relies on `AnyTime` ordering below every
//!   `WatermarkAbove` so a downgrade reads as the regression it is
//!   (CLAUDE.md §8, "a joint replica config may not collapse…").
//! - **[`ReplicaMembership`] is not a set, it is a state machine.** Which sets
//!   count for quorum, and when the outgoing one may be dropped, follow from the
//!   variant — see `crate::quorum` and `raft_storage::validate_transition`.

use std::collections::BTreeSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::hlc::Hlc;
use crate::ids::{DataId, LogIndex, NodeId, RaftGeneration, ServerName, SnapshotId};

/// Self-describing data id + opaque user blob. The id carries the creation HLC
/// (`data_id.hlc()`) and the originating node (`data_id.source_node()`), so no
/// separate metadata header is needed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Data {
    pub data_id: DataId,
    #[serde(with = "serde_bytes_compat")]
    pub blob: Bytes,
}

/// Events emitted on the stable-log broadcast channel in causal order:
/// `Appended` entries appear before any `Watermark` that covers them, so when a
/// consumer sees `Watermark(hlc)`, every entry with `data_id.hlc() <= hlc` has
/// already been emitted.
#[derive(Debug, Clone)]
pub enum StableLogEvent {
    Appended { index: LogIndex, data: Data },
    Watermark(Hlc),
}

/// Events of the unstable-tail subscription, in arrival (integration) order.
/// `Appended` may repeat; consumers dedup by `data_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnstableEvent {
    Appended(Data),
    /// Every entry with `data_id.hlc() <= hlc` is settled: the stable log
    /// delivers it, or it never converged and is gone. Drop held entries at or
    /// below it.
    Watermark(Hlc),
}

mod serde_bytes_compat {
    use bytes::Bytes;
    use serde::de::{Error, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub(in crate::meta) fn serialize<S: Serializer>(
        bytes: &Bytes,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        ser.serialize_bytes(bytes.as_ref())
    }

    /// Bulk byte-buffer deserialize. The previous `<Vec<u8>>::deserialize` went
    /// through serde's generic `Vec<T>` path = one `SeqAccess::next_element`
    /// visitor call **per byte**, which dominated CPU on the hot decode path
    /// (`Data::decode` was ~40% of all samples at inflight=128; see
    /// docs/PERFORMANCE.md entry 9). `deserialize_bytes` hands us the whole slice
    /// in one shot (postcard reads a varint length, then the raw bytes).
    pub(in crate::meta) fn deserialize<'de, D: Deserializer<'de>>(
        de: D,
    ) -> Result<Bytes, D::Error> {
        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Bytes;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte buffer")
            }

            fn visit_bytes<E: Error>(self, v: &[u8]) -> Result<Bytes, E> {
                Ok(Bytes::copy_from_slice(v))
            }

            fn visit_borrowed_bytes<E: Error>(self, v: &'de [u8]) -> Result<Bytes, E> {
                Ok(Bytes::copy_from_slice(v))
            }

            fn visit_byte_buf<E: Error>(self, v: Vec<u8>) -> Result<Bytes, E> {
                Ok(Bytes::from(v))
            }
        }

        de.deserialize_bytes(BytesVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotEntry {
    pub(crate) global_index: LogIndex,
    pub(crate) data: Data,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub start_hlc: Hlc,
    pub end_hlc: Hlc,
    pub start_index: LogIndex,
    pub end_index: LogIndex,
    pub snapshot_id: SnapshotId,
}

/// The replica-set membership stage, as committed through Raft.
///
/// The three variants are the doc's four-step membership change
/// (designdoc.md §"Membership Change") made explicit, because the step that
/// matters for durability is invisible in a `(set, Option<set>)` pair: whether
/// the leader has yet established *how much data the old set still owes a
/// snapshot*. That bound is [`Exit`], and it only exists in
/// [`Self::JointDraining`] — so "may this joint config collapse?" is a question
/// the type answers, not a comment.
///
/// Progression: `Single` → `Joint` (step 1) → `JointDraining` (steps 2 + 2b) →
/// `Single` (step 4). A retarget (`Joint`/`JointDraining` → `Joint` with a
/// different `next`) is permitted and must keep `old` identical; it drops back
/// out of `JointDraining` because the bound attests a generation quorum for
/// *that* `next`, which a new `next` has not got.
///
/// A retarget needs no bound from the abandoned `next`: `old` is required for
/// quorum at every stage until collapse (see [`crate::quorum::is_quorum`]), so a
/// bound sampled over a quorum of `old` covers every write acknowledged in the
/// whole episode — including writes acknowledged against an earlier `next`.
///
/// Transitions are enforced at the Raft write point by
/// `crate::raft::raft_storage::validate_transition`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaMembership {
    /// Steady state: one replica set, quorum is a majority of it.
    Single { replicas: BTreeSet<NodeId> },
    /// Joint config committed; the drain bound is not yet established, so this
    /// config may **not** collapse to a single set.
    Joint {
        old: BTreeSet<NodeId>,
        next: BTreeSet<NodeId>,
    },
    /// Joint config whose exit condition is armed: a quorum of the outgoing
    /// set(s) has been interrogated and `exit` bounds the HLC of every write
    /// that could have been acknowledged without a quorum of `next`.
    JointDraining {
        old: BTreeSet<NodeId>,
        next: BTreeSet<NodeId>,
        exit: Exit,
    },
}

/// The armed exit condition of a draining joint config — when the outgoing
/// replica set may be dropped.
///
/// `AnyTime` is a distinct state, not an `Hlc` sentinel. Encoding it as
/// `Hlc::ZERO` made "no write can be stranded" and "a write at the epoch"
/// the same value, and forced every reader through
/// `watermark.unwrap_or(Hlc::ZERO)` to recover the distinction.
///
/// Variant order is load-bearing: the derived `Ord` puts `AnyTime` below every
/// `WatermarkAbove`, which is what the write point's monotonicity check
/// ([`crate::raft::RaftStateError::ExitHlcRegressed`]) compares. `AnyTime` is
/// the weakest bound, so arming it after a real one is a regression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Exit {
    /// Nothing to drain: every responder in the `old` quorum reported an empty
    /// log, so no acknowledged write can be stranded there. Collapse needs no
    /// snapshot and is satisfied immediately.
    AnyTime,
    /// Collapse once the manifest watermark reaches this HLC.
    WatermarkAbove(Hlc),
}

impl Default for ReplicaMembership {
    fn default() -> Self {
        Self::Single {
            replicas: BTreeSet::new(),
        }
    }
}

impl ReplicaMembership {
    #[must_use]
    pub(crate) fn single(replicas: BTreeSet<NodeId>) -> Self {
        Self::Single { replicas }
    }

    #[must_use]
    pub(crate) fn joint(old: BTreeSet<NodeId>, next: BTreeSet<NodeId>) -> Self {
        Self::Joint { old, next }
    }

    #[must_use]
    pub fn is_joint(&self) -> bool {
        !matches!(self, Self::Single { .. })
    }

    /// The set that is authoritative today: the single set, or the *old* set of
    /// a joint config. Quorum in this set is always necessary (never, on its
    /// own, sufficient while joint — see [`crate::quorum::is_quorum`]).
    #[must_use]
    pub fn current(&self) -> &BTreeSet<NodeId> {
        match self {
            Self::Single { replicas } => replicas,
            Self::Joint { old, .. } | Self::JointDraining { old, .. } => old,
        }
    }

    /// The incoming set of a joint config, if any.
    #[must_use]
    pub fn next(&self) -> Option<&BTreeSet<NodeId>> {
        match self {
            Self::Single { .. } => None,
            Self::Joint { next, .. } | Self::JointDraining { next, .. } => Some(next),
        }
    }

    /// The armed drain bound, present only once the exit condition has been
    /// established. `None` means "this joint config may not collapse yet".
    ///
    /// [`Hlc::ZERO`] is the "nothing to drain" bound: step 2b interrogated a
    /// quorum of the outgoing set and every responder reported an empty log.
    #[must_use]
    pub fn exit(&self) -> Option<Exit> {
        match self {
            Self::Single { .. } | Self::Joint { .. } => None,
            Self::JointDraining { exit, .. } => Some(*exit),
        }
    }
}

/// Does the stable prefix cover a joint config's drain bound?
///
/// The one definition of the membership exit condition, shared by the leader's
/// step 3 (`leader::membership_change`) and the Raft write point
/// (`raft::raft_storage::validate_transition`) so the two cannot drift.
///
/// [`Exit::AnyTime`] needs no coverage at all. A real bound needs a committed
/// watermark that reaches it — a cluster that has never snapshotted (`None`)
/// covers nothing.
///
/// # Why the bound is exact
///
/// The design doc's exit condition ("a snapshot has been created from the
/// shared config") is a *completeness* requirement, not "a snapshot ran". A
/// snapshot covers only HLCs at or below its cutoff (`now - cutoff_lag`) and
/// the manifest watermark advances even for an empty window — so a write
/// acknowledged against a quorum of the **old** set alone, at an HLC above that
/// cutoff, would be in no snapshot once the old nodes stop counting toward
/// quorum. That write is lost. Hence the bound: after the step-2 generation
/// quorum the leader samples each node's replica-log `max_hlc` across a quorum
/// of `old` and commits the maximum.
///
/// Correctness rests on three facts:
///
/// 1. **Quorums of `old` intersect**, so the max over any quorum bounds
///    anything the set holds alone.
/// 2. **After the generation quorum, no write can be acknowledged without
///    `next`** — every ACK carries the config generation and the writer
///    re-evaluates on each one.
/// 3. **`old` is required for quorum at every stage until collapse**, so the
///    bound also covers writes acknowledged against an earlier `next` after a
///    retarget.
///
/// Enforced twice on purpose: by the leader's step 3, and independently at the
/// Raft write point, so neither a leader bug nor a stale in-flight proposal can
/// drop nodes holding the only copy of acknowledged writes.
#[must_use]
pub(crate) fn drain_satisfied(watermark: Option<Hlc>, exit: Exit) -> bool {
    match exit {
        Exit::AnyTime => true,
        Exit::WatermarkAbove(bound) => watermark.is_some_and(|w| w >= bound),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RaftManifestState {
    pub manifest: Vec<SnapshotMetadata>,
    pub manifest_watermark: Option<Hlc>,
    pub hlc_cutoff: Option<Hlc>,
}

/// A node's view of the cluster's committed and derived Raft state.
///
/// Two different kinds of field live here, and the distinction matters to a
/// reader. `generation`, `replica` and `manifest_state` are *replicated* — they
/// are the state machine's applied output, identical on every node that has
/// applied the same generation. `leader`, `raft_term` and `voters` are
/// *derived*, blended in from this node's openraft metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftState {
    pub leader: Option<NodeId>,
    pub raft_term: u64,

    pub generation: RaftGeneration,
    pub replica: ReplicaMembership,
    pub manifest_state: RaftManifestState,
    /// The openraft voter set as *this node's* openraft last reported it.
    ///
    /// Distinct from `replica`, the set that serves writes: a membership job's
    /// whole task is moving one toward the other.
    ///
    /// Derived rather than replicated, and it travels anyway — like `leader`
    /// and `raft_term`, which are blended by the same call and are already
    /// read remotely (`RaftMembership`'s "not the leader, `state.leader` gives
    /// the hint" retry). A reader on the far end is looking at the *sending*
    /// node's view, refreshed when `RaftTracker::integrate` adopts a newer
    /// generation, so treat it as a recent observation and not as consensus.
    ///
    /// It is deliberately not `#[serde(skip)]`. Skipping would make
    /// `decode(encode(x)) != x` for any non-empty set — a trap for anything
    /// comparing a decoded state against a local one — and would hand a
    /// receiver an empty set, which reads as "this cluster has no voters"
    /// rather than "unknown". `raft_state_roundtrips_including_voters` pins
    /// the round trip.
    pub voters: BTreeSet<NodeId>,
}

impl RaftState {
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self {
            leader: None,
            raft_term: 0,
            generation: RaftGeneration::ZERO,
            replica: ReplicaMembership::default(),
            manifest_state: RaftManifestState::default(),
            voters: BTreeSet::new(),
        }
    }
}

/// Snapshot of a replica's status, returned by `Replica::fetch_status` in a
/// single call: log frontier (catch-up), Raft state (config/leadership
/// comparison), the fully-drained effective HLC cutoff, and the log's max HLC
/// (membership drain bound). All are cheap in-memory reads; bundling them keeps
/// the trait + RPC surface narrow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReplicaStatus {
    pub(crate) node_id: NodeId,
    pub(crate) frontier: Option<LogIndex>,
    pub(crate) raft_state: RaftState,
    pub(crate) effective_cutoff: Option<Hlc>,
    pub(crate) max_hlc: Option<Hlc>,
    /// The responder's own `config.peers`, by server name.
    ///
    /// The full list rather than a computed "do you list me" boolean: it is
    /// self-describing, and a disagreement between nodes is then visible in a
    /// log instead of having to be inferred. At 3-7 short strings per
    /// membership poll the cost is irrelevant.
    pub(crate) peer_names: Vec<ServerName>,
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
    use uuid::Uuid;

    use super::*;
    use crate::ids::RaftGeneration;

    /// `Exit::AnyTime` is satisfied with no snapshot at all.
    ///
    /// It is what `arm_exit_condition` commits when every responder in the
    /// `old` quorum reported an empty log — nothing can be stranded there. That
    /// is the ordinary state of a fresh cluster's first membership change,
    /// where the watermark is also still `None`.
    ///
    /// Pins that "nothing to drain" stays distinguishable from "a write at the
    /// epoch" — a single sentinel HLC cannot express both.
    #[test]
    fn any_time_is_drained_before_any_snapshot_exists() {
        assert!(drain_satisfied(None, Exit::AnyTime));
        assert!(drain_satisfied(Some(Hlc(5)), Exit::AnyTime));

        // A real bound needs coverage, and nothing covers it before the first
        // snapshot commits.
        assert!(!drain_satisfied(None, Exit::WatermarkAbove(Hlc(1))));
        assert!(drain_satisfied(Some(Hlc(5)), Exit::WatermarkAbove(Hlc(5))));
        assert!(!drain_satisfied(Some(Hlc(4)), Exit::WatermarkAbove(Hlc(5))));
    }

    /// The write point rejects a bound that regresses, and `AnyTime` is the
    /// weakest bound there is — so it must order below every real one, or
    /// downgrading an armed bound to "nothing to drain" would slip through.
    #[test]
    fn any_time_orders_below_every_real_bound() {
        assert!(Exit::AnyTime < Exit::WatermarkAbove(Hlc::ZERO));
        assert!(Exit::AnyTime < Exit::WatermarkAbove(Hlc(1)));
        assert!(Exit::WatermarkAbove(Hlc(1)) < Exit::WatermarkAbove(Hlc(2)));
    }

    fn sample_data_id() -> DataId {
        DataId::from_parts(Hlc(100), NodeId::from_uuid(Uuid::from_u128(1)), 2)
    }

    fn sample_snapshot_meta(start: u64, end: u64, sid: u64) -> SnapshotMetadata {
        SnapshotMetadata {
            start_hlc: Hlc(start),
            end_hlc: Hlc(end),
            start_index: LogIndex(start),
            end_index: LogIndex(end),
            snapshot_id: SnapshotId::from_u128(u128::from(sid)),
        }
    }

    // ── ReplicaMembership::is_joint ──────────────────────────────────────

    fn nid(seed: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(seed))
    }

    #[test]
    fn replica_state_is_joint() {
        let single = ReplicaMembership::single([nid(1)].into_iter().collect());
        assert!(!single.is_joint());
        assert_eq!(single.next(), None);
        assert_eq!(single.exit(), None);

        let joint = ReplicaMembership::joint([nid(1)].into_iter().collect(), BTreeSet::new());
        assert!(joint.is_joint());
        assert_eq!(joint.next(), Some(&BTreeSet::new()));
        // A joint config without an armed bound may not collapse.
        assert_eq!(joint.exit(), None);

        let draining = ReplicaMembership::JointDraining {
            old: [nid(1)].into_iter().collect(),
            next: [nid(2)].into_iter().collect(),
            exit: Exit::WatermarkAbove(Hlc(42)),
        };
        assert!(draining.is_joint());
        assert_eq!(draining.current(), &[nid(1)].into_iter().collect());
        assert_eq!(draining.next(), Some(&[nid(2)].into_iter().collect()));
        assert_eq!(draining.exit(), Some(Exit::WatermarkAbove(Hlc(42))));
    }

    // ── RaftState ───────────────────────────────────────────────────────

    #[test]
    fn raft_state_empty_defaults() {
        let s = RaftState::empty();
        assert_eq!(s.leader, None);
        assert_eq!(s.raft_term, 0);
        assert_eq!(s.generation, RaftGeneration::ZERO);
        assert!(s.replica.current().is_empty());
        assert!(s.manifest_state.manifest.is_empty());
        assert_eq!(s.manifest_state.manifest_watermark, None);
        assert_eq!(s.manifest_state.hlc_cutoff, None);
    }

    #[test]
    fn raft_state_is_joint_delegates() {
        let mut s = RaftState::empty();
        assert!(!s.replica.is_joint());
        s.replica = ReplicaMembership::joint(BTreeSet::new(), BTreeSet::new());
        assert!(s.replica.is_joint());
    }

    #[test]
    fn manifest_end_index_empty_is_none() {
        assert_eq!(
            RaftState::empty()
                .manifest_state
                .manifest
                .last()
                .map(|m| m.end_index),
            None
        );
    }

    #[test]
    fn manifest_end_index_uses_last_entry() {
        let mut s = RaftState::empty();
        s.manifest_state.manifest = vec![
            sample_snapshot_meta(0, 5, 1),
            sample_snapshot_meta(6, 12, 2),
        ];
        assert_eq!(
            s.manifest_state.manifest.last().map(|m| m.end_index),
            Some(LogIndex(12))
        );
    }

    // ── serde roundtrips ────────────────────────────────────────────────

    #[test]
    fn data_serde_roundtrip() {
        let d = Data {
            data_id: sample_data_id(),
            blob: Bytes::from_static(b"hello world"),
        };
        let bytes = postcard::to_allocvec(&d).expect("encode");
        let decoded: Data = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, d);
    }

    #[test]
    fn snapshot_entry_serde_roundtrip() {
        let e = SnapshotEntry {
            global_index: LogIndex(7),
            data: Data {
                data_id: sample_data_id(),
                blob: Bytes::from_static(b"x"),
            },
        };
        let bytes = postcard::to_allocvec(&e).expect("encode");
        assert_eq!(
            postcard::from_bytes::<SnapshotEntry>(&bytes).expect("decode"),
            e
        );
    }

    #[test]
    fn snapshot_metadata_serde_roundtrip() {
        let m = sample_snapshot_meta(1, 99, 42);
        let bytes = postcard::to_allocvec(&m).expect("encode");
        assert_eq!(
            postcard::from_bytes::<SnapshotMetadata>(&bytes).expect("decode"),
            m
        );
    }

    #[test]
    fn raft_state_serde_roundtrip() {
        let mut s = RaftState::empty();
        s.generation = RaftGeneration(3);
        s.replica = ReplicaMembership::single(
            [NodeId::from_uuid(Uuid::from_u128(9))]
                .into_iter()
                .collect(),
        );
        s.manifest_state.manifest = vec![sample_snapshot_meta(0, 5, 1)];
        s.manifest_state.manifest_watermark = Some(Hlc(500));
        let bytes = postcard::to_allocvec(&s).expect("encode");
        assert_eq!(
            postcard::from_bytes::<RaftState>(&bytes).expect("decode"),
            s
        );
    }

    /// What goes on the wire comes back off it — including the *derived*
    /// fields.
    ///
    /// `raft_state_serde_roundtrip` above builds from `RaftState::empty()`, so
    /// `voters` is empty there and the case is vacuous: it passes whether or
    /// not the field is serialized. This one populates all three blended
    /// fields, so it fails outright if `voters` is ever given
    /// `#[serde(skip)]` — which was the shape this field originally shipped
    /// in, and the reason the round trip is now pinned rather than assumed.
    ///
    /// `RaftState` is the payload on every `ReplicaStatus` reply, and it
    /// derives `PartialEq`, so a non-identity round trip is a trap for anything
    /// comparing a decoded state with a local one.
    #[test]
    fn raft_state_roundtrips_including_voters() {
        let voter = |n: u128| NodeId::from_uuid(Uuid::from_u128(n));
        let mut s = RaftState::empty();
        s.generation = RaftGeneration(7);
        s.replica = ReplicaMembership::single([voter(1), voter(2)].into_iter().collect());
        // The three derived fields, all non-default so none can round-trip by
        // accident through `Default`.
        s.leader = Some(voter(1));
        s.raft_term = 11;
        s.voters = [voter(1), voter(2), voter(3)].into_iter().collect();

        let bytes = postcard::to_allocvec(&s).expect("encode");
        let decoded: RaftState = postcard::from_bytes(&bytes).expect("decode");

        assert_eq!(decoded, s, "RaftState round trip must be the identity");
        assert_eq!(
            decoded.voters, s.voters,
            "the voter set must survive the wire, not arrive empty"
        );
        assert_ne!(
            decoded.voters,
            *decoded.replica.current(),
            "voters and the replica set are different sets — a test that let \
             them coincide would not notice one being decoded into the other"
        );
    }

    #[test]
    fn empty_blob_roundtrips() {
        let d = Data {
            data_id: sample_data_id(),
            blob: Bytes::new(),
        };
        let bytes = postcard::to_allocvec(&d).expect("encode");
        let decoded: Data = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, d);
        assert!(decoded.blob.is_empty());
    }

    /// `peer_names` must survive the wire — it is the evidence a leader uses to
    /// decide a quorum no longer wants it, so an empty set arriving at the far end
    /// would read as "nobody lists you" and is exactly the wrong default.
    #[test]
    fn replica_status_roundtrips_peer_names() {
        let s = ReplicaStatus {
            node_id: NodeId::from_uuid(Uuid::from_u128(1)),
            frontier: Some(LogIndex(4)),
            raft_state: RaftState::empty(),
            effective_cutoff: Some(Hlc(9)),
            max_hlc: Some(Hlc(11)),
            peer_names: vec!["node-a".into(), "node-b".into()],
        };
        let bytes = postcard::to_allocvec(&s).expect("encode");
        let decoded: ReplicaStatus = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, s);
        assert_eq!(
            decoded.peer_names.len(),
            2,
            "peer names must not arrive empty"
        );
    }
}
