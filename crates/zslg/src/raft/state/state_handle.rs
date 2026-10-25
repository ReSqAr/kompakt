//! The node's live view of replicated raft state, published to every consumer
//! as a single `watch<Arc<RaftState>>`: the current value and the change
//! notification are one thing, so apply and metric-blending cannot interleave
//! and no second lock is needed to keep them in step.
//!
//! Writes follow the **fence split-of-labor**: structural validation
//! (manifest shape, `manifest_watermark <= hlc_cutoff`, membership
//! transitions) happens *before* commit at `raft_storage::evaluate_fence`
//! (a value rejection) and again pre-flight at the leader's propose site
//! (`raft_storage::validate_replicated_state`). [`apply_replicated`] only
//! guards the generation, which must never regress.
use std::collections::BTreeSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::debug;

use crate::api::ReplicatedRaftState;
use crate::ids::{NodeId, RaftGeneration};
use crate::meta::{RaftManifestState, RaftState};

/// The published Raft state: one value, one change notification.
///
/// A [`watch`] rather than a snapshot cell plus a broadcast, which is what this
/// was. Every consumer of the old broadcast ignored the payload (or fell back
/// to re-reading the cell on `Lagged`) and then read current state — they want
/// the *level*, not the edges — so the two halves were always one thing, kept in
/// step by hand under a second lock and paying a full `RaftState` clone per
/// publish. The watch is also unmissable: a consumer that was busy sees the
/// latest state on its next poll rather than a lag it has to remember to treat
/// as a wake-up.
///
/// `Arc<RaftState>` and not `RaftState`: `snapshot()` is called far more often
/// than the state changes, and the manifest inside makes a clone unpleasant.
#[derive(Debug, Clone)]
pub struct RaftStateHandle {
    tx: Arc<watch::Sender<Arc<RaftState>>>,
}

impl Default for RaftStateHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftStateHandle {
    #[must_use]
    pub(crate) fn new() -> Self {
        let (tx, _) = watch::channel(Arc::new(RaftState::empty()));
        Self { tx: Arc::new(tx) }
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<RaftState> {
        Arc::clone(&self.tx.borrow())
    }

    /// Every committed state transition, from the moment of subscription.
    ///
    /// Public, like the trait method it shadows. An inherent method wins over
    /// a trait method in call position, so leaving this narrower than
    /// [`crate::api::RaftStateView::subscribe`] would make `handle.subscribe()`
    /// a privacy error outside `crate::raft` even where the trait is in scope.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Arc<RaftState>> {
        self.tx.subscribe()
    }

    /// Replace the replicated portion of the state at `generation`.
    ///
    /// This runs on openraft's *post-commit* apply path (and again on the
    /// recovery replay), so it must not `Err` on the *content* of an
    /// already-committed entry — that would become a fatal `StorageError` and
    /// halt the state machine. Structural validation is fenced pre-commit
    /// (header); only the generation guard remains here: a backwards
    /// generation is an invariant violation and is rejected with
    /// [`crate::Error::StateRegression`] instead of panicking the host.
    pub(crate) fn apply_replicated(
        &self,
        state: ReplicatedRaftState,
        generation: RaftGeneration,
    ) -> Result<(), crate::Error> {
        // One critical section, the watch's own: the read-modify-write and the
        // publish are the same operation, so the separate write lock this used
        // to need is gone.
        let mut outcome = Ok(());
        self.tx.send_if_modified(|slot| {
            let prev = Arc::clone(slot);
            if generation < prev.generation {
                tracing::error!(
                    generation = ?prev.generation,
                    attempted_generation = ?generation,
                    "raft state generation went backwards; rejecting apply",
                );
                outcome = Err(crate::error::Error::StateRegression {
                    current: prev.generation,
                    attempted: generation,
                });
                return false;
            }
            *slot = Arc::new(RaftState {
                leader: prev.leader,
                raft_term: prev.raft_term,
                generation,
                replica: state.replica.clone(),
                manifest_state: RaftManifestState {
                    manifest: state.manifest.clone(),
                    manifest_watermark: state.manifest_watermark,
                    hlc_cutoff: state.hlc_cutoff,
                },
                // Carried over, not recomputed: `voters` is blended from
                // openraft's metrics and has nothing to do with the replicated
                // payload being applied. Dropping it here would blank the voter
                // set on every apply and refill it only on the next metrics
                // change.
                voters: prev.voters.clone(),
            });
            log_update(slot);
            true
        });
        outcome
    }

    /// Fold this node's openraft-derived facts into the published state.
    ///
    /// The early return compares `voters` too. Leaving it out would skip a
    /// voter-set change whenever the leader and term happened to be unchanged
    /// — which is the common case, since membership changes do not move the
    /// leader.
    pub(in crate::raft) fn blend_raft_metrics(
        &self,
        leader: Option<NodeId>,
        raft_term: u64,
        voters: BTreeSet<NodeId>,
    ) {
        self.tx.send_if_modified(|slot| {
            if slot.leader == leader && slot.raft_term == raft_term && slot.voters == voters {
                return false;
            }
            *slot = Arc::new(RaftState {
                leader,
                raft_term,
                voters,
                ..(**slot).clone()
            });
            log_update(slot);
            true
        });
    }

    pub(crate) fn replace(&self, new_state: RaftState) {
        self.tx.send_if_modified(|slot| {
            *slot = Arc::new(new_state);
            log_update(slot);
            true
        });
    }
}

fn log_update(new_state: &RaftState) {
    debug!(
        generation = ?new_state.generation,
        node_id = ?new_state.leader,
        raft_term = new_state.raft_term,
        replica = ?new_state.replica,
        manifest_len = new_state.manifest_state.manifest.len(),
        manifest_watermark = ?new_state.manifest_state.manifest_watermark,
        hlc_cutoff = ?new_state.manifest_state.hlc_cutoff,
        "RaftState updated"
    );
}

impl crate::api::RaftStateView for RaftStateHandle {
    fn snapshot(&self) -> Arc<RaftState> {
        self.snapshot()
    }

    fn subscribe(&self) -> watch::Receiver<Arc<RaftState>> {
        self.subscribe()
    }
}

impl Serialize for RaftStateHandle {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let snap = self.snapshot();
        let replicated = ReplicatedRaftState {
            replica: snap.replica.clone(),
            manifest: snap.manifest_state.manifest.clone(),
            manifest_watermark: snap.manifest_state.manifest_watermark,
            hlc_cutoff: snap.manifest_state.hlc_cutoff,
        };
        replicated.serialize(ser)
    }
}

impl<'de> Deserialize<'de> for RaftStateHandle {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let state = ReplicatedRaftState::deserialize(de)?;
        let s = Self::new();
        // A fresh handle is at generation ZERO; applying at ZERO cannot regress.
        s.apply_replicated(state, RaftGeneration::ZERO)
            .map_err(serde::de::Error::custom)?;
        Ok(s)
    }
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
    use std::collections::BTreeSet;

    use super::*;
    use crate::hlc::Hlc;
    use crate::ids::{LogIndex, NodeId, SnapshotId};
    use crate::meta::{ReplicaMembership, SnapshotMetadata};

    fn make_gen(n: u64) -> RaftGeneration {
        RaftGeneration(n)
    }

    #[test]
    fn apply_replica_set_publishes() {
        let s = RaftStateHandle::new();
        let mut rx = s.subscribe();
        let n1 = NodeId::new_v4();
        let n2 = NodeId::new_v4();
        s.apply_replicated(
            ReplicatedRaftState {
                replica: ReplicaMembership::single(BTreeSet::from([n1, n2])),
                manifest: Vec::new(),
                manifest_watermark: None,
                hlc_cutoff: None,
            },
            make_gen(7),
        )
        .expect("apply");
        let snap = s.snapshot();
        assert_eq!(snap.replica.current().len(), 2);
        assert_eq!(snap.generation, make_gen(7));
        assert!(
            rx.has_changed().expect("sender alive"),
            "the apply must publish"
        );
        assert_eq!(rx.borrow_and_update().replica.current().len(), 2);
    }

    #[test]
    fn apply_joint_then_single_clears_next() {
        let s = RaftStateHandle::new();
        let n1 = NodeId::new_v4();
        let n2 = NodeId::new_v4();
        s.apply_replicated(
            ReplicatedRaftState {
                replica: ReplicaMembership::joint(BTreeSet::from([n1]), BTreeSet::from([n2])),
                manifest: Vec::new(),
                manifest_watermark: None,
                hlc_cutoff: None,
            },
            make_gen(1),
        )
        .expect("apply");
        assert!(s.snapshot().replica.is_joint());
        s.apply_replicated(
            ReplicatedRaftState {
                replica: ReplicaMembership::single(BTreeSet::from([n2])),
                manifest: Vec::new(),
                manifest_watermark: None,
                hlc_cutoff: None,
            },
            make_gen(2),
        )
        .expect("apply");
        let snap = s.snapshot();
        assert!(!snap.replica.is_joint());
        assert_eq!(snap.replica.current().len(), 1);
    }

    #[test]
    fn apply_manifest_publishes_full_list() {
        let s = RaftStateHandle::new();
        let m1 = SnapshotMetadata {
            start_hlc: Hlc(0),
            end_hlc: Hlc(1234),
            start_index: LogIndex(0),
            end_index: LogIndex(9),
            snapshot_id: SnapshotId::from_u128(1234),
        };
        s.apply_replicated(
            ReplicatedRaftState {
                replica: ReplicaMembership::single(BTreeSet::new()),
                manifest: vec![m1.clone()],
                manifest_watermark: Some(Hlc(1234)),
                hlc_cutoff: None,
            },
            make_gen(3),
        )
        .expect("apply");
        let snap = s.snapshot();
        assert_eq!(snap.manifest_state.manifest, vec![m1]);
        assert_eq!(snap.manifest_state.manifest_watermark, Some(Hlc(1234)));
        assert_eq!(
            snap.manifest_state.manifest.last().map(|m| m.end_index),
            Some(LogIndex(9))
        );
    }

    #[test]
    fn apply_does_not_touch_leader_or_raft_term() {
        let s = RaftStateHandle::new();
        let initial = (*s.snapshot()).clone();
        s.apply_replicated(
            ReplicatedRaftState {
                replica: ReplicaMembership::single(BTreeSet::from([NodeId::new_v4()])),
                manifest: Vec::new(),
                manifest_watermark: None,
                hlc_cutoff: None,
            },
            make_gen(1),
        )
        .expect("apply");
        let after = s.snapshot();
        assert_eq!(after.leader, initial.leader);
        assert_eq!(after.raft_term, initial.raft_term);
    }

    #[test]
    fn blend_raft_metrics_updates_leader_and_term() {
        let s = RaftStateHandle::new();
        let leader = NodeId::new_v4();
        let voters = BTreeSet::from([leader]);
        s.blend_raft_metrics(Some(leader), 5, voters.clone());
        let snap = s.snapshot();
        assert_eq!(snap.leader, Some(leader));
        assert_eq!(snap.raft_term, 5);
        assert_eq!(snap.voters, voters, "the voter set is blended in too");
    }

    #[test]
    fn blend_raft_metrics_noop_when_unchanged_does_not_publish() {
        let s = RaftStateHandle::new();
        let leader = NodeId::new_v4();
        let voters = BTreeSet::from([leader]);
        s.blend_raft_metrics(Some(leader), 1, voters.clone());
        let rx = s.subscribe();
        // Re-applying identical leader/term/voters must not publish a new event.
        s.blend_raft_metrics(Some(leader), 1, voters);
        assert!(!rx.has_changed().expect("sender alive"));
    }

    #[test]
    fn blend_preserves_replica_and_manifest() {
        let s = RaftStateHandle::new();
        let n1 = NodeId::new_v4();
        s.apply_replicated(
            ReplicatedRaftState {
                replica: ReplicaMembership::single(BTreeSet::from([n1])),
                manifest: Vec::new(),
                manifest_watermark: Some(Hlc(42)),
                hlc_cutoff: None,
            },
            make_gen(1),
        )
        .expect("apply");
        s.blend_raft_metrics(Some(n1), 9, BTreeSet::from([n1]));
        let snap = s.snapshot();
        assert_eq!(snap.replica.current().clone(), BTreeSet::from([n1]));
        assert_eq!(snap.manifest_state.manifest_watermark, Some(Hlc(42)));
        assert_eq!(snap.generation, make_gen(1));
    }

    /// A replicated apply must not clobber the blended voter set.
    ///
    /// `voters` comes from openraft's metrics, not from the replicated
    /// payload, so an apply carries no opinion about it. Rebuilding the state
    /// without carrying it over would blank the voter set on every apply and
    /// leave it empty until the next metrics change — and applies are far more
    /// frequent than membership changes, so the empty window would be the
    /// common case.
    #[test]
    fn apply_replicated_preserves_the_blended_voter_set() {
        let s = RaftStateHandle::new();
        let (n1, n2) = (NodeId::new_v4(), NodeId::new_v4());
        let voters = BTreeSet::from([n1, n2]);
        s.blend_raft_metrics(Some(n1), 3, voters.clone());

        s.apply_replicated(
            ReplicatedRaftState {
                replica: ReplicaMembership::single(BTreeSet::from([n1])),
                manifest: Vec::new(),
                manifest_watermark: Some(Hlc(7)),
                hlc_cutoff: None,
            },
            make_gen(1),
        )
        .expect("apply");

        let snap = s.snapshot();
        assert_eq!(snap.voters, voters, "voters survived a replicated apply");
        assert_eq!(snap.leader, Some(n1), "so did the other blended fields");
        assert_eq!(snap.raft_term, 3);
    }

    /// A voter-set change alone must publish, even when leader and term are
    /// unchanged — which is the normal case, since a membership change does
    /// not move the leader.
    #[test]
    fn blend_raft_metrics_publishes_when_only_voters_change() {
        let s = RaftStateHandle::new();
        let (n1, n2) = (NodeId::new_v4(), NodeId::new_v4());
        s.blend_raft_metrics(Some(n1), 1, BTreeSet::from([n1]));
        let mut rx = s.subscribe();
        s.blend_raft_metrics(Some(n1), 1, BTreeSet::from([n1, n2]));
        assert!(
            rx.has_changed().expect("sender alive"),
            "a voter-set change must publish"
        );
        assert_eq!(rx.borrow_and_update().voters, BTreeSet::from([n1, n2]));
    }

    /// F-15: a backwards generation is rejected with a typed error — it must
    /// never panic the host process — and the state stays untouched.
    #[test]
    fn apply_generation_backwards_is_rejected_without_panic() {
        let s = RaftStateHandle::new();
        let empty = || ReplicatedRaftState {
            replica: ReplicaMembership::single(BTreeSet::new()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        };
        s.apply_replicated(empty(), make_gen(5)).expect("forward");
        let err = s
            .apply_replicated(empty(), make_gen(4))
            .expect_err("backwards must be rejected");
        assert!(matches!(
            err,
            crate::Error::StateRegression {
                current: RaftGeneration(5),
                attempted: RaftGeneration(4),
            }
        ));
        assert_eq!(s.snapshot().generation, make_gen(5), "state unchanged");
    }

    fn replicated_with(
        manifest: Vec<SnapshotMetadata>,
        watermark: Option<Hlc>,
        cutoff: Option<Hlc>,
    ) -> ReplicatedRaftState {
        ReplicatedRaftState {
            replica: ReplicaMembership::single(BTreeSet::new()),
            manifest,
            manifest_watermark: watermark,
            hlc_cutoff: cutoff,
        }
    }

    /// `apply_replicated` runs on the post-commit apply path, so it deliberately
    /// does NOT fence on manifest shape, watermark, or cutoff — an `Err` there
    /// would be a fatal `StorageError` on an already-committed entry. Structural
    /// validation is fenced pre-commit (`raft_storage::validate_replicated_state`,
    /// unit-tested there) and also runs pre-flight in `replicate_state`. This test
    /// pins that apply itself accepts a
    /// cutoff regression (the documented new-leader case, CLAUDE.md §12).
    #[test]
    fn apply_allows_cutoff_regression() {
        let s = RaftStateHandle::new();
        s.apply_replicated(
            replicated_with(Vec::new(), Some(Hlc(50)), Some(Hlc(100))),
            make_gen(1),
        )
        .expect("forward");
        s.apply_replicated(
            replicated_with(Vec::new(), Some(Hlc(50)), Some(Hlc(60))),
            make_gen(2),
        )
        .expect("cutoff regression is legitimate (new leader's first SetCutoff)");
        assert_eq!(s.snapshot().manifest_state.hlc_cutoff, Some(Hlc(60)));
    }

    #[test]
    fn replace_sets_full_state() {
        let s = RaftStateHandle::new();
        let mut new_state = RaftState::empty();
        new_state.generation = make_gen(11);
        new_state.raft_term = 3;
        s.replace(new_state);
        let snap = s.snapshot();
        assert_eq!(snap.generation, make_gen(11));
        assert_eq!(snap.raft_term, 3);
    }

    #[test]
    fn serde_roundtrip_preserves_replicated_fields() {
        let s = RaftStateHandle::new();
        let n1 = NodeId::new_v4();
        s.apply_replicated(
            ReplicatedRaftState {
                replica: ReplicaMembership::single(BTreeSet::from([n1])),
                manifest: Vec::new(),
                manifest_watermark: Some(Hlc(7)),
                hlc_cutoff: Some(Hlc(3)),
            },
            make_gen(2),
        )
        .expect("apply");
        let bytes = postcard::to_allocvec(&s).expect("encode");
        let restored: RaftStateHandle = postcard::from_bytes(&bytes).expect("decode");
        let snap = restored.snapshot();
        assert_eq!(snap.replica.current().clone(), BTreeSet::from([n1]));
        assert_eq!(snap.manifest_state.manifest_watermark, Some(Hlc(7)));
        assert_eq!(snap.manifest_state.hlc_cutoff, Some(Hlc(3)));
    }
}
