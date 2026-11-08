//! Tests for `handle`.

use std::collections::BTreeSet;

use super::*;
use crate::meta::ReplicaMembership;
use zslg_log_types::LogIndex;
use zslg_node_types::NodeId;
use zslg_snapshot_types::SnapshotId;
use zslg_snapshot_types::SnapshotMetadata;
use zslg_time_types::Hlc;

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
        ConsensusError::StateRegression {
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
