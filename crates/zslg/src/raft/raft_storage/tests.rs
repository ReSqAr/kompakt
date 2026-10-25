use super::*;
use crate::meta::SnapshotMetadata;
use crate::raft::RaftStateError;

/// Test-only bridge: builds a `Raft` instance backed by RocksDB storage under
/// `dir`, generic over the network factory so the in-crate test harness
/// (`crate::tests::common`) can supply its in-memory router — the one entry
/// point the broader test suite uses to reach raft storage.
#[cfg(test)]
pub(crate) async fn build_raft_for_test<NF>(
    node_id: NodeId,
    config: Arc<openraft::Config>,
    network: NF,
    dir: &std::path::Path,
    raft_state: RaftStateHandle,
) -> Result<crate::raft::Raft, Error>
where
    NF: openraft::RaftNetworkFactory<TypeConfig>,
{
    use crate::error::ToRaft as _;
    let (log_store, sm) = RocksRaftStorage::open(raft_state, open_test_storage(dir)?)
        .await?
        .into_adaptor();
    crate::raft::Raft::new(node_id, config, network, log_store, sm)
        .await
        .to_raft("Raft::new (test)")
}

/// Open a [`Storage`] rooted at `dir` for a raft store on its own — the test
/// mirror of the node's one open at boot, whose column-family list is a
/// superset of this store's two.
#[cfg(test)]
pub(crate) fn open_test_storage(dir: &std::path::Path) -> Result<Arc<Storage>, Error> {
    Storage::open_for_test(dir)
}

/// Test-only: overwrite the persisted membership + vote with a FOREIGN node id
/// (simulates on-disk raft state written by a different node — the hard-crash
/// rejoin scenario). Writes straight into the raft-meta family.
#[cfg(test)]
pub(crate) async fn seed_foreign_membership_for_test(
    storage_root: std::path::PathBuf,
) -> Result<(), Error> {
    use openraft::Membership;
    let other = NodeId::new_v4();
    let voters: std::collections::BTreeSet<NodeId> = [other].into_iter().collect();
    let membership: Membership<NodeId, NodeInfo> =
        Membership::new(vec![voters.clone()], Some(voters));
    let stored = StoredMembership::new(None::<LogId<NodeId>>, membership);
    let vote = Vote::<NodeId>::new(1, other);
    let storage = open_test_storage(&storage_root)?;
    let cf = storage.cf(Subsystem::RaftMeta)?;
    let mut batch = WriteBatch::default();
    put_meta(&mut batch, &cf, RaftMetaSlot::LastMembership, &stored)?;
    put_meta(&mut batch, &cf, RaftMetaSlot::Vote, &vote)?;
    storage.write(batch)?;
    storage.fence()
}

use crate::hlc::Hlc;
use crate::ids::{LogIndex, SnapshotId};
use crate::meta::Exit;
use crate::tests::nid;
use openraft::CommittedLeaderId;

#[tokio::test]
async fn vote_round_trips_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("raft.db");

    let n1 = nid(1);
    let vote = Vote::new(7, n1);
    {
        let mut s = storage_at(&path, RaftStateHandle::new()).await;
        s.save_vote(&vote).await.expect("save");
    }
    let mut s = storage_at(&path, RaftStateHandle::new()).await;
    let got = s.read_vote().await.expect("read");
    assert_eq!(got, Some(vote));
}

#[tokio::test]
async fn append_and_purge_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("raft.db");
    let mut s = storage_at(&path, RaftStateHandle::new()).await;

    let n1 = nid(1);
    let leader = CommittedLeaderId::new(1, n1);
    let entries: Vec<Entry<TypeConfig>> = (1u64..=5)
        .map(|i| Entry {
            log_id: LogId::new(leader, i),
            payload: EntryPayload::Blank,
        })
        .collect();
    s.append_to_log(entries).await.expect("append");
    let state = s.get_log_state().await.expect("state");
    assert_eq!(state.last_log_id.expect("last").index, 5);

    s.purge_logs_upto(LogId::new(leader, 3))
        .await
        .expect("purge");
    let state = s.get_log_state().await.expect("state2");
    assert_eq!(state.last_purged_log_id.expect("purged").index, 3);
    let kept: Vec<Entry<TypeConfig>> = s.try_get_log_entries(1..=10).await.expect("range");
    assert_eq!(kept.len(), 2);
    assert_eq!(kept[0].log_id.index, 4);
    assert_eq!(kept[1].log_id.index, 5);

    // The physical `drop_purged_prefix` in `purge_logs_upto` must unlink only
    // the purged prefix (<= 3), never the live frontier. Append past the
    // purge point and confirm all surviving + new entries still read back.
    let more: Vec<Entry<TypeConfig>> = (6u64..=7)
        .map(|i| Entry {
            log_id: LogId::new(leader, i),
            payload: EntryPayload::Blank,
        })
        .collect();
    s.append_to_log(more).await.expect("append after purge");
    let after: Vec<Entry<TypeConfig>> = s.try_get_log_entries(1..=10).await.expect("range2");
    let indices: Vec<u64> = after.iter().map(|e| e.log_id.index).collect();
    assert_eq!(indices, vec![4, 5, 6, 7]);
}

/// Reproduces the header's openraft abort without openraft in the loop: a
/// range entirely below the purge floor, which the default delegation hands
/// to `logs.first().unwrap()`. Pins both halves of the fix —
/// `try_get_log_entries` keeps answering empty, `limited_get_log_entries`
/// refuses — and that a straddling range still succeeds on both.
#[tokio::test]
async fn purged_range_read_returns_empty_which_openraft_unwraps() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("raft.db");
    let mut s = storage_at(&path, RaftStateHandle::new()).await;

    let leader = CommittedLeaderId::new(1, nid(1));
    let entries: Vec<Entry<TypeConfig>> = (1u64..=20)
        .map(|i| Entry {
            log_id: LogId::new(leader, i),
            payload: EntryPayload::Blank,
        })
        .collect();
    s.append_to_log(entries).await.expect("append");

    // The leader snapshots and purges through index 15 while a lagging
    // follower is still owed [5, 10). With max_in_snapshot_log_to_keep = 10
    // and a 20k writes/s burst, a follower falls this far behind in well
    // under a millisecond.
    s.purge_logs_upto(LogId::new(leader, 15))
        .await
        .expect("purge");

    // `try_get_log_entries` still answers with silence — legal, documented,
    // and what its non-replication callers expect.
    let owed: Vec<Entry<TypeConfig>> = s
        .try_get_log_entries(5u64..10u64)
        .await
        .expect("a purged range is not an error for this method");
    assert!(
        owed.is_empty(),
        "the whole range is below the purge floor, so nothing is served",
    );
    // This is the value openraft's DEFAULT limited_get_log_entries would
    // hand to `logs.first().map(..).unwrap()` — the abort, reproduced.
    let openraft_would_unwrap: Option<LogId<NodeId>> = owed.first().map(|e| e.log_id);
    assert!(
        openraft_would_unwrap.is_none(),
        "the default delegation yields None here, and openraft unwraps it",
    );

    // Our override refuses instead, so openraft gets an error it can act on
    // (fall back to snapshot replication) rather than a panic.
    let err = s
        .limited_get_log_entries(5u64, 10u64)
        .await
        .expect_err("a wholly-purged range must be an error, never an empty vec");
    let msg = format!("{err}");
    assert!(
        msg.contains("entirely purged"),
        "the error must say why, so the operator is not left guessing: {msg}",
    );

    // A range that straddles the purge floor must still succeed on BOTH
    // methods: the survivors answer, and openraft tolerates a short read
    // (`logs.len() <= end - start`). Failing this case would break normal
    // catch-up replication, which is the thing the fix must not do.
    let straddling: Vec<Entry<TypeConfig>> =
        s.try_get_log_entries(10u64..20u64).await.expect("straddle");
    let indices: Vec<u64> = straddling.iter().map(|e| e.log_id.index).collect();
    assert_eq!(
        indices,
        vec![16, 17, 18, 19],
        "only the live suffix survives; this case never panics",
    );
    let limited: Vec<Entry<TypeConfig>> = s
        .limited_get_log_entries(10u64, 20u64)
        .await
        .expect("a partially-purged range is a short read, not a failure");
    let limited_indices: Vec<u64> = limited.iter().map(|e| e.log_id.index).collect();
    assert_eq!(limited_indices, vec![16, 17, 18, 19]);
}

/// Purge is durable. The in-memory clamp it replaced started empty at every
/// open, so an un-compacted purged prefix became readable again — two floors
/// on one column family, agreeing only after the first `purge_logs_upto`.
#[tokio::test]
async fn invariant_purged_raft_prefix_stays_gone_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("raft.db");

    let leader = CommittedLeaderId::new(1, nid(1));
    {
        let mut s = storage_at(&path, RaftStateHandle::new()).await;
        let entries: Vec<Entry<TypeConfig>> = (0u64..10)
            .map(|i| Entry {
                log_id: LogId::new(leader, i),
                payload: EntryPayload::Blank,
            })
            .collect();
        s.append_to_log(entries).await.expect("append");
        s.purge_logs_upto(LogId::new(leader, 4))
            .await
            .expect("purge succeeds");
    }

    let mut s = storage_at(&path, RaftStateHandle::new()).await;
    let entries: Vec<Entry<TypeConfig>> = s.try_get_log_entries(0..10).await.expect("range");
    assert_eq!(
        entries.first().map(|e| e.log_id.index),
        Some(5),
        "indices 0..=4 were purged and must not come back after a reopen"
    );
}

#[tokio::test]
async fn has_durable_state_detects_vote_or_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("raft.db");
    let mut s = storage_at(&path, RaftStateHandle::new()).await;
    assert!(!s.has_durable_state().await.expect("hds"));

    s.save_vote(&Vote::new(1, nid(1))).await.expect("save");
    assert!(s.has_durable_state().await.expect("hds2"));
}

/// Open (or reopen) raft storage rooted at `path` with the given state
/// handle.
async fn storage_at(path: &std::path::Path, raft_state: RaftStateHandle) -> RocksRaftStorage {
    RocksRaftStorage::open(raft_state, open_test_storage(path).expect("open storage"))
        .await
        .expect("open raft storage")
}

fn normal_entry(
    idx: u64,
    leader: CommittedLeaderId<NodeId>,
    replicas: &[NodeId],
) -> Entry<TypeConfig> {
    fenced_entry(idx, RaftGeneration::ZERO, leader, replicas)
}

fn fenced_entry(
    idx: u64,
    expected_generation: RaftGeneration,
    leader: CommittedLeaderId<NodeId>,
    replicas: &[NodeId],
) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(leader, idx),
        payload: EntryPayload::Normal(crate::raft::FencedStateWrite {
            expected_generation,
            state: crate::api::ReplicatedRaftState {
                replica: ReplicaMembership::single(replicas.iter().copied().collect()),
                manifest: Vec::new(),
                manifest_watermark: None,
                // Per-entry discriminator: which entries actually applied
                // has to be readable from the end state, and the replica
                // set can no longer serve that role — `Single -> Single`
                // must be an identity, so every entry in a run carries the
                // same set.
                hlc_cutoff: Some(crate::hlc::Hlc(idx)),
            },
        }),
    }
}

fn snapshot_meta(start: u64, end: u64) -> SnapshotMetadata {
    SnapshotMetadata {
        start_hlc: Hlc(start),
        end_hlc: Hlc(end),
        start_index: LogIndex(start),
        end_index: LogIndex(end),
        snapshot_id: SnapshotId::from_u128(u128::from(end)),
    }
}

/// A fenced entry whose manifest is malformed (starts at index 5, not 0),
/// used to prove the fence rejects it as a *value* — never a fatal `Err`.
fn malformed_manifest_entry(
    idx: u64,
    expected_generation: RaftGeneration,
    leader: CommittedLeaderId<NodeId>,
) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(leader, idx),
        payload: EntryPayload::Normal(crate::raft::FencedStateWrite {
            expected_generation,
            state: crate::api::ReplicatedRaftState {
                replica: ReplicaMembership::single([nid(1)].into_iter().collect()),
                manifest: vec![snapshot_meta(5, 9)],
                manifest_watermark: Some(Hlc(9)),
                hlc_cutoff: None,
            },
        }),
    }
}

#[test]
fn validate_replicated_state_flags_manifest_malformations() {
    let check = |m: Vec<SnapshotMetadata>| validate_replicated_state(&replicated(m, None, None));
    assert!(check(vec![]).is_ok(), "empty is well-formed");
    assert!(
        check(vec![snapshot_meta(0, 4), snapshot_meta(5, 9)]).is_ok(),
        "contiguous-from-0 is well-formed"
    );
    // Not starting at 0.
    assert!(check(vec![snapshot_meta(5, 9)]).is_err());
    // Gap between entries (0..=4 then 6..=9).
    assert!(check(vec![snapshot_meta(0, 4), snapshot_meta(6, 9)]).is_err());
    // Overlap / unsorted (second entry restarts at 0).
    assert!(check(vec![snapshot_meta(0, 4), snapshot_meta(0, 9)]).is_err());
    // end < start within one entry.
    let mut inverted = snapshot_meta(0, 9);
    inverted.start_index = LogIndex(3);
    inverted.end_index = LogIndex(0);
    assert!(check(vec![inverted]).is_err());
}

#[test]
fn evaluate_fence_rejects_malformed_manifest_as_value() {
    let leader = CommittedLeaderId::new(1, nid(1));
    let entry = malformed_manifest_entry(1, RaftGeneration::ZERO, leader);
    let EntryPayload::Normal(write) = &entry.payload else {
        panic!("normal entry");
    };
    // Generation matches (ZERO == ZERO); only the manifest shape is wrong.
    match evaluate_fence(
        RaftGeneration::ZERO,
        &ReplicaMembership::default(),
        None,
        write,
        RaftLogIndex::from(1),
    ) {
        RaftResponse::RejectedInvalidState { reason } => {
            assert!(reason.to_string().contains("index"), "{reason}");
        }
        other => panic!("expected invalid-state rejection, got {other:?}"),
    }
}

fn replicated(
    manifest: Vec<SnapshotMetadata>,
    watermark: Option<Hlc>,
    cutoff: Option<Hlc>,
) -> crate::api::ReplicatedRaftState {
    crate::api::ReplicatedRaftState {
        replica: ReplicaMembership::single([nid(1)].into_iter().collect()),
        manifest,
        manifest_watermark: watermark,
        hlc_cutoff: cutoff,
    }
}

// ── membership transitions (validate_transition) ─────────────────────
//
// The doc invariant under test: a joint config may only collapse onto its
// incoming set once the stable prefix covers the drain bound established in
// step 2b (designdoc.md §"Membership Change"; CLAUDE.md §8).

fn set(seeds: &[u8]) -> std::collections::BTreeSet<NodeId> {
    seeds.iter().map(|&b| nid(b)).collect()
}

fn with_membership(
    replica: ReplicaMembership,
    watermark: Option<Hlc>,
) -> crate::api::ReplicatedRaftState {
    crate::api::ReplicatedRaftState {
        replica,
        manifest: Vec::new(),
        manifest_watermark: watermark,
        hlc_cutoff: watermark,
    }
}

#[test]
fn invariant_joint_exit_requires_drained_old_set() {
    let old = set(&[1, 2, 3]);
    let next = set(&[3, 4, 5]);
    let armed = ReplicaMembership::JointDraining {
        old: old.clone(),
        next: next.clone(),
        exit: Exit::WatermarkAbove(Hlc(500)),
    };

    // Watermark below the bound: the outgoing set still holds writes no
    // snapshot covers. Dropping it here is the data-loss bug.
    assert!(matches!(
        validate_transition(
            &armed,
            &with_membership(ReplicaMembership::single(next.clone()), Some(Hlc(499)))
        ),
        Err(RaftStateError::JointExitTooEarly {
            exit: Exit::WatermarkAbove(Hlc(500)),
            watermark: Some(Hlc(499))
        })
    ));
    // No snapshot at all is likewise not coverage.
    assert!(matches!(
        validate_transition(
            &armed,
            &with_membership(ReplicaMembership::single(next.clone()), None)
        ),
        Err(RaftStateError::JointExitTooEarly { .. })
    ));
    // Exactly at the bound is coverage — the bound is inclusive.
    assert!(
        validate_transition(
            &armed,
            &with_membership(ReplicaMembership::single(next.clone()), Some(Hlc(500)))
        )
        .is_ok()
    );
    assert!(
        validate_transition(
            &armed,
            &with_membership(ReplicaMembership::single(next.clone()), Some(Hlc(9000)))
        )
        .is_ok()
    );
    // Aborting back to the outgoing set drops nobody, so it needs no bound.
    assert!(
        validate_transition(
            &armed,
            &with_membership(ReplicaMembership::single(old), None)
        )
        .is_ok()
    );
}

#[test]
fn joint_exit_before_arming_is_refused() {
    // `Joint` has no bound at all: step 2's generation quorum has not been
    // established, so nothing is known about what the outgoing set holds.
    let joint = ReplicaMembership::joint(set(&[1, 2, 3]), set(&[3, 4, 5]));
    assert!(matches!(
        validate_transition(
            &joint,
            &with_membership(ReplicaMembership::single(set(&[3, 4, 5])), Some(Hlc(9000)))
        ),
        Err(RaftStateError::JointExitNotArmed)
    ));
}

#[test]
fn joint_exit_must_land_on_one_of_the_two_sets() {
    let armed = ReplicaMembership::JointDraining {
        old: set(&[1, 2, 3]),
        next: set(&[3, 4, 5]),
        exit: Exit::WatermarkAbove(Hlc(1)),
    };
    assert!(matches!(
        validate_transition(
            &armed,
            &with_membership(ReplicaMembership::single(set(&[7, 8, 9])), Some(Hlc(9000)))
        ),
        Err(RaftStateError::JointExitWrongSet { .. })
    ));
}

#[test]
fn joint_retarget_must_keep_the_outgoing_set() {
    let joint = ReplicaMembership::joint(set(&[1, 2, 3]), set(&[3, 4, 5]));
    // Retarget to a new incoming set: fine.
    assert!(
        validate_transition(
            &joint,
            &with_membership(
                ReplicaMembership::joint(set(&[1, 2, 3]), set(&[3, 6, 7])),
                None
            )
        )
        .is_ok()
    );
    // Swapping the outgoing set out from under the episode: not fine — the
    // drain bound is sampled from it and every quorum is a quorum of it.
    assert!(matches!(
        validate_transition(
            &joint,
            &with_membership(
                ReplicaMembership::joint(set(&[4, 5, 6]), set(&[3, 6, 7])),
                None
            )
        ),
        Err(RaftStateError::JointOldSetChanged { .. })
    ));
}

#[test]
fn joint_exit_bound_may_not_regress() {
    let armed = ReplicaMembership::JointDraining {
        old: set(&[1, 2, 3]),
        next: set(&[3, 4, 5]),
        exit: Exit::WatermarkAbove(Hlc(500)),
    };
    let rearm = |hlc: u64| {
        validate_transition(
            &armed,
            &with_membership(
                ReplicaMembership::JointDraining {
                    old: set(&[1, 2, 3]),
                    next: set(&[3, 4, 5]),
                    exit: Exit::WatermarkAbove(Hlc(hlc)),
                },
                None,
            ),
        )
    };
    assert!(rearm(500).is_ok(), "re-arming at the same bound is a no-op");
    assert!(rearm(600).is_ok(), "a bound may rise");
    assert!(matches!(
        rearm(499),
        Err(RaftStateError::ExitHlcRegressed {
            previous: crate::meta::Exit::WatermarkAbove(Hlc(500)),
            proposed: crate::meta::Exit::WatermarkAbove(Hlc(499))
        })
    ));
}

#[test]
fn joint_arming_may_not_change_the_incoming_set() {
    let joint = ReplicaMembership::joint(set(&[1, 2, 3]), set(&[3, 4, 5]));
    assert!(matches!(
        validate_transition(
            &joint,
            &with_membership(
                ReplicaMembership::JointDraining {
                    old: set(&[1, 2, 3]),
                    next: set(&[6, 7, 8]),
                    exit: Exit::WatermarkAbove(Hlc(1)),
                },
                None
            )
        ),
        Err(RaftStateError::JointArmingChangedNextSet { .. })
    ));
}

/// Every replica-set change must go `Single -> Joint -> JointDraining ->
/// Single`. A direct `Single -> Single` swap would drop the outgoing set
/// with no drain bound ever armed — the exact loss this fence exists to
/// prevent — and a direct *addition* would count a node that holds no data
/// toward quorum. Only a pass-through and the genesis seed are legal.
#[test]
fn a_single_config_may_not_change_its_replica_set_directly() {
    let single = ReplicaMembership::single(set(&[1, 2, 3]));

    // Pass-through: every snapshot/compaction/cutoff write re-proposes the
    // membership unchanged, so this arm is hot and must stay allowed.
    assert!(
        validate_transition(
            &single,
            &with_membership(ReplicaMembership::single(set(&[1, 2, 3])), None)
        )
        .is_ok()
    );
    // Dropping a node: C may hold the only copy of acknowledged writes.
    assert!(matches!(
        validate_transition(
            &single,
            &with_membership(ReplicaMembership::single(set(&[1, 2])), None)
        ),
        Err(RaftStateError::SingleSetChangedWithoutJoint { .. })
    ));
    // Adding one is refused too: it would count toward quorum having
    // caught up on nothing.
    assert!(matches!(
        validate_transition(
            &single,
            &with_membership(ReplicaMembership::single(set(&[1, 2, 3, 4])), None)
        ),
        Err(RaftStateError::SingleSetChangedWithoutJoint { .. })
    ));
    // Genesis: the empty set holds nothing, so seeding the first real set
    // out of it drains nothing (`bootstrap_single_node_cluster`).
    assert!(
        validate_transition(
            &ReplicaMembership::default(),
            &with_membership(ReplicaMembership::single(set(&[1])), None)
        )
        .is_ok()
    );
}

#[test]
fn joint_entry_outgoing_set_must_be_the_set_in_force() {
    let single = ReplicaMembership::single(set(&[1, 2, 3]));
    assert!(
        validate_transition(
            &single,
            &with_membership(
                ReplicaMembership::joint(set(&[1, 2, 3]), set(&[3, 4, 5])),
                None
            )
        )
        .is_ok()
    );
    assert!(matches!(
        validate_transition(
            &single,
            &with_membership(ReplicaMembership::joint(set(&[9]), set(&[3, 4, 5])), None)
        ),
        Err(RaftStateError::JointEntryWrongOldSet { .. })
    ));
    // A single config cannot arm a bound for a joint config it is not in.
    assert!(
        validate_transition(
            &single,
            &with_membership(
                ReplicaMembership::JointDraining {
                    old: set(&[1, 2, 3]),
                    next: set(&[3, 4, 5]),
                    exit: Exit::WatermarkAbove(Hlc(1)),
                },
                None
            )
        )
        .is_err()
    );
}

#[test]
fn evaluate_fence_rejects_early_joint_exit_as_value() {
    // A committed-but-illegal transition must be a *value* rejection, never
    // an `Err` — an `Err` here halts the state machine on an already
    // committed entry (see `evaluate_fence`).
    let write = crate::raft::FencedStateWrite {
        expected_generation: RaftGeneration::ZERO,
        state: with_membership(ReplicaMembership::single(set(&[3, 4, 5])), Some(Hlc(10))),
    };
    let previous = ReplicaMembership::JointDraining {
        old: set(&[1, 2, 3]),
        next: set(&[3, 4, 5]),
        exit: Exit::WatermarkAbove(Hlc(500)),
    };
    match evaluate_fence(
        RaftGeneration::ZERO,
        &previous,
        None,
        &write,
        RaftLogIndex::from(1),
    ) {
        RaftResponse::RejectedInvalidState { reason } => assert!(
            matches!(reason, RaftStateError::JointExitTooEarly { .. }),
            "{reason}"
        ),
        other => panic!("expected invalid-state rejection, got {other:?}"),
    }
}

#[test]
fn drain_satisfied_boundaries() {
    use crate::meta::{Exit, drain_satisfied};
    // "Nothing to drain" is satisfied even before any snapshot.
    assert!(drain_satisfied(None, Exit::AnyTime));
    assert!(drain_satisfied(Some(Hlc(1)), Exit::AnyTime));
    // A real bound is not.
    assert!(!drain_satisfied(None, Exit::WatermarkAbove(Hlc(1))));
    assert!(!drain_satisfied(
        Some(Hlc(9)),
        Exit::WatermarkAbove(Hlc(10))
    ));
    assert!(drain_satisfied(
        Some(Hlc(10)),
        Exit::WatermarkAbove(Hlc(10))
    ));
}

#[test]
fn validate_rejects_watermark_above_cutoff() {
    // watermark > cutoff: rejected.
    assert!(matches!(
        validate_replicated_state(&replicated(vec![], Some(Hlc(100)), Some(Hlc(50)))),
        Err(RaftStateError::WatermarkAboveCutoff {
            watermark: Hlc(100),
            cutoff: Hlc(50)
        })
    ));
    // watermark == cutoff and watermark < cutoff: allowed.
    assert!(validate_replicated_state(&replicated(vec![], Some(Hlc(50)), Some(Hlc(50)))).is_ok());
    assert!(validate_replicated_state(&replicated(vec![], Some(Hlc(50)), Some(Hlc(100)))).is_ok());
    // Either side absent: allowed (no committed coverage to violate).
    assert!(validate_replicated_state(&replicated(vec![], None, Some(Hlc(50)))).is_ok());
    assert!(validate_replicated_state(&replicated(vec![], Some(Hlc(50)), None)).is_ok());
}

/// CLAUDE.md §8: a replica's HLC cutoff may never go backwards. The leader's
/// integrator already declines a non-advancing `AdvanceCutoff` intent
/// (`integrator.rs`, `Applied::NoOp`), but that is one writer's discipline —
/// the same argument `JointExitTooEarly` makes for checking at the write point
/// applies here: a leader bug, a lost leadership mid-change, or a stale
/// in-flight proposal must not be able to commit a regression.
#[test]
fn invariant_committed_cutoff_may_not_regress() {
    let check = |previous: Option<Hlc>, proposed: Option<Hlc>| {
        validate_cutoff_advance(previous, &replicated(vec![], None, proposed))
    };

    assert!(matches!(
        check(Some(Hlc(200)), Some(Hlc(100))),
        Err(RaftStateError::CutoffRegressed {
            previous: Hlc(200),
            proposed: Hlc(100)
        })
    ));
    assert!(
        check(Some(Hlc(200)), Some(Hlc(200))).is_ok(),
        "re-commit of the same cutoff"
    );
    assert!(check(Some(Hlc(200)), Some(Hlc(300))).is_ok(), "advance");
    assert!(check(None, Some(Hlc(100))).is_ok(), "first cutoff");
    // Deliberately NOT a rule: nothing sets a committed cutoff back to `None`,
    // and rejecting it here would turn a state reseed into a value rejection.
    assert!(check(Some(Hlc(200)), None).is_ok());
}

/// Like every other structural violation, a regression is a *value* rejection
/// on the apply path, never a fatal `Err` (see the module header).
#[test]
fn evaluate_fence_rejects_a_regressed_cutoff_as_value() {
    let write = crate::raft::FencedStateWrite {
        expected_generation: RaftGeneration::ZERO,
        state: replicated(vec![], None, Some(Hlc(50))),
    };
    match evaluate_fence(
        RaftGeneration::ZERO,
        &ReplicaMembership::default(),
        Some(Hlc(100)),
        &write,
        RaftLogIndex::from(1),
    ) {
        RaftResponse::RejectedInvalidState { reason } => assert!(
            matches!(reason, RaftStateError::CutoffRegressed { .. }),
            "{reason}"
        ),
        other => panic!("expected invalid-state rejection, got {other:?}"),
    }
}

#[test]
fn evaluate_fence_rejects_watermark_above_cutoff_as_value() {
    let write = crate::raft::FencedStateWrite {
        expected_generation: RaftGeneration::ZERO,
        state: replicated(vec![], Some(Hlc(100)), Some(Hlc(50))),
    };
    // Generation matches; only the watermark/cutoff ordering is wrong — so it
    // is a *value* rejection, never a fatal `Err` on the apply path.
    match evaluate_fence(
        RaftGeneration::ZERO,
        &ReplicaMembership::default(),
        None,
        &write,
        RaftLogIndex::from(1),
    ) {
        RaftResponse::RejectedInvalidState { reason } => assert!(
            matches!(reason, RaftStateError::WatermarkAboveCutoff { .. }),
            "{reason}"
        ),
        other => panic!("expected invalid-state rejection, got {other:?}"),
    }
}

/// The core A2-rework regression: a *committed* entry carrying a malformed
/// manifest must be rejected as a **value** on the apply path (never a fatal
/// `StorageError` that would halt the state machine), must not mutate the
/// state, and — the poison-pill guard — the store must still reopen cleanly,
/// replaying the bad entry as a rejection rather than failing `from_db`
/// forever.
#[tokio::test]
async fn malformed_manifest_is_rejected_and_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("r.db");
    let leader = CommittedLeaderId::new(1, nid(1));
    let entries = vec![
        fenced_entry(1, RaftGeneration::ZERO, leader, &[nid(1), nid(2)]),
        malformed_manifest_entry(2, RaftGeneration(1), leader),
    ];
    {
        let raft_state = RaftStateHandle::new();
        let mut s = storage_at(&path, raft_state.clone()).await;
        s.append_to_log(entries.clone()).await.expect("append");
        // Must NOT return Err — the malformed entry is a value rejection.
        let r = s.apply_to_state_machine(&entries).await.expect("apply");
        assert!(matches!(r[0], RaftResponse::Applied));
        assert!(matches!(r[1], RaftResponse::RejectedInvalidState { .. }));
        let snap = raft_state.snapshot();
        assert_eq!(
            snap.generation,
            RaftGeneration(1),
            "malformed entry must not apply"
        );
        assert!(snap.manifest_state.manifest.is_empty());
    }
    // Reopen: the recovery replay must survive the committed malformed entry.
    let restored = RaftStateHandle::new();
    let _s = storage_at(&path, restored.clone()).await;
    assert_eq!(restored.snapshot().generation, RaftGeneration(1));
}

#[tokio::test]
async fn delete_conflict_logs_since_removes_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = storage_at(&dir.path().join("r.db"), RaftStateHandle::new()).await;
    let leader = CommittedLeaderId::new(1, nid(1));
    let entries: Vec<Entry<TypeConfig>> = (1u64..=5)
        .map(|i| Entry {
            log_id: LogId::new(leader, i),
            payload: EntryPayload::Blank,
        })
        .collect();
    s.append_to_log(entries).await.expect("append");

    s.delete_conflict_logs_since(LogId::new(leader, 3))
        .await
        .expect("delete");
    let kept: Vec<Entry<TypeConfig>> = s.try_get_log_entries(1..=10).await.expect("range");
    assert_eq!(
        kept.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[tokio::test]
async fn apply_normal_entry_updates_raft_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let raft_state = RaftStateHandle::new();
    let mut s = storage_at(&dir.path().join("r.db"), raft_state.clone()).await;

    let leader = CommittedLeaderId::new(1, nid(1));
    let replicas = [nid(1), nid(2), nid(3)];
    s.apply_to_state_machine(&[normal_entry(1, leader, &replicas)])
        .await
        .expect("apply");

    let snap = raft_state.snapshot();
    assert_eq!(snap.replica.current().len(), 3);
    // Generation is the applied entry's log index.
    assert_eq!(snap.generation.raw(), 1);

    // last_applied_state persists the applied log id.
    let (last_applied, _) = s.last_applied_state().await.expect("last applied");
    assert_eq!(last_applied.expect("some").index, 1);
}

#[tokio::test]
async fn snapshot_build_install_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let raft_state = RaftStateHandle::new();
    let mut s = storage_at(&dir.path().join("r.db"), raft_state.clone()).await;

    let leader = CommittedLeaderId::new(1, nid(1));
    let replicas = [nid(1), nid(2)];
    s.apply_to_state_machine(&[normal_entry(1, leader, &replicas)])
        .await
        .expect("apply");

    // Build a snapshot, then install it into a fresh store.
    let snapshot = s.build_snapshot().await.expect("build");
    let current = s.get_current_snapshot().await.expect("get current");
    assert!(
        current.is_some(),
        "build_snapshot must persist a current snapshot"
    );

    let fresh_state = RaftStateHandle::new();
    let mut fresh = storage_at(&dir.path().join("r2.db"), fresh_state.clone()).await;
    fresh
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .expect("install");

    // Installed snapshot restores the replica set into the fresh handle.
    assert_eq!(fresh_state.snapshot().replica.current().len(), 2);
}

#[tokio::test]
async fn raft_state_restored_from_snapshot_on_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("r.db");
    let leader = CommittedLeaderId::new(1, nid(1));
    let replicas = [nid(1), nid(2), nid(3)];
    {
        let mut s = storage_at(&path, RaftStateHandle::new()).await;
        s.apply_to_state_machine(&[normal_entry(1, leader, &replicas)])
            .await
            .expect("apply");
        s.build_snapshot().await.expect("build");
    }
    // Reopen: from_db must restore raft_state from the persisted snapshot blob.
    let restored = RaftStateHandle::new();
    let _s = storage_at(&path, restored.clone()).await;
    assert_eq!(restored.snapshot().replica.current().len(), 3);
}

#[tokio::test]
async fn fenced_apply_rejects_stale_generation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let raft_state = RaftStateHandle::new();
    let mut s = storage_at(&dir.path().join("r.db"), raft_state.clone()).await;
    let leader = CommittedLeaderId::new(1, nid(1));

    // Entry 1, fence ZERO on a fresh state machine → applied, generation 1.
    let r = s
        .apply_to_state_machine(&[fenced_entry(1, RaftGeneration::ZERO, leader, &[nid(1)])])
        .await
        .expect("apply 1");
    assert!(matches!(r[0], RaftResponse::Applied));
    assert_eq!(raft_state.snapshot().generation, RaftGeneration(1));

    // Entry 2 with a STALE fence (ZERO, but applied generation is 1):
    // committed in the log, but a no-op on the state machine.
    let r = s
        .apply_to_state_machine(&[fenced_entry(
            2,
            RaftGeneration::ZERO,
            leader,
            &[nid(1), nid(2), nid(3)],
        )])
        .await
        .expect("apply 2");
    assert!(matches!(
        r[0],
        RaftResponse::RejectedStaleGeneration {
            expected: RaftGeneration(0),
            current: RaftGeneration(1),
        }
    ));
    let snap = raft_state.snapshot();
    assert_eq!(
        snap.generation,
        RaftGeneration(1),
        "rejected write must not apply"
    );
    assert_eq!(snap.manifest_state.hlc_cutoff, Some(crate::hlc::Hlc(1)));

    // Entry 3 with the CORRECT fence (1) → applied, generation 3.
    let r = s
        .apply_to_state_machine(&[fenced_entry(3, RaftGeneration(1), leader, &[nid(1)])])
        .await
        .expect("apply 3");
    assert!(matches!(r[0], RaftResponse::Applied));
    let snap = raft_state.snapshot();
    assert_eq!(snap.generation, RaftGeneration(3));
    assert_eq!(snap.manifest_state.hlc_cutoff, Some(crate::hlc::Hlc(3)));
}

#[tokio::test]
async fn reopen_replays_fence_verdicts_deterministically() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("r.db");
    let leader = CommittedLeaderId::new(1, nid(1));
    // 1: accepted (fence 0); 2: REJECTED (stale fence 0); 3: accepted (fence 1).
    let entries = vec![
        fenced_entry(1, RaftGeneration::ZERO, leader, &[nid(1)]),
        fenced_entry(2, RaftGeneration::ZERO, leader, &[nid(1)]),
        fenced_entry(3, RaftGeneration(1), leader, &[nid(1)]),
    ];
    {
        let raft_state = RaftStateHandle::new();
        let mut s = storage_at(&path, raft_state.clone()).await;
        s.append_to_log(entries.clone()).await.expect("append");
        let r = s.apply_to_state_machine(&entries).await.expect("apply");
        assert!(matches!(r[0], RaftResponse::Applied));
        assert!(matches!(r[1], RaftResponse::RejectedStaleGeneration { .. }));
        assert!(matches!(r[2], RaftResponse::Applied));
    }
    // Reopen: the forward replay must re-evaluate every fence and reach the
    // same end state — the rejected entry 2 must stay rejected, so its
    // content never lands in the restored state.
    let restored = RaftStateHandle::new();
    let _s = storage_at(&path, restored.clone()).await;
    let snap = restored.snapshot();
    assert_eq!(snap.generation, RaftGeneration(3));
    assert_eq!(
        snap.manifest_state.hlc_cutoff,
        Some(crate::hlc::Hlc(3)),
        "replayed state must reflect entries 1+3, never the rejected entry 2"
    );
}

/// F-13 invariant: reopening while the raft log still contains Normal
/// entries at or below the snapshot blob's index (log purge lagging
/// snapshot creation — crash between `build_snapshot` and purge) must
/// neither fail nor regress the state. The forward replay starts
/// strictly above `blob_last_applied`, so those entries are never
/// re-applied; if a future storage change ever broke that, the
/// regression now surfaces as `Err(StateRegression)` instead of a
/// crash-looping panic (F-15).
#[tokio::test]
async fn reopen_with_unpurged_log_entries_below_snapshot_is_clean() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("r.db");
    let leader = CommittedLeaderId::new(1, nid(1));
    let entries = vec![
        fenced_entry(1, RaftGeneration::ZERO, leader, &[nid(1)]),
        fenced_entry(2, RaftGeneration(1), leader, &[nid(1)]),
    ];
    {
        let raft_state = RaftStateHandle::new();
        let mut s = storage_at(&path, raft_state.clone()).await;
        // Entries are in the LOG and applied; the snapshot blob covers
        // them; the log is deliberately NOT purged.
        s.append_to_log(entries.clone()).await.expect("append");
        let r = s.apply_to_state_machine(&entries).await.expect("apply");
        assert!(r.iter().all(|x| matches!(x, RaftResponse::Applied)));
        s.build_snapshot().await.expect("build snapshot");
    }
    let restored = RaftStateHandle::new();
    let _s = storage_at(&path, restored.clone()).await;
    let snap = restored.snapshot();
    assert_eq!(snap.generation, RaftGeneration(2));
    assert_eq!(snap.manifest_state.hlc_cutoff, Some(crate::hlc::Hlc(2)));
}

/// F-14 invariant: a corrupt raft snapshot blob must fail `from_db`
/// loudly. Silently skipping it would run the node with empty state (no
/// replica set, no HLC cutoff) while openraft believes the state machine
/// is current.
#[tokio::test]
async fn invariant_corrupt_snapshot_blob_is_fatal_at_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("r.db");
    {
        // Plant a corrupt blob straight into the raft-meta family.
        let storage = open_test_storage(&path).expect("open storage");
        let stored = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "corrupt-test".to_owned(),
            },
            data: vec![0xFF, 0x00, 0xAB],
        };
        let cf = storage.cf(Subsystem::RaftMeta).expect("cf");
        let mut batch = WriteBatch::default();
        put_meta(&mut batch, &cf, RaftMetaSlot::SnapshotBlob, &stored).expect("record");
        storage.write(batch).expect("write");
        storage.fence().expect("fence");
    }
    let err = RocksRaftStorage::open(
        RaftStateHandle::new(),
        open_test_storage(&path).expect("open storage"),
    )
    .await
    .expect_err("corrupt blob must be fatal");
    let msg = err.to_string();
    assert!(
        msg.contains("corrupt raft snapshot blob"),
        "unexpected error: {msg}"
    );
}

// ── key codec ───────────────────────────────────────────────────────

#[test]
fn raft_log_key_roundtrips() {
    let primary = RaftLogIndex::from(0xDEAD_BEEFu64);
    let secondary = RaftLogIndex::from(0xCAFE_BABEu64);
    let key = encode_log_key(primary, secondary);
    assert_eq!(key.len(), LOG_KEY_WIDTH);
    assert_eq!(&key[..LOG_PRIMARY_WIDTH], &encode_log_primary(primary)[..]);
    assert_eq!(&key[LOG_PRIMARY_WIDTH..], &encode_log_rank(secondary)[..]);
    assert_eq!(decode_log_key(&key), Some((primary, secondary)));
}

#[test]
fn raft_log_key_decode_rejects_wrong_length() {
    assert_eq!(decode_log_key(&[0u8; 4]), None);
    assert_eq!(decode_log_key(&[]), None);
}

/// The slot byte is the whole key, so two slots sharing one would silently
/// overwrite each other's singleton.
#[test]
fn raft_meta_slots_have_distinct_one_byte_keys() {
    let mut seen = std::collections::BTreeSet::new();
    for slot in [
        RaftMetaSlot::Vote,
        RaftMetaSlot::Committed,
        RaftMetaSlot::LastApplied,
        RaftMetaSlot::LastPurged,
        RaftMetaSlot::LastMembership,
        RaftMetaSlot::SnapshotIndex,
        RaftMetaSlot::SnapshotBlob,
    ] {
        let key = encode_meta_key(slot);
        assert_eq!(key.len(), META_KEY_WIDTH);
        assert!(seen.insert(key), "{slot:?} shares a key with another slot");
    }
}

/// Load-bearing: retirement is a `DeleteRange` over **encoded bytes**, so if
/// the byte order were not the tuple order, `purge_logs_upto` would drop the
/// wrong entries.
#[test]
fn key_order_matches_tuple_order() {
    use proptest::prelude::*;

    proptest!(|(
        (p1, s1) in (any::<u64>(), any::<u64>()),
        (p2, s2) in (any::<u64>(), any::<u64>()),
    )| {
        let k1 = encode_log_key(p1.into(), s1.into());
        let k2 = encode_log_key(p2.into(), s2.into());
        prop_assert_eq!(k1.cmp(&k2), (p1, s1).cmp(&(p2, s2)));
    });
}
