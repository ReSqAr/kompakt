//! Snapshots through the object store and back into a learner: round trip, watermark-only snapshots, extension, idempotence, ordering.

use super::*;

#[tokio::test]
async fn snapshot_round_trip_through_object_store() {
    let h = boot_fake().await;

    let entries = vec![
        make_data(1, 100, b"a"),
        make_data(2, 200, b"b"),
        make_data(3, 300, b"c"),
    ];
    append_entries(&h.local_store, &entries).await;

    let inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: h.local_store.as_ref(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*h.snapshot_store(),
        raft_state: &h.raft_state,
        compression: CompressionLevel::fast(),
    };
    let result = create_snapshot_entry(&inputs, Hlc(300))
        .await
        .expect("create");
    commit_snapshot_result_via(h.consensus.as_ref(), &h.raft_state, &result).await;
    let SnapshotResult::Committed {
        new_watermark,
        entries: written,
        ..
    } = result
    else {
        panic!("expected Committed; got {result:?}");
    };
    assert_eq!(written, 3);
    assert_eq!(new_watermark, Hlc(300));

    let snap = h.raft_state.snapshot();
    assert_eq!(snap.manifest_state.manifest.len(), 1);
    assert_eq!(snap.manifest_state.manifest_watermark, Some(Hlc(300)));

    let learner_dir = tempfile::tempdir().expect("tempdir");
    let learner_path = learner_dir.path().join("stable.db");
    let learner_stable = zslg_learner::testing::stable_log_at(&learner_path).await;

    integrate_manifest_for_test(
        h.snapshot_store(),
        learner_stable.as_ref(),
        &snap.manifest_state,
    )
    .await
    .expect("load");

    assert_eq!(learner_stable.end_hlc(), Some(Hlc(300)));
    assert_eq!(learner_stable.end_index(), Some(LogIndex(2)));
    let stable_entries: Vec<(zslg_log::LogIndex, zslg_log::Data)> = learner_stable
        .range(None, LogIndex(3))
        .items()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(stable_entries.len(), 3);
    for (i, (idx, d)) in stable_entries.iter().enumerate() {
        let expected_idx = u64::try_from(i).expect("fits");
        assert_eq!(idx.raw(), expected_idx);
        assert_eq!(d.data_id.hlc(), entries[i].data_id.hlc());
        assert_eq!(d.data_id, entries[i].data_id);
        assert_eq!(d.blob, entries[i].blob);
    }
}

#[tokio::test]
async fn snapshot_watermark_advances_even_with_no_new_entries() {
    let h = boot_fake().await;

    let entries = vec![make_data(1, 100, b"a"), make_data(2, 200, b"b")];
    append_entries(&h.local_store, &entries).await;

    let inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: h.local_store.as_ref(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*h.snapshot_store(),
        raft_state: &h.raft_state,
        compression: CompressionLevel::fast(),
    };
    let first = create_snapshot_entry(&inputs, Hlc(200))
        .await
        .expect("first");
    commit_snapshot_result_via(h.consensus.as_ref(), &h.raft_state, &first).await;
    assert!(matches!(first, SnapshotResult::Committed { .. }));

    let second = create_snapshot_entry(&inputs, Hlc(300))
        .await
        .expect("second");
    commit_snapshot_result_via(h.consensus.as_ref(), &h.raft_state, &second).await;
    assert!(
        matches!(
            second,
            SnapshotResult::Committed {
                entries: 0,
                new_watermark: Hlc(300),
                ..
            }
        ),
        "expected watermark-only Committed, got {second:?}",
    );
    let self1 = &h.raft_state.snapshot();
    assert_eq!(self1.manifest_state.manifest_watermark, Some(Hlc(300)));
}

#[tokio::test]
async fn second_snapshot_extends_stable_log_via_load() {
    let h = boot_fake().await;

    let batch1 = vec![make_data(1, 100, b"a"), make_data(2, 110, b"b")];
    append_entries(&h.local_store, &batch1).await;

    let inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: h.local_store.as_ref(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*h.snapshot_store(),
        raft_state: &h.raft_state,
        compression: CompressionLevel::fast(),
    };
    let first = create_snapshot_entry(&inputs, Hlc(110))
        .await
        .expect("first");
    commit_snapshot_result_via(h.consensus.as_ref(), &h.raft_state, &first).await;
    assert!(matches!(first, SnapshotResult::Committed { .. }));
    let manifest_state_after_first = h.raft_state.snapshot().manifest_state.clone();

    let batch2 = [make_data(3, 200, b"c"), make_data(4, 210, b"d")];
    for (i, d) in batch2.iter().enumerate() {
        let idx = LogIndex(u64::try_from(2 + i).expect("fits"));
        integrate_batch(&h.local_store, upstream(1), vec![(idx, d.clone())])
            .await
            .expect("integrate");
    }

    integrate_manifest_for_test(
        h.snapshot_store(),
        h.stable_log.as_ref(),
        &manifest_state_after_first,
    )
    .await
    .expect("loader1");
    assert_eq!(h.stable_log.end_index(), Some(LogIndex(1)));

    let second = create_snapshot_entry(&inputs, Hlc(210))
        .await
        .expect("second");
    commit_snapshot_result_via(h.consensus.as_ref(), &h.raft_state, &second).await;
    let SnapshotResult::Committed { new_watermark, .. } = second else {
        panic!("expected Committed");
    };
    assert_eq!(new_watermark, Hlc(210));

    let manifest_state_after_second = h.raft_state.snapshot().manifest_state.clone();
    let learner_dir = tempfile::tempdir().expect("tempdir");
    let learner_path = learner_dir.path().join("stable.db");
    let learner_stable =
        zslg_learner::testing::stable_log_engine(learner_path.with_extension("stable"))
            .await
            .expect("open stable log");
    integrate_manifest_for_test(
        h.snapshot_store(),
        &learner_stable,
        &manifest_state_after_second,
    )
    .await
    .expect("loader2");
    assert_eq!(learner_stable.end_index(), Some(LogIndex(3)));
    assert_eq!(learner_stable.end_hlc(), Some(Hlc(210)));

    let entries: Vec<(zslg_log::LogIndex, zslg_log::Data)> = learner_stable
        .range(None, LogIndex(4))
        .items()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(entries.len(), 4);
    let hlcs: Vec<u64> = entries.iter().map(|(_, d)| d.data_id.hlc().raw()).collect();
    assert_eq!(hlcs, vec![100, 110, 200, 210]);
}

#[tokio::test]
async fn integrate_manifest_is_idempotent() {
    let h = boot_fake().await;

    append_entries(&h.local_store, &[make_data(1, 100, b"a")]).await;

    let inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: h.local_store.as_ref(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*h.snapshot_store(),
        raft_state: &h.raft_state,
        compression: CompressionLevel::fast(),
    };
    assert!(matches!(
        create_snapshot_entry(&inputs, Hlc(100))
            .await
            .expect("create"),
        SnapshotResult::Committed { .. }
    ));
    let manifest_state = h.raft_state.snapshot().manifest_state.clone();

    let learner_dir = tempfile::tempdir().expect("tempdir");
    let learner_path = learner_dir.path().join("stable.db");
    let learner_stable =
        zslg_learner::testing::stable_log_engine(learner_path.with_extension("stable"))
            .await
            .expect("from_db");

    integrate_manifest_for_test(h.snapshot_store(), &learner_stable, &manifest_state)
        .await
        .expect("first load");
    let frontier1 = learner_stable.end_index();
    let watermark1 = learner_stable.end_hlc();

    integrate_manifest_for_test(h.snapshot_store(), &learner_stable, &manifest_state)
        .await
        .expect("second load");
    assert_eq!(learner_stable.end_index(), frontier1);
    assert_eq!(learner_stable.end_hlc(), watermark1);
}

/// CLAUDE.md §14-style named invariant test (EXPERT_REVIEW F-27): snapshot
/// entries — and therefore the stable order — are lexicographically ordered
/// by (HLC, data_id). The HLC is embedded in DataId and dominates the sort.
#[tokio::test]
async fn invariant_stable_order_is_hlc_order() {
    let h = boot_fake().await;

    // Anti-correlated HLC and random byte order to stress ordering.
    let entries: Vec<Data> = (0u8..10)
        .map(|i| {
            let hlc = 100u64.checked_add(u64::from(i)).expect("hlc");
            let r = u64::from(0xF0u8.checked_sub(i).expect("random"));
            Data {
                data_id: DataId::from_parts(Hlc(hlc), NodeId::from_uuid(Uuid::nil()), r),
                blob: Bytes::copy_from_slice(&[i]),
            }
        })
        .collect();
    append_entries(&h.local_store, &entries).await;

    let inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: h.local_store.as_ref(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*h.snapshot_store(),
        raft_state: &h.raft_state,
        compression: CompressionLevel::fast(),
    };
    let result = create_snapshot_entry(&inputs, Hlc(1_000))
        .await
        .expect("create");
    commit_snapshot_result_via(h.consensus.as_ref(), &h.raft_state, &result).await;

    // Snapshot metadata must carry the true HLC range.
    let snap = h.raft_state.snapshot();
    let meta = snap
        .manifest_state
        .manifest
        .last()
        .expect("manifest entry")
        .clone();
    assert_eq!(
        meta.start_hlc,
        Hlc(100),
        "start_hlc must be the window minimum"
    );
    assert_eq!(meta.end_hlc, Hlc(109), "end_hlc must be the window maximum");

    // Load into a fresh learner and verify the stable order.
    let learner_dir = tempfile::tempdir().expect("tempdir");
    let learner_stable =
        zslg_learner::testing::stable_log_engine(learner_dir.path().join("stable.stable"))
            .await
            .expect("from_db");
    integrate_manifest_for_test(
        h.snapshot_store(),
        &learner_stable,
        &h.raft_state.snapshot().manifest_state,
    )
    .await
    .expect("load");

    let stable_entries: Vec<(LogIndex, Data)> = learner_stable
        .range(None, LogIndex(100))
        .items()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(stable_entries.len(), entries.len());
    let prev: Option<(Hlc, DataId)> = None;
    for (idx, d) in &stable_entries {
        let key = (d.data_id.hlc(), d.data_id);
        if let Some(p) = prev {
            assert!(
                key > p,
                "stable log not (HLC, data_id) ordered at {idx:?}: {key:?} after {p:?}"
            );
        }
    }
}
