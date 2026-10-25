#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_arguments,
    clippy::result_large_err,
    reason = "test code"
)]
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use uuid::Uuid;

use crate::api::{LocalStoreMut, ReplicatedRaftState, StableLogSnapshot, WindowItem};
use crate::hlc::Hlc;
use crate::ids::{DataId, LogIndex, NodeId, PeerAddr};
use crate::leader::jobs::snapshot::create::{
    SnapshotEntryInputs, SnapshotResult, create_snapshot_entry,
};
use crate::learner::LocalStore;
use crate::learner::StableLog;
use crate::learner::integrate_manifest_for_test;
use crate::meta::{Data, ReplicaMembership};
use crate::object_store::CompressionLevel;
use crate::object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use crate::raft::Raft;
use crate::raft::RaftNode;
use crate::raft::RaftStateHandle;

use crate::tests::make_data;
use crate::tests::upstream;

async fn integrate_batch(
    store: &LocalStore,
    source: NodeId,
    records: Vec<(LogIndex, Data)>,
) -> Result<(), crate::Error> {
    if records.is_empty() {
        return Ok(());
    }
    let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
    for (idx, rec) in records {
        let _ = tx.send(Ok((source, idx, rec)));
    }
    let (ch, closer) = crate::channel::Channels::from(rx, "test integrate");
    // Close before the last sender drops, or the consumer forms its
    // verdict as "disconnected, not yet closed" — an abort.
    closer.close();
    drop(tx);
    store.integrate_loop(ch).await
}

use crate::tests::{boot_snapshot_harness as boot, commit_snapshot_result};

async fn append_entries(local_store: &LocalStore, entries: &[Data]) {
    for (i, d) in entries.iter().enumerate() {
        let idx = LogIndex(u64::try_from(i).expect("fits"));
        integrate_batch(local_store, upstream(1), vec![(idx, d.clone())])
            .await
            .expect("integrate");
    }
}

/// A window read that dies mid-scan must **fail** the snapshot, not commit a
/// short one. Committing it would record a self-consistent manifest (which
/// `validate_replicated_state` accepts — it checks manifest *shape*, never
/// *coverage*) while `manifest_watermark` advanced to the full cutoff, so the
/// entries the scan never reached would fall below the next window's lower
/// bound and be lost for good.
#[tokio::test]
async fn snapshot_refuses_to_commit_an_aborted_window() {
    use crate::api::LocalStoreView;
    use crate::channel::Channels;
    use crate::error::Error;

    use tokio::sync::watch;

    /// Yields a prefix of the real window, then drops its `Closer` — exactly
    /// what a panicking `stream_scan` leaves behind.
    struct TruncatingLocalStore {
        inner: Arc<LocalStore>,
    }

    #[async_trait::async_trait]
    impl LocalStoreView for TruncatingLocalStore {
        fn stream_above_hlc(&self, threshold: Option<Hlc>) -> Channels<Result<Data, Error>> {
            self.inner.stream_above_hlc(threshold)
        }
        fn stream_window_ordered(
            &self,
            _above: Option<Hlc>,
            _upto: Hlc,
            _max_entries: Option<std::num::NonZeroUsize>,
        ) -> Channels<Result<WindowItem, Error>> {
            let (tx, rx) = crossbeam_channel::bounded(4);
            let (ch, closer) = Channels::from(rx, "truncated window");
            tx.send(Ok(WindowItem::Entry(make_data(1, 100, b"a"))))
                .unwrap();
            drop(tx);
            drop(closer); // never closed: the scan died
            ch
        }
        fn subscribe_integrated(&self) -> watch::Receiver<()> {
            self.inner.subscribe_integrated()
        }
        fn subscribe_unstable(&self) -> crate::channel::Channels<crate::meta::Data> {
            self.inner.subscribe_unstable()
        }
        async fn subscription_cursor(&self, upstream: NodeId) -> Result<Option<LogIndex>, Error> {
            self.inner.subscription_cursor(upstream).await
        }
    }

    let h = boot().await;
    append_entries(
        &h.local_store,
        &[
            make_data(1, 100, b"a"),
            make_data(2, 200, b"b"),
            make_data(3, 300, b"c"),
        ],
    )
    .await;

    let before = h.raft_state.snapshot().manifest_state.manifest_watermark;

    let truncating = TruncatingLocalStore {
        inner: h.local_store.clone(),
    };
    let inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: &truncating,
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*h.snapshot_store(),
        raft_state: &h.raft_state,
        compression: CompressionLevel::fast(),
    };

    let err = create_snapshot_entry(&inputs, Hlc(300))
        .await
        .expect_err("an aborted window must fail the snapshot");
    assert!(
        matches!(err, Error::StreamTruncated { ref what } if what == "truncated window"),
        "expected StreamTruncated, got: {err:?}"
    );

    assert_eq!(
        h.raft_state.snapshot().manifest_state.manifest_watermark,
        before,
        "a failed snapshot must not advance the watermark"
    );
}

#[tokio::test]
async fn snapshot_round_trip_through_object_store() {
    let h = boot().await;
    let _ = h.leader_id;

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
    commit_snapshot_result(&h.raft, &h.raft_state, &result).await;
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
    let learner_stable = crate::tests::stable_log_at(&learner_path).await;

    integrate_manifest_for_test(
        h.snapshot_store(),
        learner_stable.as_ref(),
        &snap.manifest_state,
    )
    .await
    .expect("load");

    assert_eq!(learner_stable.end_hlc(), Some(Hlc(300)));
    assert_eq!(learner_stable.end_index(), Some(LogIndex(2)));
    let stable_entries: Vec<(crate::ids::LogIndex, crate::meta::Data)> = learner_stable
        .range(LogIndex(0), LogIndex(3))
        .into_iter()
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
    let h = boot().await;
    let _ = h.leader_id;

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
    commit_snapshot_result(&h.raft, &h.raft_state, &first).await;
    assert!(matches!(first, SnapshotResult::Committed { .. }));

    let second = create_snapshot_entry(&inputs, Hlc(300))
        .await
        .expect("second");
    commit_snapshot_result(&h.raft, &h.raft_state, &second).await;
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
async fn snapshot_rejects_backwards_cutoff() {
    let h = boot().await;
    let _ = h.leader_id;

    append_entries(
        &h.local_store,
        &[make_data(1, 100, b"a"), make_data(2, 200, b"b")],
    )
    .await;

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
    commit_snapshot_result(&h.raft, &h.raft_state, &first).await;

    let result = create_snapshot_entry(&inputs, Hlc(150))
        .await
        .expect("retry");
    assert_eq!(result, SnapshotResult::Skipped);
}

#[tokio::test]
async fn second_snapshot_extends_stable_log_via_load() {
    let h = boot().await;
    let _ = h.leader_id;

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
    commit_snapshot_result(&h.raft, &h.raft_state, &first).await;
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
    commit_snapshot_result(&h.raft, &h.raft_state, &second).await;
    let SnapshotResult::Committed { new_watermark, .. } = second else {
        panic!("expected Committed");
    };
    assert_eq!(new_watermark, Hlc(210));

    let manifest_state_after_second = h.raft_state.snapshot().manifest_state.clone();
    let learner_dir = tempfile::tempdir().expect("tempdir");
    let learner_path = learner_dir.path().join("stable.db");
    let learner_stable = StableLog::open_engine(learner_path.with_extension("stable"))
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

    let entries: Vec<(crate::ids::LogIndex, crate::meta::Data)> = learner_stable
        .range(LogIndex(0), LogIndex(4))
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(entries.len(), 4);
    let hlcs: Vec<u64> = entries.iter().map(|(_, d)| d.data_id.hlc().raw()).collect();
    assert_eq!(hlcs, vec![100, 110, 200, 210]);
}

#[tokio::test]
async fn integrate_manifest_is_idempotent() {
    let h = boot().await;
    let _ = h.leader_id;

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
    let learner_stable = StableLog::open_engine(learner_path.with_extension("stable"))
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
    let h = boot().await;
    let _ = h.leader_id;

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
    commit_snapshot_result(&h.raft, &h.raft_state, &result).await;

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
    let learner_stable = StableLog::open_engine(learner_dir.path().join("stable.stable"))
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
        .range(LogIndex(0), LogIndex(100))
        .into_iter()
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

// ── F-24 fence: generation CAS on state writes ──────────────────────────

#[tokio::test]
async fn fenced_write_rejected_on_stale_generation() {
    use crate::ids::RaftGeneration;
    use crate::raft::{FencedStateWrite, RaftResponse};

    let h = boot().await;
    let _ = h.leader_id;

    // Seed once (bumps the applied generation past ZERO).
    let seeded: std::collections::BTreeSet<NodeId> = [NodeId::new_v4()].into_iter().collect();
    crate::tests::write_state_fenced(
        &h.raft,
        &h.raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(seeded.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed");
    let gen_after_seed = h.raft_state.snapshot().generation;
    assert!(gen_after_seed > RaftGeneration::ZERO);

    // A write fenced on the stale ZERO generation commits in the Raft log
    // but must be a no-op on the state machine.
    let response = h
        .raft
        .client_write(FencedStateWrite {
            expected_generation: RaftGeneration::ZERO,
            state: ReplicatedRaftState {
                replica: ReplicaMembership::single(std::collections::BTreeSet::new()),
                manifest: Vec::new(),
                manifest_watermark: Some(Hlc(999_999)),
                hlc_cutoff: Some(Hlc(999_999)),
            },
        })
        .await
        .expect("commit succeeds; apply is fenced");
    assert!(matches!(
        response.data,
        RaftResponse::RejectedStaleGeneration { .. }
    ));

    let snap = h.raft_state.snapshot();
    assert_eq!(
        snap.generation, gen_after_seed,
        "rejected write must not apply"
    );
    assert_eq!(snap.replica.current().clone(), seeded);
    assert_eq!(snap.manifest_state.manifest_watermark, None);
}

/// EXPERT_REVIEW F-24 invariant: a snapshot commit computed from a stale
/// state read must NOT clobber state committed concurrently (e.g. by another
/// leader during a leadership flap). The generation fence rejects it and the
/// concurrent manifest entry + watermark survive.
///
/// NOTE: Ignored post-Channels refactoring. The old async `stream_window_ordered`
/// allowed async injection (foreign manifest commit) during the window-ordered
/// read. The sync `Channels<T>` return type runs on the caller's thread — async
/// injection from a tokio worker panics (`block_on` from within a runtime).
/// Restoring this test requires bridging sync injection into the async pipeline
/// (e.g. signal + yield in `create_snapshot_entry` before the snapshot commit).
#[tokio::test]
#[ignore = "needs async injection bridge for sync Channels pipeline"]
async fn invariant_stale_snapshot_commit_is_fenced() {
    use std::sync::atomic::AtomicBool;

    use crate::api::Replica;
    use crate::api::{LocalStoreView, Router};
    use crate::channel::Channels;
    use crate::error::Error;
    use crate::hlc::{Hlc, HlcParts};
    use crate::ids::{LogIndex, NodeId, RaftGeneration, SnapshotId};
    use crate::leader::jobs::snapshot::create::{SnapshotInputs, build_snapshot};
    use crate::meta::{Data, SnapshotMetadata};

    use crate::tests::{FixedClock, ReadyReplica};

    use tokio::sync::watch;

    /// Replica stub: always drained, no end_index — quorum checks pass instantly.
    struct OnePeerRouter {
        replica: Arc<dyn Replica>,
    }

    impl Router for OnePeerRouter {
        fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

        fn remote(&self, _node_id: NodeId) -> Arc<dyn crate::api::ReplicaClient> {
            crate::tests::replica_only(self.replica.clone())
        }

        fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn crate::api::ReplicaClient> {
            unimplemented!("OnePeerRouter: direct dial not used")
        }
    }

    /// LocalStoreView wrapper that commits a foreign manifest entry through
    /// the REAL Raft when the snapshot pipeline starts reading data — i.e.
    /// inside the window between the pipeline's state read and its final
    /// commit (simulating another leader's snapshot landing concurrently).
    struct InjectingLocalStore {
        inner: Arc<LocalStore>,
        raft: Raft,
        raft_state: RaftStateHandle,
        foreign: SnapshotMetadata,
        injected: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl LocalStoreView for InjectingLocalStore {
        fn stream_above_hlc(&self, threshold: Option<Hlc>) -> Channels<Result<Data, Error>> {
            self.inner.stream_above_hlc(threshold)
        }

        fn stream_window_ordered(
            &self,
            above: Option<Hlc>,
            upto: Hlc,
            max_entries: Option<std::num::NonZeroUsize>,
        ) -> Channels<Result<WindowItem, Error>> {
            self.inner.stream_window_ordered(above, upto, max_entries)
        }

        fn subscribe_integrated(&self) -> watch::Receiver<()> {
            self.inner.subscribe_integrated()
        }

        fn subscribe_unstable(&self) -> crate::channel::Channels<crate::meta::Data> {
            self.inner.subscribe_unstable()
        }

        async fn subscription_cursor(&self, upstream: NodeId) -> Result<Option<LogIndex>, Error> {
            self.inner.subscription_cursor(upstream).await
        }
    }

    let h = boot().await;
    let _ = h.leader_id;

    // Seed a one-node replica set so the quorum fan-out has a target.
    let replica_id = NodeId::new_v4();
    crate::tests::write_state_fenced(
        &h.raft,
        &h.raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single([replica_id].into_iter().collect()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed replica set");

    // One row so the snapshot is non-empty.
    integrate_batch(
        &h.local_store,
        upstream(1),
        vec![(LogIndex(0), make_data(1, 100, b"x"))],
    )
    .await
    .expect("integrate");

    // The "other leader's" snapshot, committed mid-window.
    let foreign = SnapshotMetadata {
        start_hlc: Hlc::from_parts(HlcParts {
            physical_ms: 4_000,
            logical: 0,
        }),
        end_hlc: Hlc::from_parts(HlcParts {
            physical_ms: 5_000,
            logical: 0,
        }),
        start_index: LogIndex(0),
        end_index: LogIndex(0),
        snapshot_id: SnapshotId::from_u128(5_000),
    };

    let injecting = Arc::new(InjectingLocalStore {
        inner: h.local_store,
        raft: h.raft.clone(),
        raft_state: h.raft_state.clone(),
        foreign: foreign.clone(),
        injected: Arc::new(AtomicBool::new(false)),
    });
    let router: Arc<dyn Router> = Arc::new(OnePeerRouter {
        replica: Arc::new(ReadyReplica {
            raft_state: h.raft_state.clone(),
        }),
    });
    let clock = FixedClock {
        now: Hlc::from_parts(HlcParts {
            physical_ms: 3_000,
            logical: 0,
        }),
    };

    let snapshot_store = Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: h.object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    ));
    let leadership: Arc<dyn crate::api::RaftLeadership> = Arc::new(RaftNode::new(
        h.raft.clone(),
        crate::network::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));
    let inputs = SnapshotInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: injecting,
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*snapshot_store,
        raft_state: &h.raft_state,
        router,
        leadership,
        self_node_id: h.leader_id,
        quorum_poll_interval: Duration::from_millis(5),
        quorum_wait_timeout: Duration::from_secs(5),
        compression: CompressionLevel::fast(),
    };

    // The build's premise (the manifest end index its global indices continue
    // from) moved under it, so publishing must be refused rather than rebased.
    let build = build_snapshot(&inputs, clock.now)
        .await
        .expect("the build itself succeeds; only publishing is refused");
    let (intents, _cancel) = crate::leader::Integrator::spawn_for_test(
        Arc::new(RaftNode::new(
            h.raft.clone(),
            crate::network::permissive_gate(),
            std::time::Duration::from_secs(30),
        )),
        Arc::new(h.raft_state.clone()),
    );
    let outcome = intents
        .submit(
            build
                .commit_intent()
                .expect("a committed build has an intent"),
        )
        .await
        .expect("submit");
    assert!(
        matches!(outcome, crate::leader::IntentOutcome::Superseded { .. }),
        "expected the stale build to be superseded, got: {outcome:?}"
    );

    // The concurrently committed state survives untouched.
    let final_state = h.raft_state.snapshot();
    assert!(
        final_state
            .manifest_state
            .manifest
            .iter()
            .any(|m| m.snapshot_id == foreign.snapshot_id),
        "foreign manifest entry was clobbered: {:?}",
        final_state.manifest_state.manifest,
    );
    assert_eq!(
        final_state.manifest_state.manifest_watermark,
        Some(foreign.end_hlc),
        "watermark must not regress below the concurrently committed value",
    );
    let _ = RaftGeneration::ZERO;
}

/// A wedged (arbitrarily slow) replica must not block snapshot creation:
/// the cutoff-ACK fan-out needs a quorum, not every replica (EXPERT_REVIEW
/// §10.2 — slow-replica fault mode).
#[tokio::test]
async fn snapshot_quorum_tolerates_one_wedged_replica() {
    use crate::api::Replica;
    use crate::api::Router;
    use crate::hlc::HlcParts;
    use crate::leader::jobs::snapshot::create::{SnapshotInputs, build_snapshot};

    use crate::tests::{FaultyReplica, FixedClock, MapPeerRouter, ReadyReplica};

    let h = boot().await;
    let _ = h.leader_id;

    // Three-replica set: quorum is 2.
    let replica_ids: [NodeId; 3] = std::array::from_fn(|_| NodeId::new_v4());
    crate::tests::write_state_fenced(
        &h.raft,
        &h.raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(replica_ids.iter().copied().collect()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed replica set");

    let local_store = Arc::new(h.local_store);
    integrate_batch(
        &local_store,
        upstream(1),
        vec![(LogIndex(0), make_data(1, 100, b"x"))],
    )
    .await
    .expect("integrate");

    let ready = |raft_state| -> Arc<dyn Replica> { Arc::new(ReadyReplica { raft_state }) };
    let (wedged, faults) = FaultyReplica::new(ready(h.raft_state.clone()));
    faults.wedge();

    let router: Arc<dyn Router> = Arc::new(MapPeerRouter {
        replicas: [
            (replica_ids[0], ready(h.raft_state.clone())),
            (replica_ids[1], ready(h.raft_state.clone())),
            (replica_ids[2], wedged as Arc<dyn Replica>),
        ]
        .into_iter()
        .collect(),
    });
    let clock = FixedClock {
        now: Hlc::from_parts(HlcParts {
            physical_ms: 3_000,
            logical: 0,
        }),
    };

    let snapshot_store = Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: h.object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    ));
    let leadership: Arc<dyn crate::api::RaftLeadership> = Arc::new(RaftNode::new(
        h.raft.clone(),
        crate::network::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));
    let inputs = SnapshotInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: local_store.as_ref().clone(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*snapshot_store,
        raft_state: &h.raft_state,
        router,
        leadership,
        self_node_id: h.leader_id,
        quorum_poll_interval: Duration::from_millis(5),
        quorum_wait_timeout: Duration::from_secs(5),
        compression: CompressionLevel::fast(),
    };

    let result = build_snapshot(&inputs, clock.now)
        .await
        .expect("snapshot must succeed with a 2/3 quorum");
    assert!(
        matches!(result, SnapshotResult::Committed { .. }),
        "expected a committed snapshot, got: {result:?}",
    );
}

/// A leader demoted during the quorum wait must not go on to upload. Nothing
/// unsafe follows if it does — the integrator and the apply fence both refuse
/// the commit — but the object is then unreferenced waste for cleanup to
/// collect, and re-checking after the slow part is cheap.
#[tokio::test]
async fn a_leader_demoted_during_the_quorum_wait_writes_no_object() {
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::api::Replica;
    use crate::api::Router;
    use crate::api::{RaftLeadership, SnapshotStore};
    use crate::channel::Channels;
    use crate::error::Error;
    use crate::ids::ServerName;
    use crate::ids::SnapshotId;
    use crate::leader::jobs::snapshot::create::{SnapshotInputs, build_snapshot};
    use crate::meta::SnapshotEntry;
    use crate::tests::ReadyReplica;

    /// Replica stub: always drained, no end_index — quorum checks pass instantly.
    struct OnePeerRouter {
        replica: Arc<dyn Replica>,
    }

    impl Router for OnePeerRouter {
        fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

        fn remote(&self, _node_id: NodeId) -> Arc<dyn crate::api::ReplicaClient> {
            crate::tests::replica_only(self.replica.clone())
        }

        fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn crate::api::ReplicaClient> {
            unimplemented!("OnePeerRouter: direct dial not used")
        }
    }

    /// A [`RaftLeadership`] fake whose current leader is settable mid-test —
    /// unlike the real `RaftNode`, this doesn't need an actual demotion to
    /// drive through raft, just a flag flip before `build_snapshot` runs.
    struct FakeLeadership {
        leader: Mutex<Option<NodeId>>,
    }

    impl FakeLeadership {
        fn new(leader: NodeId) -> Self {
            Self {
                leader: Mutex::new(Some(leader)),
            }
        }

        fn set_leader(&self, leader: Option<NodeId>) {
            *self.leader.lock().expect("lock") = leader;
        }
    }

    #[async_trait::async_trait]
    impl RaftLeadership for FakeLeadership {
        fn current_leader(&self) -> Option<NodeId> {
            *self.leader.lock().expect("lock")
        }

        fn voter_ids(&self) -> BTreeSet<NodeId> {
            BTreeSet::new()
        }

        fn voter_server_names(&self) -> Vec<(NodeId, ServerName)> {
            Vec::new()
        }

        async fn remove_member(&self, _node_id: NodeId) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Counts `write_snapshot` calls by delegating everything else to a real
    /// [`RemoteSnapshotStore`], so a build that gets past the leadership
    /// re-check would show up here.
    struct CountingSnapshotStore {
        inner: Arc<RemoteSnapshotStore>,
        writes: AtomicUsize,
    }

    impl CountingSnapshotStore {
        fn new(inner: Arc<RemoteSnapshotStore>) -> Self {
            Self {
                inner,
                writes: AtomicUsize::new(0),
            }
        }

        fn writes(&self) -> usize {
            self.writes.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl SnapshotStore for CountingSnapshotStore {
        async fn write_snapshot(
            &self,
            id: SnapshotId,
            entries: Channels<Result<SnapshotEntry, Error>>,
            compression: CompressionLevel,
            _reason: crate::metrics::ObjectStoreReason,
        ) -> Result<usize, Error> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.inner
                .write_snapshot(
                    id,
                    entries,
                    compression,
                    crate::metrics::ObjectStoreReason::SnapshotCreate,
                )
                .await
        }

        async fn read_snapshot(
            &self,
            id: SnapshotId,
            _reason: crate::metrics::ObjectStoreReason,
        ) -> Result<Channels<Result<SnapshotEntry, Error>>, Error> {
            self.inner
                .read_snapshot(id, crate::metrics::ObjectStoreReason::ArchiveRead)
                .await
        }

        fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
            self.inner.shutdown_token()
        }

        async fn has_bootstrap_fence(&self) -> Result<bool, Error> {
            self.inner.has_bootstrap_fence().await
        }

        async fn write_bootstrap_fence(&self) -> Result<(), Error> {
            self.inner.write_bootstrap_fence().await
        }

        async fn cleanup_snapshots(
            &self,
            live_snapshots: BTreeSet<SnapshotId>,
            cleanup_retention: Duration,
        ) -> Result<crate::api::CleanupReport, Error> {
            self.inner
                .cleanup_snapshots(live_snapshots, cleanup_retention)
                .await
        }
    }

    let h = boot().await;

    // Seed a one-node replica set so the quorum fan-out has a target, and one
    // row so the window is non-empty (an empty window would take the earlier
    // `NoopEmptyWindow` return and never reach the re-check).
    let replica_id = NodeId::new_v4();
    crate::tests::write_state_fenced(
        &h.raft,
        &h.raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single([replica_id].into_iter().collect()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed replica set");

    integrate_batch(
        &h.local_store,
        upstream(1),
        vec![(LogIndex(0), make_data(1, 100, b"x"))],
    )
    .await
    .expect("integrate");

    let router: Arc<dyn Router> = Arc::new(OnePeerRouter {
        replica: Arc::new(ReadyReplica {
            raft_state: h.raft_state.clone(),
        }),
    });
    let store = CountingSnapshotStore::new(Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: h.object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    )));

    let leadership = FakeLeadership::new(h.leader_id);
    // Demote before the build starts: `await_quorum_ready` is satisfied by the
    // one-node replica set above, so the re-check after it is the only thing
    // that can stop the upload.
    leadership.set_leader(None);

    let inputs = SnapshotInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: h.local_store.clone(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &store,
        raft_state: &h.raft_state,
        router,
        leadership: Arc::new(leadership),
        self_node_id: h.leader_id,
        quorum_poll_interval: Duration::from_millis(10),
        quorum_wait_timeout: Duration::from_secs(5),
        compression: CompressionLevel::fast(),
    };

    let result = build_snapshot(&inputs, Hlc(500)).await.expect("build");
    assert_eq!(
        result,
        SnapshotResult::Skipped,
        "a demoted node must not build"
    );
    assert_eq!(store.writes(), 0, "and must not write an object");
}
