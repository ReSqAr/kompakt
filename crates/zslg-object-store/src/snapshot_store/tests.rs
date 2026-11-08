//! Tests for `snapshot_store`.

use crossbeam_combinators::Finite;
use crossbeam_combinators::testing::ChannelTestExt as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use object_store::memory::InMemory;

use super::*;
use zslg_consensus::RaftStateHandle;
use zslg_log::LogIndex;
use zslg_snapshot::CompressionLevel;
use zslg_snapshot::SnapshotMetadata;
use zslg_time::Hlc;

/// Retention for the cleanup tests below. `cleanup_snapshots` takes it
/// per-call rather than reading it off the store.
const TEST_RETENTION: Duration = Duration::from_secs(60);

/// Pre-load a bounded channel with `records` for `write_snapshot`, the test-side
/// mirror of the production producer tasks. The capacity covers every record so
/// `try_send` never blocks.
fn entries_channel(records: Vec<SnapshotEntry>) -> Channel<Result<SnapshotEntry, Error>, Finite> {
    Channel::from_vec(records.into_iter().map(Ok).collect())
}

fn store() -> Arc<RemoteSnapshotStore> {
    Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: Arc::new(InMemory::new()),
        },
        CancellationToken::new(),
    ))
}

fn snapshot_store_with_time_source(
    time_source: Box<dyn Fn() -> u64 + Send + Sync>,
) -> Arc<RemoteSnapshotStore> {
    Arc::new(RemoteSnapshotStore::with_time_source(
        Arc::new(InMemory::new()),
        time_source,
        CancellationToken::new(),
    ))
}

use crate::test_entry as entry;

// ── snapshot write + read roundtrip ──────────────────────────────────

#[tokio::test]
async fn roundtrip_single_record() {
    let store = store();
    let id = SnapshotId::from_u128(1);
    let records = vec![entry(0)];
    let n = store
        .write_snapshot(
            id,
            entries_channel(records.clone()),
            CompressionLevel::fast(),
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await
        .expect("write");
    assert_eq!(n.count, records.len());
    let rx = store
        .read_snapshot(id, zslg_snapshot::ObjectStoreReason::ArchiveRead)
        .await
        .expect("read");
    let got: Vec<SnapshotEntry> = rx.items().filter_map(|r| r.ok()).collect();
    assert_eq!(got, records);
}

#[tokio::test]
async fn roundtrip_multiple_records_preserves_order() {
    let store = store();
    let id = SnapshotId::from_u128(2);
    let records: Vec<SnapshotEntry> = (0..10).map(entry).collect();
    let n = store
        .write_snapshot(
            id,
            entries_channel(records.clone()),
            CompressionLevel::fast(),
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await
        .expect("write");
    assert_eq!(n.count, records.len());
    let rx = store
        .read_snapshot(id, zslg_snapshot::ObjectStoreReason::ArchiveRead)
        .await
        .expect("read");
    let got: Vec<SnapshotEntry> = rx.items().filter_map(|r| r.ok()).collect();
    assert_eq!(got, records);
}

#[tokio::test]
async fn roundtrip_empty_stream() {
    let store = store();
    let records: Vec<SnapshotEntry> = vec![];
    let n = store
        .write_snapshot(
            SnapshotId::from_u128(3),
            entries_channel(records),
            CompressionLevel::fast(),
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await
        .expect("write");
    assert_eq!(n.count, 0);
    assert_eq!(n.last, None, "an empty write has no last entry");
}

/// On an encode error mid-stream the partial multipart upload is aborted, not
/// orphaned: `write_snapshot` surfaces the error and leaves no object at the path
/// (regression guard for the abort-on-error path — docs/PERFORMANCE.md entry 24).
#[tokio::test]
async fn write_snapshot_error_aborts_upload_no_orphan() {
    let store = store();
    let id = SnapshotId::from_u128(42);
    // One good record, then an error mid-stream (exercises the zstd encode path's
    // writer-recovery + abort).
    let (tx, rx) = crossbeam_channel::bounded(2);
    tx.try_send(Ok(entry(0))).unwrap();
    tx.try_send(Err(Error::Log(LogError::Storage(
        "injected mid-write failure".into(),
    ))))
    .unwrap();
    drop(tx);

    let result = store
        .write_snapshot(
            id,
            Channel::closed(rx),
            CompressionLevel::standard(),
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await;
    assert!(
        result.is_err(),
        "encode error must propagate from write_snapshot"
    );

    // Upload aborted (not finished) → no snapshot object remains at the path.
    assert!(
        store
            .read_snapshot(id, zslg_snapshot::ObjectStoreReason::ArchiveRead)
            .await
            .is_err(),
        "no object should remain after an aborted write"
    );
}

/// Cancellation must surface as an `Err` item on the channel — never as a
/// silent early end-of-stream. A silently-truncated read is what let
/// `integrate_manifest` treat a partial snapshot as fully loaded and commit
/// later snapshots above the hole (docs/STABLE_LOG_GAP_FIX.md).
#[tokio::test]
async fn invariant_read_snapshot_cancellation_delivers_error() {
    let cancel = CancellationToken::new();
    let store = Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: Arc::new(InMemory::new()),
        },
        cancel.clone(),
    ));
    let id = SnapshotId::from_u128(77);
    let records: Vec<SnapshotEntry> = (0..10).map(entry).collect();
    store
        .write_snapshot(
            id,
            entries_channel(records),
            CompressionLevel::fast(),
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await
        .expect("write");

    // Cancel BEFORE reading: the decode worker notices right after its first
    // delivered entry and must terminate the stream with an Err item.
    cancel.cancel();
    let mut rx = store
        .read_snapshot(id, zslg_snapshot::ObjectStoreReason::ArchiveRead)
        .await
        .expect("read opens");
    let mut saw_err = false;
    let mut delivered = 0usize;
    while let Ok(item) = rx.recv() {
        match item {
            Ok(_) => delivered = delivered.saturating_add(1),
            Err(_) => saw_err = true,
        }
    }
    assert!(
        saw_err,
        "cancelled read delivered {delivered} entries and then ended \
         silently — the consumer cannot distinguish this from a complete load"
    );
}

#[tokio::test]
async fn read_missing_snapshot_errors() {
    let store = store();
    let result = store
        .read_snapshot(
            SnapshotId::from_u128(999),
            zslg_snapshot::ObjectStoreReason::ArchiveRead,
        )
        .await;
    assert!(result.is_err());
}

// ── bootstrap fence ──────────────────────────────────────────────────

#[tokio::test]
async fn fence_absent_on_fresh_store() {
    let store = store();
    assert!(!store.has_bootstrap_fence().await.expect("ok"));
}

#[tokio::test]
async fn write_then_read_detects_fence() {
    let store = store();
    store.write_bootstrap_fence().await.expect("write");
    assert!(store.has_bootstrap_fence().await.expect("ok"));
}

#[tokio::test]
async fn write_fence_is_idempotent() {
    let store = store();
    store.write_bootstrap_fence().await.expect("first");
    store.write_bootstrap_fence().await.expect("second");
    assert!(store.has_bootstrap_fence().await.expect("ok"));
}

// ── snapshot path formatting ─────────────────────────────────────────

#[test]
fn snapshot_path_format() {
    // A fresh (end-less) id: start index and random suffix.
    assert_eq!(
        snapshot_path(SnapshotId::from_parts(LogIndex(1024), None, 0x2a)).as_ref(),
        "snapshots/zslg_00000000000000001024_000000000000002a.snap"
    );
    // A merged id: the inclusive index range it covers.
    assert_eq!(
        snapshot_path(SnapshotId::from_parts(
            LogIndex(0),
            Some(LogIndex(2047)),
            0x1234
        ))
        .as_ref(),
        "snapshots/zslg_00000000000000000000_00000000000000002047_0000000000001234.snap"
    );
    // The widest values still fit their padded fields.
    assert_eq!(
        snapshot_path(SnapshotId::from_parts(
            LogIndex(u64::MAX),
            Some(LogIndex(u64::MAX)),
            u64::MAX
        ))
        .as_ref(),
        "snapshots/zslg_18446744073709551615_18446744073709551615_ffffffffffffffff.snap"
    );
    // Exactly one `.` (the extension) — nothing for path tooling to trip on.
    assert_eq!(
        snapshot_path(SnapshotId::from_u128(42))
            .as_ref()
            .matches('.')
            .count(),
        1
    );
}

/// The name is the only overwrite guard (no CAS), so distinct ids must give
/// distinct names — in particular an open id whose random suffix happens to
/// look like an index must not collide with a closed one.
#[test]
fn snapshot_name_is_injective() {
    let ids = [
        SnapshotId::from_parts(LogIndex(1), None, 2),
        SnapshotId::from_parts(LogIndex(1), Some(LogIndex(2)), 0),
        SnapshotId::from_parts(LogIndex(1), Some(LogIndex(0)), 2),
        SnapshotId::from_parts(LogIndex(2), None, 1),
        SnapshotId::from_parts(LogIndex(1), Some(LogIndex(2)), 2),
    ];
    let names: BTreeSet<String> = ids.iter().copied().map(snapshot_name).collect();
    assert_eq!(names.len(), ids.len());
}

#[test]
fn snapshot_path_is_under_prefix() {
    let p = snapshot_path(SnapshotId::from_u128(7));
    assert!(p.as_ref().starts_with(SNAPSHOT_PREFIX));
}

#[test]
fn bootstrap_fence_path_is_stable() {
    assert_eq!(bootstrap_fence_path().as_ref(), "raft-bootstrap-fence");
}

// ── cleanup ──────────────────────────────────────────────────────────

#[allow(
    dead_code,
    reason = "used by cleanup tests that verify manifest-based filtering"
)]
fn set_live_manifest(apply: &RaftStateHandle, snapshot_id: SnapshotId) {
    apply
        .apply_replicated(
            zslg_consensus::ReplicatedRaftState {
                replica: ReplicaMembership::single(std::collections::BTreeSet::new()),
                manifest: vec![SnapshotMetadata {
                    start_hlc: Hlc(0),
                    end_hlc: Hlc(10),
                    start_index: LogIndex(0),
                    end_index: LogIndex(0),
                    snapshot_id,
                }],
                manifest_watermark: Some(Hlc(10)),
                hlc_cutoff: Some(Hlc(10)),
            },
            zslg_consensus::RaftGeneration(1),
        )
        .expect("apply");
}

#[tokio::test]
async fn cleanup_skips_files_within_retention_window() {
    let store = store();
    let sid = SnapshotId::from_u128(42);
    store
        .write_snapshot(
            sid,
            entries_channel(vec![entry(0)]),
            CompressionLevel::fast(),
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await
        .expect("write");

    let report = store
        .cleanup_snapshots(BTreeSet::from([sid]), TEST_RETENTION)
        .await
        .expect("ok");
    assert!(
        report.snapshots_deleted.is_empty(),
        "must not delete own snapshot: {report:?}",
    );
}

#[tokio::test]
async fn cleanup_deletes_orphaned_snapshot_beyond_retention() {
    // Use a fake time source that reports a time far in the future.
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .saturating_add(7_200_000);
    let now_ms = u64::try_from(now_ms).unwrap_or(u64::MAX); // 2h later
    let store = snapshot_store_with_time_source(Box::new(move || now_ms));
    let orphan_sid = SnapshotId::from_u128(99);
    store
        .write_snapshot(
            orphan_sid,
            entries_channel(vec![entry(0)]),
            CompressionLevel::fast(),
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await
        .expect("write orphan");

    let live_sid = SnapshotId::from_u128(1);
    store
        .write_snapshot(
            live_sid,
            entries_channel(vec![entry(1)]),
            CompressionLevel::fast(),
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await
        .expect("write live");

    let report = store
        .cleanup_snapshots(BTreeSet::from([live_sid]), TEST_RETENTION)
        .await
        .expect("ok");

    assert_eq!(report.snapshots_deleted, vec![snapshot_path(orphan_sid)]);
    // Live snapshot still reachable.
    let _ = store
        .read_snapshot(live_sid, zslg_snapshot::ObjectStoreReason::ArchiveRead)
        .await
        .expect("live snapshot kept");
}

#[tokio::test]
async fn cleanup_noop_when_no_live_snapshots() {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .saturating_add(7_200_000);
    let now_ms = u64::try_from(now_ms).unwrap_or(u64::MAX);
    let store = snapshot_store_with_time_source(Box::new(move || now_ms));
    let orphan_sid = SnapshotId::from_u128(7);
    store
        .write_snapshot(
            orphan_sid,
            entries_channel(vec![entry(0)]),
            CompressionLevel::fast(),
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await
        .expect("write orphan");

    let report = store
        .cleanup_snapshots(BTreeSet::new(), TEST_RETENTION)
        .await
        .expect("ok");
    assert_eq!(report.snapshots_deleted.len(), 1);
}
