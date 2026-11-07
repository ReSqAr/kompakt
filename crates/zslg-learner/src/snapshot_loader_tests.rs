//! The snapshot loader against the real archive reader and a scripted snapshot
//! store: the integration of `zslg-stable-log` and the archive implementation.

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]
mod tests {
    use crossbeam_combinators::Channel;
    use crossbeam_combinators::testing::ChannelTestExt as _;
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Mutex;
    use std::time::Duration;
    use zslg_snapshot::SnapshotError;
    use zslg_snapshot::{CleanupReport, SnapshotStore, SnapshotWrite};

    use async_trait::async_trait;

    use crate::testing::seed_raft_state;
    use tokio_util::sync::CancellationToken;
    use zslg_consensus::RaftManifestState;
    use zslg_log::{LogError, StableLogEvent, StableLogSnapshot};
    use zslg_stable_log::Error;
    use zslg_stable_log::integrate_manifest;

    use crate::StableLog;
    use std::sync::Arc;
    use zslg_log::LogIndex;
    use zslg_log::SnapshotEntry;
    use zslg_log_fake::make_data;
    use zslg_snapshot::CompressionLevel;
    use zslg_snapshot::SnapshotId;
    use zslg_snapshot::SnapshotMetadata;
    use zslg_stable_archive::ObjectStoreArchiveReader;
    use zslg_time::Hlc;

    /// Build the production archive reader over a scripted store, seeded with
    /// `manifest_state` so its snapshot selection sees the same manifest the
    /// integration is driven with.
    fn archive_of(
        store: Arc<ScriptedSnapshotStore>,
        manifest_state: &RaftManifestState,
    ) -> ObjectStoreArchiveReader {
        ObjectStoreArchiveReader::new(store, seed_raft_state(manifest_state))
    }

    /// Per-snapshot script: the entries to deliver, and `truncate_after` — when
    /// `Some(k)`, deliver only the first `k` entries and then close the channel
    /// WITHOUT an error item. That is exactly what the pre-fix decode worker did
    /// when its cancellation error was dropped by a full channel: a silent
    /// truncation indistinguishable from a clean end-of-stream.
    type Script = (Vec<SnapshotEntry>, Option<usize>);

    struct ScriptedSnapshotStore {
        snapshots: Mutex<HashMap<SnapshotId, Script>>,
        reads: Mutex<Vec<SnapshotId>>,
    }

    impl ScriptedSnapshotStore {
        fn new() -> Self {
            Self {
                snapshots: Mutex::new(HashMap::new()),
                reads: Mutex::new(Vec::new()),
            }
        }

        fn script(
            &self,
            id: SnapshotId,
            entries: Vec<SnapshotEntry>,
            truncate_after: Option<usize>,
        ) {
            self.snapshots
                .lock()
                .expect("lock")
                .insert(id, (entries, truncate_after));
        }

        fn reads(&self) -> Vec<SnapshotId> {
            self.reads.lock().expect("lock").clone()
        }
    }

    impl ScriptedSnapshotStore {
        pub(crate) async fn write_snapshot_in(
            &self,
            _id: SnapshotId,
            _entries: crossbeam_combinators::Channel<
                Result<SnapshotEntry, Error>,
                crossbeam_combinators::Finite,
            >,
            _compression: CompressionLevel,
            _reason: zslg_snapshot::ObjectStoreReason,
        ) -> Result<zslg_snapshot::SnapshotWrite, Error> {
            unimplemented!("loader tests never write snapshots")
        }

        pub(crate) async fn read_snapshot_in(
            &self,
            id: SnapshotId,
            _reason: zslg_snapshot::ObjectStoreReason,
        ) -> Result<
            crossbeam_combinators::Channel<
                Result<SnapshotEntry, Error>,
                crossbeam_combinators::Finite,
            >,
            Error,
        > {
            self.reads.lock().expect("lock").push(id);
            let (entries, truncate_after) = self
                .snapshots
                .lock()
                .expect("lock")
                .get(&id)
                .cloned()
                .ok_or_else(|| {
                    Error::Snapshot(SnapshotError::ObjectStore(format!(
                        "no scripted snapshot {id:?}"
                    )))
                })?;
            let deliver = truncate_after.unwrap_or(entries.len());
            Ok(crossbeam_combinators::Channel::from_vec(
                entries.into_iter().take(deliver).map(Ok).collect(),
            ))
        }

        pub(crate) async fn has_bootstrap_fence_in(&self) -> Result<bool, Error> {
            unimplemented!("loader tests never touch the fence")
        }

        pub(crate) async fn write_bootstrap_fence_in(&self) -> Result<(), Error> {
            unimplemented!("loader tests never touch the fence")
        }

        pub(crate) async fn cleanup_snapshots_in(
            &self,
            _live_snapshots: BTreeSet<SnapshotId>,
            _cleanup_retention: Duration,
        ) -> Result<CleanupReport, Error> {
            unimplemented!("loader tests never clean up")
        }
    }

    #[async_trait]
    impl SnapshotStore for ScriptedSnapshotStore {
        async fn write_snapshot(
            &self,
            id: SnapshotId,
            entries: Channel<
                Result<SnapshotEntry, zslg_snapshot::Error>,
                crossbeam_combinators::Finite,
            >,
            compression: CompressionLevel,
            reason: zslg_snapshot::ObjectStoreReason,
        ) -> Result<SnapshotWrite, zslg_snapshot::Error> {
            self.write_snapshot_in(id, entries.err_into(), compression, reason)
                .await
                .map_err(Into::into)
        }
        async fn read_snapshot(
            &self,
            id: SnapshotId,
            reason: zslg_snapshot::ObjectStoreReason,
        ) -> Result<
            Channel<Result<SnapshotEntry, zslg_snapshot::Error>, crossbeam_combinators::Finite>,
            zslg_snapshot::Error,
        > {
            self.read_snapshot_in(id, reason)
                .await
                .map(Channel::err_into)
                .map_err(Into::into)
        }
        fn shutdown_token(&self) -> CancellationToken {
            CancellationToken::new()
        }

        async fn has_bootstrap_fence(&self) -> Result<bool, zslg_snapshot::Error> {
            self.has_bootstrap_fence_in().await.map_err(Into::into)
        }
        async fn write_bootstrap_fence(&self) -> Result<(), zslg_snapshot::Error> {
            self.write_bootstrap_fence_in().await.map_err(Into::into)
        }
        async fn cleanup_snapshots(
            &self,
            live_snapshots: std::collections::BTreeSet<SnapshotId>,
            cleanup_retention: std::time::Duration,
        ) -> Result<CleanupReport, zslg_snapshot::Error> {
            self.cleanup_snapshots_in(live_snapshots, cleanup_retention)
                .await
                .map_err(Into::into)
        }
    }

    fn entries(range: std::ops::RangeInclusive<u64>) -> Vec<SnapshotEntry> {
        range
            .map(|i| SnapshotEntry {
                global_index: LogIndex(i),
                data: make_data(u8::try_from(i).expect("small test index"), 100 + i, &[1]),
            })
            .collect()
    }

    fn meta(id: SnapshotId, start: u64, end: u64) -> SnapshotMetadata {
        SnapshotMetadata {
            start_hlc: Hlc(100 + start),
            end_hlc: Hlc(100 + end),
            start_index: LogIndex(start),
            end_index: LogIndex(end),
            snapshot_id: id,
        }
    }

    async fn fresh_stable_log() -> (tempfile::TempDir, StableLog) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = crate::testing::stable_log_engine(dir.path().join("stable.stable"))
            .await
            .expect("from_db");
        (dir, log)
    }

    /// The happy path: two snapshots that tile 0..=9 are streamed (selection
    /// handled inside the archive reader) and appended in index order; the
    /// watermark then advances to the manifest watermark.
    #[tokio::test]
    async fn integrate_streams_full_manifest_in_order() {
        let (_dir, log) = fresh_stable_log().await;
        let store = Arc::new(ScriptedSnapshotStore::new());
        let id_n = SnapshotId::from_u128(1);
        let id_m = SnapshotId::from_u128(2);
        store.script(id_n, entries(0..=4), None);
        store.script(id_m, entries(5..=9), None);

        let manifest_state = RaftManifestState {
            manifest: vec![meta(id_n, 0, 4), meta(id_m, 5, 9)],
            manifest_watermark: Some(Hlc(200)),
            hlc_cutoff: Some(Hlc(200)),
        };
        let archive = archive_of(store, &manifest_state);

        integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect("full manifest integrates");

        assert_eq!(log.end_index(), Some(LogIndex(9)));
        assert_eq!(log.end_hlc(), Some(Hlc(200)), "watermark advances");
        let indices: Vec<u64> = log
            .range(None, LogIndex(100))
            .items()
            .filter_map(|r| r.ok().map(|(i, _)| i.raw()))
            .collect();
        assert_eq!(indices, (0..=9).collect::<Vec<u64>>());
    }

    /// A snapshot stream that ends early WITHOUT an error item must abort the
    /// integration via the completeness check, never advancing `end_hlc` to the
    /// manifest watermark past never-loaded entries — even when it is the FINAL
    /// (only) snapshot, where there is no later index jump to trip `append`'s
    /// gap fence.
    #[tokio::test]
    async fn invariant_truncated_final_snapshot_does_not_advance_watermark() {
        let (_dir, log) = fresh_stable_log().await;
        let store = Arc::new(ScriptedSnapshotStore::new());
        let id_n = SnapshotId::from_u128(1);
        // The only snapshot covers 0..=4 but silently delivers only 0..=2.
        store.script(id_n, entries(0..=4), Some(3));

        let manifest_state = RaftManifestState {
            manifest: vec![meta(id_n, 0, 4)],
            manifest_watermark: Some(Hlc(200)),
            hlc_cutoff: Some(Hlc(200)),
        };
        let archive = archive_of(store, &manifest_state);

        let err = integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect_err("truncated final snapshot must abort integration");
        assert!(
            err.to_string().contains("incomplete"),
            "completeness error expected, got: {err}"
        );
        // The delivered prefix is durable, but the watermark stays put.
        assert_eq!(log.end_index(), Some(LogIndex(2)));
        assert_eq!(
            log.end_hlc(),
            Some(Hlc(101)),
            "watermark must not advance past the unloaded tail — nor claim the \
             last delivered entry's HLC, whose group the truncation may have split"
        );

        // Resumable: the full stream on retry completes the load and only then
        // advances the watermark.
        let store = Arc::new(ScriptedSnapshotStore::new());
        store.script(id_n, entries(0..=4), None);
        let archive = archive_of(store, &manifest_state);
        integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect("resumed integration succeeds");
        assert_eq!(log.end_index(), Some(LogIndex(4)));
        assert_eq!(log.end_hlc(), Some(Hlc(200)));
    }

    /// A manifest whose only snapshot does not connect to the current log (here:
    /// an empty log and a snapshot starting at index 5) is refused by `append`'s
    /// gap fence — a durable gap is never committed.
    #[tokio::test]
    async fn integrate_rejects_snapshot_not_connecting_to_log() {
        let (_dir, log) = fresh_stable_log().await;
        let store = Arc::new(ScriptedSnapshotStore::new());
        let id_m = SnapshotId::from_u128(2);
        store.script(id_m, entries(5..=9), None);

        let manifest_state = RaftManifestState {
            manifest: vec![meta(id_m, 5, 9)],
            manifest_watermark: Some(Hlc(200)),
            hlc_cutoff: Some(Hlc(200)),
        };
        let archive = archive_of(store, &manifest_state);

        let err = integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect_err("disconnected snapshot must be rejected");
        assert!(
            matches!(err, Error::Log(LogError::StreamTruncated { ref what }) if what == "archive tiling"),
            "archive-layer gap error expected, got: {err:?}"
        );
        assert_eq!(log.end_index(), None, "nothing committed above a hole");
        assert_eq!(log.end_hlc(), None, "watermark untouched");
    }

    /// Two covering snapshots that do not tile contiguously (0..=4 then 6..=9,
    /// index 5 missing) are caught by the archive stream's running contiguity
    /// guard — a `StableLogGap`, before anything past the hole is committed.
    #[tokio::test]
    async fn integrate_rejects_gap_between_snapshots() {
        let (_dir, log) = fresh_stable_log().await;
        let store = Arc::new(ScriptedSnapshotStore::new());
        let id_n = SnapshotId::from_u128(1);
        let id_m = SnapshotId::from_u128(2);
        store.script(id_n, entries(0..=4), None);
        store.script(id_m, entries(6..=9), None);

        let manifest_state = RaftManifestState {
            manifest: vec![meta(id_n, 0, 4), meta(id_m, 6, 9)],
            manifest_watermark: Some(Hlc(200)),
            hlc_cutoff: Some(Hlc(200)),
        };
        let archive = archive_of(store, &manifest_state);

        let err = integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect_err("a gap between covering snapshots must be rejected");
        assert!(
            matches!(err, Error::Log(LogError::StreamTruncated { ref what }) if what == "archive tiling"),
            "archive contiguity error expected, got: {err:?}"
        );
        assert_eq!(
            log.end_index(),
            Some(LogIndex(4)),
            "first snapshot (0..=4) committed; only the second snapshot's gap aborted"
        );
    }

    /// An HLC-non-monotonic snapshot (index ascends, HLC descends) is caught by
    /// the archive stream's `NonDecreasing<Hlc>` guard at the source, as an
    /// abort the loader refuses — not a panic.
    #[tokio::test]
    async fn integrate_rejects_hlc_regression_in_snapshot() {
        let (_dir, log) = fresh_stable_log().await;
        let store = Arc::new(ScriptedSnapshotStore::new());
        let id = SnapshotId::from_u128(1);
        // Index 0 at HLC 200, index 1 at HLC 100 — indices ascend, HLC regresses.
        let bad = vec![
            SnapshotEntry {
                global_index: LogIndex(0),
                data: make_data(1, 200, &[1]),
            },
            SnapshotEntry {
                global_index: LogIndex(1),
                data: make_data(2, 100, &[1]),
            },
        ];
        store.script(id, bad, None);

        let manifest_state = RaftManifestState {
            manifest: vec![meta(id, 0, 1)],
            manifest_watermark: Some(Hlc(200)),
            hlc_cutoff: Some(Hlc(200)),
        };
        let archive = archive_of(store, &manifest_state);

        let err = integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect_err("HLC regression must be rejected");
        assert!(
            matches!(err, Error::Log(LogError::StreamTruncated { ref what }) if what == "archive HLC"),
            "an HLC regression aborts the archive stream (no panic), got: {err:?}"
        );
        assert_eq!(
            log.end_index(),
            Some(LogIndex(0)),
            "entry 0 committed before HLC regression on entry 1"
        );
    }

    /// A truncated middle snapshot is resumable: once the store delivers the full
    /// stream, re-integration fills the missing tail (idempotently skipping what
    /// already landed) and only then advances the watermark.
    #[tokio::test]
    async fn truncated_snapshot_load_resumes_cleanly() {
        let (_dir, log) = fresh_stable_log().await;
        let id_n = SnapshotId::from_u128(1);
        let id_m = SnapshotId::from_u128(2);

        let manifest_state = RaftManifestState {
            manifest: vec![meta(id_n, 0, 4), meta(id_m, 5, 9)],
            manifest_watermark: Some(Hlc(200)),
            hlc_cutoff: Some(Hlc(200)),
        };

        // First attempt: N truncates at 0..=2, then M (5..=9) trips the gap
        // fence — nothing past the hole commits.
        let store = Arc::new(ScriptedSnapshotStore::new());
        store.script(id_n, entries(0..=4), Some(3));
        store.script(id_m, entries(5..=9), None);
        let archive = archive_of(store, &manifest_state);
        integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect_err("first attempt hits the gap");

        // The transient cause clears: the full stream is delivered on retry.
        let store = Arc::new(ScriptedSnapshotStore::new());
        store.script(id_n, entries(0..=4), None);
        store.script(id_m, entries(5..=9), None);
        let archive = archive_of(store, &manifest_state);
        integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect("resumed integration succeeds");

        assert_eq!(log.end_index(), Some(LogIndex(9)));
        assert_eq!(log.end_hlc(), Some(Hlc(200)), "watermark advances only now");

        // The log is complete and contiguous: every index 0..=9 present.
        let indices: Vec<u64> = log
            .range(None, LogIndex(100))
            .items()
            .filter_map(|r| r.ok().map(|(i, _)| i.raw()))
            .collect();
        assert_eq!(indices, (0..=9).collect::<Vec<u64>>());
    }

    /// Already-loaded snapshots below the current stable end are never opened:
    /// selection starts at `end + 1`, so a second integration of the same
    /// manifest only re-reads the straddling frontier snapshot (idempotent).
    #[tokio::test]
    async fn integrate_skips_already_loaded_snapshots() {
        let (_dir, log) = fresh_stable_log().await;
        let id_n = SnapshotId::from_u128(1);
        let id_m = SnapshotId::from_u128(2);

        let manifest_state = RaftManifestState {
            manifest: vec![meta(id_n, 0, 4), meta(id_m, 5, 9)],
            manifest_watermark: Some(Hlc(200)),
            hlc_cutoff: Some(Hlc(200)),
        };

        let store = Arc::new(ScriptedSnapshotStore::new());
        store.script(id_n, entries(0..=4), None);
        store.script(id_m, entries(5..=9), None);
        let archive = archive_of(store.clone(), &manifest_state);
        integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect("first load");
        assert_eq!(store.reads(), vec![id_n, id_m]);

        // Second integration: end is 9, so `from` is 10 — no snapshot has an
        // entry at or above it, nothing is opened.
        integrate_manifest(&archive, &log, &manifest_state)
            .await
            .expect("idempotent re-integration");
        assert_eq!(
            store.reads(),
            vec![id_n, id_m],
            "no snapshot re-read once fully loaded",
        );
    }

    #[tokio::test]
    async fn read_snapshot_then_append_matches() {
        use zslg_log::SnapshotEntry;
        use zslg_snapshot::CompressionLevel;
        use zslg_snapshot::SnapshotId;
        use zslg_snapshot::SnapshotStore as _;

        // End-to-end of the production load path: write a snapshot through the
        // public SnapshotStore API, then `read_snapshot` (off-reactor decode →
        // bounded channel) straight into `append` — no byte/framing detail here.
        let store = zslg_object_store::RemoteSnapshotStore::new(
            zslg_object_store::SnapshotStoreConfig {
                object_store: Arc::new(object_store::memory::InMemory::new()),
            },
            tokio_util::sync::CancellationToken::new(),
        );
        let id = SnapshotId::open(LogIndex(0));
        let entries: Vec<SnapshotEntry> = (0..5u64)
            .map(|i| SnapshotEntry {
                global_index: LogIndex(i),
                data: zslg_log_fake::make_data(1, 100 + i, b"x"),
            })
            .collect();
        store
            .write_snapshot(
                id,
                Channel::from_vec(entries.into_iter().map(Ok).collect()),
                CompressionLevel::standard(),
                zslg_snapshot::ObjectStoreReason::SnapshotCreate,
            )
            .await
            .expect("write_snapshot");

        let dir = tempfile::tempdir().expect("tempdir");
        let log = crate::testing::stable_log_engine(dir.path().join("s.stable"))
            .await
            .expect("open");
        let mut rx = log.subscribe_events();

        let entries_rx = store
            .read_snapshot(id, zslg_snapshot::ObjectStoreReason::ArchiveRead)
            .await
            .expect("read_snapshot");
        let n = log
            .append(entries_rx.err_into::<crate::Error>().err_into())
            .await
            .expect("append");
        assert_eq!(n, 5);
        assert_eq!(log.end_index(), Some(LogIndex(4)));
        assert_eq!(
            log.end_hlc(),
            Some(Hlc(103)),
            "end_hlc settles just below the max appended HLC, whose group may be partial"
        );

        // Causal order: the first event is the first Appended entry.
        let ev = rx
            .recv()
            .expect("the awaited append above queued this event");
        assert!(
            matches!(ev, StableLogEvent::Appended { index, .. } if index == LogIndex(0)),
            "first event must be Appended(0), got {ev:?}"
        );
    }
}
