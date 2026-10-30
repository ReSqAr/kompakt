//! Loads object-store snapshots (referenced by the raft manifest) into the
//! learner's `stable_log`. Consumed by `spawn_learner` — a learner concern, not
//! a leader one (snapshot *creation* lives in `crate::leader::snapshot`).
//!
//! Snapshot *selection* lives entirely behind [`StableArchiveReader`]: this
//! loader only names an index window (`from` the current stable end, unbounded
//! above) and drains the one resulting stream into the stable log. It keeps the
//! manifest solely for its own bookkeeping — the completeness check and the
//! watermark advance — never to pick snapshots.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::api::{
    ArchiveBounds, ArchiveLowerBound, ArchiveUpperBound, RaftStateView, StableArchiveReader,
    StableLogSnapshot,
};
use crate::error::Error;
use crate::ids::LogIndex;
use crate::meta::RaftManifestState;

pub(in crate::learner) async fn integrate_manifest(
    archive: &dyn StableArchiveReader,
    stable_log: &dyn StableLogSnapshot,
    manifest_state: &RaftManifestState,
) -> Result<(), Error> {
    // Stream from just past the current stable end — or the earliest snapshot
    // when the log is empty. Snapshots below the lower bound are never opened.
    // `append` is the single gap fence: it runs the encode/commit/fsync loop on
    // its own blocking thread (CLAUDE.md §8), commits contiguous batches, and
    // refuses an input that jumps past `end + 1`, so a missing or partial
    // snapshot cannot land as a durable gap (docs/STABLE_LOG_GAP_FIX.md).
    let from = match stable_log.end_index() {
        Some(end) => ArchiveLowerBound::FromIndex(LogIndex::next_after(Some(end))),
        None => ArchiveLowerBound::Earliest,
    };
    // Debug, not info: this runs on every raft-state change, which on an idle
    // cluster is every cutoff tick.
    debug!(
        manifest_entries = manifest_state.manifest.len(),
        "integrating stable-log manifest from object store",
    );
    let rx = archive
        .stream(ArchiveBounds {
            from,
            until: ArchiveUpperBound::Unbounded,
        })
        .await?;
    stable_log.append(rx).await?;

    // Completeness (belt to `append`'s gap fence): `append` returning Ok proves
    // the stream ended, not that it covered the manifest. A snapshot that
    // short-loaded its tail (e.g. a stream ending early without an error item)
    // leaves the stable end below the manifest's covered end — abort so the
    // watermark advance below never claims coverage past a never-loaded tail.
    // The load stays resumable: committed batches persist and the next attempt
    // re-streams from the new end.
    let manifest_end = manifest_state.manifest.iter().map(|m| m.end_index).max();
    if let Some(target) = manifest_end {
        let loaded = stable_log.end_index();
        if loaded.is_none_or(|end| end < target) {
            return Err(Error::Storage(format!(
                "stable-log manifest integration incomplete: loaded end_index {:?} < manifest \
                 end_index {} — a snapshot stream ended early or a snapshot is missing; \
                 aborting manifest integration (resumable on the next attempt)",
                loaded.map(LogIndex::raw),
                target.raw(),
            )));
        }
    }

    if let Some(manifest_wm) = manifest_state.manifest_watermark
        && stable_log.end_hlc().is_none_or(|l| manifest_wm > l)
    {
        // After integrating a manifest, `end_hlc` always becomes its
        // watermark — even a watermark-only advance with no snapshot files. The
        // loaded index is just `end_index`, already advanced by the appends
        // above. This keeps the load path mechanical (no empty-manifest special
        // case) so downstream waiters can simply track `end_hlc`.
        stable_log.record_end_hlc(manifest_wm).await?;
    }

    Ok(())
}

/// Test-only ergonomic wrapper: build an [`ObjectStoreArchiveReader`] over a
/// handle seeded with `manifest_state` (the archive reader selects snapshots
/// from its raft manifest) and delegate. Lets leader/learner tests drive a load
/// from a bare snapshot store without constructing the reader themselves.
#[cfg(test)]
pub(crate) async fn integrate_manifest_for_test(
    snapshot_store: Arc<dyn crate::api::SnapshotStore>,
    stable_log: &dyn StableLogSnapshot,
    manifest_state: &RaftManifestState,
) -> Result<(), Error> {
    let archive = crate::learner::stable_archive::ObjectStoreArchiveReader::new(
        snapshot_store,
        seed_raft_state(manifest_state),
    );
    integrate_manifest(&archive, stable_log, manifest_state).await
}

/// A `RaftStateView` handle whose manifest is `manifest_state` — the archive
/// reader reads its covering snapshots from here.
#[cfg(test)]
fn seed_raft_state(manifest_state: &RaftManifestState) -> Arc<dyn RaftStateView> {
    let handle = crate::raft::RaftStateHandle::default();
    let mut rs = crate::meta::RaftState::empty();
    rs.manifest_state = manifest_state.clone();
    handle.replace(rs);
    Arc::new(handle)
}

pub(in crate::learner) struct SnapshotLoader {
    archive: Arc<dyn StableArchiveReader>,
    stable_log: Arc<dyn StableLogSnapshot>,
    raft_state: Arc<dyn RaftStateView>,
    cancel: CancellationToken,
}

impl SnapshotLoader {
    #[must_use]
    pub(in crate::learner) fn new(
        archive: Arc<dyn StableArchiveReader>,
        stable_log: Arc<dyn StableLogSnapshot>,
        raft_state: Arc<dyn RaftStateView>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            archive,
            stable_log,
            raft_state,
            cancel,
        }
    }

    /// Load the current manifest once, awaited at startup *before* the live
    /// subscription starts (`spawn_learner`). This establishes `end_hlc` so the
    /// local-store integrate gate skips the whole already-stable prefix instead
    /// of writing then purging it. Best-effort: `maybe_integrate` logs-and-
    /// continues on error, so a transient object-store failure just means the
    /// gate skips less and [`run`](Self::run) retries on the next manifest change.
    pub(in crate::learner) async fn load_initial(&self) {
        let initial = self.raft_state.snapshot();
        info!(
            manifest_entries = initial.manifest_state.manifest.len(),
            "snapshot loader: initial load before subscription",
        );
        self.maybe_integrate(&initial.manifest_state).await;
    }

    pub(in crate::learner) async fn run(self) -> Result<(), Error> {
        let mut rx = self.raft_state.subscribe();

        // Re-integrate the current state after subscribing to close the gap
        // between `load_initial` (at startup) and this subscription — idempotent
        // (already-loaded snapshots are skipped, `end_hlc` is monotonic).
        let initial = self.raft_state.snapshot();
        info!(
            manifest_entries = initial.manifest_state.manifest.len(),
            "snapshot loader: starting with initial manifest",
        );
        self.maybe_integrate(&initial.manifest_state).await;

        loop {
            let state = tokio::select! {
                () = self.cancel.cancelled() => {
                    info!("snapshot loader: shutdown");
                    return Ok(());
                }
                changed = rx.changed() => match changed {
                    Ok(()) => rx.borrow_and_update().clone(),
                    Err(_) => {
                        info!("snapshot loader: raft state channel closed");
                        return Ok(());
                    }
                },
            };
            self.maybe_integrate(&state.manifest_state).await;
        }
    }

    async fn maybe_integrate(&self, manifest_state: &RaftManifestState) {
        if let Err(e) = integrate_manifest(
            self.archive.as_ref(),
            self.stable_log.as_ref(),
            manifest_state,
        )
        .await
        {
            warn!(error = %e, "snapshot load failed");
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;
    use crate::api::{CleanupReport, SnapshotStore};

    use crate::hlc::Hlc;
    use crate::ids::{LogIndex, SnapshotId};
    use crate::learner::StableLog;
    use crate::learner::stable_archive::ObjectStoreArchiveReader;
    use crate::meta::{SnapshotEntry, SnapshotMetadata};
    use crate::object_store::CompressionLevel;
    use crate::tests::make_data;
    use std::sync::Arc;

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

    #[async_trait]
    impl SnapshotStore for ScriptedSnapshotStore {
        async fn write_snapshot(
            &self,
            _id: SnapshotId,
            _entries: crossbeam_combinators::Channels<Result<SnapshotEntry, Error>>,
            _compression: CompressionLevel,
            _reason: crate::metrics::ObjectStoreReason,
        ) -> Result<usize, Error> {
            unimplemented!("loader tests never write snapshots")
        }

        async fn read_snapshot(
            &self,
            id: SnapshotId,
            _reason: crate::metrics::ObjectStoreReason,
        ) -> Result<crossbeam_combinators::Channels<Result<SnapshotEntry, Error>>, Error> {
            self.reads.lock().expect("lock").push(id);
            let (entries, truncate_after) = self
                .snapshots
                .lock()
                .expect("lock")
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::ObjectStore(format!("no scripted snapshot {id:?}")))?;
            let deliver = truncate_after.unwrap_or(entries.len());
            Ok(crossbeam_combinators::Channels::from_vec(
                entries.into_iter().take(deliver).map(Ok).collect(),
            ))
        }

        fn shutdown_token(&self) -> CancellationToken {
            CancellationToken::new()
        }

        async fn has_bootstrap_fence(&self) -> Result<bool, Error> {
            unimplemented!("loader tests never touch the fence")
        }

        async fn write_bootstrap_fence(&self) -> Result<(), Error> {
            unimplemented!("loader tests never touch the fence")
        }

        async fn cleanup_snapshots(
            &self,
            _live_snapshots: BTreeSet<SnapshotId>,
            _cleanup_retention: Duration,
        ) -> Result<CleanupReport, Error> {
            unimplemented!("loader tests never clean up")
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
        let log = StableLog::open_engine(dir.path().join("stable.stable"))
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
            .range(LogIndex(0), LogIndex(100))
            .into_iter()
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
            matches!(err, Error::StableLogGap(ref m) if m.contains("archive stream")),
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
            matches!(err, Error::StableLogGap(ref m)
                if m.contains("archive stream") && m.contains("expected index 5")),
            "archive contiguity error expected, got: {err:?}"
        );
        assert_eq!(
            log.end_index(),
            Some(LogIndex(4)),
            "first snapshot (0..=4) committed; only the second snapshot's gap aborted"
        );
    }

    /// An HLC-non-monotonic snapshot (index ascends, HLC descends) is caught by
    /// the archive stream's monotonicity guard — the read path has no downstream
    /// HLC assert, so this must fire at the source.
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
            matches!(err, Error::Storage(ref m)
                if m.contains("archive stream reader panicked")),
            "HLC regression panics in spawn_blocking → Err sent on tx → append returns Err, got: {err:?}"
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
            .range(LogIndex(0), LogIndex(100))
            .into_iter()
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
}
