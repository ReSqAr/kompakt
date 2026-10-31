//! Streaming snapshots to and from an object-store backend.
//!
//! Both directions are the same three-stage pipeline, mirrored:
//!
//! ```text
//!   write   entries ──> [bounded chan] ──> spawn_blocking ──> multipart parts
//!                                          encode + zstd
//!
//!   read    object  ──> byte stream    ──> spawn_blocking ──> [bounded chan] ──> entries
//!                                          zstd + decode
//! ```
//!
//! The `spawn_blocking` stage is not optional: zstd and postcard are CPU-bound,
//! and running them on a reactor worker stalls every other task on it
//! (docs/PERFORMANCE.md entry 23). The bounded channel is what paces the worker
//! to its counterpart, so a hundreds-of-MB snapshot never lands in memory whole
//! and peak usage is independent of snapshot size.
//!
//! Two consequences worth stating, because both are failure modes rather than
//! design notes:
//!
//! - **A partial write must be aborted, not left.** The format cannot express
//!   "incomplete", so an abandoned multipart upload's parts would be an object
//!   indistinguishable from a shorter snapshot.
//! - **A truncated read must reach the consumer as an error.** A dropped error
//!   looks exactly like a clean end-of-stream, and a consumer that keeps
//!   integrating would advance its watermark past entries it never loaded
//!   (CLAUDE.md §20; docs/STABLE_LOG_GAP_FIX.md Finding 2).
//!
//! Object names are injective in the [`SnapshotId`] and never reused, since with
//! no CAS available that is the only thing preventing an overwrite.

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::WriteMultipart;
use object_store::path::Path;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::Error;
use crate::api::{CleanupReport, SnapshotStore};
use crate::error::ToStorage as _;
use crate::ids::SnapshotId;
#[cfg(test)]
use crate::meta::ReplicaMembership;
use crate::meta::SnapshotEntry;
use crate::metrics::{self, ObjectStoreOp, OpState};
use crate::object_store::CompressionLevel;
use crate::object_store::framing;
use crossbeam_combinators::Channels;

const SNAPSHOT_PREFIX: &str = "snapshots";

/// Sync `io::Write` over a multipart upload — the write-side mirror of the read
/// path's [`SyncIoBridge`].
///
/// `WriteMultipart::put` buffers and uploads parts in the background, so `write`
/// never blocks on I/O and the upload is completed later by an async `finish()`
/// back on the reactor.
struct MultipartWriter {
    write: WriteMultipart,
    stored_bytes: u64,
}

impl io::Write for MultipartWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Infallible on any <=64-bit platform; surface (don't mask) the impossible
        // case rather than coercing to u64::MAX and tripping checked_add below.
        let len =
            u64::try_from(buf.len()).map_err(|_| io::Error::other("write buffer exceeds u64"))?;
        match self.stored_bytes.checked_add(len) {
            Some(total) => {
                self.stored_bytes = total;
                self.write.put(Bytes::copy_from_slice(buf));
                Ok(buf.len())
            }
            None => Err(io::Error::other("stored byte count overflow")),
        }
    }

    /// A no-op: `write` already handed everything to `WriteMultipart::put`, and
    /// only the async `finish()` completes the upload.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Decoded entries buffered between the decode worker and its consumer — the
/// read path's half of the header's bounded-channel pacing.
const SNAPSHOT_READ_CHANNEL_CAP: usize = 256;

#[must_use]
fn snapshot_path(id: SnapshotId) -> Path {
    Path::from(format!("{SNAPSHOT_PREFIX}/{}.snap", snapshot_name(id)))
}

/// `zslg_{start}_{end}_{random:016x}`, or `zslg_{start}_{random:016x}` when the
/// id carries no end index (a freshly created snapshot, minted before its
/// length is known). `start`/`end` are the inclusive global-index range, in
/// decimal zero-padded to 20 digits (`u64::MAX`'s width), so a bucket listing
/// sorts in log order and reads as a tiling of the stable log: merged
/// snapshots show the run they replaced, fresh ones where they begin.
///
/// The name embeds the *whole* id and the two shapes differ in field count, so
/// it stays injective — the only thing preventing an overwrite with no CAS. The
/// `zslg_` prefix says which system wrote the object; `.snap` is the only `.`.
#[must_use]
fn snapshot_name(id: SnapshotId) -> String {
    let start = id.start_index().raw();
    let random = id.random();
    match id.end_index() {
        Some(end) => format!("zslg_{start:020}_{:020}_{random:016x}", end.raw()),
        None => format!("zslg_{start:020}_{random:016x}"),
    }
}

#[must_use]
fn bootstrap_fence_path() -> Path {
    Path::from("raft-bootstrap-fence")
}

fn make_fence_body() -> Bytes {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    Bytes::from(format!("raft-bootstrap-fence-{now_millis}\n"))
}

/// The snapshot-store constructor args, produced by the owning node config.
pub(crate) struct SnapshotStoreConfig {
    pub object_store: Arc<dyn ObjectStore>,
}

pub(crate) struct RemoteSnapshotStore {
    store: Arc<dyn ObjectStore>,
    time_source: Box<dyn Fn() -> u64 + Send + Sync>,
    /// Fired on node shutdown. Dropping a runtime waits for in-flight blocking
    /// tasks, so a decode worker parked on `blocking_send` to a consumer that
    /// has stopped being polled would wedge teardown outright. Mandatory in
    /// [`new`](Self::new) rather than optional, so a node cannot forget it.
    cancel: CancellationToken,
}

fn wall_clock_ms_now() -> u64 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(ms).unwrap_or(u64::MAX)
}

impl RemoteSnapshotStore {
    /// `cancel` is the node's shutdown token (mandatory): it lets an in-flight
    /// `read_snapshot` decode worker stop promptly instead of blocking runtime drop.
    #[must_use]
    pub(crate) fn new(config: SnapshotStoreConfig, cancel: CancellationToken) -> Self {
        Self::with_time_source(config.object_store, Box::new(wall_clock_ms_now), cancel)
    }

    #[must_use]
    fn with_time_source(
        store: Arc<dyn ObjectStore>,
        time_source: Box<dyn Fn() -> u64 + Send + Sync>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            store,
            time_source,
            cancel,
        }
    }

    /// The `?`-driven core of [`SnapshotStore::write_snapshot`], returning
    /// `(record_count, stored_bytes, uncompressed_body_bytes)`.
    async fn write_snapshot_inner(
        &self,
        path: &Path,
        mut entries: Channels<Result<SnapshotEntry, Error>>,
        compression: CompressionLevel,
    ) -> Result<(usize, u64, u64), Error> {
        let upload = self
            .store
            .put_multipart(path)
            .await
            .map_err(|e| Error::ObjectStore(format!("put_multipart {path}: {e}")))?;

        let cancel = self.cancel.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let writer = MultipartWriter {
                write: WriteMultipart::new(upload),
                stored_bytes: 0,
            };
            framing::encode_framed_records_streaming::<SnapshotEntry, _>(
                writer,
                compression,
                move || {
                    if cancel.is_cancelled() {
                        return Some(Err(Error::Storage("snapshot write cancelled".into())));
                    }
                    match entries.recv() {
                        Ok(item) => Some(item),
                        Err(crossbeam_combinators::End::Complete) => None,
                        // No declared completion, so this prefix must not be
                        // finalized as if it were the whole snapshot (§20).
                        Err(crossbeam_combinators::End::Aborted { reason }) => {
                            Some(Err(Error::StreamTruncated {
                                what: reason.label().to_owned(),
                            }))
                        }
                    }
                },
            )
        });

        let (mw_opt, encoded) = worker.await.to_storage("snapshot write worker panicked")?;

        match encoded {
            Ok((count, uncompressed)) => {
                let mw = mw_opt.ok_or_else(|| {
                    Error::Storage("snapshot encode succeeded but returned no writer".into())
                })?;
                // `finish(self)` consumes it, so read the count first.
                let stored = mw.stored_bytes;
                mw.write
                    .finish()
                    .await
                    .map_err(|e| Error::ObjectStore(format!("multipart finish {path}: {e}")))?;
                Ok((count, stored, uncompressed))
            }
            Err(e) => {
                // Best-effort: a failed abort just leaves the parts to the
                // object-store cleanup pass, which is where they would have gone
                // anyway had the writer been unrecoverable.
                if let Some(mw) = mw_opt
                    && let Err(ae) = mw.write.abort().await
                {
                    warn!(?path, error = %ae,
                        "aborting partial snapshot upload failed; parts left for cleanup");
                }
                Err(e)
            }
        }
    }
}

#[async_trait::async_trait]
impl SnapshotStore for RemoteSnapshotStore {
    async fn write_snapshot(
        &self,
        id: SnapshotId,
        entries: Channels<Result<SnapshotEntry, Error>>,
        compression: CompressionLevel,
        reason: crate::metrics::ObjectStoreReason,
    ) -> Result<usize, Error> {
        let path = snapshot_path(id);

        let probe = metrics::object_store_request_probe(ObjectStoreOp::Put);
        let result = self.write_snapshot_inner(&path, entries, compression).await;
        probe.finish(OpState::from_ok(result.is_ok()));

        let (entry_count, stored, uncompressed) = result?;
        metrics::object_store_transfer_bytes(ObjectStoreOp::Put, reason, stored);
        metrics::snapshot_compression_ratio(uncompressed, stored);
        crate::metrics::snapshot_size(stored);
        Ok(entry_count)
    }

    async fn read_snapshot(
        &self,
        id: SnapshotId,
        reason: crate::metrics::ObjectStoreReason,
    ) -> Result<Channels<Result<SnapshotEntry, Error>>, Error> {
        let path = snapshot_path(id);

        let probe = metrics::object_store_request_probe(ObjectStoreOp::Get);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|e| Error::ObjectStore(format!("get {path}: {e}")));
        probe.finish(OpState::from_ok(result.is_ok()));
        let get_result = result?;
        let byte_len = get_result.meta.size;
        if byte_len > 0 {
            metrics::object_store_transfer_bytes(ObjectStoreOp::Get, reason, byte_len);
        }
        let byte_stream = get_result
            .into_stream()
            .map_err(|e| std::io::Error::other(e.to_string()));

        let sync_reader = SyncIoBridge::new(StreamReader::new(byte_stream));
        let cancel = self.cancel.clone();
        let (tx, rx) = crossbeam_channel::bounded(SNAPSHOT_READ_CHANNEL_CAP);
        let (ch, closer) = Channels::from(rx, "read_snapshot");
        tokio::task::spawn_blocking(move || {
            let res = crate::object_store::framing::decode_framed_records_streaming::<
                SnapshotEntry,
                _,
            >(sync_reader, |entry: SnapshotEntry| {
                // Send first, *then* check for cancellation: the blocking send is
                // the backpressure, and checking before it would spin.
                tx.send(Ok(entry))
                    .map_err(|_| Error::Storage("snapshot read: consumer dropped".into()))?;
                if cancel.is_cancelled() {
                    return Err(Error::Storage("snapshot read cancelled".into()));
                }
                Ok(())
            });
            if let Err(e) = res {
                // Unconditional, cancelled or not — see the header. It cannot wedge
                // shutdown: a consumer either drains to close or drops its receiver,
                // and the latter makes this fail fast.
                let _ = tx.send(Err(e));
            }
            // Reached only on the normal path: a panic drops `closer` instead,
            // leaving the consumer with `End::Aborted`.
            closer.close();
        });
        Ok(ch)
    }

    fn shutdown_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    async fn has_bootstrap_fence(&self) -> Result<bool, Error> {
        let path = bootstrap_fence_path();
        let probe = metrics::object_store_request_probe(ObjectStoreOp::Head);
        let result = match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::ObjectStore(format!("head {path}: {e}"))),
        };
        probe.finish(OpState::from_ok(result.is_ok()));
        result
    }

    #[allow(
        clippy::expect_used,
        reason = "fence body is a small constant string, len fits u64"
    )]
    async fn write_bootstrap_fence(&self) -> Result<(), Error> {
        let path = bootstrap_fence_path();
        let body = make_fence_body();
        let byte_len = u64::try_from(body.len()).expect("fence body fits u64");
        let probe = metrics::object_store_request_probe(ObjectStoreOp::Put);
        let result: Result<(), Error> = self
            .store
            .put(&path, body.into())
            .await
            .map(|_put_result| ())
            .map_err(|e| Error::ObjectStore(format!("put {path}: {e}")));
        let ok = result.is_ok();
        probe.finish(OpState::from_ok(ok));
        if ok {
            metrics::object_store_transfer_bytes(
                ObjectStoreOp::Put,
                crate::metrics::ObjectStoreReason::Bootstrap,
                byte_len,
            );
        }
        result
    }

    async fn cleanup_snapshots(
        &self,
        live_snapshots: BTreeSet<SnapshotId>,
        cleanup_retention: Duration,
    ) -> Result<CleanupReport, Error> {
        let live_snapshot_paths: BTreeSet<Path> =
            live_snapshots.iter().copied().map(snapshot_path).collect();

        let mut snapshots_deleted = Vec::new();
        let snapshot_prefix = Path::from(SNAPSHOT_PREFIX);

        let snapshot_orphans = list_orphans(
            &self.store,
            &snapshot_prefix,
            &live_snapshot_paths,
            cleanup_retention,
            &self.time_source,
        )
        .await?;

        for path in snapshot_orphans {
            match self.store.delete(&path).await {
                Ok(()) => snapshots_deleted.push(path),
                Err(e) => warn!(?path, error = %e, "failed to delete orphaned snapshot"),
            }
        }

        info!(
            snapshots = snapshots_deleted.len(),
            live_count = live_snapshots.len(),
            "cleanup tick complete",
        );
        Ok(CleanupReport { snapshots_deleted })
    }
}

use futures::StreamExt as _;

async fn list_orphans(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    keep: &BTreeSet<Path>,
    retention: Duration,
    time_source: &(dyn Fn() -> u64 + Send + Sync),
) -> Result<Vec<Path>, Error> {
    let now_ms = time_source();
    let now = UNIX_EPOCH
        .checked_add(Duration::from_millis(now_ms))
        .unwrap_or(SystemTime::now());
    let cutoff = now.checked_sub(retention).unwrap_or(now);

    let mut orphans = Vec::new();
    let mut stream = store.list(Some(prefix));
    while let Some(item) = stream.next().await {
        let meta = item.map_err(|e| Error::ObjectStore(format!("list under {prefix}: {e}")))?;
        if keep.contains(&meta.location) {
            continue;
        }
        let modified: SystemTime = meta.last_modified.into();
        if modified > cutoff {
            debug!(
                ?meta.location,
                ?modified,
                ?cutoff,
                "skipping recent orphan candidate",
            );
            continue;
        }
        orphans.push(meta.location);
    }
    Ok(orphans)
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
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use object_store::memory::InMemory;

    use super::*;
    use crate::hlc::Hlc;
    use crate::ids::LogIndex;
    use crate::meta::SnapshotMetadata;
    use crate::object_store::CompressionLevel;
    use crate::raft::RaftStateHandle;

    /// Retention for the cleanup tests below. `cleanup_snapshots` takes it
    /// per-call rather than reading it off the store.
    const TEST_RETENTION: Duration = Duration::from_secs(60);

    /// Pre-load a bounded channel with `records` for `write_snapshot`, the test-side
    /// mirror of the production producer tasks. The capacity covers every record so
    /// `try_send` never blocks.
    fn entries_channel(records: Vec<SnapshotEntry>) -> Channels<Result<SnapshotEntry, Error>> {
        Channels::from_vec(records.into_iter().map(Ok).collect())
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

    use super::super::test_entry as entry;

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
                crate::metrics::ObjectStoreReason::SnapshotCreate,
            )
            .await
            .expect("write");
        assert_eq!(n, records.len());
        let rx = store
            .read_snapshot(id, crate::metrics::ObjectStoreReason::ArchiveRead)
            .await
            .expect("read");
        let got: Vec<SnapshotEntry> = rx.into_iter().filter_map(|r| r.ok()).collect();
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
                crate::metrics::ObjectStoreReason::SnapshotCreate,
            )
            .await
            .expect("write");
        assert_eq!(n, records.len());
        let rx = store
            .read_snapshot(id, crate::metrics::ObjectStoreReason::ArchiveRead)
            .await
            .expect("read");
        let got: Vec<SnapshotEntry> = rx.into_iter().filter_map(|r| r.ok()).collect();
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
                crate::metrics::ObjectStoreReason::SnapshotCreate,
            )
            .await
            .expect("write");
        assert_eq!(n, 0);
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
        tx.try_send(Err(Error::Storage("injected mid-write failure".into())))
            .unwrap();
        drop(tx);

        let result = store
            .write_snapshot(
                id,
                Channels::closed(rx),
                CompressionLevel::standard(),
                crate::metrics::ObjectStoreReason::SnapshotCreate,
            )
            .await;
        assert!(
            result.is_err(),
            "encode error must propagate from write_snapshot"
        );

        // Upload aborted (not finished) → no snapshot object remains at the path.
        assert!(
            store
                .read_snapshot(id, crate::metrics::ObjectStoreReason::ArchiveRead)
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
                crate::metrics::ObjectStoreReason::SnapshotCreate,
            )
            .await
            .expect("write");

        // Cancel BEFORE reading: the decode worker notices right after its first
        // delivered entry and must terminate the stream with an Err item.
        cancel.cancel();
        let mut rx = store
            .read_snapshot(id, crate::metrics::ObjectStoreReason::ArchiveRead)
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
                crate::metrics::ObjectStoreReason::ArchiveRead,
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
                crate::api::ReplicatedRaftState {
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
                crate::ids::RaftGeneration(1),
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
                crate::metrics::ObjectStoreReason::SnapshotCreate,
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
                crate::metrics::ObjectStoreReason::SnapshotCreate,
            )
            .await
            .expect("write orphan");

        let live_sid = SnapshotId::from_u128(1);
        store
            .write_snapshot(
                live_sid,
                entries_channel(vec![entry(1)]),
                CompressionLevel::fast(),
                crate::metrics::ObjectStoreReason::SnapshotCreate,
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
            .read_snapshot(live_sid, crate::metrics::ObjectStoreReason::ArchiveRead)
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
                crate::metrics::ObjectStoreReason::SnapshotCreate,
            )
            .await
            .expect("write orphan");

        let report = store
            .cleanup_snapshots(BTreeSet::new(), TEST_RETENTION)
            .await
            .expect("ok");
        assert_eq!(report.snapshots_deleted.len(), 1);
    }
}
