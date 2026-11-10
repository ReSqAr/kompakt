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
use zslg_log::LogError;
use zslg_snapshot::SnapshotError;
use zslg_snapshot::{CleanupReport, EntryMeta, SnapshotStore, SnapshotWrite};

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

use crate::framing;
use crate::metrics::{self, ObjectStoreOp};
use crossbeam_combinators::Channel;
use metrics_kit::OpState;
use zslg_log::SnapshotEntry;
use zslg_log::ToStorage as _;
use zslg_snapshot::CompressionLevel;
use zslg_snapshot::SnapshotId;

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
pub struct SnapshotStoreConfig {
    pub object_store: Arc<dyn ObjectStore>,
}

pub struct RemoteSnapshotStore {
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
    pub fn new(config: SnapshotStoreConfig, cancel: CancellationToken) -> Self {
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
        mut entries: Channel<Result<SnapshotEntry, Error>, crossbeam_combinators::Finite>,
        compression: CompressionLevel,
    ) -> Result<SnapshotWrite, Error> {
        let upload = self.store.put_multipart(path).await.map_err(|e| {
            Error::Snapshot(SnapshotError::ObjectStore(format!(
                "put_multipart {path}: {e}"
            )))
        })?;

        let cancel = self.cancel.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let writer = MultipartWriter {
                write: WriteMultipart::new(upload),
                stored_bytes: 0,
            };
            let mut last: Option<EntryMeta> = None;
            let encoded = framing::encode_framed_records_streaming::<SnapshotEntry, _>(
                writer,
                compression,
                || {
                    if cancel.is_cancelled() {
                        return Some(Err(Error::Log(LogError::Storage(
                            "snapshot write cancelled".into(),
                        ))));
                    }
                    match entries.recv() {
                        Ok(item) => {
                            if let Ok(entry) = &item {
                                last = Some(EntryMeta {
                                    index: entry.global_index,
                                    hlc: entry.data.data_id.hlc(),
                                });
                            }
                            Some(item)
                        }
                        Err(crossbeam_combinators::End::Complete) => None,
                        // No declared completion, so this prefix must not be
                        // finalized as if it were the whole snapshot (§20).
                        Err(crossbeam_combinators::End::Aborted { reason }) => {
                            Some(Err(Error::Log(LogError::StreamTruncated {
                                what: reason.label().to_owned(),
                            })))
                        }
                    }
                },
            );
            (encoded, last)
        });

        let ((mw_opt, encoded), last) =
            worker.await.to_storage("snapshot write worker panicked")?;

        match encoded {
            Ok((count, uncompressed)) => {
                let mw = mw_opt.ok_or_else(|| {
                    Error::Log(LogError::Storage(
                        "snapshot encode succeeded but returned no writer".into(),
                    ))
                })?;
                // `finish(self)` consumes it, so read the count first.
                let stored = mw.stored_bytes;
                mw.write.finish().await.map_err(|e| {
                    Error::Snapshot(SnapshotError::ObjectStore(format!(
                        "multipart finish {path}: {e}"
                    )))
                })?;
                Ok(SnapshotWrite {
                    count,
                    last,
                    stored_bytes: stored,
                    uncompressed_bytes: uncompressed,
                })
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
        entries: Channel<Result<SnapshotEntry, Error>, crossbeam_combinators::Finite>,
        compression: CompressionLevel,
        reason: zslg_snapshot::ObjectStoreReason,
    ) -> Result<SnapshotWrite, Error> {
        let path = snapshot_path(id);

        let probe = metrics::object_store_request_probe(ObjectStoreOp::Put);
        let result = self.write_snapshot_inner(&path, entries, compression).await;
        probe.finish(OpState::from_ok(result.is_ok()));

        let written = result?;
        metrics::object_store_transfer_bytes(ObjectStoreOp::Put, reason, written.stored_bytes);
        metrics::snapshot_compression_ratio(written.uncompressed_bytes, written.stored_bytes);
        crate::metrics::snapshot_size(written.stored_bytes);
        Ok(written)
    }

    async fn read_snapshot(
        &self,
        id: SnapshotId,
        reason: zslg_snapshot::ObjectStoreReason,
    ) -> Result<Channel<Result<SnapshotEntry, Error>, crossbeam_combinators::Finite>, Error> {
        let path = snapshot_path(id);

        let probe = metrics::object_store_request_probe(ObjectStoreOp::Get);
        let result =
            self.store.get(&path).await.map_err(|e| {
                Error::Snapshot(SnapshotError::ObjectStore(format!("get {path}: {e}")))
            });
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
        let ch =
            Channel::produce_blocking(SNAPSHOT_READ_CHANNEL_CAP, "read_snapshot", move |feed| {
                let res = crate::framing::decode_framed_records_streaming::<SnapshotEntry, _>(
                    sync_reader,
                    |entry: SnapshotEntry| {
                        // Send first, *then* check for cancellation: the blocking send is
                        // the backpressure, and checking before it would spin.
                        if feed.send(Ok(entry)).is_err() {
                            return Err(Error::Log(LogError::Storage(
                                "snapshot read: consumer dropped".into(),
                            )));
                        }
                        if cancel.is_cancelled() {
                            return Err(Error::Log(LogError::Storage(
                                "snapshot read cancelled".into(),
                            )));
                        }
                        Ok(())
                    },
                );
                if let Err(e) = res {
                    // Unconditional, cancelled or not — see the header. It cannot wedge
                    // shutdown: a consumer either drains to close or drops its receiver,
                    // and the latter makes this fail fast.
                    let _ = feed.send(Err(e));
                }
                feed.close()
            });
        Ok(ch)
    }

    async fn has_bootstrap_fence(&self) -> Result<bool, Error> {
        let path = bootstrap_fence_path();
        let probe = metrics::object_store_request_probe(ObjectStoreOp::Head);
        let result = match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::Snapshot(SnapshotError::ObjectStore(format!(
                "head {path}: {e}"
            )))),
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
            .map_err(|e| Error::Snapshot(SnapshotError::ObjectStore(format!("put {path}: {e}"))));
        let ok = result.is_ok();
        probe.finish(OpState::from_ok(ok));
        if ok {
            metrics::object_store_transfer_bytes(
                ObjectStoreOp::Put,
                zslg_snapshot::ObjectStoreReason::Bootstrap,
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

    fn shutdown_token(&self) -> CancellationToken {
        self.cancel.clone()
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
        let meta = item.map_err(|e| {
            Error::Snapshot(SnapshotError::ObjectStore(format!(
                "list under {prefix}: {e}"
            )))
        })?;
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
mod tests;
