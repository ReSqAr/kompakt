//! Snapshot objects in the object store — the cluster's durable, shared history
//! and the only state a new node needs to catch up.
//!
//! The seam is drawn so that *no* caller ever sees a byte of a snapshot:
//! framing, postcard and zstd all live behind it, and both directions speak
//! `SnapshotEntry` over a bounded channel. That is what keeps a
//! hundreds-of-megabyte object from ever being in memory whole, and it is why
//! neither side is a `Stream`: the codec runs in `spawn_blocking`, and the
//! consumer drains on its own blocking thread to accumulate whole write batches
//! — a reactor-side relay throttles that into a per-entry fsync storm
//! (docs/PERFORMANCE.md entry 23). A full channel blocking the sender *is* the
//! backpressure pacing producer to encoder.
//!
//! Objects are immutable and never overwritten (CLAUDE.md §11), so cleanup is a
//! separate, retention-delayed operation over ids the manifest no longer names.

use std::collections::BTreeSet;
use std::time::Duration;

use object_store::path::Path;
use tokio_util::sync::CancellationToken;

use crate::Error;
use crossbeam_combinators::Channel;
use zslg_log_types::SnapshotEntry;
use zslg_snapshot_types::CompressionLevel;
use zslg_snapshot_types::SnapshotId;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CleanupReport {
    pub(crate) snapshots_deleted: Vec<Path>,
}

/// Where an entry sits: its global index and HLC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EntryMeta {
    pub(crate) index: zslg_log_types::LogIndex,
    pub(crate) hlc: zslg_time_types::Hlc,
}

/// What a snapshot write produced: the entries and the object's sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotWrite {
    pub(crate) count: usize,
    /// The last entry written; `None` for an empty write.
    pub(crate) last: Option<EntryMeta>,
    pub(crate) stored_bytes: u64,
    pub(crate) uncompressed_bytes: u64,
}

#[async_trait::async_trait]
pub(crate) trait SnapshotStore: Send + Sync {
    /// Write a snapshot object from `entries`. The feeder must cancel on
    /// [`shutdown_token`](Self::shutdown_token).
    ///
    /// `reason` names *why* this transfer happens; it is a metric label, not
    /// behaviour. The caller states it because only the caller knows — see
    /// [`ObjectStoreReason`](zslg_snapshot_types::ObjectStoreReason).
    async fn write_snapshot(
        &self,
        id: SnapshotId,
        entries: Channel<Result<SnapshotEntry, Error>, crossbeam_combinators::Finite>,
        compression: CompressionLevel,
        reason: zslg_snapshot_types::ObjectStoreReason,
    ) -> Result<SnapshotWrite, Error>;

    /// Read a snapshot's decoded entries — the only way to get data out of a
    /// snapshot object. The outer `Result` is the open failure (e.g. missing
    /// snapshot); a per-record decode error arrives as an `Err` item.
    /// `reason` names *why* this transfer happens; it is a metric label, not
    /// behaviour. Reads run at a multiple of writes and the same immutable
    /// object is legitimately fetched many times, so the split is the only way
    /// to tell compaction re-reading its own output from learners re-fetching
    /// a purged prefix.
    async fn read_snapshot(
        &self,
        id: SnapshotId,
        reason: zslg_snapshot_types::ObjectStoreReason,
    ) -> Result<Channel<Result<SnapshotEntry, Error>, crossbeam_combinators::Finite>, Error>;

    /// The token this store's blocking workers cancel on. A feeder must cancel
    /// on *this* token, not its own: an encode worker parked on `blocking_recv`
    /// cannot observe shutdown itself, so only the feeder dropping its sender
    /// unblocks it — otherwise the worker wedges runtime drop at teardown
    /// (docs/PERFORMANCE.md entry 23).
    #[must_use]
    fn shutdown_token(&self) -> CancellationToken;

    async fn has_bootstrap_fence(&self) -> Result<bool, Error>;

    async fn write_bootstrap_fence(&self) -> Result<(), Error>;

    async fn cleanup_snapshots(
        &self,
        live_snapshots: BTreeSet<SnapshotId>,
        cleanup_retention: Duration,
    ) -> Result<CleanupReport, Error>;
}
