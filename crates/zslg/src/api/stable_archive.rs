//! The archived (purged) prefix of the stable log, read just-in-time from
//! object-store snapshots. When the stable log retires an old index prefix
//! locally (TTL purge), a read that reaches into that prefix is served through
//! this seam instead, so the caller still sees one gapless, index-ordered
//! stream (design doc §794).
//!
//! Implementors own *all* of the object-store detail: which manifest snapshots
//! tile the requested window, opening and decoding them, and clamping to the
//! bounds. Callers specify only the [`ArchiveBounds`] — never a snapshot id or a
//! manifest — so snapshot selection lives in exactly one place.

use crate::channel::Channels;
use crate::error::Error;
use crate::ids::LogIndex;
use crate::meta::SnapshotEntry;

/// Inclusive lower bound of an archive stream.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ArchiveLowerBound {
    /// Start at the earliest entry any covering snapshot holds.
    Earliest,
    /// Start at this global index (inclusive).
    FromIndex(LogIndex),
}

/// Upper bound of an archive stream. Because index order == HLC order in the
/// stable log, any upper bound prunes entries at or above it; the implementor
/// may use snapshot metadata to skip whole snapshots beyond the cap.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ArchiveUpperBound {
    /// Everything the manifest tiles from the lower bound onward (the loader).
    Unbounded,
    /// Up to, but excluding, this global index.
    IndexExclusive(LogIndex),
}

/// The window an [`StableArchiveReader::stream`] call covers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ArchiveBounds {
    pub(crate) from: ArchiveLowerBound,
    pub(crate) until: ArchiveUpperBound,
}

#[async_trait::async_trait]
pub(crate) trait StableArchiveReader: Send + Sync {
    /// Archived entries within `bounds`, ascending by index, tiled from
    /// whichever snapshots cover them. A bounded channel rather than a `Stream`
    /// so the consumer can drain on its own blocking thread and batch commits
    /// (CLAUDE.md §8) — the idiom of
    /// [`SnapshotStore::read_snapshot`](crate::api::SnapshotStore::read_snapshot).
    ///
    /// The outer `Result` is a setup failure (nothing to stream); a per-snapshot
    /// open or decode error arrives as an `Err` item, so a consumer that fences
    /// on the first error still aborts cleanly.
    async fn stream(
        &self,
        bounds: ArchiveBounds,
    ) -> Result<Channels<Result<SnapshotEntry, Error>>, Error>;
}
