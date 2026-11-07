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

use crate::Error;
use crossbeam_combinators::{Contiguous, Finite, OrderedChannel};
use zslg_log_types::LogIndex;
use zslg_log_types::SnapshotEntry;

/// The archive stream: gapless indices, tiled from snapshots.
pub type ArchiveStream = OrderedChannel<Result<SnapshotEntry, Error>, Contiguous<LogIndex>, Finite>;

/// Upper bound of an archive stream. Because index order == HLC order in the
/// stable log, any upper bound prunes entries at or above it; the implementor
/// may use snapshot metadata to skip whole snapshots beyond the cap.
#[derive(Debug, Clone, Copy)]
pub enum ArchiveUpperBound {
    /// Everything the manifest tiles from the lower bound onward (the loader).
    Unbounded,
    /// Up to, but excluding, this global index.
    IndexExclusive(LogIndex),
}

/// The window an [`StableArchiveReader::stream`] call covers: `(after, until)`.
#[derive(Debug, Clone, Copy)]
pub struct ArchiveBounds {
    /// The last index already held (exclusive start); `None` = from the first.
    pub after: Option<LogIndex>,
    pub until: ArchiveUpperBound,
}

pub trait StableArchiveReader: Send + Sync {
    /// Archived entries within `bounds`, ascending by index, tiled from
    /// whichever snapshots cover them, opened lazily one at a time. A bounded
    /// window that the manifest does not tile ends `Aborted`, as does a gap or
    /// an HLC regression; an open or decode error arrives as an `Err` item.
    #[must_use]
    fn stream(&self, bounds: ArchiveBounds) -> ArchiveStream;
}
