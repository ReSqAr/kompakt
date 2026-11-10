//! Snapshots: identity, metadata, compression and compaction vocabulary, and the errors of keeping them.

use metrics_kit::label_enum;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use zslg_lifecycle_types::ConfigError;
use zslg_log_types::LogIndex;
use zslg_time_types::Hlc;

/// Snapshot identifier: the first global index the snapshot covers, the last
/// one when it was known at mint time, and a 64-bit random suffix.
///
/// The indices are there for whoever browses the bucket — the object name
/// renders them (`object_store::snapshot_store::snapshot_name`), so the store
/// reads as a tiling of the stable log. A freshly created snapshot has no
/// `end_index`: its id is minted before the window is streamed, so its length
/// is not yet known. A compaction-merged one does, since it replaces a known run
/// of manifest entries. Either way the manifest's [`SnapshotMetadata`]
/// is the authority on what a snapshot covers; the id only names it.
///
/// The random suffix is what makes every minted id globally unique: two
/// leaders racing over the same window (a crashed or deposed one included)
/// mint the same start index, and the no-CAS, immutable, never-overwrite object
/// store (CLAUDE.md §11) must not let the loser clobber the committed object.
///
/// Ordering is `(start_index, end_index, random)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SnapshotId {
    start_index: LogIndex,
    end_index: Option<LogIndex>,
    random: u64,
}

impl SnapshotId {
    /// Assemble from explicit parts.
    #[must_use]
    pub fn from_parts(start_index: LogIndex, end_index: Option<LogIndex>, random: u64) -> Self {
        Self {
            start_index,
            end_index,
            random,
        }
    }

    /// A fresh id for a snapshot whose extent is known only from its start —
    /// the snapshot-creation path, which mints before streaming the window.
    #[must_use]
    pub fn open(start_index: LogIndex) -> Self {
        Self::from_parts(start_index, None, rand::random())
    }

    /// A fresh id for a snapshot covering exactly `start_index..=end_index` —
    /// the compaction-merge path.
    #[must_use]
    pub fn closed(start_index: LogIndex, end_index: LogIndex) -> Self {
        Self::from_parts(start_index, Some(end_index), rand::random())
    }

    /// The first global index the snapshot covers.
    #[must_use]
    pub fn start_index(self) -> LogIndex {
        self.start_index
    }

    /// The last global index the snapshot covers, if known when it was minted.
    #[must_use]
    pub fn end_index(self) -> Option<LogIndex> {
        self.end_index
    }

    /// The random suffix.
    #[must_use]
    pub fn random(self) -> u64 {
        self.random
    }

    /// Test shorthand: the high 64 bits become the start index, the low 64 the
    /// random suffix, with no end index.
    #[must_use]
    #[doc(hidden)]
    #[allow(clippy::expect_used, reason = "each half of a u128 fits u64")]
    pub fn from_u128(val: u128) -> Self {
        let high = u64::try_from(val >> 64).expect("high half fits u64");
        let low = u64::try_from(val & u128::from(u64::MAX)).expect("low half fits u64");
        Self::from_parts(LogIndex(high), None, low)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub start_hlc: Hlc,
    pub end_hlc: Hlc,
    pub start_index: LogIndex,
    pub end_index: LogIndex,
    pub snapshot_id: SnapshotId,
}

/// How hard zstd works on a snapshot object. The *algorithm* is not a choice —
/// snapshot objects are always one streaming zstd frame — so this carries only
/// the level, `0..=22`, rising from cheapest to densest.
///
/// The range is checked once, here, so every downstream consumer can hand the
/// value straight to zstd without re-validating.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionLevel(u8);

impl CompressionLevel {
    /// zstd's densest setting. Levels above this are rejected by the library.
    pub const MAX: u8 = 22;

    /// Level 1 — the cheapest setting that actually compresses. Note that level
    /// *0* is not faster: zstd reads it as "use the library default", which is
    /// [`Self::standard`].
    #[must_use]
    pub const fn fast() -> Self {
        Self(1)
    }

    /// Level 3 — zstd's own default, and ours.
    #[must_use]
    pub const fn standard() -> Self {
        Self(3)
    }

    /// `Err(Error::InvalidConfiguration)` above [`Self::MAX`].
    pub fn new(level: u8) -> Result<Self, ConfigError> {
        if level > Self::MAX {
            return Err(ConfigError::InvalidConfiguration(format!(
                "compression zstd level must be 0–{}, got {level}",
                Self::MAX
            )));
        }
        Ok(Self(level))
    }

    /// The level in zstd's own signed representation. Widening is infallible.
    #[must_use]
    pub fn get(self) -> i32 {
        i32::from(self.0)
    }
}

impl Default for CompressionLevel {
    fn default() -> Self {
        Self::standard()
    }
}

/// A time window; snapshots sharing a bucket at this width may merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompactionTier(Duration);

impl CompactionTier {
    #[must_use]
    pub fn as_millis(&self) -> u64 {
        zslg_time_types::duration_as_ms_clamped(self.0)
    }

    #[must_use]
    pub fn as_duration(&self) -> Duration {
        self.0
    }
}

impl From<Duration> for CompactionTier {
    fn from(d: Duration) -> Self {
        Self(d)
    }
}

impl std::ops::Add<Duration> for CompactionTier {
    type Output = Self;
    fn add(self, rhs: Duration) -> Self {
        Self(self.0.saturating_add(rhs))
    }
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
    use super::*;

    // ── SnapshotId ──────────────────────────────────────────────────────

    #[test]
    fn snapshot_id_fresh_ids_are_unique() {
        // The random suffix means two ids minted over the *same* range differ —
        // this is what keeps two leaders racing over one window (or one merge
        // group) from ever writing the same object (the no-CAS overwrite hazard).
        assert_ne!(SnapshotId::open(LogIndex(0)), SnapshotId::open(LogIndex(0)));
        assert_ne!(
            SnapshotId::closed(LogIndex(4), LogIndex(9)),
            SnapshotId::closed(LogIndex(4), LogIndex(9))
        );
    }

    #[test]
    fn snapshot_id_parts_roundtrip() {
        let id = SnapshotId::from_parts(LogIndex(7), Some(LogIndex(11)), 0xcafe_babe_dead_beef);
        assert_eq!(id.start_index(), LogIndex(7));
        assert_eq!(id.end_index(), Some(LogIndex(11)));
        assert_eq!(id.random(), 0xcafe_babe_dead_beef);
        let open = SnapshotId::open(LogIndex(3));
        assert_eq!(open.start_index(), LogIndex(3));
        assert_eq!(open.end_index(), None);
    }

    #[test]
    fn snapshot_id_ordering_follows_start_index() {
        let a = SnapshotId::from_parts(LogIndex(1), Some(LogIndex(u64::MAX)), u64::MAX);
        let b = SnapshotId::from_parts(LogIndex(2), None, 0);
        assert!(a < b);
    }
}

/// Snapshots in object storage: the backend and the compression.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum SnapshotError {
    /// Object-store I/O failure.
    #[error("object store error: {0}")]
    ObjectStore(String),

    /// zstd compression/decompression failure.
    #[error("compression error: {0}")]
    Compression(String),
}

impl SnapshotError {
    /// A stable, low-cardinality label naming the variant — for metric labels
    /// and structured logging. Keep these strings stable; they are a metric
    /// label dimension.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ObjectStore(_) => "object_store",
            Self::Compression(_) => "compression",
        }
    }
}

label_enum!(
    #[doc = "`reason` label of `OBJECT_STORE_TRANSFER_BYTES_TOTAL` — why the \
             transfer happened, named by the caller rather than inferred. \
             Snapshot objects are immutable and never GC'd, so the same object \
             is legitimately read many times; this says by whom."]
    pub ObjectStoreReason {
        /// A snapshot the leader created from a cutoff window.
        SnapshotCreate => "snapshot_create",
        /// Manifest compaction: reads the snapshots it merges, writes the merge.
        /// Both halves land here, so the ratio is readable directly.
        Compaction => "compaction",
        /// A read below the stable-log purge floor, served just-in-time from
        /// the never-GC'd snapshots (`learner/stable_archive`).
        ArchiveRead => "archive_read",
        /// The `raft-bootstrap-fence` marker — the cluster-initialized check.
        Bootstrap => "bootstrap",
    }
);
