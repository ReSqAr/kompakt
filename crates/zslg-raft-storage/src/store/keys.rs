//! The key codec: how raft log entries and meta slots are laid out in their
//! column families.

use std::fmt::Debug;
use zslg_consensus::RaftLogIndex;

// SUPER

// ── key codec ───────────────────────────────────────────────────────────────
//
// `key = primary ‖ secondary`, both big-endian so the byte ordering IS the
// tuple ordering — load-bearing, because retirement is a `DeleteRange` over
// encoded-byte bounds (`key_order_matches_tuple_order`); if the byte order
// were not the tuple order, `purge_logs_upto` would drop the wrong entries.
// Fixed width, so `decode_log_key` splits at a constant.
//
// Folding the secondary into the key is lossless because no store here ever
// holds two live versions of one primary with differing secondaries — see
// docs/ROCKSDB_MIGRATION.md §1.2. For the raft log the secondary is the *same*
// index as the primary: it is the retire rank, which is what makes
// `purge_logs_upto(i)` a single `DeleteRange` over the prefix `[0, i]`.

pub(crate) const LOG_PRIMARY_WIDTH: usize = 8;

pub(super) const LOG_RANK_WIDTH: usize = 8;

pub(crate) const LOG_KEY_WIDTH: usize = LOG_PRIMARY_WIDTH + LOG_RANK_WIDTH;

/// The raft-meta key is the slot byte alone: seven LWW singletons, never
/// retired, so there is no secondary to fold in.
pub(crate) const META_KEY_WIDTH: usize = 1;

#[must_use]
pub(super) fn encode_log_key(primary: RaftLogIndex, rank: RaftLogIndex) -> [u8; LOG_KEY_WIDTH] {
    let mut key = [0u8; LOG_KEY_WIDTH];
    let (head, tail) = key.split_at_mut(LOG_PRIMARY_WIDTH);
    head.copy_from_slice(&primary.raw().to_be_bytes());
    tail.copy_from_slice(&rank.raw().to_be_bytes());
    key
}

#[must_use]
pub(super) fn encode_log_primary(primary: RaftLogIndex) -> [u8; LOG_PRIMARY_WIDTH] {
    primary.raw().to_be_bytes()
}

/// The retire rank on its own. Purge no longer arms an in-memory clamp
/// against it (that's a durable `DeleteRange` now), so this is test-only:
/// it pins that the rank half of the key still round-trips.
#[cfg(test)]
#[must_use]
pub(super) fn encode_log_rank(rank: RaftLogIndex) -> [u8; LOG_RANK_WIDTH] {
    rank.raw().to_be_bytes()
}

#[must_use]
pub(super) fn decode_log_key(bytes: &[u8]) -> Option<(RaftLogIndex, RaftLogIndex)> {
    let primary = <[u8; LOG_PRIMARY_WIDTH]>::try_from(bytes.get(..LOG_PRIMARY_WIDTH)?).ok()?;
    let rank =
        <[u8; LOG_RANK_WIDTH]>::try_from(bytes.get(LOG_PRIMARY_WIDTH..LOG_KEY_WIDTH)?).ok()?;
    Some((
        RaftLogIndex::from(u64::from_be_bytes(primary)),
        RaftLogIndex::from(u64::from_be_bytes(rank)),
    ))
}

#[must_use]
pub(super) fn encode_meta_key(slot: RaftMetaSlot) -> [u8; META_KEY_WIDTH] {
    [slot.as_byte()]
}

/// The raft-meta singletons — the seven former string keys, now a fixed-width
/// (1-byte) enum key. Never retired; every value is overwritten in place (LWW),
/// and a multi-slot `WriteBatch` is atomic — the openraft apply/snapshot
/// atomicity requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum RaftMetaSlot {
    Vote,
    Committed,
    LastApplied,
    LastPurged,
    LastMembership,
    SnapshotIndex,
    SnapshotBlob,
}

impl RaftMetaSlot {
    const fn as_byte(self) -> u8 {
        match self {
            Self::Vote => 0,
            Self::Committed => 1,
            Self::LastApplied => 2,
            Self::LastPurged => 3,
            Self::LastMembership => 4,
            Self::SnapshotIndex => 5,
            Self::SnapshotBlob => 6,
        }
    }
}
