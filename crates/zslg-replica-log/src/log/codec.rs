//! The key and value codec: how an entry is laid out in the replica-log column family.

use crate::Error;
use zslg_log::LogError;
use zslg_log::{DataId, LogIndex};
use zslg_time::Hlc;

// ── key codec ───────────────────────────────────────────────────────────────
//
// `key = index ‖ running_max_hlc`, `value = data_id ‖ blob`. Both key halves
// are big-endian, so the byte ordering IS the tuple ordering.
//
// **The secondary is not a property of its own entry.** It is the largest HLC
// over the whole log prefix ENDING AT this entry — a running maximum, not this
// entry's `data_id.hlc()`. Reading it as "this entry's HLC" is wrong, and looks
// right on a quiet log where the two coincide.
//
// Why: retirement's threshold is an HLC, but writers append out of HLC order,
// so `index` order is assignment order and the set of entries with
// `own_hlc <= T` is scattered through the keyspace. A running max is monotone
// by construction, so `runmax <= T` is a contiguous prefix — which is exactly
// what `DeleteRange` can express, and it is why this log needs no compaction
// filter and no read-side clamp. `runmax >= own_hlc`, so the rule is
// conservative in the safe direction: it never retires an entry whose own HLC
// is above the threshold, only sometimes retains one whose HLC is below it.
//
// Consequences worth knowing:
//   * entry N's key cannot be built from entry N alone — it needs the log's
//     history. Nothing tries: appends take it from the live frontier, and every
//     read seeks by the 8-byte `encode_primary(index)` prefix.
//   * two entries may share a secondary. Uniqueness is `index`, as it always
//     was — the old `data_id` secondary never contributed to it either.
//   * the last key's secondary IS the log's `max_hlc`, so recovering that bound
//     at open is a `seek_to_last`, not a sweep.
//
// `data_id` moved into the value because it is no longer part of the key's
// identity or its ordering. `WriteBatch::put_cf_vectored` writes the two halves
// without concatenating them, so the blob is still copied exactly once.

pub(crate) const PRIMARY_WIDTH: usize = 8;

pub(crate) const SECONDARY_WIDTH: usize = 8;

pub(crate) const KEY_WIDTH: usize = PRIMARY_WIDTH + SECONDARY_WIDTH;

/// The `data_id` prefix every value carries ahead of the caller's blob.
pub(crate) const VALUE_PREFIX_WIDTH: usize = 32;

#[must_use]
pub(crate) fn encode_key(index: LogIndex, running_max: Hlc) -> [u8; KEY_WIDTH] {
    let mut k = [0u8; KEY_WIDTH];
    k[..PRIMARY_WIDTH].copy_from_slice(&index.raw().to_be_bytes());
    k[PRIMARY_WIDTH..].copy_from_slice(&running_max.raw().to_be_bytes());
    k
}

#[must_use]
pub(crate) fn encode_primary(index: LogIndex) -> [u8; PRIMARY_WIDTH] {
    index.raw().to_be_bytes()
}

#[must_use]
pub(crate) fn decode_key(bytes: &[u8]) -> Option<(LogIndex, Hlc)> {
    let index = u64::from_be_bytes(bytes.get(..PRIMARY_WIDTH)?.try_into().ok()?);
    let running_max = u64::from_be_bytes(bytes.get(PRIMARY_WIDTH..KEY_WIDTH)?.try_into().ok()?);
    Some((LogIndex(index), Hlc(running_max)))
}

/// Split a stored value into its `data_id` prefix and the caller's blob. The
/// blob borrows the input, so an iterator's pinned block must outlive the call.
#[must_use]
pub(crate) fn decode_value(bytes: &[u8]) -> Option<(DataId, &[u8])> {
    let (id, value) = bytes.split_first_chunk::<VALUE_PREFIX_WIDTH>()?;
    Some((DataId(*id), value))
}

pub(crate) fn undecodable(key: &[u8]) -> Error {
    Error::Log(LogError::Storage(format!(
        "undecodable replica log key ({} bytes): {key:02x?}",
        key.len()
    )))
}

/// A live key whose value is missing. Unreachable through RocksDB's iterator
/// contract (`value()` is `None` only when `key()` is), but the alternative —
/// substituting an empty payload — would broadcast an empty `Data` to
/// subscribers rather than fail. §20: never serve garbage silently.
pub(crate) fn missing_value(key: &[u8]) -> Error {
    Error::Log(LogError::Storage(format!(
        "replica log key with no value ({} bytes): {key:02x?}",
        key.len()
    )))
}

/// A live key whose value is present but shorter than `VALUE_PREFIX_WIDTH` —
/// too short to even hold a `data_id`. A distinct fault from
/// [`missing_value`]: an absent value can only mean RocksDB's iterator
/// contract was violated, while a truncated one points at corruption or a
/// writer bug in the value encoding, which is worth telling apart when
/// diagnosing without re-running (CLAUDE.md §5).
pub(crate) fn truncated_value(key: &[u8], len: usize) -> Error {
    Error::Log(LogError::Storage(format!(
        "replica log value too short to hold a data_id ({len} bytes) for key ({} bytes): {key:02x?}",
        key.len()
    )))
}
