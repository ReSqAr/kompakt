//! The key codec: `key = index ‖ hlc`, both big-endian.

use zslg_log::LogError;

use crate::Error;
use zslg_log::LogIndex;
use zslg_time::Hlc;

//
// `key = index ‖ hlc`: the primary is the gapless global [`LogIndex`], the
// secondary the entry's creation HLC (`data_id.hlc()`), which drives the
// optional TTL retirement (`purge.rs`). Because arrivals are index-ordered and
// index order == HLC order here (snapshot windows are keyed `(hlc, data_id)`
// and disjoint above the prior watermark), retiring by HLC secondary retires
// exactly an index prefix — see [`StableLog::set_purge_threshold`]. When purge
// is disabled (the default) no threshold is ever armed, so the log never
// retires.
//
// Both halves are big-endian, so the byte ordering IS the tuple ordering —
// load-bearing, because retirement is a `DeleteRange` over encoded-byte
// bounds (`stable_key_order_matches_tuple_order`): if the byte order were not
// the tuple order, `set_purge_threshold` would drop the wrong entries.
// Fixed width, so [`decode_key`] splits at a constant.
//
// Folding the secondary into the key is lossless because no store here ever
// holds two live versions of one primary with differing secondaries — see
// docs/ROCKSDB_MIGRATION.md §1.2.

pub(crate) const PRIMARY_WIDTH: usize = 8;
pub(crate) const SECONDARY_WIDTH: usize = 8;
pub(crate) const KEY_WIDTH: usize = PRIMARY_WIDTH + SECONDARY_WIDTH;

#[must_use]
pub(crate) fn encode_key(index: LogIndex, hlc: Hlc) -> [u8; KEY_WIDTH] {
    let mut k = [0u8; KEY_WIDTH];
    k[..PRIMARY_WIDTH].copy_from_slice(&index.raw().to_be_bytes());
    k[PRIMARY_WIDTH..].copy_from_slice(&hlc.raw().to_be_bytes());
    k
}

#[must_use]
pub(crate) fn encode_primary(index: LogIndex) -> [u8; PRIMARY_WIDTH] {
    index.raw().to_be_bytes()
}

/// The key's secondary half, encoded alone. No production caller needs this
/// any more — purge now walks and range-deletes rather than comparing an
/// armed threshold against this encoding — but the key-order tests still
/// build one to check ordering byte-for-byte.
#[cfg(test)]
#[must_use]
pub(crate) fn encode_secondary(hlc: Hlc) -> [u8; SECONDARY_WIDTH] {
    hlc.raw().to_be_bytes()
}

#[must_use]
pub(crate) fn decode_key(bytes: &[u8]) -> Option<(LogIndex, Hlc)> {
    let index = u64::from_be_bytes(bytes.get(..PRIMARY_WIDTH)?.try_into().ok()?);
    let hlc = u64::from_be_bytes(bytes.get(PRIMARY_WIDTH..KEY_WIDTH)?.try_into().ok()?);
    Some((LogIndex(index), Hlc(hlc)))
}

pub(crate) fn undecodable(key: &[u8]) -> Error {
    Error::Log(LogError::Storage(format!(
        "undecodable stable log key ({} bytes): {key:02x?}",
        key.len()
    )))
}

/// A live key whose value is missing. Unreachable through RocksDB's iterator
/// contract (`value()` is `None` only when `key()` is); erroring rather than
/// substituting an empty payload keeps the durability layer from serving
/// garbage silently (§20).
pub(crate) fn missing_value(key: &[u8]) -> Error {
    Error::Log(LogError::Storage(format!(
        "stable log key with no value ({} bytes): {key:02x?}",
        key.len()
    )))
}
