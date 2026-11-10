//! Reads over the replica-log column family: the newest entry, the purge boundary, range scans.

use super::{
    REPLICA_LOG_CF, decode_key, decode_value, encode_primary, missing_value, truncated_value,
    undecodable,
};
use crate::Error;
use rocksdb_kit::Storage;
use zslg_log::LogError;
use zslg_log::{DataId, LogIndex};
use zslg_time::Hlc;

// ── storage access ──────────────────────────────────────────────────────────
//
// The facade's `max_key` / `scan_range` / `get`, as local functions over the
// node's [`Storage`]. Purge is a durable `DeleteRange`
// (see [`ReplicaLogStore::set_purge_threshold`]), so a retired prefix is
// genuinely absent — no read-side clamp to consult here any more.

/// The newest entry — the facade's `max_key`, whose secondary IS the log's
/// `max_hlc` because the secondary is a running maximum. One `seek_to_last`;
/// the sweep this replaced (`max_live_secondary`) was O(live entries).
pub(crate) fn newest_live_entry(storage: &Storage) -> Result<Option<(LogIndex, Hlc)>, Error> {
    let cf = storage.cf(REPLICA_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek_to_last();
    if let Some(key) = it.key() {
        // PINNING RULE: `key` borrows the iterator's current block and pins it.
        // Decode here; the borrow must not cross an `.await` or a batch commit.
        return decode_key(key).map(Some).ok_or_else(|| undecodable(key));
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb reverse scan (replica log): {e}"
        )))
    })?;
    Ok(None)
}

/// The exclusive index bound of `running_max <= threshold`.
///
/// The secondary is monotone, so those entries are a prefix — but the key is
/// indexed by `index`, so the boundary is not computable from `threshold`
/// alone. Range tombstones are honoured by iterators, so `seek_to_first` lands
/// on the first *live* key and this walk costs O(newly covered), not
/// O(everything ever purged).
pub(crate) fn purge_boundary(storage: &Storage, threshold: Hlc) -> Result<Option<LogIndex>, Error> {
    let cf = storage.cf(REPLICA_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek_to_first();
    let mut boundary = None;
    while let Some(key) = it.key() {
        // PINNING RULE: `key` borrows the iterator's current block and pins it.
        // Decode here; let the borrow end before the iterator moves.
        let (index, running_max) = decode_key(key).ok_or_else(|| undecodable(key))?;
        if running_max > threshold {
            break;
        }
        boundary = Some(LogIndex::next_after(Some(index)));
        it.next();
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb purge-boundary scan (replica log): {e}"
        )))
    })?;
    Ok(boundary)
}

/// What a range scan feeds. The payload is borrowed from the iterator's block
/// for the duration of the call only — a sink that keeps it must copy.
pub(crate) type EntrySink<'a> = dyn FnMut(LogIndex, DataId, &[u8]) -> bool + 'a;

/// Index-ordered scan of `[from, to]`. `sink` returning `false` stops it.
pub(crate) fn for_each_entry(
    storage: &Storage,
    from: LogIndex,
    to: LogIndex,
    sink: &mut EntrySink<'_>,
) -> Result<(), Error> {
    let cf = storage.cf(REPLICA_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek(encode_primary(from));
    while let Some(key) = it.key() {
        // PINNING RULE: `key` and `value` below both borrow the iterator's
        // current block and pin it. Neither may cross an `.await` or a batch
        // commit; the sink copies what it wants to keep.
        let (index, _running_max) = decode_key(key).ok_or_else(|| undecodable(key))?;
        if index > to {
            break;
        }
        let Some(value) = it.value() else {
            return Err(missing_value(key));
        };
        let (data_id, payload) =
            decode_value(value).ok_or_else(|| truncated_value(key, value.len()))?;
        if !sink(index, data_id, payload) {
            return Ok(());
        }
        it.next();
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb scan (replica log): {e}"
        )))
    })
}
