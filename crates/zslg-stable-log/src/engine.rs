//! Storage access over the stable log's column family.

use zslg_log::LogError;

use crate::key::{decode_key, encode_primary, missing_value, undecodable};
use crate::{Error, STABLE_LOG_CF};
use rocksdb_kit::Storage;
use zslg_log::LogIndex;
use zslg_time::Hlc;

// ── storage access ──────────────────────────────────────────────────────────
//
// The facade's `max_key` / `scan_range` / `get` / `maintain`, as local
// functions over this store's [`Storage`]. Purge is a durable `DeleteRange`
// (see [`StableLog::set_purge_threshold`]), so a purged prefix is genuinely
// absent — no read-side clamp to consult here any more.

/// The newest live entry — the facade's `max_key`, plus the secondary the same
/// key already carries.
pub(crate) fn newest_live_entry(storage: &Storage) -> Result<Option<(LogIndex, Hlc)>, Error> {
    let cf = storage.cf(STABLE_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek_to_last();
    if let Some(key) = it.key() {
        // PINNING RULE: `key` borrows the iterator's current block, which stays
        // pinned in the block cache for as long as the borrow lives. It must not
        // cross an `.await` or a batch commit — decode it here and let the borrow
        // end before the iterator moves.
        return decode_key(key).map(Some).ok_or_else(|| undecodable(key));
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb reverse scan (stable log): {e}"
        )))
    })?;
    Ok(None)
}

/// The watermark an entry at `hlc` settles: everything strictly below it.
///
/// The log is gapless and HLC-ordered by index, so once an entry at `hlc` is
/// present every smaller HLC's entries are too — but not necessarily every
/// entry *at* `hlc`, which may straddle a load batch or an interrupted load.
/// `None` only below the first representable HLC.
pub(crate) fn settled_below(hlc: Hlc) -> Option<Hlc> {
    hlc.raw().checked_sub(1).map(Hlc)
}

/// The exclusive index bound of `hlc <= threshold`.
///
/// Index order == HLC order in the stable log (see the module header), so
/// those entries are a key-order prefix — but the key is indexed by `index`,
/// so the boundary cannot be computed from `threshold` alone. Range
/// tombstones are honoured by iterators, so `seek_to_first` lands on the
/// first *live* key and this walk costs O(newly covered), not
/// O(everything ever purged).
pub(crate) fn purge_boundary(storage: &Storage, threshold: Hlc) -> Result<Option<LogIndex>, Error> {
    let cf = storage.cf(STABLE_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek_to_first();
    let mut boundary = None;
    while let Some(key) = it.key() {
        // PINNING RULE: `key` borrows the iterator's current block and pins
        // it. Decode here; let the borrow end before the iterator moves.
        let (index, hlc) = decode_key(key).ok_or_else(|| undecodable(key))?;
        if hlc > threshold {
            break;
        }
        boundary = Some(LogIndex::next_after(Some(index)));
        it.next();
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb purge-boundary scan (stable log): {e}"
        )))
    })?;
    Ok(boundary)
}

/// What a range scan feeds. The payload is borrowed from the iterator's block
/// for the duration of the call only — a sink that keeps it must copy.
pub(crate) type EntrySink<'a> = dyn FnMut(LogIndex, &[u8]) -> bool + 'a;

/// Index-ordered scan of `[from, until)`. `sink` returning `false` stops it.
pub(crate) fn for_each_entry(
    storage: &Storage,
    from: LogIndex,
    until: LogIndex,
    sink: &mut EntrySink<'_>,
) -> Result<(), Error> {
    let cf = storage.cf(STABLE_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek(encode_primary(from));
    while let Some(key) = it.key() {
        // PINNING RULE: `key` and `value` below both borrow the iterator's
        // current block and pin it. Neither may cross an `.await` or a batch
        // commit; the sink copies what it wants to keep.
        let (index, _hlc) = decode_key(key).ok_or_else(|| undecodable(key))?;
        if index >= until {
            break;
        }
        let Some(value) = it.value() else {
            return Err(missing_value(key));
        };
        if !sink(index, value) {
            return Ok(());
        }
        it.next();
    }
    it.status()
        .map_err(|e| Error::Log(LogError::Storage(format!("rocksdb scan (stable log): {e}"))))
}

/// Is `index` live — present and not covered by the armed retire threshold?
///
/// A point `get` is impossible: the key carries the secondary, which the caller
/// does not know, so this seeks the primary prefix instead.
#[cfg(test)]
pub(crate) fn index_is_live(storage: &Storage, index: LogIndex) -> Result<bool, Error> {
    let mut found = false;
    // saturating: `index + 1` is the exclusive end of a one-index window. At
    // `u64::MAX` the window collapses to empty, which is the right answer for a
    // log that cannot hold an index above it anyway.
    for_each_entry(
        storage,
        index,
        LogIndex(index.raw().saturating_add(1)),
        &mut |_idx, _payload| {
            found = true;
            false
        },
    )?;
    Ok(found)
}
