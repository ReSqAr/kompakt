//! Engine helpers over the two column families: point and range reads, and the
//! atomic batch commit.

use std::collections::HashMap;
use std::ops::Bound;
use zslg_log::LogError;

use rocksdb_kit::Batch;

use crate::key::{decode_data_key, encode_cursor_key, encode_data_key};
use bytes::Bytes;
use crossbeam_combinators::{Closed, Feed, Finite};
use rocksdb_kit::Storage;

use super::{LOCAL_STORE_CURSORS_CF, LOCAL_STORE_DATA_CF, LocalStore};
use crate::Error;
use zslg_consensus::NodeId;
use zslg_log::Data;
use zslg_log::{DataId, LogIndex};
use zslg_time::Hlc;

// ===========================================================================
// Free helpers — record builders, engine helpers, periodic fsync.
// ===========================================================================

/// A point read of one upstream's cursor.
pub(super) fn read_cursor(storage: &Storage, upstream: NodeId) -> Result<Option<LogIndex>, Error> {
    let cf = storage.cf(LOCAL_STORE_CURSORS_CF)?;
    let Some(pinned) = storage
        .get_pinned(&cf, encode_cursor_key(upstream))
        .map_err(|e| {
            Error::Log(LogError::Storage(format!(
                "read local-store cursor for {upstream:?}: {e}"
            )))
        })?
    else {
        return Ok(None);
    };
    // PINNING RULE: `pinned` holds its block in the block cache for as long as
    // it lives. It must not cross an `.await` or a batch commit — decode here
    // and let it drop at the end of this expression.
    let bytes: [u8; 8] = pinned.as_ref().try_into().unwrap_or([0u8; 8]);
    Ok(Some(LogIndex(u64::from_be_bytes(bytes))))
}

/// What a data scan feeds. The payload is borrowed from the iterator's block
/// for the duration of the call only — a sink that keeps it must copy.
pub(super) type DataSink<'a> = dyn FnMut(DataId, &[u8]) -> bool + 'a;

/// `DataId`-ordered scan of the data family. `sink` returning `false` stops it.
///
/// Purge is a durable `DeleteRange` (`purge_below_hlc`), so a purged prefix is
/// simply absent from the keyspace here — no read-side clamp to apply.
pub(super) fn for_each_data(
    storage: &Storage,
    range: (Bound<DataId>, Bound<DataId>),
    sink: &mut DataSink<'_>,
) -> Result<(), Error> {
    let cf = storage.cf(LOCAL_STORE_DATA_CF)?;
    let mut it = storage.scan(&cf);
    let (start, end) = range;
    match start {
        Bound::Unbounded => it.seek_to_first(),
        Bound::Included(p) => it.seek(encode_data_key(p)),
        Bound::Excluded(p) => {
            // A data key IS its primary and primaries are unique, so seeking
            // `p` itself lands on it (to skip) or past it.
            let primary = encode_data_key(p);
            it.seek(primary);
            if it.key().is_some_and(|key| key == primary.as_slice()) {
                it.next();
            }
        }
    }
    while let Some(key) = it.key() {
        // PINNING RULE: `key` and `value` below both borrow the iterator's
        // current block and pin it in the block cache. Neither may cross an
        // `.await` or a batch commit; the sink copies what it wants to keep.
        let Some(data_id) = decode_data_key(key) else {
            return Err(Error::Log(LogError::Storage(format!(
                "undecodable local-store data key ({} bytes)",
                key.len()
            ))));
        };
        let past_end = match end {
            Bound::Unbounded => false,
            Bound::Included(e) => data_id > e,
            Bound::Excluded(e) => data_id >= e,
        };
        if past_end {
            break;
        }
        // `value()` is `None` only when `key()` is, so this is unreachable —
        // but substituting an empty payload for a missing one would hand a
        // caller a silently truncated record (§20).
        let Some(value) = it.value() else {
            return Err(Error::Log(LogError::Storage(format!(
                "local-store data key with no value ({} bytes): {key:02x?}",
                key.len()
            ))));
        };
        if !sink(data_id, value) {
            return Ok(());
        }
        it.next();
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb scan (local store data): {e}"
        )))
    })
}

/// Stage one batch's data rows and cursor advances into a **single**
/// `WriteBatch` and commit it. Atomic across both column families — the win one
/// DB unlocks; see [`BatchIntegrator::drain_one`](super::integrator::BatchIntegrator::drain_one).
///
/// The local store never deletes, and identical payloads per id make
/// last-write-wins a no-op, so there is no get-before-insert on the data side.
/// Cursors do read first, to keep the stored value a running max.
pub(super) fn commit_batch(
    store: &LocalStore,
    data_rows: &[(DataId, Bytes)],
    cursors: &HashMap<NodeId, LogIndex>,
) -> Result<(), Error> {
    let data_cf = store.storage.cf(LOCAL_STORE_DATA_CF)?;
    let cursor_cf = store.storage.cf(LOCAL_STORE_CURSORS_CF)?;
    // No global write lock: batches commit atomically and are ordered at the
    // WAL, so opening one never blocks on another writer.
    let mut batch = Batch::new();
    for (data_id, blob) in data_rows {
        batch.put(&data_cf, encode_data_key(*data_id), blob);
    }
    for (source, source_index) in cursors {
        // The cursor only ever moves forward (`max` against the stored value),
        // so out-of-order application within a source is harmless. The
        // integrate worker is the sole cursor writer, so the read-then-write is
        // race-free.
        let new = match read_cursor(&store.storage, *source)? {
            Some(prev) => LogIndex(prev.raw().max(source_index.raw())),
            None => *source_index,
        };
        batch.put(&cursor_cf, encode_cursor_key(*source), new.0.to_be_bytes());
    }
    store.storage.write(batch).map_err(Into::into)
}

/// The scan bound strictly above every entry at `hlc` (or the start).
pub(super) fn above_hlc(hlc: Option<Hlc>) -> Bound<DataId> {
    match hlc {
        None => Bound::Unbounded,
        Some(h) => Bound::Excluded(DataId::from_parts(h, NodeId::MAX, u64::MAX)),
    }
}

/// Drive one blocking range scan into `feed`. A scan or decode error is the
/// stream's final item; nothing follows it. [`for_each_data`] clamps at the
/// retire threshold, so no purge-cutoff filter is needed here.
pub(super) fn stream_scan(
    storage: &Storage,
    range: (Bound<DataId>, Bound<DataId>),
    feed: Feed<Result<Data, Error>, Finite>,
) -> Closed {
    let scan = for_each_data(
        storage,
        range,
        // The record *is* the key plus the blob — nothing to decode. The copy
        // is paid here because this `Data` outlives the scan on a channel (and
        // the payload is iterator-pinned, so it must not).
        &mut |data_id, payload| {
            let d = Data {
                data_id,
                blob: Bytes::copy_from_slice(payload),
            };
            feed.send(Ok(d)).is_ok()
        },
    );
    if let Err(e) = scan {
        let _ = feed.send(Err(e));
    }
    feed.close()
}
