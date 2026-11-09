//! Scans over the raft log column family.

use crate::Error;
use raft_kit::Entry;
use raft_kit::LogId;
use raft_kit::RaftLogId;
use rocksdb_kit::Storage;
use std::ops::{Bound, RangeBounds};
use zslg_consensus::NodeId;
use zslg_consensus::RaftLogIndex;
use zslg_log::LogError;
use zslg_log::ToCodec;
use zslg_raft::TypeConfig;

use super::{RAFT_LOG_CF, decode_log_key, encode_log_key, encode_log_primary};
// SUPER

/// The callback [`for_each_log_entry`] feeds: `(index, payload)`, answering
/// `false` to stop the walk. The payload is borrowed for the call only.
pub(super) type LogSink<'a> = dyn FnMut(RaftLogIndex, &[u8]) -> Result<bool, Error> + 'a;

/// Primary-ordered walk of the live raft log over `[lo, hi)` (`hi = None` is
/// unbounded), skipping retired entries. `sink` answering `false` stops it.
///
/// PINNING RULE: the payload handed to `sink` borrows the iterator's pinned
/// block-cache block. It must not cross an `.await` or a batch commit, so a
/// `sink` that wants to *write* what it sees collects keys and builds its
/// `WriteBatch` after this returns (`delete_conflict_logs_since`).
pub(super) fn for_each_log_entry(
    storage: &Storage,
    lo: RaftLogIndex,
    hi: Option<RaftLogIndex>,
    sink: &mut LogSink<'_>,
) -> Result<(), Error> {
    let cf = storage.cf(RAFT_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek(encode_log_primary(lo));
    while let Some(key) = it.key() {
        let Some((index, _rank)) = decode_log_key(key) else {
            return Err(Error::Log(LogError::Storage(format!(
                "undecodable key in {}",
                RAFT_LOG_CF.name()
            ))));
        };
        if hi.is_some_and(|end| index >= end) {
            break;
        }
        // `value()` is `None` only when `key()` is, so this is unreachable —
        // but substituting an empty payload for a missing one would feed
        // openraft an undecodable entry instead of failing (§20).
        let Some(value) = it.value() else {
            return Err(Error::Log(LogError::Storage(format!(
                "key with no value in {} ({} bytes)",
                RAFT_LOG_CF.name(),
                key.len()
            ))));
        };
        if !sink(index, value)? {
            return Ok(());
        }
        it.next();
    }
    it.status()
        .map_err(|e| Error::Log(LogError::Storage(format!("scan raft log: {e}"))))
}

/// The newest live log index, or `None` if every stored entry is retired.
pub(super) fn newest_live_index(storage: &Storage) -> Result<Option<RaftLogIndex>, Error> {
    let cf = storage.cf(RAFT_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek_to_last();
    if let Some(key) = it.key() {
        return decode_log_key(key)
            .map(|(index, _)| Some(index))
            .ok_or_else(|| {
                Error::Log(LogError::Storage(format!(
                    "undecodable key in {}",
                    RAFT_LOG_CF.name()
                )))
            });
    }
    it.status()
        .map_err(|e| Error::Log(LogError::Storage(format!("scan raft log: {e}"))))?;
    Ok(None)
}

/// The newest live entry's log id: the newest stored key may sit in the purged
/// zone, so walk backwards until a live entry answers, flooring at the purge
/// point.
pub(super) fn newest_live_entry(
    storage: &Storage,
    floor: Option<RaftLogIndex>,
) -> Result<Option<LogId<NodeId>>, Error> {
    let Some(index) = newest_live_index(storage)? else {
        return Ok(None);
    };
    if floor.is_some_and(|f| index <= f) {
        return Ok(None);
    }
    let cf = storage.cf(RAFT_LOG_CF)?;
    let Some(pinned) = storage
        .get_pinned(&cf, encode_log_key(index, index))
        .map_err(|e| {
            Error::Log(LogError::Storage(format!(
                "read raft log entry {index}: {e}"
            )))
        })?
    else {
        return Ok(None);
    };
    // PINNING RULE: `pinned` holds its block in the block cache. It must not
    // cross an `.await` or a batch commit — decode here and let it drop.
    let entry: Entry<TypeConfig> =
        postcard::from_bytes::<Entry<TypeConfig>>(&pinned).to_codec("decode raft entry")?;
    Ok(Some(*RaftLogId::get_log_id(&entry)))
}

pub(super) fn bounds_to_pair<RB: RangeBounds<u64>>(range: &RB) -> (RaftLogIndex, RaftLogIndex) {
    let lo = match range.start_bound() {
        Bound::Included(&v) => v,
        Bound::Excluded(&v) => v.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let hi = match range.end_bound() {
        Bound::Included(&v) => v.saturating_add(1),
        Bound::Excluded(&v) => v,
        Bound::Unbounded => u64::MAX,
    };
    (RaftLogIndex::from(lo), RaftLogIndex::from(hi))
}
