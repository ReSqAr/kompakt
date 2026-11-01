//! Position keys for `crossbeam_combinators::OrderedChannel`.
//!
//! The item type is often the same (`Data`, `Result<...>`), but the position
//! differs by source. These impls keep the key concept at the boundary without
//! putting it on the item.

use crossbeam_combinators::{
    Channel, Contiguous, Key, OrderedChannel, StrictlyIncreasing, Successor,
};

use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::{DataId, LogIndex};
use crate::meta::{Data, SnapshotEntry, StableLogEvent};

impl Successor for LogIndex {
    fn next_after(prev: Option<Self>) -> Self {
        Self::next_after(prev)
    }
}

impl Key<Result<(LogIndex, Data), Error>> for LogIndex {
    fn key(item: &Result<(LogIndex, Data), Error>) -> Option<Self> {
        item.as_ref().ok().map(|(index, _)| *index)
    }
}

impl Key<Result<StableLogEvent, Error>> for LogIndex {
    fn key(item: &Result<StableLogEvent, Error>) -> Option<Self> {
        match item {
            Ok(StableLogEvent::Appended { index, .. }) => Some(*index),
            Ok(StableLogEvent::Watermark(_)) | Err(_) => None,
        }
    }
}

impl Key<Result<SnapshotEntry, Error>> for LogIndex {
    fn key(item: &Result<SnapshotEntry, Error>) -> Option<Self> {
        item.as_ref().ok().map(|entry| entry.global_index)
    }
}

impl Key<Result<Data, Error>> for DataId {
    fn key(item: &Result<Data, Error>) -> Option<Self> {
        item.as_ref().ok().map(|data| data.data_id)
    }
}

/// An archive entry's HLC: index order implies HLC order, guarded by `also`.
impl Key<Result<SnapshotEntry, Error>> for Hlc {
    fn key(item: &Result<SnapshotEntry, Error>) -> Option<Self> {
        item.as_ref().ok().map(|entry| entry.data.data_id.hlc())
    }
}

/// A replayed stable-log entry's HLC, guarded the same way.
impl Key<Result<(LogIndex, Data), Error>> for Hlc {
    fn key(item: &Result<(LogIndex, Data), Error>) -> Option<Self> {
        item.as_ref().ok().map(|(_, data)| data.data_id.hlc())
    }
}

/// A local-store entry's HLC group: what a snapshot chunk must not split.
impl Key<Result<Data, Error>> for Hlc {
    fn key(item: &Result<Data, Error>) -> Option<Self> {
        item.as_ref().ok().map(|data| data.data_id.hlc())
    }
}

/// The replica append stream: strictly increasing indices, forward purge gaps
/// allowed.
pub(crate) type ReplicaStream =
    OrderedChannel<Result<(LogIndex, Data), Error>, StrictlyIncreasing<LogIndex>>;

/// Wrap a replica stream with the strict policy, after the subscriber's cursor.
pub(crate) fn replica_stream(
    ch: Channel<Result<(LogIndex, Data), Error>>,
    after: Option<LogIndex>,
) -> ReplicaStream {
    OrderedChannel::named(ch, after, "replica subscription")
}

/// The stable-log read: gapless indices, replaying from `from` inclusive.
pub type StableStream = OrderedChannel<Result<StableLogEvent, Error>, Contiguous<LogIndex>>;

/// A local-store scan (above an HLC, or a bounded window): strictly increasing
/// `DataId`s.
pub(crate) type LocalStoreStream = OrderedChannel<Result<Data, Error>, StrictlyIncreasing<DataId>>;

/// The archive stream: gapless indices, tiled from snapshots.
pub(crate) type ArchiveStream = OrderedChannel<Result<SnapshotEntry, Error>, Contiguous<LogIndex>>;

/// Wrap a local-store scan with its `StrictlyIncreasing` contract.
pub(crate) fn local_store_stream(
    ch: Channel<Result<Data, Error>>,
    what: &'static str,
) -> LocalStoreStream {
    OrderedChannel::named(ch, None, what)
}

/// Wrap a stable-log stream with the gapless policy, from its inclusive start
/// (the subscription takes `from` until it takes `after`).
pub(crate) fn stable_stream(
    ch: Channel<Result<StableLogEvent, Error>>,
    from: LogIndex,
) -> StableStream {
    OrderedChannel::named(ch, from.before(), "stable log stream")
}
