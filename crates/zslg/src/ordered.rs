//! Position keys for `crossbeam_combinators::OrderedChannel`.
//!
//! The item type is often the same (`Data`, `Result<...>`), but the position
//! differs by source. These impls keep the key concept at the boundary without
//! putting it on the item.

use crossbeam_combinators::{
    Channel, Contiguous, Key, OrderedChannel, StrictlyIncreasing, Successor,
};

use crate::api::WindowItem;
use crate::error::Error;
use crate::ids::{DataId, LogIndex};
use crate::meta::{Data, SnapshotEntry, StableLogEvent};

impl Successor for LogIndex {
    fn next(self) -> Self {
        Self::next_after(Some(self))
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

impl Key<Result<WindowItem, Error>> for DataId {
    fn key(item: &Result<WindowItem, Error>) -> Option<Self> {
        match item {
            Ok(WindowItem::Entry(data)) => Some(data.data_id),
            Ok(WindowItem::Complete | WindowItem::TruncatedAt(_)) | Err(_) => None,
        }
    }
}

/// The replica append stream: strictly increasing indices, forward purge gaps
/// allowed.
pub(crate) type ReplicaStream =
    OrderedChannel<Result<(LogIndex, Data), Error>, StrictlyIncreasing<LogIndex>>;

/// Wrap a replica stream with the strict policy and its cursor.
pub(crate) fn replica_stream(
    ch: Channel<Result<(LogIndex, Data), Error>>,
    from: Option<LogIndex>,
) -> ReplicaStream {
    OrderedChannel::named(ch, Some(LogIndex::next_after(from)), "replica subscription")
}

/// The stable-log read: gapless indices, replaying from `from` inclusive.
pub type StableStream = OrderedChannel<Result<StableLogEvent, Error>, Contiguous<LogIndex>>;

/// The local store's above-HLC scan: strictly increasing `DataId`s.
pub(crate) type LocalStoreStream = OrderedChannel<Result<Data, Error>, StrictlyIncreasing<DataId>>;

/// The local store's bounded window scan; terminals are no-key items.
pub(crate) type WindowStream =
    OrderedChannel<Result<WindowItem, Error>, StrictlyIncreasing<DataId>>;

/// The archive stream: gapless indices, tiled from snapshots.
pub(crate) type ArchiveStream = OrderedChannel<Result<SnapshotEntry, Error>, Contiguous<LogIndex>>;

/// Wrap a local-store scan with its `StrictlyIncreasing` contract.
pub(crate) fn local_store_stream(
    ch: Channel<Result<Data, Error>>,
    what: &'static str,
) -> LocalStoreStream {
    OrderedChannel::named(ch, None, what)
}

/// Wrap a local-store window scan with its `StrictlyIncreasing` contract.
pub(crate) fn window_stream(
    ch: Channel<Result<WindowItem, Error>>,
    what: &'static str,
) -> WindowStream {
    OrderedChannel::named(ch, None, what)
}

/// Wrap an archive stream with its `Contiguous` contract.
pub(crate) fn archive_stream(
    ch: Channel<Result<SnapshotEntry, Error>>,
    from: LogIndex,
) -> ArchiveStream {
    OrderedChannel::named(ch, Some(from), "archive stream")
}

/// Wrap a stable-log stream with the gapless policy and its inclusive start.
pub(crate) fn stable_stream(
    ch: Channel<Result<StableLogEvent, Error>>,
    from: LogIndex,
) -> StableStream {
    OrderedChannel::named(ch, Some(from), "stable log stream")
}
