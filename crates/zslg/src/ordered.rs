//! Position keys for `crossbeam_combinators::OrderedChannel`.
//!
//! The item type is often the same (`Data`, `Result<...>`), but the position
//! differs by source. These impls keep the key concept at the boundary without
//! putting it on the item.

use crossbeam_combinators::{Channels, Gapless, Key, OrderedChannel, StrictlyMonotone, Successor};

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
    OrderedChannel<Result<(LogIndex, Data), Error>, StrictlyMonotone<LogIndex>>;

/// Wrap a replica stream with the strict policy and its cursor.
pub(crate) fn replica_stream(
    ch: Channels<Result<(LogIndex, Data), Error>>,
    from: Option<LogIndex>,
) -> ReplicaStream {
    OrderedChannel::new(ch, Some(LogIndex::next_after(from)))
}

/// The stable-log read: gapless indices, replaying from `from` inclusive.
pub type StableStream = OrderedChannel<Result<StableLogEvent, Error>, Gapless<LogIndex>>;

/// Wrap a stable-log stream with the gapless policy and its inclusive start.
pub(crate) fn stable_stream(
    ch: Channels<Result<StableLogEvent, Error>>,
    from: LogIndex,
) -> StableStream {
    OrderedChannel::new(ch, Some(from))
}
