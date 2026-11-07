//! Position keys for `crossbeam_combinators::OrderedChannel`.
//!
//! The item type is often the same (`Data`, `Result<...>`), but the position
//! differs by source. These impls keep the key concept at the boundary without
//! putting it on the item.

use crossbeam_combinators::{Key, Successor};

use crate::error::Error;
use crate::ids::{DataId, LogIndex};
use crate::meta::{Data, SnapshotEntry, StableLogEvent};
use zslg_time_types::Hlc;

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

impl zslg_time_types::HasHlc for Data {
    fn hlc(&self) -> Option<Hlc> {
        Some(self.data_id.hlc())
    }
}

impl zslg_time_types::HasHlc for SnapshotEntry {
    fn hlc(&self) -> Option<Hlc> {
        Some(self.data.data_id.hlc())
    }
}
