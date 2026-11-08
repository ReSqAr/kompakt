//! An in-process log and store, so a downstream crate can test against nvzn
//! without standing up zslg.
//!
//! nvzn reads its events from a [`zslg::Reader`] and writes them through a
//! [`zslg::Writer`]. Both are traits, so a test does not need a cluster — but
//! implementing them is ~140 lines of channel plumbing that every consumer
//! would otherwise write for itself. [`TestLog`] is that plumbing, and
//! [`Dispatcher::in_memory`](crate::Dispatcher::in_memory) is the store half.
//!
//! ```text
//!   TestLog ──writer()──> Writer ──append──┐
//!           ──reader()──> Reader           │
//!                            │             │
//!        subscribe_stable_log│             │  history replayed from `from`,
//!                            └─────────────┘  then live via crossbeam
//!
//!        unstable_tail ───> whatever push_unstable staged, as one batch
//!        subscribe_unstable_tail ─> Watermark · staged entries above it ·
//!                                   then each push_unstable and advance, live
//! ```
//!
//! A live unstable subscriber is not resynced: [`TestLog::hang_up_unstable`]
//! ends it `Aborted` where zslg would rescan, which is enough to test how a
//! consumer reacts to the stream ending.
//!
//! A write is published as `Appended` followed by `Watermark`, so the fold
//! reaches it without a separate nudge and `Reader::sync` returns once it has.
//! `LogIndex` and `DataId` come from a counter, so a log's contents are a
//! function of the order things were appended and nothing else.
//!
//! Behind the `testing` feature. The expects in here are a test harness's, and
//! they are why: a poisoned mutex or a closed replay channel is a broken
//! fixture, not a condition a caller could handle.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "a test harness; a broken fixture is not a recoverable condition"
)]

use crate::{RocksDBStore, Store};
use std::path::Path;
use std::sync::Arc;

/// Long enough that the fence never fires mid-test; the tests that care
/// about it set their own.
pub const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// A store that keeps nothing: the projection side of an in-process rig.
#[must_use]
pub fn inmemory() -> Arc<dyn Store> {
    Arc::new(crate::InMemoryStore::default())
}

/// A store tuning for tests: the production open path, on a budget small
/// enough that a suite may hold many of them at once.
#[must_use]
pub fn store_config(dir: &Path) -> crate::StoreConfig {
    let mut config = crate::StoreConfig::new(dir);
    config.memory = zslg::ByteSize::from_mib(16);
    config.flush_interval = FLUSH_INTERVAL;
    config
}

#[must_use]
pub fn rocksdb(dir: &Path) -> Arc<dyn Store> {
    Arc::new(rocksdb_store(dir))
}

#[must_use]
pub fn rocksdb_store(dir: &Path) -> RocksDBStore {
    RocksDBStore::new(&store_config(dir)).expect("open rocksdb")
}

pub use zslg::testing::{TestLog, data_id};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use zslg::{Hlc, UnstableEvent};

    fn hlc(ms: u64) -> Hlc {
        Hlc::from_bytes(ms.to_be_bytes())
    }

    /// Watermark first, then the staged entries above it, then live pushes and
    /// advances — and a hang-up ends it `Aborted` after what was queued.
    #[test]
    fn an_unstable_subscription_replays_then_follows_live() {
        let log = TestLog::new();
        log.watermark(10);
        log.push_unstable_at(data_id(5, 1), Bytes::from_static(b"settled"));
        log.push_unstable_at(data_id(20, 2), Bytes::from_static(b"staged"));

        let mut sub = log.reader().subscribe_unstable_tail();
        let mut next = || sub.recv().expect("an event").expect("ok");
        assert_eq!(next(), UnstableEvent::Watermark(hlc(10)));
        assert!(matches!(next(), UnstableEvent::Appended(d) if d.data_id == data_id(20, 2)));

        log.push_unstable_at(data_id(30, 3), Bytes::from_static(b"live"));
        log.push_unstable_at(data_id(8, 4), Bytes::from_static(b"late"));
        log.watermark(25);
        assert!(matches!(next(), UnstableEvent::Appended(d) if d.data_id == data_id(30, 3)));
        assert_eq!(next(), UnstableEvent::Watermark(hlc(25)));

        log.hang_up_unstable();
        assert_eq!(
            sub.recv().err(),
            Some(crossbeam_combinators::AbortReason::ProducerGone {
                what: "test-log unstable",
            })
        );
    }
}
