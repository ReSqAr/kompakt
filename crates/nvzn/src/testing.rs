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
use bytes::Bytes;
use crossbeam_combinators::Channels;
use std::path::Path;
use std::sync::{Arc, Mutex};
use zslg::{Data, DataId, Hlc, LogIndex, StableLogEvent, UnstableEvent};

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

/// A [`DataId`] from a counter, so ids are ordered by the order they were
/// handed out rather than by a clock a test cannot control.
#[must_use]
pub fn data_id(hlc_ms: u64, seed: u64) -> DataId {
    DataId::test_id(Hlc::from_bytes(hlc_ms.to_be_bytes()), seed)
}

#[derive(Default)]
struct Shared {
    history: Vec<StableLogEvent>,
    unstable: Vec<Data>,
    subscribed_from: Vec<LogIndex>,
    next_index: u64,
    next_seed: u64,
}

type StableSender = crossbeam_channel::Sender<Result<StableLogEvent, zslg::Error>>;
type UnstableSender = crossbeam_channel::Sender<Result<UnstableEvent, zslg::Error>>;

/// An append-only log in this process, handing out [`zslg::Reader`] and
/// [`zslg::Writer`] views of itself.
///
/// Hold the `TestLog` for as long as the rig is in use: the subscriber senders
/// live behind a `Weak` on it, so dropping it stops the fold.
pub struct TestLog {
    shared: Arc<Mutex<Shared>>,
    senders: Arc<Mutex<Vec<StableSender>>>,
    unstable_senders: Arc<Mutex<Vec<UnstableSender>>>,
    wm_tx: tokio::sync::watch::Sender<Option<Hlc>>,
}

impl TestLog {
    #[must_use]
    pub fn new() -> Self {
        let (wm_tx, _) = tokio::sync::watch::channel(None);
        Self {
            shared: Arc::new(Mutex::new(Shared::default())),
            senders: Arc::new(Mutex::new(Vec::new())),
            unstable_senders: Arc::new(Mutex::new(Vec::new())),
            wm_tx,
        }
    }

    #[must_use]
    pub fn reader(&self) -> Arc<dyn zslg::Reader> {
        Arc::new(TestReader {
            shared: Arc::clone(&self.shared),
            senders: Arc::downgrade(&self.senders),
            unstable_senders: Arc::downgrade(&self.unstable_senders),
            wm_rx: self.wm_tx.subscribe(),
        })
    }

    #[must_use]
    pub fn writer(&self) -> Arc<dyn zslg::Writer> {
        Arc::new(TestWriter {
            shared: Arc::clone(&self.shared),
            senders: Arc::downgrade(&self.senders),
            unstable_senders: Arc::downgrade(&self.unstable_senders),
            wm_tx: self.wm_tx.clone(),
        })
    }

    /// The `from` of every [`zslg::Reader::subscribe_stable_log`] served so
    /// far — what a test asserts on to show where a fold resumed.
    #[must_use]
    pub fn subscribed_from(&self) -> Vec<LogIndex> {
        self.shared
            .lock()
            .expect("test mutex")
            .subscribed_from
            .clone()
    }

    /// Drop every live subscriber, as a partition would.
    pub fn disconnect(&self) {
        self.senders.lock().expect("test mutex").clear();
    }

    /// Drop every live unstable-tail subscriber: each ends `Aborted` after
    /// what was already queued, as a zslg fan-out hang-up does.
    pub fn hang_up_unstable(&self) {
        self.unstable_senders.lock().expect("test mutex").clear();
    }

    pub fn append(&mut self, blob: Bytes) {
        let (index, seed) = self.next();
        self.append_at(index, data_id(seed, seed), blob);
        self.watermark(seed);
    }

    pub fn append_at(&mut self, index: u64, did: DataId, blob: Bytes) {
        emit(
            &self.shared,
            &self.senders,
            StableLogEvent::Appended {
                index: LogIndex::from_bytes(index.to_be_bytes()),
                data: Data { data_id: did, blob },
            },
        );
    }

    pub fn watermark(&self, hlc_ms: u64) {
        let hlc = Hlc::from_bytes(hlc_ms.to_be_bytes());
        emit(&self.shared, &self.senders, StableLogEvent::Watermark(hlc));
        advance_watermark(&self.shared, &self.unstable_senders, &self.wm_tx, hlc);
    }

    /// Stage a payload above the watermark, where only a
    /// [`Live`](crate::Live) view and the unstable-tail subscribers see it.
    pub fn push_unstable(&mut self, blob: Bytes) -> DataId {
        let (_, seed) = self.next();
        let did = data_id(seed, seed);
        self.push_unstable_at(did, blob);
        did
    }

    /// Live subscribers get it only if it is above the watermark, as zslg
    /// would announce it.
    pub fn push_unstable_at(&self, did: DataId, blob: Bytes) {
        let data = Data { data_id: did, blob };
        let mut shared = self.shared.lock().expect("test mutex");
        shared.unstable.push(data.clone());
        if self.wm_tx.borrow().is_none_or(|w| did.hlc() > w) {
            fan_out_unstable(&self.unstable_senders, &UnstableEvent::Appended(data));
        }
        drop(shared);
    }

    fn next(&mut self) -> (u64, u64) {
        next_ids(&self.shared)
    }
}

impl Default for TestLog {
    fn default() -> Self {
        Self::new()
    }
}

fn next_ids(shared: &Arc<Mutex<Shared>>) -> (u64, u64) {
    let mut shared = shared.lock().expect("test mutex");
    let index = shared.next_index;
    shared.next_index = index.checked_add(1).expect("test log overflow");
    let seed = shared.next_seed;
    shared.next_seed = seed.checked_add(1).expect("test log overflow");
    (index, seed)
}

fn emit(shared: &Arc<Mutex<Shared>>, senders: &Arc<Mutex<Vec<StableSender>>>, ev: StableLogEvent) {
    shared.lock().expect("test mutex").history.push(ev.clone());
    senders
        .lock()
        .expect("test mutex")
        .retain(|tx| tx.send(Ok(ev.clone())).is_ok());
}

/// Raise the watermark and announce it to the unstable subscribers, under the
/// `shared` lock so a concurrent subscribe sees either the old level and the
/// announcement, or the new level.
fn advance_watermark(
    shared: &Arc<Mutex<Shared>>,
    unstable_senders: &Arc<Mutex<Vec<UnstableSender>>>,
    wm_tx: &tokio::sync::watch::Sender<Option<Hlc>>,
    hlc: Hlc,
) {
    let shared = shared.lock().expect("test mutex");
    wm_tx.send_replace(Some(hlc));
    fan_out_unstable(unstable_senders, &UnstableEvent::Watermark(hlc));
    drop(shared);
}

fn fan_out_unstable(senders: &Arc<Mutex<Vec<UnstableSender>>>, ev: &UnstableEvent) {
    senders
        .lock()
        .expect("test mutex")
        .retain(|tx| tx.send(Ok(ev.clone())).is_ok());
}

struct TestReader {
    shared: Arc<Mutex<Shared>>,
    senders: std::sync::Weak<Mutex<Vec<StableSender>>>,
    unstable_senders: std::sync::Weak<Mutex<Vec<UnstableSender>>>,
    wm_rx: tokio::sync::watch::Receiver<Option<Hlc>>,
}

#[async_trait::async_trait]
impl zslg::Reader for TestReader {
    async fn sync(&self) -> Result<(), zslg::Error> {
        Ok(())
    }

    fn stable_watermark(&self) -> Option<Hlc> {
        *self.wm_rx.borrow()
    }

    /// The HLC of the last write the log took: every write so far is at or
    /// below it, and an `append` makes the watermark reach it.
    fn hlc_now(&self) -> Hlc {
        let next = self.shared.lock().expect("test mutex").next_seed;
        Hlc::from_bytes(next.saturating_sub(1).to_be_bytes())
    }

    fn subscribe_stable_log(
        &self,
        from: LogIndex,
    ) -> Channels<Result<StableLogEvent, zslg::Error>> {
        let (tx, rx) = crossbeam_channel::bounded(4096);

        let mut shared = self.shared.lock().expect("test mutex");
        shared.subscribed_from.push(from);
        for ev in &shared.history {
            if let StableLogEvent::Appended { index, .. } = ev
                && *index < from
            {
                continue;
            }
            tx.send(Ok(ev.clone()))
                .expect("replay into a fresh channel");
        }
        drop(shared);

        if let Some(senders) = self.senders.upgrade() {
            senders.lock().expect("test mutex").push(tx);
        }

        Channels::from_infinite(rx, "test-log")
    }

    fn unstable_tail(&self) -> Channels<Result<Data, zslg::Error>> {
        let items = self
            .shared
            .lock()
            .expect("test mutex")
            .unstable
            .iter()
            .cloned()
            .map(Ok)
            .collect();
        Channels::from_vec(items)
    }

    /// One cycle of zslg's: the watermark, the staged entries above it, then
    /// live. No resync — a hang-up ends it `Aborted`.
    fn subscribe_unstable_tail(&self) -> Channels<Result<UnstableEvent, zslg::Error>> {
        let (tx, rx) = crossbeam_channel::unbounded();

        let shared = self.shared.lock().expect("test mutex");
        let watermark = self.stable_watermark();
        if let Some(w) = watermark {
            tx.send(Ok(UnstableEvent::Watermark(w)))
                .expect("replay into a fresh channel");
        }
        for data in shared
            .unstable
            .iter()
            .filter(|d| watermark.is_none_or(|w| d.data_id.hlc() > w))
        {
            tx.send(Ok(UnstableEvent::Appended(data.clone())))
                .expect("replay into a fresh channel");
        }
        if let Some(senders) = self.unstable_senders.upgrade() {
            senders.lock().expect("test mutex").push(tx);
        }
        drop(shared);

        Channels::from_infinite(rx, "test-log unstable")
    }
}

struct TestWriter {
    shared: Arc<Mutex<Shared>>,
    senders: std::sync::Weak<Mutex<Vec<StableSender>>>,
    unstable_senders: std::sync::Weak<Mutex<Vec<UnstableSender>>>,
    wm_tx: tokio::sync::watch::Sender<Option<Hlc>>,
}

#[async_trait::async_trait]
impl zslg::Writer for TestWriter {
    async fn write(&self, blob: Bytes) -> Result<DataId, zslg::Error> {
        let (index, seed) = next_ids(&self.shared);
        let did = data_id(seed, seed);
        let hlc = Hlc::from_bytes(seed.to_be_bytes());

        if let Some(senders) = self.senders.upgrade() {
            emit(
                &self.shared,
                &senders,
                StableLogEvent::Appended {
                    index: LogIndex::from_bytes(index.to_be_bytes()),
                    data: Data { data_id: did, blob },
                },
            );
            emit(&self.shared, &senders, StableLogEvent::Watermark(hlc));
        }
        match self.unstable_senders.upgrade() {
            Some(unstable) => advance_watermark(&self.shared, &unstable, &self.wm_tx, hlc),
            None => {
                self.wm_tx.send_replace(Some(hlc));
            }
        }

        Ok(did)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Some(crossbeam_combinators::End::Aborted {
                what: "test-log unstable"
            })
        );
    }
}
