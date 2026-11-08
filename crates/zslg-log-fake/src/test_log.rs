//! An in-process log serving the `Reader` and `Writer` ports, so a consumer can
//! test against the log without a cluster. Re-exported as `zslg::testing` and
//! `nvzn::testing`.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use crossbeam_combinators::{Channel, Finite, Infinite};
use zslg_log::{Data, DataId, LogIndex, StableLogEvent, UnstableEvent};
use zslg_time::Hlc;

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
    subscribed_after: Vec<Option<LogIndex>>,
    next_index: u64,
    next_seed: u64,
}

type StableSender = crossbeam_channel::Sender<Result<StableLogEvent, zslg_log::Error>>;
type UnstableSender = crossbeam_channel::Sender<Result<UnstableEvent, zslg_log::Error>>;

/// An append-only log in this process, handing out [`zslg_log::Reader`] and
/// [`zslg_log::Writer`] views of itself.
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
    pub fn reader(&self) -> Arc<dyn zslg_log::Reader> {
        Arc::new(TestReader {
            shared: Arc::clone(&self.shared),
            senders: Arc::downgrade(&self.senders),
            unstable_senders: Arc::downgrade(&self.unstable_senders),
            wm_rx: self.wm_tx.subscribe(),
        })
    }

    #[must_use]
    pub fn writer(&self) -> Arc<dyn zslg_log::Writer> {
        Arc::new(TestWriter {
            shared: Arc::clone(&self.shared),
            senders: Arc::downgrade(&self.senders),
            unstable_senders: Arc::downgrade(&self.unstable_senders),
            wm_tx: self.wm_tx.clone(),
        })
    }

    /// The `after` of every [`zslg_log::Reader::subscribe_stable_log`] served so
    /// far — what a test asserts on to show where a fold resumed.
    #[must_use]
    pub fn subscribed_after(&self) -> Vec<Option<LogIndex>> {
        self.shared
            .lock()
            .expect("test mutex")
            .subscribed_after
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
impl zslg_log::Reader for TestReader {
    async fn sync(&self) -> Result<(), zslg_log::Error> {
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

    fn subscribe_stable_log(&self, after: Option<LogIndex>) -> zslg_log::StableStream {
        let (tx, rx) = crossbeam_channel::bounded(4096);

        let mut shared = self.shared.lock().expect("test mutex");
        shared.subscribed_after.push(after);
        for ev in &shared.history {
            if let StableLogEvent::Appended { index, .. } = ev
                && Some(*index) <= after
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

        zslg_log::StableStream::new(Channel::from_infinite(rx, "test-log"), after)
    }

    fn unstable_tail(&self) -> Channel<Result<Data, zslg_log::Error>, Finite> {
        let items = self
            .shared
            .lock()
            .expect("test mutex")
            .unstable
            .iter()
            .cloned()
            .map(Ok)
            .collect();
        Channel::from_vec(items)
    }

    /// One cycle of zslg's: the watermark, the staged entries above it, then
    /// live. No resync — a hang-up ends it `Aborted`.
    fn subscribe_unstable_tail(&self) -> Channel<Result<UnstableEvent, zslg_log::Error>, Infinite> {
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

        Channel::from_infinite(rx, "test-log unstable")
    }
}

struct TestWriter {
    shared: Arc<Mutex<Shared>>,
    senders: std::sync::Weak<Mutex<Vec<StableSender>>>,
    unstable_senders: std::sync::Weak<Mutex<Vec<UnstableSender>>>,
    wm_tx: tokio::sync::watch::Sender<Option<Hlc>>,
}

#[async_trait::async_trait]
impl zslg_log::Writer for TestWriter {
    async fn write(&self, blob: Bytes) -> Result<DataId, zslg_log::Error> {
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
