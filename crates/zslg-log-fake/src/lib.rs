//! In-memory implementations of the log ports, for tests of the crates that
//! consume them.

mod replica_write;
mod test_log;

pub use replica_write::replica_write;

pub use test_log::{TestLog, data_id};

use std::collections::BTreeMap;

use async_trait::async_trait;
use crossbeam_combinators::{Channel, Finite, Infinite, Level, LevelSetter};
use parking_lot::Mutex;
use zslg_consensus::NodeId;
use zslg_log::{
    Data, DataId, LocalStoreMut, LocalStoreStream, LocalStoreView, LogIndex, SnapshotEntry,
    StableLogEvent, StableLogSnapshot,
};
use zslg_time::Hlc;

#[derive(Default)]
struct State {
    entries: BTreeMap<LogIndex, Data>,
    end_hlc: Option<Hlc>,
    purge_threshold: Option<Hlc>,
    subscribers: Vec<crossbeam_channel::Sender<StableLogEvent>>,
}

/// A `StableLogSnapshot` held in memory: appended entries are kept, the end HLC
/// is the highest of what was appended or recorded, and the watermark is
/// whatever the test sets.
pub struct FakeStableLog {
    state: Mutex<State>,
    watermark: Level<Option<Hlc>>,
    watermark_setter: LevelSetter<Option<Hlc>>,
}

impl Default for FakeStableLog {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeStableLog {
    #[must_use]
    pub fn new() -> Self {
        let (watermark_setter, watermark) = Level::new("fake stable log watermark", None);
        Self {
            state: Mutex::new(State::default()),
            watermark,
            watermark_setter,
        }
    }

    fn publish(state: &mut State, event: &StableLogEvent) {
        state
            .subscribers
            .retain(|tx| tx.send(event.clone()).is_ok());
    }

    /// Move the end HLC forward to `to`, publishing a `Watermark` event only for
    /// a genuine advance.
    fn advance(&self, state: &mut State, to: Hlc) {
        if state.end_hlc.is_none_or(|prev| to > prev) {
            state.end_hlc = Some(to);
            Self::publish(state, &StableLogEvent::Watermark(to));
            self.watermark_setter.set(Some(to));
        }
    }

    /// The purge threshold last requested.
    #[must_use]
    pub fn purge_threshold(&self) -> Option<Hlc> {
        self.state.lock().purge_threshold
    }
}

#[async_trait]
impl StableLogSnapshot for FakeStableLog {
    fn end_index(&self) -> Option<LogIndex> {
        self.state.lock().entries.keys().next_back().copied()
    }

    fn end_hlc(&self) -> Option<Hlc> {
        self.state.lock().end_hlc
    }

    async fn append(
        &self,
        mut entries: Channel<Result<SnapshotEntry, zslg_log::Error>, Finite>,
    ) -> Result<usize, zslg_log::Error> {
        let mut n = 0;
        while let Ok(item) = entries.recv() {
            let entry = item?;
            let hlc = entry.data.data_id.hlc();
            let mut state = self.state.lock();
            state.entries.insert(entry.global_index, entry.data.clone());
            Self::publish(
                &mut state,
                &StableLogEvent::Appended {
                    index: entry.global_index,
                    data: entry.data,
                },
            );
            // Settled only strictly below the entry: the entry's own HLC waits
            // for `record_end_hlc`, as in the real log.
            if let Some(settled) = hlc.raw().checked_sub(1).map(Hlc) {
                self.advance(&mut state, settled);
            }
            n += 1;
        }
        Ok(n)
    }

    async fn record_end_hlc(&self, end_hlc: Hlc) -> Result<(), zslg_log::Error> {
        let mut state = self.state.lock();
        self.advance(&mut state, end_hlc);
        Ok(())
    }

    fn subscribe_events(&self) -> Channel<StableLogEvent, Infinite> {
        let (tx, rx) = crossbeam_channel::unbounded();
        self.state.lock().subscribers.push(tx);
        Channel::from_infinite(rx, "fake stable log events")
    }

    fn watermark(&self) -> Level<Option<Hlc>> {
        self.watermark.clone()
    }

    fn range(
        &self,
        after: Option<LogIndex>,
        until: LogIndex,
    ) -> Channel<Result<(LogIndex, Data), zslg_log::Error>, Finite> {
        let from = LogIndex::next_after(after);
        let state = self.state.lock();
        // The purged prefix (`hlc <= threshold`) is clamped out of `range` at once.
        let threshold = state.purge_threshold;
        let items = state
            .entries
            .range(from..until)
            .filter(|(_, d)| threshold.is_none_or(|t| d.data_id.hlc() > t))
            .map(|(i, d)| Ok((*i, d.clone())))
            .collect();
        Channel::from_vec(items)
    }

    fn set_purge_threshold(&self, hlc_threshold: Hlc) -> Result<(), zslg_log::Error> {
        self.state.lock().purge_threshold = Some(hlc_threshold);
        Ok(())
    }
}

/// A deterministic [`zslg_log::DataId`]: nil creator node, HLC-seeded, with
/// `seed` as the tiebreaker.
#[must_use]
pub fn make_meta(seed: u8, hlc: u64) -> zslg_log::DataId {
    zslg_log::DataId::from_parts(
        Hlc(hlc),
        zslg_consensus::NodeId::from_uuid(uuid::Uuid::nil()),
        u64::from(seed),
    )
}

/// A [`Data`] with a deterministic id and the given payload.
#[must_use]
pub fn make_data(seed: u8, hlc: u64, blob: &[u8]) -> Data {
    Data {
        data_id: make_meta(seed, hlc),
        blob: bytes::Bytes::copy_from_slice(blob),
    }
}

#[derive(Default)]
struct StoreState {
    data: BTreeMap<DataId, Data>,
    cursors: BTreeMap<NodeId, LogIndex>,
}

/// A `LocalStoreMut` held in memory: integrated records are kept by id, and the
/// per-upstream cursor is the highest index integrated from that upstream.
pub struct FakeLocalStore {
    state: Mutex<StoreState>,
    integrated: tokio::sync::watch::Sender<()>,
}

impl Default for FakeLocalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeLocalStore {
    #[must_use]
    pub fn new() -> Self {
        let (integrated, _) = tokio::sync::watch::channel(());
        Self {
            state: Mutex::new(StoreState::default()),
            integrated,
        }
    }

    /// Everything integrated so far, in id order.
    #[must_use]
    pub fn records(&self) -> Vec<Data> {
        self.state.lock().data.values().cloned().collect()
    }
}

#[async_trait]
impl LocalStoreView for FakeLocalStore {
    fn stream_above_hlc(&self, threshold: Option<Hlc>) -> LocalStoreStream {
        let items = self
            .state
            .lock()
            .data
            .values()
            .filter(|d| threshold.is_none_or(|t| d.data_id.hlc() > t))
            .cloned()
            .map(Ok)
            .collect();
        Channel::from_vec(items).ordered(None, "fake local store")
    }

    fn stream_window(&self, above: Option<Hlc>, upto: Hlc) -> LocalStoreStream {
        let items = self
            .state
            .lock()
            .data
            .values()
            .filter(|d| {
                let h = d.data_id.hlc();
                above.is_none_or(|a| h > a) && h <= upto
            })
            .cloned()
            .map(Ok)
            .collect();
        Channel::from_vec(items).ordered(None, "fake local store window")
    }

    fn subscribe_integrated(&self) -> tokio::sync::watch::Receiver<()> {
        self.integrated.subscribe()
    }

    fn subscribe_unstable(&self) -> Channel<Data, Infinite> {
        let (_tx, rx) = crossbeam_channel::unbounded();
        Channel::from_infinite(rx, "fake local store unstable")
    }

    async fn subscription_cursor(
        &self,
        upstream: NodeId,
    ) -> Result<Option<LogIndex>, zslg_log::Error> {
        Ok(self.state.lock().cursors.get(&upstream).copied())
    }
}

#[async_trait]
impl LocalStoreMut for FakeLocalStore {
    async fn integrate_loop(
        &self,
        mut records: Channel<Result<(NodeId, LogIndex, Data), zslg_log::Error>, Finite>,
    ) -> Result<(), zslg_log::Error> {
        while let Ok(item) = records.recv() {
            let (source, index, data) = item?;
            let mut state = self.state.lock();
            let cursor = state.cursors.entry(source).or_insert(index);
            *cursor = (*cursor).max(index);
            state.data.insert(data.data_id, data);
        }
        self.integrated.send_replace(());
        Ok(())
    }

    async fn purge_below_hlc(&self, threshold: Hlc) -> Result<(), zslg_log::Error> {
        self.state
            .lock()
            .data
            .retain(|_, d| d.data_id.hlc() > threshold);
        Ok(())
    }
}
