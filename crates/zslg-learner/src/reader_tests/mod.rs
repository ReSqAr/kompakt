//! The reader against a real local store (with a fake stable log) and scripted
//! snapshot stores: an integration of `zslg-reader` and `zslg-local-store`.
//! The rigs and fakes live here; the tests are split by what they read.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]

mod quorum_and_clock;
mod stable_tail;
mod unstable_tail;

use super::*;

use crossbeam_combinators::testing::ChannelTestExt as _;

use std::sync::Arc;

use std::time::Duration;

use zslg_lifecycle_types::LifecycleError;

use zslg_log::SnapshotEntry;

use zslg_snapshot::SnapshotError;

use zslg_snapshot::{CleanupReport, SnapshotWrite};

use zslg_snapshot::{CompressionLevel, SnapshotId};

use zslg_log::Reader;

use crossbeam_combinators::{Cancellation, Channel, Finite, Infinite};

use zslg_log::StableLogEvent;

use zslg_log::{LocalStoreMut, StableLogSnapshot};

use crate::{LocalStore, QuorumReadDebouncer};

use zslg_consensus::NodeId;

use zslg_log::LogIndex;

use zslg_log_fake::FakeStableLog;

use zslg_time::Hlc;

/// A node clock for the reader under test.
pub(super) fn test_clock() -> Arc<dyn zslg_time::Clock> {
    Arc::new(zslg_hlc_clock::HlcClock::new(Duration::from_secs(60)))
}

use async_trait::async_trait;

use futures::{StreamExt, TryStreamExt as _};

use zslg_log::Data;

use super::ReaderImpl;

use zslg_consensus::RaftStateHandle;

use zslg_stable_archive::ObjectStoreArchiveReader;

pub(super) struct NoOpCheck;

#[async_trait]
impl zslg_consensus::QuorumCheck for NoOpCheck {
    async fn check(&self) -> Result<(), zslg_consensus::Error> {
        Ok(())
    }
}

pub(super) fn local_reader(
    local_store: Arc<LocalStore>,
    stable_log: Arc<FakeStableLog>,
    deadtime: Duration,
) -> ReaderImpl {
    reader_with_source(
        local_store,
        stable_log,
        deadtime,
        Arc::new(ScriptedSnapshotStore::default()),
        Arc::new(RaftStateHandle::default()),
    )
}

pub(super) fn reader_with_source(
    local_store: Arc<LocalStore>,
    stable_log: Arc<FakeStableLog>,
    deadtime: Duration,
    snapshot_store: Arc<dyn zslg_snapshot::SnapshotStore>,
    raft_state: Arc<dyn zslg_consensus::RaftStateView>,
) -> ReaderImpl {
    reader_shut_down_by(
        local_store,
        stable_log,
        deadtime,
        snapshot_store,
        raft_state,
        Cancellation::never(),
    )
}

pub(super) fn reader_shut_down_by(
    local_store: Arc<LocalStore>,
    stable_log: Arc<FakeStableLog>,
    deadtime: Duration,
    snapshot_store: Arc<dyn zslg_snapshot::SnapshotStore>,
    raft_state: Arc<dyn zslg_consensus::RaftStateView>,
    shutdown: Cancellation,
) -> ReaderImpl {
    let debouncer = Arc::new(QuorumReadDebouncer::for_test(
        Arc::new(NoOpCheck),
        deadtime,
        Duration::from_secs(5),
    ));
    let archive: Arc<dyn zslg_log::StableArchiveReader> =
        Arc::new(ObjectStoreArchiveReader::new(snapshot_store, raft_state));
    ReaderImpl::new(
        local_store,
        stable_log,
        archive,
        debouncer,
        shutdown,
        test_clock(),
    )
}

/// Minimal `SnapshotStore` for reader tests: `read_snapshot` replays a
/// scripted per-id entry list; the other methods are never called here.
#[derive(Default)]
pub(super) struct ScriptedSnapshotStore {
    snapshots: std::sync::Mutex<
        std::collections::HashMap<zslg_snapshot::SnapshotId, Vec<zslg_log::SnapshotEntry>>,
    >,
}

impl ScriptedSnapshotStore {
    fn script(&self, id: zslg_snapshot::SnapshotId, entries: Vec<zslg_log::SnapshotEntry>) {
        self.snapshots.lock().expect("lock").insert(id, entries);
    }
}

#[async_trait]
impl zslg_snapshot::SnapshotStore for ScriptedSnapshotStore {
    async fn write_snapshot(
        &self,
        _id: SnapshotId,
        _entries: Channel<Result<SnapshotEntry, zslg_snapshot::Error>, Finite>,
        _compression: CompressionLevel,
        _reason: zslg_snapshot::ObjectStoreReason,
    ) -> Result<SnapshotWrite, zslg_snapshot::Error> {
        unimplemented!("reader tests never write snapshots")
    }
    async fn read_snapshot(
        &self,
        id: SnapshotId,
        _reason: zslg_snapshot::ObjectStoreReason,
    ) -> Result<Channel<Result<SnapshotEntry, zslg_snapshot::Error>, Finite>, zslg_snapshot::Error>
    {
        let entries = self
            .snapshots
            .lock()
            .expect("lock")
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                zslg_snapshot::Error::Snapshot(SnapshotError::ObjectStore(format!(
                    "no scripted snapshot {id:?}"
                )))
            })?;
        Ok(Channel::from_vec(entries.into_iter().map(Ok).collect()))
    }
    fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        tokio_util::sync::CancellationToken::new()
    }
    async fn has_bootstrap_fence(&self) -> Result<bool, zslg_snapshot::Error> {
        unimplemented!()
    }
    async fn write_bootstrap_fence(&self) -> Result<(), zslg_snapshot::Error> {
        unimplemented!()
    }
    async fn cleanup_snapshots(
        &self,
        _live_snapshots: std::collections::BTreeSet<SnapshotId>,
        _cleanup_retention: Duration,
    ) -> Result<CleanupReport, zslg_snapshot::Error> {
        unimplemented!()
    }
}

use zslg_consensus_fake::upstream;

use zslg_log_fake::make_data;

/// Pre-load a closed `Channel` for the channel-based `append`.
pub(super) fn rx_of(
    items: impl IntoIterator<Item = Result<(LogIndex, Data), crate::Error>>,
) -> crossbeam_combinators::Channel<Result<zslg_log::SnapshotEntry, crate::Error>, Finite> {
    crossbeam_combinators::Channel::from_vec(
        items
            .into_iter()
            .map(|item| {
                item.map(|(global_index, data)| zslg_log::SnapshotEntry { global_index, data })
            })
            .collect(),
    )
}

pub(super) async fn integrate_batch(
    store: &LocalStore,
    source: NodeId,
    records: Vec<(LogIndex, Data)>,
) -> Result<(), crate::Error> {
    if records.is_empty() {
        return Ok(());
    }
    let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
    for (idx, rec) in records {
        let _ = tx.send(Ok((source, idx, rec)));
    }
    let (ch, closer) = crossbeam_combinators::Channel::from(rx, "test integrate");
    // Close before the last sender drops, or the consumer forms its
    // verdict as "disconnected, not yet closed" — an abort.
    closer.close();
    drop(tx);
    store.integrate_loop(ch).await.map_err(Into::into)
}

/// A read-only `StableLogSnapshot` whose `range` replays a fixed — possibly
/// HOLEY — entry list. The purge clamp is prefix-clean and cannot punch an
/// interior hole on demand, so the reader's gap-fill is exercised by
/// scripting the scan directly (this models the unclamped /
/// crash-resurrected on-disk set a reopen can expose before the clamp is
/// re-armed).
pub(super) struct ScriptedStableLog {
    /// One `range` call per replay cycle — the observable cost of a resync.
    ranges: std::sync::atomic::AtomicUsize,
    entries: std::sync::Mutex<Vec<(LogIndex, Data)>>,
    end_index: std::sync::Mutex<Option<LogIndex>>,
    end_hlc: std::sync::Mutex<Option<Hlc>>,
    events: crossbeam_combinators::Fanout<StableLogEvent>,
    events_registry: crossbeam_combinators::Registry<StableLogEvent>,
    watermark: crossbeam_combinators::LevelSetter<Option<Hlc>>,
    watermark_level: crossbeam_combinators::Level<Option<Hlc>>,
}

impl ScriptedStableLog {
    fn new(entries: Vec<(LogIndex, Data)>) -> Self {
        Self::with_broadcast_capacity(entries, 16)
    }

    /// A shallow ring makes `RecvError::Lagged` deterministic: append more
    /// than `capacity` entries without draining and the subscriber is
    /// guaranteed to be lapped.
    fn with_broadcast_capacity(entries: Vec<(LogIndex, Data)>, capacity: usize) -> Self {
        let end_index = entries.last().map(|(i, _)| *i);
        let end_hlc = entries.last().map(|(_, d)| d.data_id.hlc());
        let (events, events_registry) = crossbeam_combinators::broadcast(
            "stable log subscription",
            crossbeam_combinators::FanoutConfig::with_sub_cap(
                std::num::NonZeroUsize::new(capacity).expect("capacity > 0"),
            ),
            crossbeam_combinators::OverflowPolicy::Shed,
        );
        let (watermark, watermark_level) =
            crossbeam_combinators::Level::new("scripted moves", end_hlc);
        Self {
            ranges: std::sync::atomic::AtomicUsize::new(0),
            entries: std::sync::Mutex::new(entries),
            end_index: std::sync::Mutex::new(end_index),
            end_hlc: std::sync::Mutex::new(end_hlc),
            events,
            events_registry,
            watermark,
            watermark_level,
        }
    }

    /// Raise the watermark and broadcast it, as a snapshot load does.
    fn push_watermark(&self, hlc: Hlc) {
        *self.end_hlc.lock().expect("lock") = Some(hlc);
        let _ = self
            .events
            .publish(zslg_log::StableLogEvent::Watermark(hlc));
        self.watermark.set(Some(hlc));
    }

    /// Durably append (visible to `range`/`end_index`) and broadcast, in
    /// that order — the ordering production takes, and the one the
    /// resubscribe-before-frontier argument is proved against.
    fn push(&self, index: LogIndex, data: Data) {
        let hlc = data.data_id.hlc();
        self.entries
            .lock()
            .expect("lock")
            .push((index, data.clone()));
        *self.end_index.lock().expect("lock") = Some(index);
        *self.end_hlc.lock().expect("lock") = Some(hlc);
        let _ = self
            .events
            .publish(zslg_log::StableLogEvent::Appended { index, data });
    }
}

#[async_trait]
impl StableLogSnapshot for ScriptedStableLog {
    fn end_index(&self) -> Option<LogIndex> {
        *self.end_index.lock().expect("lock")
    }
    fn end_hlc(&self) -> Option<Hlc> {
        *self.end_hlc.lock().expect("lock")
    }
    async fn append(
        &self,
        _entries: Channel<Result<zslg_log::SnapshotEntry, zslg_log::Error>, Finite>,
    ) -> Result<usize, zslg_log::Error> {
        unimplemented!("scripted stable log is read-only")
    }
    async fn record_end_hlc(&self, _end_hlc: Hlc) -> Result<(), zslg_log::Error> {
        unimplemented!("scripted stable log is read-only")
    }
    fn subscribe_events(&self) -> Channel<zslg_log::StableLogEvent, Infinite> {
        self.events_registry.subscribe_infinite()
    }
    fn watermark(&self) -> crossbeam_combinators::Level<Option<Hlc>> {
        self.watermark_level.clone()
    }
    fn range(
        &self,
        after: Option<LogIndex>,
        until: LogIndex,
    ) -> Channel<Result<(LogIndex, Data), zslg_log::Error>, Finite> {
        self.ranges
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let items: Vec<Result<(LogIndex, Data), zslg_log::Error>> = self
            .entries
            .lock()
            .expect("lock")
            .iter()
            .filter(|(i, _)| after.is_none_or(|a| *i > a) && *i < until)
            .cloned()
            .map(Ok)
            .collect();
        Channel::from_vec(items)
    }
    fn set_purge_threshold(&self, _hlc_threshold: Hlc) -> Result<(), zslg_log::Error> {
        Ok(())
    }
}

/// Wire a reader over a scripted (holey) stable log + covering archive. The
/// returned `TempDir` backs the (unused-by-`stable_tail`) local store and
/// must be kept alive for the reader's lifetime.
pub(super) async fn scripted_reader(
    stable_log: Arc<dyn StableLogSnapshot>,
    snapshot_store: Arc<dyn zslg_snapshot::SnapshotStore>,
    raft_state: Arc<dyn zslg_consensus::RaftStateView>,
) -> (tempfile::TempDir, ReaderImpl) {
    let debouncer = Arc::new(QuorumReadDebouncer::for_test(
        Arc::new(NoOpCheck),
        Duration::from_millis(5),
        Duration::from_secs(5),
    ));
    let archive: Arc<dyn zslg_log::StableArchiveReader> =
        Arc::new(ObjectStoreArchiveReader::new(snapshot_store, raft_state));
    let dir = tempfile::tempdir().expect("tempdir");
    let local_store = crate::testing::local_store_at(dir.path().join("local.stable"))
        .await
        .expect("from_db");
    (
        dir,
        ReaderImpl::new(
            local_store,
            stable_log,
            archive,
            debouncer,
            Cancellation::never(),
            test_clock(),
        ),
    )
}

pub(super) fn covering_manifest(
    snap_id: zslg_snapshot::SnapshotId,
    end: u64,
) -> zslg_consensus::RaftState {
    use zslg_consensus::RaftManifestState;
    use zslg_snapshot::SnapshotMetadata;
    let end_hlc = Hlc(end.saturating_add(100));
    let mut rs = zslg_consensus::RaftState::empty();
    rs.manifest_state = RaftManifestState {
        manifest: vec![SnapshotMetadata {
            start_hlc: Hlc(100),
            end_hlc,
            start_index: LogIndex(0),
            end_index: LogIndex(end),
            snapshot_id: snap_id,
        }],
        manifest_watermark: Some(end_hlc),
        hlc_cutoff: Some(end_hlc),
    };
    rs
}

// ── subscribe_unstable_tail ─────────────────────────────────────────────

/// A reader over a local store and the stable log it reads its watermark
/// from, on one `Storage` (the production shape), with the stable
/// watermark at `watermark` and unstable queues `capacity` deep.
async fn unstable_rig(
    capacity: usize,
    watermark: Hlc,
    shutdown: Cancellation,
) -> (
    tempfile::TempDir,
    Arc<LocalStore>,
    Arc<FakeStableLog>,
    ReaderImpl,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = zslg_local_store::LocalStoreConfig::for_test();
    config.unstable_subscriber_queue_capacity =
        std::num::NonZeroUsize::new(capacity).expect("capacity > 0");
    let (local, stable) = crate::testing::local_store_with(dir.path().join("rig"), &config)
        .await
        .expect("open");
    stable.record_end_hlc(watermark).await.expect("watermark");
    let reader = reader_shut_down_by(
        local.clone(),
        stable.clone(),
        Duration::from_millis(5),
        Arc::new(ScriptedSnapshotStore::default()),
        Arc::new(RaftStateHandle::default()),
        shutdown,
    );
    (dir, local, stable, reader)
}

/// The next event, without blocking the runtime the store's workers run on.
pub(super) async fn next_unstable(
    stream: &mut Channel<Result<zslg_log::UnstableEvent, zslg_log::Error>, Infinite>,
) -> zslg_log::UnstableEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(item) = stream.try_recv() {
            return item.expect("an unstable subscription never errors here");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "unstable subscription stalled"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Nothing further arrives within a short grace.
pub(super) async fn assert_quiet(
    stream: &mut Channel<Result<zslg_log::UnstableEvent, zslg_log::Error>, Infinite>,
) {
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(stream.try_recv().is_none(), "expected no further event");
}

pub(super) async fn integrate_one(local: &LocalStore, index: u64, data: Data) {
    integrate_batch(local, upstream(1), vec![(LogIndex(index), data)])
        .await
        .expect("integrate");
}

#[derive(Debug, Clone)]
pub(super) enum UnstableOp {
    /// Integrate a new entry this many HLC ticks above the watermark
    /// (0 = at it, so the store skips it).
    Integrate(u8),
    /// Integrate an earlier entry again, from another upstream.
    Duplicate(usize),
    Advance(u8),
    /// Pull whatever is ready. Long runs without one overrun the queue.
    Drain,
}

pub(super) fn unstable_op() -> impl proptest::strategy::Strategy<Value = UnstableOp> {
    use proptest::prelude::*;
    prop_oneof![
        4 => (0u8..40).prop_map(UnstableOp::Integrate),
        1 => any::<usize>().prop_map(UnstableOp::Duplicate),
        2 => (1u8..30).prop_map(UnstableOp::Advance),
        3 => Just(UnstableOp::Drain),
    ]
}

/// Pull what is ready, asserting no `Appended` is at or below the
/// watermark current at emission and that `Watermark`s never regress.
pub(super) fn drain_ready(
    stream: &mut Channel<Result<zslg_log::UnstableEvent, zslg_log::Error>, Infinite>,
    stable: &FakeStableLog,
    delivered: &mut std::collections::BTreeSet<zslg_log::DataId>,
    last_watermark: &mut Option<Hlc>,
) -> Result<(), proptest::test_runner::TestCaseError> {
    use proptest::prelude::*;
    while let Some(item) = stream.try_recv() {
        match item.expect("no errors") {
            zslg_log::UnstableEvent::Appended(data) => {
                let current = stable.end_hlc();
                prop_assert!(
                    current.is_none_or(|w| data.data_id.hlc() > w),
                    "{:?} emitted at or below the watermark {current:?}",
                    data.data_id.hlc()
                );
                delivered.insert(data.data_id);
            }
            zslg_log::UnstableEvent::Watermark(hlc) => {
                prop_assert!(Some(hlc) >= *last_watermark, "watermark regressed");
                *last_watermark = Some(hlc);
            }
        }
    }
    Ok(())
}
