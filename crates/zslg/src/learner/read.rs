//! The [`Reader`] implementation: three sources spliced into streams a caller
//! can trust to be gapless and in order.
//!
//! ```text
//!                  ┌── archive (object store) ──┐
//!   stable_tail ───┤                            ├──> replay ──> live tail
//!   subscribe_..   └── stable_log (local scan) ─┘      ^        (subscription)
//!                        HistoricalReplay              └─ hung up ───┘
//!
//!   unstable_tail ──> local_store.stream_above_hlc(end_hlc)
//!
//!   subscribe_unstable_tail ──> Watermark · scan above it ──> live fan-out
//!                                   ^                         + watermark moves
//!                                   └──────── hung up ───────────┘
//! ```
//!
//! A stable read is cut at the frontier captured when a cycle begins:
//! `HistoricalReplay` covers `[cursor, end_index]` by scanning and the live
//! subscription covers everything above it. The subscription is taken *before*
//! the frontier is read, so an append racing it is duplicated (filtered by
//! index) rather than lost.
//!
//! That cut is not made once. [`Channels::unfold`] re-makes it every time the
//! fan-out hangs up on this consumer for falling behind, so a slow reader never
//! sees an error — it silently drops back into replay from its own cursor.
//! Which is the whole reason the cursor is one monotone value rather than a
//! captured frontier plus a start index: it has to survive across cycles.
//!
//! `HistoricalReplay` cannot trust the local scan to be contiguous. The retire
//! clamp lives in memory and starts unarmed at every open, and the compaction
//! that physically drops covered entries runs asynchronously, so what a scan
//! sees is not guaranteed to be the clean prefix the clamp describes — a gap
//! may appear anywhere, not just as a leading one. Rather than reading a purge floor and splitting on it, the replay
//! discovers gaps from the data — scan until an index jumps, fill `[expected,
//! jump)` from the archive, emit the jump-triggering entry, repeat — and a
//! running guard (`emit_checked`) rejects any residual gap or HLC regression
//! instead of forwarding it.
//!
//! `unstable_tail` is the other side of the watermark and has no such
//! machinery: it is a one-shot scan of everything the local store holds above
//! `end_hlc`, deliberately weak (see the module header).
//!
//! `subscribe_unstable_tail` follows that side live, and cannot have a cursor:
//! the unstable half arrives out of HLC order. So it is the same cycle with
//! the cursor removed — every resync rescans the window, duplicates across the
//! seam are the consumer's to dedup, and one rule, applied at emission by
//! `WatermarkGate`, keeps it off the stable side: nothing at or below the
//! current watermark.

use crate::Error;
use crate::api::{
    ArchiveBounds, ArchiveLowerBound, ArchiveUpperBound, Clock, LocalStoreView, Reader,
    StableArchiveReader, StableLogSnapshot,
};
use crate::channel::{Cancellation, Channels};
use crate::error::ToStorage;
use crate::hlc::Hlc;
use crate::ids::{LogIndex, NodeId};
use crate::learner::QuorumReadDebouncer;
use crate::meta::{Data, StableLogEvent, UnstableEvent};
use crate::metrics::{self, ReadEntrySource, ReadSource};
use crate::raft::EvictionStatus;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Whether this node may serve client reads and writes.
///
/// Gated on ONE input, deliberately narrow: has a reachable peer
/// **affirmatively** denied this node's rejoin attempt
/// (`raft::startup::rejoin::EvictionStatus`, set only on
/// `JoinAttempt::Denied`). NOT the openraft voter set — under a
/// `retain: false` eviction (what production eviction actually performs) the
/// leader stops replicating to this node entirely, so its own view of the
/// voter set would freeze at "still a member" and never reflect the removal.
/// And NOT "unsettled"/unreachable — a partition yields `Retry`, never
/// `Denied`, so a partitioned node keeps serving reads: silence is not
/// evidence, only an answer is (the same principle the design doc applies to
/// leader self-eviction). A prolonged-partition staleness bound would be a
/// second input here — the seam is deliberate, and implementing it is out of
/// scope for this plan.
///
/// The write path does not consult this: `Writer::write` only mints a
/// `DataId` and fans it out over the router (`writer.rs`), so this node's own
/// membership is irrelevant to acting as a write *coordinator*. Only reads —
/// which serve this node's local view of replicated state — need to be
/// refused once that view is known to have stopped advancing.
#[must_use]
pub(crate) fn can_serve_clients(eviction: &EvictionStatus) -> bool {
    !eviction.is_denied()
}

/// Wraps a [`Reader`] so client reads are refused with [`Error::NotReady`]
/// once a reachable peer has affirmatively denied this node's rejoin attempt
/// — a node removed from the group stays up (CLAUDE.md) but its
/// replicated-state view has stopped advancing, so serving from it would
/// return a snapshot that silently ceases to move.
///
/// `stable_watermark` is deliberately **not** gated: it has no error channel
/// and is a local read that cannot mislead a caller into thinking it saw
/// fresh data.
pub(crate) struct GatedReader {
    inner: Arc<dyn Reader>,
    eviction: EvictionStatus,
    node_id: NodeId,
}

impl GatedReader {
    #[must_use]
    pub(crate) fn new(inner: Arc<dyn Reader>, eviction: EvictionStatus, node_id: NodeId) -> Self {
        Self {
            inner,
            eviction,
            node_id,
        }
    }

    fn ready(&self) -> bool {
        can_serve_clients(&self.eviction)
    }

    fn not_ready(&self) -> Error {
        Error::NotReady(format!(
            "node {} has been denied re-admission to the group",
            self.node_id
        ))
    }
}

#[async_trait::async_trait]
impl Reader for GatedReader {
    async fn sync(&self) -> Result<(), Error> {
        if !self.ready() {
            return Err(self.not_ready());
        }
        self.inner.sync().await
    }

    fn stable_watermark(&self) -> Option<Hlc> {
        // Not gated: no error channel, and a local read that cannot mislead a
        // caller into thinking it saw fresh data (see the module-level note).
        self.inner.stable_watermark()
    }

    fn hlc_now(&self) -> Hlc {
        // Not gated, for the same reason as `stable_watermark`.
        self.inner.hlc_now()
    }

    fn subscribe_stable_log(&self, from: LogIndex) -> Channels<Result<StableLogEvent, Error>> {
        if !self.ready() {
            return Channels::from_vec(vec![Err(self.not_ready())]);
        }
        self.inner.subscribe_stable_log(from)
    }

    fn unstable_tail(&self) -> Channels<Result<Data, Error>> {
        if !self.ready() {
            return Channels::from_vec(vec![Err(self.not_ready())]);
        }
        self.inner.unstable_tail()
    }

    fn subscribe_unstable_tail(&self) -> Channels<Result<UnstableEvent, Error>> {
        if !self.ready() {
            return Channels::from_vec(vec![Err(self.not_ready())]);
        }
        self.inner.subscribe_unstable_tail()
    }
}

const STREAM_CHANNEL_CAP: usize = 64;

pub(in crate::learner) struct ReaderImpl {
    local_store: Arc<dyn LocalStoreView>,
    stable_log: Arc<dyn StableLogSnapshot>,
    /// Just-in-time source for the archived (purged) prefix of the stable log.
    /// When the local engine has retired an old index prefix, the reader serves
    /// those indices from object-store snapshots instead, so the caller sees one
    /// seamless, gapless, index-ordered stream (design doc §794).
    archive: Arc<dyn StableArchiveReader>,
    debouncer: Arc<QuorumReadDebouncer>,
    /// Bound to every stream handed out, so node shutdown ends them with
    /// [`End::Cancelled`](crate::channel::End::Cancelled) even while a consumer
    /// is parked in `recv` on a quiet log.
    shutdown: Cancellation,
    /// The node's clock, the one its writes are minted from (`hlc_now`).
    clock: Arc<dyn Clock>,
}

/// The reader anchors the stable tail: a subscription holds only `Weak`s to the
/// log and archive, so every live subscription ends when this drops.
impl Drop for ReaderImpl {
    fn drop(&mut self) {
        tracing::info!("ReaderImpl dropped; every stable-log subscription ends here");
    }
}

impl ReaderImpl {
    #[must_use]
    pub(in crate::learner) fn new(
        local_store: Arc<dyn LocalStoreView>,
        stable_log: Arc<dyn StableLogSnapshot>,
        archive: Arc<dyn StableArchiveReader>,
        debouncer: Arc<QuorumReadDebouncer>,
        shutdown: Cancellation,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            local_store,
            stable_log,
            archive,
            debouncer,
            shutdown,
            clock,
        }
    }
}

// ── Historical replay ─────────────────────────────────────────────────────

/// Replays a bounded index window as one gapless, HLC-monotonic stream,
/// filling from the archive whatever the local scan skips (module header).
struct HistoricalReplay {
    archive: Arc<dyn StableArchiveReader>,
    stable_log: Arc<dyn StableLogSnapshot>,
    /// Next index the merged stream must emit.
    expected: LogIndex,
    /// HLC of the last emitted entry (for monotonicity guard).
    last_hlc: Option<Hlc>,
    /// Output channel — the stream's sending half.
    tx: crossbeam_channel::Sender<Result<(LogIndex, Data), Error>>,
}

impl HistoricalReplay {
    /// Return a stream that replays `[from_idx, until)` gaplessly.
    fn stream(
        archive: Arc<dyn StableArchiveReader>,
        stable_log: Arc<dyn StableLogSnapshot>,
        from_idx: LogIndex,
        until: LogIndex,
    ) -> crate::channel::Channels<Result<(LogIndex, Data), Error>> {
        let (tx, rx) = crossbeam_channel::bounded(STREAM_CHANNEL_CAP);
        let replay = Self {
            archive,
            stable_log,
            expected: from_idx,
            last_hlc: None,
            tx,
        };
        let (ch, closer) = crate::channel::Channels::from(rx, "HistoricalReplay");
        let from_raw = from_idx.raw();
        tokio::spawn(async move {
            replay.run(until).await;
            // Finite: the replay covers a bounded index window, so a clean close
            // is what tells a consumer the window was fully served. An
            // `Aborted` labelled `HistoricalReplay` without this line means the
            // task was dropped before it could close.
            tracing::debug!(
                from = from_raw,
                until = until.raw(),
                "stable tail: historical replay window served"
            );
            closer.close();
        });
        ch
    }

    /// Three-phase blocking pipeline: drain `range()` until a gap, fill the
    /// gap from the archive, then emit the gap-trigger item. Repeats until
    /// either `until` is reached or the consumer drops.
    async fn run(self, until: LogIndex) {
        let mut this = self;
        loop {
            // Phase 1: drain range() through emit_checked until a gap.
            let phase1_tx = this.tx.clone();
            let (replay, gap) = match tokio::task::spawn_blocking(move || {
                let ch = this.stable_log.range(this.expected, until);
                for item in ch.into_iter() {
                    let (idx, data) = match item {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = this.tx.send(Err(e));
                            return (this, None);
                        }
                    };
                    if idx > this.expected {
                        return (this, Some((idx, data)));
                    }
                    this.emit_checked(idx, data, ReadEntrySource::StableLog);
                }
                (this, None)
            })
            .await
            .to_storage("range drain panicked")
            {
                Ok(v) => v,
                Err(e) => {
                    let _ = phase1_tx.send(Err(e));
                    return;
                }
            };
            this = replay;
            let Some((gap_idx, gap_data)) = gap else {
                return;
            };

            // Phase 2: fill [expected, gap_idx) from archive.
            let arch = match this
                .archive
                .stream(ArchiveBounds {
                    from: ArchiveLowerBound::FromIndex(this.expected),
                    until: ArchiveUpperBound::IndexExclusive(gap_idx),
                })
                .await
            {
                Ok(ch) => ch,
                Err(e) => {
                    let _ = this.tx.send(Err(e));
                    return;
                }
            };
            let phase2_tx = this.tx.clone();
            let replay = match tokio::task::spawn_blocking(move || {
                for item in arch.into_iter() {
                    let entry = match item {
                        Ok(e) => e,
                        Err(e) => {
                            let _ = this.tx.send(Err(e));
                            return this;
                        }
                    };
                    this.emit_checked(entry.global_index, entry.data, ReadEntrySource::ObjectStore);
                }
                this
            })
            .await
            .to_storage("archive drain panicked")
            {
                Ok(v) => v,
                Err(e) => {
                    let _ = phase2_tx.send(Err(e));
                    return;
                }
            };
            this = replay;

            // Phase 3: emit the item that triggered the gap.
            this.emit_checked(gap_idx, gap_data, ReadEntrySource::StableLog);
        }
    }

    fn emit_checked(&mut self, idx: LogIndex, data: Data, source: ReadEntrySource) {
        if idx < self.expected {
            return;
        }
        assert_eq!(
            idx,
            self.expected,
            "emit_checked: gap from {} to {} — caller must fill",
            self.expected.raw(),
            idx.raw()
        );
        let hlc = data.data_id.hlc();
        assert!(
            self.last_hlc.is_none_or(|prev| hlc >= prev),
            "HistoricalReplay: HLC regressed from {:?} to {:?}",
            self.last_hlc,
            hlc
        );
        self.expected = LogIndex::next_after(Some(idx));
        self.last_hlc = Some(hlc);
        metrics::read_entry(source);
        let _ = self.tx.send(Ok((idx, data)));
    }
}

// ── Resyncing tail ─────────────────────────────────────────────────────────

/// One gapless, index-ordered stream from `from`, spliced out of repeated
/// cycles of *replay the durable window, then follow the live tail*:
///
/// ```text
///   ┌──────────────────────────────────────────────┐
///   │  subscribe · replay [cursor, frontier] · live │──┐
///   └──────────────────────────────────────────────┘  │
///                      ▲     hung up (fell behind)     │
///                      └───────────────────────────────┘
/// ```
///
/// **Falling behind is not an error here.** A consumer slower than its
/// subscriber queue is deep gets hung up on by the fan-out, which arrives as
/// [`End::Aborted`] *after* every event already queued — the in-band lag signal
/// (`learner::fanout`). [`Channels::unfold`] hands that verdict
/// back and this closure answers it the way §20 prescribes for an infinite
/// stream: resume from the last position. The caller sees one seamless stream;
/// falling behind costs latency, not data.
///
/// Two orderings inside the cycle carry the whole correctness argument:
///
/// 1. **Subscribe before reading the frontier.** Everything at or below the
///    frontier is covered by the replay; everything above it must therefore be
///    on the new subscription. It is: had an entry above the frontier been
///    appended before the subscribe, `end_index` would already have been at
///    least its index when we read it afterwards.
/// 2. **Read `end_hlc` before `end_index`.** The watermark emitted when the
///    replay finishes must not cover an entry the replay did not emit; taking
///    it first bounds it by the frontier that follows.
///
/// The cursor is the only mutable state: the replay's start index, the live
/// filter, the duplicate suppressor across the seam, and the resume point after
/// a hang-up, all in one value.
fn resyncing_tail(
    archive: Arc<dyn StableArchiveReader>,
    stable_log: Arc<dyn StableLogSnapshot>,
    from: LogIndex,
) -> Channels<Result<StableLogEvent, Error>> {
    let cursor = Arc::new(AtomicU64::new(from.raw()));
    let read_cursor = Arc::clone(&cursor);

    let weak_archive = Arc::downgrade(&archive);
    let weak_stable_log = Arc::downgrade(&stable_log);
    drop((archive, stable_log));

    Channels::unfold((), move |(), previous| {
        // Declining is the only termination a consumer ever observes, and it
        // reaches them as the *previous* segment's `Aborted` — under the live
        // fan-out's label, which points at the wrong culprit. Hence the lines.
        // Debug, not warn: the ordinary cause is the node being torn down.
        let Some(archive) = weak_archive.upgrade() else {
            tracing::debug!(
                cursor = read_cursor.load(Ordering::Relaxed),
                ?previous,
                "stable tail: the archive reader has been dropped, so this \
                 subscription cannot be resumed and ends here"
            );
            return None;
        };
        let Some(stable_log) = weak_stable_log.upgrade() else {
            tracing::debug!(
                cursor = read_cursor.load(Ordering::Relaxed),
                ?previous,
                "stable tail: the stable log has been dropped, so this \
                 subscription cannot be resumed and ends here"
            );
            return None;
        };

        if previous.is_some() {
            // Only a hung-up (or exhausted) previous cycle brings us back here.
            metrics::stable_read_resync();
        }
        let at = LogIndex(read_cursor.load(Ordering::Relaxed));

        let live = stable_log.subscribe_events();
        let watermark = stable_log.end_hlc();
        let frontier = stable_log.end_index();

        if previous.is_some() {
            tracing::warn!(
                log_index = at.raw(),
                gap = frontier.map_or(0, |f| f.raw().saturating_sub(at.raw())),
                ?previous,
                "stable read fell behind its subscriber queue; replaying from \
                 the durable log"
            );
        }

        tracing::debug!(
            cycle = if previous.is_some() {
                "resync"
            } else {
                "initial"
            },
            log_index = at.raw(),
            frontier = frontier.map(LogIndex::raw),
            ?watermark,
            "stable tail: opening a segment"
        );

        let replay = HistoricalReplay::stream(
            Arc::clone(&archive),
            Arc::clone(&stable_log),
            at,
            LogIndex::next_after(frontier),
        )
        .map(|item| item.map(|(index, data)| StableLogEvent::Appended { index, data }));

        // The watermark trails the entries it covers, per the causal-order
        // contract in `api/reader.rs`. A hang-up can eat a `Watermark` event, so
        // re-emitting the current one per cycle is what stops a consumer that
        // fell behind once from sitting on a stale watermark forever.
        let watermark = Channels::from_vec(
            watermark
                .map(|hlc| Ok(StableLogEvent::Watermark(hlc)))
                .into_iter()
                .collect(),
        );

        Some(replay.chain(watermark).chain(live.map(Ok)))
    })
    .filter_map(move |item| match item {
        // One comparison filters the captured frontier and drops the replay's
        // duplicates: everything at or below the cursor has already been served.
        Ok(StableLogEvent::Appended { index, data }) => {
            (index.raw() >= cursor.load(Ordering::Relaxed)).then(|| {
                cursor.store(LogIndex::next_after(Some(index)).raw(), Ordering::Relaxed);
                Ok(StableLogEvent::Appended { index, data })
            })
        }
        other => Some(other),
    })
}

// ── Resyncing unstable tail ────────────────────────────────────────────────

/// What a cycle feeds the [`WatermarkGate`], before it becomes an
/// [`UnstableEvent`].
enum UnstableStep {
    /// The watermark a cycle read before its scan.
    Opened(Option<Hlc>),
    Entry(Data),
    /// The stable watermark moved; the gate reads where it is now.
    WatermarkMoved,
}

/// The one rule every emitted event passes: nothing at or below the
/// watermark, judged at emission. The current watermark is the highest of the
/// stable log's level and what this subscription last emitted, so an entry the
/// stable log has already settled is dropped even before its `Watermark` goes
/// out.
struct WatermarkGate {
    level: tokio::sync::watch::Receiver<Option<Hlc>>,
    emitted: parking_lot::Mutex<Option<Hlc>>,
}

impl WatermarkGate {
    fn admit(&self, step: UnstableStep) -> Option<UnstableEvent> {
        let mut emitted = self.emitted.lock();
        match step {
            UnstableStep::Opened(watermark) => {
                *emitted = (*emitted).max(watermark);
                emitted.map(UnstableEvent::Watermark)
            }
            UnstableStep::WatermarkMoved => {
                let level = *self.level.borrow();
                (level > *emitted).then(|| {
                    *emitted = level;
                    level.map(UnstableEvent::Watermark)
                })?
            }
            UnstableStep::Entry(data) => {
                let watermark = (*emitted).max(*self.level.borrow());
                watermark
                    .is_none_or(|w| data.data_id.hlc() > w)
                    .then_some(UnstableEvent::Appended(data))
            }
        }
    }
}

/// Every local entry above the watermark, then the live integrations, cycled
/// whenever the fan-out hangs up:
///
/// ```text
///   ┌───────────────────────────────────────────────────────────────┐
///   │ ① subscribe · ② w = end_hlc · Watermark(w) · ③ scan above w · │──┐
///   │   live entries + Watermark per move                           │  │
///   └───────────────────────────────────────────────────────────────┘  │
///                        ▲        hung up (fell behind)                 │
///                        └──────────────────────────────────────────────┘
/// ```
///
/// Subscribing before reading the watermark is the whole seam argument:
/// anything integrated before ① is visible to ③, anything after it is on the
/// live leg. The overlap is duplicates, never loss, and with no monotone
/// position to resume from, every cycle rescans the unstable window.
fn resyncing_unstable_tail(
    local_store: Arc<dyn LocalStoreView>,
    stable_log: Arc<dyn StableLogSnapshot>,
) -> Channels<Result<UnstableEvent, Error>> {
    let gate = WatermarkGate {
        level: stable_log.watch_watermark(),
        emitted: parking_lot::Mutex::new(None),
    };
    let weak_local_store = Arc::downgrade(&local_store);
    let weak_stable_log = Arc::downgrade(&stable_log);
    drop((local_store, stable_log));

    Channels::unfold((), move |(), previous| {
        // As in `resyncing_tail`: declining is the only termination a consumer
        // observes, under the previous segment's label, hence the lines.
        let Some(local_store) = weak_local_store.upgrade() else {
            tracing::debug!(
                ?previous,
                "unstable tail: the local store has been dropped, so this \
                 subscription ends here"
            );
            return None;
        };
        let Some(stable_log) = weak_stable_log.upgrade() else {
            tracing::debug!(
                ?previous,
                "unstable tail: the stable log has been dropped, so this \
                 subscription ends here"
            );
            return None;
        };

        let live = local_store.subscribe_unstable();
        let watermark = stable_log.end_hlc();

        if previous.is_some() {
            metrics::unstable_read_resync();
            tracing::warn!(
                ?watermark,
                ?previous,
                "unstable tail subscription fell behind its queue; rescanning \
                 the unstable window"
            );
        }

        let opening = Channels::from_vec(vec![Ok(UnstableStep::Opened(watermark))]);
        let scan = local_store.stream_above_hlc(watermark).map(|item| {
            item.map(|data| {
                metrics::read_entry(ReadEntrySource::LocalStore);
                UnstableStep::Entry(data)
            })
        });
        let live = live.map(|data| Ok(UnstableStep::Entry(data))).merge(
            stable_log
                .watermark_moves()
                .map(|()| Ok(UnstableStep::WatermarkMoved)),
        );
        Some(opening.chain(scan).chain(live))
    })
    .filter_map(move |step| match step {
        Ok(step) => gate.admit(step).map(Ok),
        Err(e) => Some(Err(e)),
    })
}

// ── Reader trait impl ──────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Reader for ReaderImpl {
    async fn sync(&self) -> Result<(), Error> {
        let result = self.debouncer.sync_quorum().await;
        metrics::quorum_read(metrics::OpState::from_ok(result.is_ok()));
        result
    }

    fn stable_watermark(&self) -> Option<Hlc> {
        self.stable_log.end_hlc()
    }

    fn hlc_now(&self) -> Hlc {
        self.clock.now()
    }

    fn subscribe_stable_log(&self, from: LogIndex) -> Channels<Result<StableLogEvent, Error>> {
        metrics::read_request(ReadSource::Stable);
        resyncing_tail(self.archive.clone(), self.stable_log.clone(), from).until(&self.shutdown)
    }

    fn unstable_tail(&self) -> Channels<Result<Data, Error>> {
        metrics::read_request(ReadSource::Unstable);
        let local_store = self.local_store.clone();
        let stable_log = self.stable_log.clone();
        // The stable watermark = end_hlc. Everything at or below it has been
        // settled into the stable prefix (served by stable_tail) or has been
        // superseded and must not appear here — so non-quorum writes below the
        // watermark disappear from the unstable view by the next snapshot.
        let watermark = stable_log.end_hlc();
        local_store
            .stream_above_hlc(watermark)
            .map(|item| {
                if item.is_ok() {
                    metrics::read_entry(ReadEntrySource::LocalStore);
                }
                item
            })
            .until(&self.shutdown)
    }

    fn subscribe_unstable_tail(&self) -> Channels<Result<UnstableEvent, Error>> {
        metrics::read_request(ReadSource::UnstableSubscription);
        resyncing_unstable_tail(self.local_store.clone(), self.stable_log.clone())
            .until(&self.shutdown)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::api::QuorumCheck;
    use crate::api::{LocalStoreMut, StableLogSnapshot};
    use crate::channel::{Cancellation, Channels};
    use crate::{Reader, StableLogEvent};

    use crate::hlc::Hlc;
    use crate::ids::{LogIndex, NodeId};
    use crate::learner::{LocalStore, QuorumReadDebouncer, StableLog};

    /// A node clock for the reader under test.
    fn test_clock() -> Arc<dyn crate::api::Clock> {
        Arc::new(crate::hlc::HlcClock::new(Duration::from_secs(60)))
    }
    use crate::meta::Data;
    use async_trait::async_trait;
    use futures::StreamExt;

    use super::ReaderImpl;
    use crate::learner::stable_archive::ObjectStoreArchiveReader;
    use crate::raft::RaftStateHandle;

    struct NoOpCheck;

    #[async_trait]
    impl QuorumCheck for NoOpCheck {
        async fn check(&self) -> Result<(), crate::Error> {
            Ok(())
        }
    }

    fn local_reader(
        local_store: Arc<LocalStore>,
        stable_log: Arc<StableLog>,
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

    fn reader_with_source(
        local_store: Arc<LocalStore>,
        stable_log: Arc<StableLog>,
        deadtime: Duration,
        snapshot_store: Arc<dyn crate::api::SnapshotStore>,
        raft_state: Arc<dyn crate::api::RaftStateView>,
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

    fn reader_shut_down_by(
        local_store: Arc<LocalStore>,
        stable_log: Arc<StableLog>,
        deadtime: Duration,
        snapshot_store: Arc<dyn crate::api::SnapshotStore>,
        raft_state: Arc<dyn crate::api::RaftStateView>,
        shutdown: Cancellation,
    ) -> ReaderImpl {
        let debouncer = Arc::new(QuorumReadDebouncer::for_test(
            Arc::new(NoOpCheck),
            deadtime,
            Duration::from_secs(5),
        ));
        let archive: Arc<dyn crate::api::StableArchiveReader> =
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
    struct ScriptedSnapshotStore {
        snapshots: std::sync::Mutex<
            std::collections::HashMap<crate::ids::SnapshotId, Vec<crate::meta::SnapshotEntry>>,
        >,
    }

    impl ScriptedSnapshotStore {
        fn script(&self, id: crate::ids::SnapshotId, entries: Vec<crate::meta::SnapshotEntry>) {
            self.snapshots.lock().expect("lock").insert(id, entries);
        }
    }

    #[async_trait]
    impl crate::api::SnapshotStore for ScriptedSnapshotStore {
        async fn write_snapshot(
            &self,
            _id: crate::ids::SnapshotId,
            _entries: crate::channel::Channels<Result<crate::meta::SnapshotEntry, crate::Error>>,
            _compression: crate::object_store::CompressionLevel,
            _reason: crate::metrics::ObjectStoreReason,
        ) -> Result<usize, crate::Error> {
            unimplemented!("reader tests never write snapshots")
        }
        async fn read_snapshot(
            &self,
            id: crate::ids::SnapshotId,
            _reason: crate::metrics::ObjectStoreReason,
        ) -> Result<
            crate::channel::Channels<Result<crate::meta::SnapshotEntry, crate::Error>>,
            crate::Error,
        > {
            let entries = self
                .snapshots
                .lock()
                .expect("lock")
                .get(&id)
                .cloned()
                .ok_or_else(|| crate::Error::ObjectStore(format!("no scripted snapshot {id:?}")))?;
            Ok(crate::channel::Channels::from_vec(
                entries.into_iter().map(Ok).collect(),
            ))
        }
        fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
            tokio_util::sync::CancellationToken::new()
        }
        async fn has_bootstrap_fence(&self) -> Result<bool, crate::Error> {
            unimplemented!()
        }
        async fn write_bootstrap_fence(&self) -> Result<(), crate::Error> {
            unimplemented!()
        }
        async fn cleanup_snapshots(
            &self,
            _live: std::collections::BTreeSet<crate::ids::SnapshotId>,
            _retention: Duration,
        ) -> Result<crate::api::CleanupReport, crate::Error> {
            unimplemented!()
        }
    }

    use crate::tests::make_data;
    use crate::tests::upstream;

    /// Pre-load a closed `Channels` for the channel-based `append`.
    fn rx_of(
        items: impl IntoIterator<Item = Result<(LogIndex, Data), crate::Error>>,
    ) -> crate::channel::Channels<Result<crate::meta::SnapshotEntry, crate::Error>> {
        crate::channel::Channels::from_vec(
            items
                .into_iter()
                .map(|item| {
                    item.map(|(global_index, data)| crate::meta::SnapshotEntry {
                        global_index,
                        data,
                    })
                })
                .collect(),
        )
    }

    async fn integrate_batch(
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
        let (ch, closer) = crate::channel::Channels::from(rx, "test integrate");
        // Close before the last sender drops, or the consumer forms its
        // verdict as "disconnected, not yet closed" — an abort.
        closer.close();
        drop(tx);
        store.integrate_loop(ch).await
    }

    /// `hlc_now` reads the node's clock, the one its writes are minted from: it
    /// is above every HLC that clock issued before the call.
    #[tokio::test]
    async fn hlc_now_is_above_every_hlc_the_node_issued_before() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable_log = crate::tests::stable_log_at(&dir.path().join("stable.db")).await;
        let local_store = LocalStore::simple_at(dir.path().join("local.stable"))
            .await
            .expect("from_db");
        let clock = test_clock();
        let debouncer = Arc::new(QuorumReadDebouncer::for_test(
            Arc::new(NoOpCheck),
            Duration::ZERO,
            Duration::from_secs(5),
        ));
        let archive: Arc<dyn crate::api::StableArchiveReader> =
            Arc::new(ObjectStoreArchiveReader::new(
                Arc::new(ScriptedSnapshotStore::default()),
                Arc::new(RaftStateHandle::default()),
            ));
        let reader = ReaderImpl::new(
            local_store,
            stable_log,
            archive,
            debouncer,
            Cancellation::never(),
            clock.clone(),
        );

        let minted: Vec<Hlc> = (0..100).map(|_| clock.now()).collect();
        let now = reader.hlc_now();
        assert!(
            minted.iter().all(|h| *h < now),
            "hlc_now is above every earlier HLC"
        );
    }

    #[tokio::test]
    async fn stable_tail_emits_historical_then_live() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable_path = dir.path().join("stable.db");
        let local_path = dir.path().join("local.db");
        let stable_log = crate::tests::stable_log_at(&stable_path).await;
        let local_log = LocalStore::simple_at(local_path.with_extension("stable"))
            .await
            .expect("from_db");

        stable_log
            .append(rx_of([Ok((LogIndex(0), make_data(1, 100, b"a")))]))
            .await
            .expect("append");
        stable_log
            .append(rx_of([Ok((LogIndex(1), make_data(2, 200, b"b")))]))
            .await
            .expect("append");

        let reader = local_reader(
            local_log.clone(),
            stable_log.clone(),
            Duration::from_millis(5),
        );
        let mut stream = reader
            .subscribe_stable_log(LogIndex::ZERO)
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_stream();

        let (i0, d0) = stream.next().await.expect("item0").expect("ok");
        assert_eq!(i0, LogIndex(0));
        assert_eq!(d0.data_id.hlc(), Hlc(100));

        let (i1, d1) = stream.next().await.expect("item1").expect("ok");
        assert_eq!(i1, LogIndex(1));
        assert_eq!(d1.data_id.hlc(), Hlc(200));

        stable_log
            .append(rx_of([Ok((LogIndex(2), make_data(3, 300, b"c")))]))
            .await
            .expect("append");
        let live = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("recv timeout")
            .expect("item")
            .expect("ok");
        assert_eq!(live.0, LogIndex(2));
        assert_eq!(live.1.data_id.hlc(), Hlc(300));
    }

    #[tokio::test]
    async fn stable_tail_filters_below_from() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable_path = dir.path().join("stable.db");
        let local_path = dir.path().join("local.db");
        let stable_log = crate::tests::stable_log_at(&stable_path).await;
        let local_log = LocalStore::simple_at(local_path.with_extension("stable"))
            .await
            .expect("from_db");

        for (idx, hlc) in [(0u64, 100u64), (1, 200), (2, 300)] {
            stable_log
                .append(rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .await
                .expect("append");
        }

        let reader = local_reader(local_log, stable_log.clone(), Duration::from_millis(5));

        let mut stream = reader
            .subscribe_stable_log(LogIndex(1))
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_stream();
        let mut hlcs = Vec::new();
        for _ in 0..2 {
            let item = stream.next().await.expect("item").expect("ok");
            hlcs.push(item.1.data_id.hlc().raw());
        }
        assert_eq!(hlcs, vec![200, 300]);
    }

    /// After the local prefix is purged, `stable_tail` transparently serves the
    /// purged indices from object-store snapshots and the retained suffix from
    /// the local log — one seamless, gapless, index-ordered stream (design
    /// doc §794). The split is discovered by peeking the first index the local
    /// scan yields, not by reading a separate purge-floor value.
    #[tokio::test]
    async fn stable_tail_serves_purged_prefix_from_object_store() {
        use crate::ids::SnapshotId;
        use crate::meta::{RaftManifestState, SnapshotEntry, SnapshotMetadata};

        let dir = tempfile::tempdir().expect("tempdir");
        let stable_log = crate::tests::stable_log_at(&dir.path().join("stable.db")).await;
        let local_log = LocalStore::simple_at(dir.path().join("local.stable"))
            .await
            .expect("from_db");

        // Append indices 0..=4 with HLCs 100..=104.
        for (idx, hlc) in [(0u64, 100u64), (1, 101), (2, 102), (3, 103), (4, 104)] {
            stable_log
                .append(rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .await
                .expect("append");
        }

        // Purge everything at or below HLC 102 → indices 0,1,2 gone locally
        // (the engine clamps them out of `range` immediately); 3,4 retained.
        stable_log.set_purge_threshold(Hlc(102)).expect("purge");
        let local_only: Vec<u64> = stable_log
            .range(LogIndex(0), LogIndex(5))
            .into_iter()
            .map(|r| r.expect("ok").0.raw())
            .collect();
        assert_eq!(
            local_only,
            vec![3, 4],
            "local range clamps the purged prefix"
        );

        // Object store holds a snapshot covering the full 0..=4 range.
        let snap_id = SnapshotId::from_u128(1);
        let store = Arc::new(ScriptedSnapshotStore::default());
        store.script(
            snap_id,
            [(0u64, 100u64), (1, 101), (2, 102), (3, 103), (4, 104)]
                .into_iter()
                .map(|(idx, hlc)| SnapshotEntry {
                    global_index: LogIndex(idx),
                    data: make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                })
                .collect(),
        );
        let handle = Arc::new(RaftStateHandle::default());
        let mut rs = crate::meta::RaftState::empty();
        rs.manifest_state = RaftManifestState {
            manifest: vec![SnapshotMetadata {
                start_hlc: Hlc(100),
                end_hlc: Hlc(104),
                start_index: LogIndex(0),
                end_index: LogIndex(4),
                snapshot_id: snap_id,
            }],
            manifest_watermark: Some(Hlc(104)),
            hlc_cutoff: Some(Hlc(104)),
        };
        handle.replace(rs);

        let reader = reader_with_source(
            local_log,
            stable_log.clone(),
            Duration::from_millis(5),
            store,
            handle,
        );

        // The caller asks from index 0 and gets a contiguous 0..=4: 0,1,2 from
        // object store (purged locally → the scan starts at 3, so [0,3) is
        // filled from the archive), 3,4 from the local log.
        let mut stream = reader
            .subscribe_stable_log(LogIndex(0))
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_stream();
        let mut got: Vec<(u64, u64)> = Vec::new();
        for _ in 0..5 {
            let (idx, data) = stream.next().await.expect("item").expect("ok");
            got.push((idx.raw(), data.data_id.hlc().raw()));
        }
        assert_eq!(
            got,
            vec![(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)],
            "purged prefix + local suffix merge seamlessly and in order",
        );
    }

    /// A read-only `StableLogSnapshot` whose `range` replays a fixed — possibly
    /// HOLEY — entry list. The purge clamp is prefix-clean and cannot punch an
    /// interior hole on demand, so the reader's gap-fill is exercised by
    /// scripting the scan directly (this models the unclamped /
    /// crash-resurrected on-disk set a reopen can expose before the clamp is
    /// re-armed).
    struct ScriptedStableLog {
        /// One `range` call per replay cycle — the observable cost of a resync.
        ranges: std::sync::atomic::AtomicUsize,
        entries: std::sync::Mutex<Vec<(LogIndex, Data)>>,
        end_index: std::sync::Mutex<Option<LogIndex>>,
        end_hlc: std::sync::Mutex<Option<Hlc>>,
        events: crate::learner::fanout::Fanout<StableLogEvent>,
        watermark: tokio::sync::watch::Sender<Option<Hlc>>,
        watermark_wakes: crate::learner::fanout::Wakes,
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
            let events = crate::learner::fanout::Fanout::new(
                std::num::NonZeroUsize::new(capacity).expect("capacity > 0"),
                "stable log subscription",
            );
            let (watermark, _) = tokio::sync::watch::channel(end_hlc);
            Self {
                ranges: std::sync::atomic::AtomicUsize::new(0),
                entries: std::sync::Mutex::new(entries),
                end_index: std::sync::Mutex::new(end_index),
                end_hlc: std::sync::Mutex::new(end_hlc),
                events,
                watermark,
                watermark_wakes: crate::learner::fanout::Wakes::new("scripted moves"),
            }
        }

        /// Raise the watermark and broadcast it, as a snapshot load does.
        fn push_watermark(&self, hlc: Hlc) {
            *self.end_hlc.lock().expect("lock") = Some(hlc);
            self.events
                .send(&crate::meta::StableLogEvent::Watermark(hlc));
            self.watermark.send_replace(Some(hlc));
            self.watermark_wakes.wake();
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
            self.events
                .send(&crate::meta::StableLogEvent::Appended { index, data });
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
            _entries: Channels<Result<crate::meta::SnapshotEntry, crate::Error>>,
        ) -> Result<usize, crate::Error> {
            unimplemented!("scripted stable log is read-only")
        }
        async fn record_end_hlc(&self, _end_hlc: Hlc) -> Result<(), crate::Error> {
            unimplemented!("scripted stable log is read-only")
        }
        fn subscribe_events(&self) -> Channels<crate::meta::StableLogEvent> {
            self.events.subscribe()
        }
        fn watch_watermark(&self) -> tokio::sync::watch::Receiver<Option<Hlc>> {
            self.watermark.subscribe()
        }
        fn watermark_moves(&self) -> Channels<()> {
            self.watermark_wakes.subscribe()
        }
        fn range(
            &self,
            from: LogIndex,
            until: LogIndex,
        ) -> Channels<Result<(LogIndex, Data), crate::Error>> {
            self.ranges
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let items: Vec<Result<(LogIndex, Data), crate::Error>> = self
                .entries
                .lock()
                .expect("lock")
                .iter()
                .filter(|(i, _)| *i >= from && *i < until)
                .cloned()
                .map(Ok)
                .collect();
            Channels::from_vec(items)
        }
        fn set_purge_threshold(&self, _hlc_threshold: Hlc) -> Result<(), crate::Error> {
            Ok(())
        }
    }

    /// A consumer slower than the broadcast ring must not see the lag. The
    /// ring is 4 deep and 12 entries are appended while the stream is not
    /// polled, so the subscriber is lapped several times over; the stream must
    /// still deliver 0..=11 exactly once each, in order, with no error — it
    /// re-enters historical replay from its own cursor instead of surfacing
    /// `RecvError::Lagged`.
    #[tokio::test]
    async fn subscribe_stable_log_replays_instead_of_lagging() {
        let log = Arc::new(ScriptedStableLog::with_broadcast_capacity(
            vec![(LogIndex(0), make_data(0, 100, b"0"))],
            4,
        ));
        let (_dir, reader) = scripted_reader(
            log.clone(),
            Arc::new(ScriptedSnapshotStore::default()),
            Arc::new(RaftStateHandle::default()),
        )
        .await;

        let mut stream = reader.subscribe_stable_log(LogIndex::ZERO).into_stream();

        // Lap the ring many times over without polling the stream.
        for i in 1u64..12 {
            log.push(LogIndex(i), make_data(1, 100 + i, b"x"));
        }

        let mut seen: Vec<u64> = Vec::new();
        while seen.len() < 12 {
            let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("stream stalled")
                .expect("stream ended early")
                .expect("a lagged consumer must never see an error");
            if let crate::meta::StableLogEvent::Appended { index, .. } = ev {
                seen.push(index.raw());
            }
        }
        assert_eq!(
            seen,
            (0..12).collect::<Vec<u64>>(),
            "every index exactly once, in order, across the resync"
        );
    }

    /// A `Watermark` lost to a hang-up must not stay lost. It is published
    /// *after* the fan-out has already dropped this subscriber, so the only way
    /// the consumer can learn it is the next cycle re-reading it from the log —
    /// which it must, or a consumer that fell behind once would sit on a stale
    /// watermark for as long as the log stayed quiet.
    #[tokio::test]
    async fn a_watermark_lost_to_a_hang_up_is_redelivered_by_the_next_cycle() {
        let log = Arc::new(ScriptedStableLog::with_broadcast_capacity(
            vec![(LogIndex(0), make_data(0, 100, b"0"))],
            4,
        ));
        let (_dir, reader) = scripted_reader(
            log.clone(),
            Arc::new(ScriptedSnapshotStore::default()),
            Arc::new(RaftStateHandle::default()),
        )
        .await;

        let mut stream = reader.subscribe_stable_log(LogIndex::ZERO).into_stream();

        // Overrun the 4-deep queue, so the subscriber is hung up on ...
        for i in 1u64..8 {
            log.push(LogIndex(i), make_data(1, 100 + i, b"x"));
        }
        // ... and only then publish the watermark, which therefore reaches
        // nobody: it exists solely in the log's state.
        log.push_watermark(Hlc(5000));

        let mut appended = 0usize;
        let mut watermarks: Vec<Hlc> = Vec::new();
        for _ in 0..20 {
            let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("stream stalled")
                .expect("stream ended early")
                .expect("a consumer that fell behind must never see an error");
            match ev {
                crate::meta::StableLogEvent::Appended { .. } => appended += 1,
                crate::meta::StableLogEvent::Watermark(hlc) => watermarks.push(hlc),
            }
            if appended == 8 && watermarks.iter().any(|hlc| *hlc >= Hlc(5000)) {
                return;
            }
        }
        panic!(
            "expected all 8 entries and a watermark >= 5000; got {appended} entries \
             and {watermarks:?}"
        );
    }

    /// The same guarantee on the other public entry point: `stable_tail` is
    /// the entry-only view of the same stream, so a consumer that falls behind
    /// gets one gapless index-ordered run, not an error.
    #[tokio::test]
    async fn stable_tail_replays_instead_of_erroring_when_it_falls_behind() {
        let log = Arc::new(ScriptedStableLog::with_broadcast_capacity(
            vec![(LogIndex(0), make_data(0, 100, b"0"))],
            4,
        ));
        let (_dir, reader) = scripted_reader(
            log.clone(),
            Arc::new(ScriptedSnapshotStore::default()),
            Arc::new(RaftStateHandle::default()),
        )
        .await;

        let mut stream = reader
            .subscribe_stable_log(LogIndex::ZERO)
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_stream();
        for i in 1u64..12 {
            log.push(LogIndex(i), make_data(1, 100 + i, b"x"));
        }

        let mut seen: Vec<u64> = Vec::new();
        while seen.len() < 12 {
            let (index, _) = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("stream stalled")
                .expect("stream ended early")
                .expect("a consumer that fell behind must never see an error");
            seen.push(index.raw());
        }
        assert_eq!(seen, (0..12).collect::<Vec<u64>>());
    }

    /// After a cycle the stream must be genuinely live again, not one append
    /// away from being hung up on afresh. Each cycle takes a *new* subscription
    /// with an empty queue, so it starts with the full depth of headroom; the
    /// old one is gone (that is what a hang-up is). The entries look identical
    /// however often it cycles, because the cursor filter hides the duplicate
    /// work — so what this asserts is the cost: one replay per hang-up and not
    /// one more.
    #[tokio::test]
    async fn a_resync_settles_back_into_live_delivery() {
        use std::sync::atomic::Ordering;

        let log = Arc::new(ScriptedStableLog::with_broadcast_capacity(
            vec![(LogIndex(0), make_data(0, 100, b"0"))],
            2,
        ));
        let (_dir, reader) = scripted_reader(
            log.clone(),
            Arc::new(ScriptedSnapshotStore::default()),
            Arc::new(RaftStateHandle::default()),
        )
        .await;

        let mut stream = reader.subscribe_stable_log(LogIndex::ZERO).into_stream();

        // Lap the 2-deep ring, then drain everything it dropped.
        for i in 1u64..6 {
            log.push(LogIndex(i), make_data(1, 100 + i, b"x"));
        }
        let mut seen = 0usize;
        while seen < 6 {
            let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("stream stalled")
                .expect("stream ended early")
                .expect("no error");
            if matches!(ev, crate::meta::StableLogEvent::Appended { .. }) {
                seen += 1;
            }
        }
        let after_lag = log.ranges.load(Ordering::Relaxed);
        assert_eq!(after_lag, 2, "the opening cycle plus one for the lag");

        // A later append must arrive over the live tail, with no further replay.
        log.push(LogIndex(6), make_data(1, 200, b"y"));
        loop {
            let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("stream stalled")
                .expect("stream ended early")
                .expect("no error");
            if let crate::meta::StableLogEvent::Appended { index, .. } = ev {
                assert_eq!(index, LogIndex(6));
                break;
            }
        }
        assert_eq!(
            log.ranges.load(Ordering::Relaxed),
            after_lag,
            "a live append after a resync must not trigger another replay — \
             the resync left the receiver at the ring's head, not behind it"
        );
    }

    /// Wire a reader over a scripted (holey) stable log + covering archive. The
    /// returned `TempDir` backs the (unused-by-`stable_tail`) local store and
    /// must be kept alive for the reader's lifetime.
    async fn scripted_reader(
        stable_log: Arc<dyn StableLogSnapshot>,
        snapshot_store: Arc<dyn crate::api::SnapshotStore>,
        raft_state: Arc<dyn crate::api::RaftStateView>,
    ) -> (tempfile::TempDir, ReaderImpl) {
        let debouncer = Arc::new(QuorumReadDebouncer::for_test(
            Arc::new(NoOpCheck),
            Duration::from_millis(5),
            Duration::from_secs(5),
        ));
        let archive: Arc<dyn crate::api::StableArchiveReader> =
            Arc::new(ObjectStoreArchiveReader::new(snapshot_store, raft_state));
        let dir = tempfile::tempdir().expect("tempdir");
        let local_store = LocalStore::simple_at(dir.path().join("local.stable"))
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

    fn covering_manifest(snap_id: crate::ids::SnapshotId, end: u64) -> crate::meta::RaftState {
        use crate::meta::{RaftManifestState, SnapshotMetadata};
        let end_hlc = Hlc(end.saturating_add(100));
        let mut rs = crate::meta::RaftState::empty();
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

    /// A hole in the *interior*, not just a missing leading prefix: the local
    /// scan yields 0,1 then 4,5 (2,3 retired mid-stream) while the archive holds
    /// the full range. `stable_tail` must fill the interior 2,3 from object store
    /// and deliver one contiguous, in-order 0..=5. Splitting on a single purge
    /// floor cannot serve this; discovering the gap from the data can.
    #[tokio::test]
    async fn stable_tail_fills_interior_hole_from_object_store() {
        use crate::ids::SnapshotId;
        use crate::meta::SnapshotEntry;

        let local: Vec<(LogIndex, Data)> = [(0u64, 100u64), (1, 101), (4, 104), (5, 105)]
            .into_iter()
            .map(|(i, h)| {
                (
                    LogIndex(i),
                    make_data(u8::try_from(i + 1).expect("fits"), h, b""),
                )
            })
            .collect();
        let stable_log = Arc::new(ScriptedStableLog::new(local));

        let snap_id = SnapshotId::from_u128(1);
        let store = Arc::new(ScriptedSnapshotStore::default());
        store.script(
            snap_id,
            (0..=5u64)
                .map(|i| SnapshotEntry {
                    global_index: LogIndex(i),
                    data: make_data(u8::try_from(i + 1).expect("fits"), 100 + i, b""),
                })
                .collect(),
        );
        let handle = Arc::new(RaftStateHandle::default());
        handle.replace(covering_manifest(snap_id, 5));

        let (_dir, reader) = scripted_reader(stable_log, store, handle).await;
        let mut stream = reader
            .subscribe_stable_log(LogIndex(0))
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_stream();
        let mut got: Vec<(u64, u64)> = Vec::new();
        for _ in 0..6 {
            let (idx, data) = stream.next().await.expect("item").expect("ok");
            got.push((idx.raw(), data.data_id.hlc().raw()));
        }
        assert_eq!(
            got,
            vec![(0, 100), (1, 101), (2, 102), (3, 103), (4, 104), (5, 105)],
            "interior hole 2,3 filled from object store — seamless, gapless, in order",
        );
    }

    /// If the archive cannot tile a hole (manifest covers only 0..=2 but the
    /// local scan skips to 4), the reader must surface a `StableLogGap` rather
    /// than silently jumping the gap: the served prefix stops and errors.
    #[tokio::test]
    async fn stable_tail_errors_when_archive_cannot_fill_hole() {
        use crate::ids::SnapshotId;
        use crate::meta::SnapshotEntry;

        let local: Vec<(LogIndex, Data)> = [(0u64, 100u64), (1, 101), (4, 104), (5, 105)]
            .into_iter()
            .map(|(i, h)| {
                (
                    LogIndex(i),
                    make_data(u8::try_from(i + 1).expect("fits"), h, b""),
                )
            })
            .collect();
        let stable_log = Arc::new(ScriptedStableLog::new(local));

        // Archive covers only 0..=2 — index 3 of the [2,4) hole is untiled.
        let snap_id = SnapshotId::from_u128(1);
        let store = Arc::new(ScriptedSnapshotStore::default());
        store.script(
            snap_id,
            (0..=2u64)
                .map(|i| SnapshotEntry {
                    global_index: LogIndex(i),
                    data: make_data(u8::try_from(i + 1).expect("fits"), 100 + i, b""),
                })
                .collect(),
        );
        let handle = Arc::new(RaftStateHandle::default());
        handle.replace(covering_manifest(snap_id, 2));

        let (_dir, reader) = scripted_reader(stable_log, store, handle).await;
        let mut stream = reader
            .subscribe_stable_log(LogIndex(0))
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_stream();

        // 0,1 (local) then 2 (archive) come through; then the untiled 3 errors.
        for expected in [(0u64, 100u64), (1, 101), (2, 102)] {
            let (idx, data) = stream.next().await.expect("item").expect("ok");
            assert_eq!((idx.raw(), data.data_id.hlc().raw()), expected);
        }
        let err = stream
            .next()
            .await
            .expect("an item")
            .expect_err("an untiled hole must surface as an error");
        assert!(
            matches!(err, crate::Error::StableLogGap(ref m) if m.contains("not fully tiled")),
            "expected a StableLogGap completeness error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn unstable_tail_returns_snapshot_above_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable_path = dir.path().join("stable.db");
        let local_path = dir.path().join("local.db");
        let stable_log = crate::tests::stable_log_at(&stable_path).await;
        let local_log = LocalStore::simple_at(local_path.with_extension("stable"))
            .await
            .expect("from_db");

        stable_log
            .append(rx_of([Ok((
                LogIndex(0),
                make_data(0, 150, b"stable-seed"),
            ))]))
            .await
            .expect("watermark via append");

        integrate_batch(
            &local_log,
            upstream(1),
            vec![(LogIndex(0), make_data(1, 100, b"old"))],
        )
        .await
        .expect("integrate");
        integrate_batch(
            &local_log,
            upstream(1),
            vec![(LogIndex(1), make_data(2, 200, b"new"))],
        )
        .await
        .expect("integrate");

        let reader = local_reader(
            local_log.clone(),
            stable_log.clone(),
            Duration::from_millis(5),
        );

        let mut stream = reader.unstable_tail().into_stream();
        let first = stream.next().await.expect("item").expect("ok");
        assert_eq!(first.data_id.hlc(), Hlc(200));
        assert_eq!(first.blob.as_ref(), b"new");
        assert!(
            stream.next().await.is_none(),
            "unstable_tail is one-shot; no further items"
        );

        integrate_batch(
            &local_log,
            upstream(1),
            vec![(LogIndex(2), make_data(3, 300, b"newer"))],
        )
        .await
        .expect("integrate");

        let mut stream2 = reader.unstable_tail().into_stream();
        let a = stream2.next().await.expect("a").expect("ok");
        let b = stream2.next().await.expect("b").expect("ok");
        assert_eq!(a.data_id.hlc(), Hlc(200));
        assert_eq!(b.data_id.hlc(), Hlc(300));
        assert!(stream2.next().await.is_none());
    }

    // ── subscribe_unstable_tail ─────────────────────────────────────────────

    type UnstableStream = Channels<Result<crate::UnstableEvent, crate::Error>>;

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
        Arc<StableLog>,
        ReaderImpl,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crate::learner::local_store::LocalStoreConfig::for_test();
        config.unstable_subscriber_queue_capacity =
            std::num::NonZeroUsize::new(capacity).expect("capacity > 0");
        let (local, stable) = LocalStore::simple_with(dir.path().join("rig"), &config)
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
    async fn next_unstable(stream: &mut UnstableStream) -> crate::UnstableEvent {
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
    async fn assert_quiet(stream: &mut UnstableStream) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(stream.try_recv().is_none(), "expected no further event");
    }

    async fn integrate_one(local: &LocalStore, index: u64, data: Data) {
        integrate_batch(local, upstream(1), vec![(LogIndex(index), data)])
            .await
            .expect("integrate");
    }

    /// The cycle opens with the watermark, replays what is already above it,
    /// then follows live integrations.
    #[tokio::test]
    async fn subscribe_unstable_tail_replays_then_follows_live() {
        let (_dir, local, _stable, reader) = unstable_rig(16, Hlc(50), Cancellation::never()).await;
        let a = make_data(1, 100, b"a");
        let b = make_data(2, 200, b"b");
        integrate_one(&local, 0, a.clone()).await;
        integrate_one(&local, 1, b.clone()).await;

        let mut stream = reader.subscribe_unstable_tail();
        assert_eq!(
            next_unstable(&mut stream).await,
            crate::UnstableEvent::Watermark(Hlc(50))
        );
        assert_eq!(
            next_unstable(&mut stream).await,
            crate::UnstableEvent::Appended(a)
        );
        assert_eq!(
            next_unstable(&mut stream).await,
            crate::UnstableEvent::Appended(b)
        );

        let c = make_data(3, 300, b"c");
        integrate_one(&local, 2, c.clone()).await;
        assert_eq!(
            next_unstable(&mut stream).await,
            crate::UnstableEvent::Appended(c)
        );
        assert_quiet(&mut stream).await;
    }

    /// A consumer that overruns its queue is hung up on and silently resynced:
    /// the next cycle opens with the watermark again and its rescan delivers
    /// everything the hang-up cost, with no error.
    #[tokio::test]
    async fn a_hung_up_unstable_subscription_resyncs_without_loss() {
        let (_dir, local, _stable, reader) = unstable_rig(2, Hlc(50), Cancellation::never()).await;
        let mut stream = reader.subscribe_unstable_tail();
        assert_eq!(
            next_unstable(&mut stream).await,
            crate::UnstableEvent::Watermark(Hlc(50))
        );

        let written: Vec<Data> = (0..6u8)
            .map(|i| make_data(i, 100 + u64::from(i), b"x"))
            .collect();
        for (i, data) in (0u64..).zip(&written) {
            integrate_one(&local, i, data.clone()).await;
        }

        let mut seen = std::collections::BTreeSet::new();
        let mut watermarks = 0usize;
        while seen.len() < written.len() {
            match next_unstable(&mut stream).await {
                crate::UnstableEvent::Appended(data) => {
                    seen.insert(data.data_id);
                }
                crate::UnstableEvent::Watermark(hlc) => {
                    assert_eq!(hlc, Hlc(50));
                    watermarks += 1;
                }
            }
        }
        assert!(
            seen.iter().eq(written.iter().map(|d| &d.data_id)),
            "every entry, across the resync"
        );
        assert_eq!(watermarks, 1, "the resync cycle opens with the watermark");
    }

    /// A `Watermark` follows every advance of the stable watermark, even with
    /// no entry traffic to carry it.
    #[tokio::test]
    async fn the_unstable_watermark_follows_each_advance() {
        let (_dir, _local, stable, reader) = unstable_rig(16, Hlc(50), Cancellation::never()).await;
        let mut stream = reader.subscribe_unstable_tail();
        assert_eq!(
            next_unstable(&mut stream).await,
            crate::UnstableEvent::Watermark(Hlc(50))
        );
        for hlc in [60, 70] {
            stable.record_end_hlc(Hlc(hlc)).await.expect("advance");
            assert_eq!(
                next_unstable(&mut stream).await,
                crate::UnstableEvent::Watermark(Hlc(hlc))
            );
        }
        assert_quiet(&mut stream).await;
    }

    /// An entry the watermark passes while it waits in the queue is the stable
    /// log's to deliver by the time it would be emitted, so it is dropped.
    #[tokio::test]
    async fn an_entry_the_watermark_passed_while_queued_is_not_emitted() {
        let (_dir, local, stable, reader) = unstable_rig(16, Hlc(50), Cancellation::never()).await;
        let mut stream = reader.subscribe_unstable_tail();
        assert_eq!(
            next_unstable(&mut stream).await,
            crate::UnstableEvent::Watermark(Hlc(50))
        );

        integrate_one(&local, 0, make_data(1, 100, b"settles")).await;
        stable.record_end_hlc(Hlc(150)).await.expect("advance");

        assert_eq!(
            next_unstable(&mut stream).await,
            crate::UnstableEvent::Watermark(Hlc(150))
        );
        assert_quiet(&mut stream).await;
    }

    #[tokio::test]
    async fn an_evicted_node_refuses_the_unstable_subscription() {
        let (_dir, _local, _stable, reader) =
            unstable_rig(16, Hlc(50), Cancellation::never()).await;
        let gated = super::GatedReader::new(
            Arc::new(reader),
            crate::raft::EvictionStatus::denied_for_test(),
            upstream(9),
        );
        let mut stream = gated.subscribe_unstable_tail();
        assert!(matches!(stream.recv(), Ok(Err(crate::Error::NotReady(_)))));
    }

    /// Node shutdown ends a quiet unstable subscription with `End::Cancelled`.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_cancels_a_quiet_unstable_subscription() {
        let (handle, shutdown) = crate::channel::CancelHandle::new();
        let (_dir, _local, _stable, reader) = unstable_rig(16, Hlc(50), shutdown).await;
        let mut tail = reader.subscribe_unstable_tail();
        let blocked = tokio::task::spawn_blocking(move || {
            loop {
                // The opening watermark comes first; then the store is quiet.
                if let Err(end) = tail.recv() {
                    return end;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!blocked.is_finished(), "the subscription ended on its own");

        handle.cancel();
        let end = tokio::time::timeout(Duration::from_secs(5), blocked)
            .await
            .expect("shutdown did not wake the subscription")
            .expect("recv panicked");
        assert_eq!(end, crate::channel::End::Cancelled);
    }

    #[derive(Debug, Clone)]
    enum UnstableOp {
        /// Integrate a new entry this many HLC ticks above the watermark
        /// (0 = at it, so the store skips it).
        Integrate(u8),
        /// Integrate an earlier entry again, from another upstream.
        Duplicate(usize),
        Advance(u8),
        /// Pull whatever is ready. Long runs without one overrun the queue.
        Drain,
    }

    fn unstable_op() -> impl proptest::strategy::Strategy<Value = UnstableOp> {
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
    fn drain_ready(
        stream: &mut UnstableStream,
        stable: &StableLog,
        delivered: &mut std::collections::BTreeSet<crate::ids::DataId>,
        last_watermark: &mut Option<Hlc>,
    ) -> Result<(), proptest::test_runner::TestCaseError> {
        use proptest::prelude::*;
        while let Some(item) = stream.try_recv() {
            match item.expect("no errors") {
                crate::UnstableEvent::Appended(data) => {
                    let current = stable.end_hlc();
                    prop_assert!(
                        current.is_none_or(|w| data.data_id.hlc() > w),
                        "{:?} emitted at or below the watermark {current:?}",
                        data.data_id.hlc()
                    );
                    delivered.insert(data.data_id);
                }
                crate::UnstableEvent::Watermark(hlc) => {
                    prop_assert!(Some(hlc) >= *last_watermark, "watermark regressed");
                    *last_watermark = Some(hlc);
                }
            }
        }
        Ok(())
    }

    /// Across arbitrary interleavings of integrations, duplicates, watermark
    /// advances and hang-ups (a queue two deep, overrun by any three
    /// undrained integrations), the subscription ends up having delivered
    /// everything `unstable_tail` holds, and never an entry at or below the
    /// watermark current when it was emitted.
    #[test]
    fn proptest_unstable_subscription_delivers_the_unstable_tail() {
        use futures::TryStreamExt as _;
        use proptest::prelude::*;

        proptest!(ProptestConfig::with_cases(24), |(ops in proptest::collection::vec(unstable_op(), 1..40))| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("rt");
            rt.block_on(async {
                let (_dir, local, stable, reader) =
                    unstable_rig(2, Hlc(1000), Cancellation::never()).await;
                let mut stream = reader.subscribe_unstable_tail();
                let mut delivered = std::collections::BTreeSet::new();
                let mut last_watermark = None;
                let mut written: Vec<Data> = Vec::new();
                let mut next_index = 0u64;

                for op in ops {
                    let watermark = stable.end_hlc().expect("set").raw();
                    match op {
                        UnstableOp::Integrate(above) => {
                            let seed = u8::try_from(written.len()).expect("fits");
                            let data = make_data(seed, watermark + u64::from(above), b"p");
                            integrate_one(&local, next_index, data.clone()).await;
                            written.push(data);
                            next_index += 1;
                        }
                        UnstableOp::Duplicate(pick) => {
                            if !written.is_empty() {
                                let data = written[pick % written.len()].clone();
                                integrate_batch(
                                    &local,
                                    upstream(2),
                                    vec![(LogIndex(next_index), data)],
                                )
                                .await
                                .expect("integrate");
                                next_index += 1;
                            }
                        }
                        UnstableOp::Advance(by) => {
                            stable
                                .record_end_hlc(Hlc(watermark + u64::from(by)))
                                .await
                                .expect("advance");
                        }
                        UnstableOp::Drain => {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                            drain_ready(&mut stream, &stable, &mut delivered, &mut last_watermark)?;
                        }
                    }
                }

                let expected: std::collections::BTreeSet<crate::ids::DataId> = reader
                    .unstable_tail()
                    .into_stream()
                    .map_ok(|data| data.data_id)
                    .try_collect()
                    .await
                    .expect("unstable_tail");
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                while !delivered.is_superset(&expected) || last_watermark != stable.end_hlc() {
                    prop_assert!(
                        tokio::time::Instant::now() < deadline,
                        "missing {:?}; watermark {last_watermark:?} vs {:?}",
                        expected.difference(&delivered).collect::<Vec<_>>(),
                        stable.end_hlc()
                    );
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    drain_ready(&mut stream, &stable, &mut delivered, &mut last_watermark)?;
                }
                Ok(())
            })?;
        });
    }

    /// `subscribe_stable_log(from)` replays history from `from` before going live.
    #[tokio::test]
    async fn subscribe_stable_log_from_replays_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable_log = crate::tests::stable_log_at(&dir.path().join("stable.db")).await;
        let local_log = LocalStore::simple_at(dir.path().join("local.stable"))
            .await
            .expect("from_db");

        stable_log
            .append(rx_of([Ok((LogIndex(0), make_data(1, 100, b"a")))]))
            .await
            .expect("append");
        stable_log
            .append(rx_of([Ok((LogIndex(1), make_data(2, 200, b"b")))]))
            .await
            .expect("append");

        let reader = local_reader(local_log, stable_log.clone(), Duration::from_millis(5));
        let mut stream = reader.subscribe_stable_log(LogIndex(0)).into_stream();

        for expected in [0u64, 1] {
            let ev = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("recv timeout")
                .expect("item")
                .expect("ok");
            match ev {
                crate::meta::StableLogEvent::Appended { index, .. } => {
                    assert_eq!(index.raw(), expected);
                }
                other => panic!("expected historical Appended({expected}), got {other:?}"),
            }
        }
    }

    /// Node shutdown ends a stable-log subscription parked on a log that has
    /// gone quiet with `End::Cancelled`. Nothing else would wake it: the reader
    /// still holds the stable log, so its fan-out never hangs up.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_cancels_a_quiet_stable_log_subscription() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable_log = crate::tests::stable_log_at(&dir.path().join("stable.db")).await;
        let local_log = LocalStore::simple_at(dir.path().join("local.stable"))
            .await
            .expect("from_db");
        stable_log
            .append(rx_of([Ok((LogIndex(0), make_data(1, 100, b"a")))]))
            .await
            .expect("append");

        let (handle, shutdown) = crate::channel::CancelHandle::new();
        let reader = reader_shut_down_by(
            local_log,
            stable_log.clone(),
            Duration::from_millis(5),
            Arc::new(ScriptedSnapshotStore::default()),
            Arc::new(RaftStateHandle::default()),
            shutdown,
        );
        let mut tail = reader.subscribe_stable_log(LogIndex(0));
        let blocked = tokio::task::spawn_blocking(move || {
            loop {
                // The one historical entry comes first; then the log is quiet.
                if let Err(end) = tail.recv() {
                    return end;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!blocked.is_finished(), "the tail ended on its own");

        handle.cancel();
        let end = tokio::time::timeout(Duration::from_secs(5), blocked)
            .await
            .expect("shutdown did not wake the subscription")
            .expect("recv panicked");
        assert_eq!(end, crate::channel::End::Cancelled);
        drop(stable_log);
    }

    #[tokio::test]
    async fn quorum_read_returns_for_local_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable_path = dir.path().join("stable.db");
        let local_path = dir.path().join("local.db");
        let stable_log = crate::tests::stable_log_at(&stable_path).await;
        let local_log = LocalStore::simple_at(local_path.with_extension("stable"))
            .await
            .expect("from_db");

        let reader = local_reader(local_log, stable_log, Duration::from_millis(5));
        reader.sync().await.expect("ok");
    }

    #[tokio::test]
    async fn read_your_own_write_via_quorum_then_unstable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable_path = dir.path().join("stable.db");
        let local_path = dir.path().join("local.db");
        let stable_log = crate::tests::stable_log_at(&stable_path).await;
        let local_log = LocalStore::simple_at(local_path.with_extension("stable"))
            .await
            .expect("from_db");

        let d = make_data(1, 100, b"hi");
        integrate_batch(&local_log, upstream(1), vec![(LogIndex(0), d.clone())])
            .await
            .expect("integrate");

        let reader = local_reader(
            local_log.clone(),
            stable_log.clone(),
            Duration::from_millis(5),
        );
        reader.sync().await.expect("quorum");
        let mut stream = reader.unstable_tail().into_stream();
        let first = stream.next().await.expect("item").expect("ok");
        assert_eq!(first, d);
    }

    #[tokio::test]
    async fn unstable_tail_hides_failed_writes_below_watermark() {
        use futures::TryStreamExt as _;

        let dir = tempfile::tempdir().expect("tempdir");
        let local = LocalStore::simple_at(dir.path().join("local.stable"))
            .await
            .expect("from_db");
        let stable = crate::tests::stable_log_at(&dir.path().join("stable.db")).await;

        // Two non-quorum "failed" writes (HLC 100, 200), accepted only here and
        // never snapshotted.
        for (i, hlc) in [(1u8, 100u64), (2, 200)] {
            integrate_batch(
                &local,
                upstream(1),
                vec![(LogIndex(u64::from(i)), make_data(i, hlc, &[i]))],
            )
            .await
            .expect("integrate");
        }

        let reader = local_reader(local.clone(), stable.clone(), Duration::from_millis(5));

        // Before the watermark passes them, the weak unstable view shows them.
        let before: Vec<Data> = reader
            .unstable_tail()
            .into_stream()
            .try_collect()
            .await
            .expect("collect");
        assert_eq!(
            before.len(),
            2,
            "recent unstable writes are visible at first"
        );

        // The cluster moves on: a watermark-only advance to 5000 (no snapshot data,
        // because the failed writes never reached quorum).
        stable.record_end_hlc(Hlc(5000)).await.expect("record");
        assert_eq!(reader.stable_watermark(), Some(Hlc(5000)));

        // Now they are below the watermark → they must disappear from the view.
        let after: Vec<Data> = reader
            .unstable_tail()
            .into_stream()
            .try_collect()
            .await
            .expect("collect");
        assert!(
            after.is_empty(),
            "failed writes below the watermark must disappear (convergence); got {after:?}"
        );

        // A genuinely recent write above the watermark is still shown.
        integrate_batch(
            &local,
            upstream(1),
            vec![(LogIndex(99), make_data(9, 6000, b"recent"))],
        )
        .await
        .expect("integrate");
        let recent: Vec<Data> = reader
            .unstable_tail()
            .into_stream()
            .try_collect()
            .await
            .expect("collect");
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].data_id.hlc(), Hlc(6000));
    }
}
