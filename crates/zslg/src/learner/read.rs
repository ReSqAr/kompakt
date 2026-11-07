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
//! That cut is not made once. [`Resumable`] re-makes it every
//! time the fan-out hangs up on this consumer for falling behind, so a slow
//! reader never sees an error — it silently drops back into replay from its own
//! cursor. Which is the whole reason the cursor is one monotone value rather
//! than a captured frontier plus a start index: it has to survive across
//! cycles.
//!
//! `HistoricalReplay` cannot trust the local scan to be contiguous. The retire
//! clamp lives in memory and starts unarmed at every open, and the compaction
//! that physically drops covered entries runs asynchronously, so what a scan
//! sees is not guaranteed to be the clean prefix the clamp describes — a gap
//! may appear anywhere, not just as a leading one. The scan is an
//! `OrderedChannel<StrictlyIncreasing<LogIndex>>`; `stitch` fills each forward
//! gap from the archive and verifies the fill, returning a `Contiguous` channel.
//! A consumer-side HLC guard rejects a regression before forwarding.
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
    ArchiveBounds, ArchiveUpperBound, Clock, LocalStoreView, Reader, StableArchiveReader,
    StableLogSnapshot,
};
use crate::learner::QuorumReadDebouncer;
use crate::metrics::{self, ReadEntrySource, ReadSource};
use crossbeam_combinators::{
    Cancellation, Channel, Contiguous, Finite, Gap, Infinite, NonDecreasing, Opening, Resumable,
    Seam, StrictlyIncreasing, resync,
};
use std::sync::Arc;
use zslg_consensus_types::EvictionStatus;
use zslg_lifecycle_types::LifecycleError;
use zslg_log_types::LogIndex;
use zslg_log_types::{Data, StableLogEvent, UnstableEvent};
use zslg_node_types::NodeId;
use zslg_time_types::Hlc;

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
        Error::Lifecycle(LifecycleError::NotReady(format!(
            "node {} has been denied re-admission to the group",
            self.node_id
        )))
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

    fn subscribe_stable_log(&self, after: Option<LogIndex>) -> crate::StableStream {
        if !self.ready() {
            return Channel::once(Err(self.not_ready()))
                .chain(Channel::aborted("gated reader"))
                .ordered(after, "stable log stream");
        }
        self.inner.subscribe_stable_log(after)
    }

    fn unstable_tail(&self) -> Channel<Result<Data, Error>, Finite> {
        if !self.ready() {
            return Channel::once(Err(self.not_ready()));
        }
        self.inner.unstable_tail()
    }

    fn subscribe_unstable_tail(&self) -> Channel<Result<UnstableEvent, Error>, Infinite> {
        if !self.ready() {
            return Channel::once(Err(self.not_ready())).chain(Channel::aborted("gated reader"));
        }
        self.inner.subscribe_unstable_tail()
    }
}

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
    /// [`AbortReason::Cancelled`](crossbeam_combinators::AbortReason::Cancelled) even while a consumer
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

/// Replays `(after, until)` as one gapless, HLC-monotonic stream, filling from
/// the archive whatever the local scan skips (module header):
///
/// ```text
///   scan (after, until) ─ StrictlyIncreasing ─ stitch(archive) ─ Contiguous
///     ─ covers(..until) ─ also NonDecreasing<Hlc>
/// ```
///
/// Lazy, on the consumer's thread: a gap opens its archive read on first use,
/// a trailing gap or an HLC regression ends the stream `Aborted`.
fn historical_replay(
    archive: Arc<dyn StableArchiveReader>,
    stable_log: &dyn StableLogSnapshot,
    after: Option<LogIndex>,
    until: LogIndex,
) -> Channel<Result<(LogIndex, Data), Error>, Finite> {
    stable_log
        .range(after, until)
        .map(|item| {
            if item.is_ok() {
                metrics::read_entry(ReadEntrySource::StableLog);
            }
            item
        })
        .ordered::<StrictlyIncreasing<LogIndex>>(after, "stable scan")
        .stitch(move |gap: Gap<LogIndex>| {
            archive
                .stream(ArchiveBounds {
                    after: gap.after,
                    until: ArchiveUpperBound::IndexExclusive(gap.got),
                })
                .into_channel()
                .map(|item| {
                    if item.is_ok() {
                        metrics::read_entry(ReadEntrySource::ObjectStore);
                    }
                    item.map(|entry| (entry.global_index, entry.data))
                })
        })
        .covers(..until, "historical replay")
        .also::<NonDecreasing<Hlc>>(None, "historical replay HLC")
        .into_channel()
}

// ── Resyncing tail ─────────────────────────────────────────────────────────

/// The frontier and watermark captured after the live subscription.
#[derive(Clone, Copy)]
struct StableCut {
    frontier: Option<LogIndex>,
    watermark: Option<Hlc>,
}

/// One resync cycle: the live subscription and the durable window it continues,
/// both strictly after `after`.
struct StableSeam {
    archive: Arc<dyn StableArchiveReader>,
    stable_log: Arc<dyn StableLogSnapshot>,
    after: Option<LogIndex>,
}

impl Seam for StableSeam {
    type Item = Result<StableLogEvent, Error>;
    type Cut = StableCut;

    fn subscribe(&self) -> Channel<Self::Item, Infinite> {
        self.stable_log.subscribe_events().map(Ok)
    }

    fn cut(&self) -> Self::Cut {
        // Watermark before frontier: the watermark emitted after the replay
        // must not cover an entry the replay did not emit.
        let watermark = self.stable_log.end_hlc();
        let frontier = self.stable_log.end_index();
        StableCut {
            frontier,
            watermark,
        }
    }

    fn above(
        &self,
        live: Channel<Self::Item, Infinite>,
        cut: Self::Cut,
    ) -> Channel<Self::Item, Infinite> {
        // Drop the live overlap with the replay (at or below the frontier) and
        // anything the caller already has (at or below `after`): the stream
        // must continue exactly at `after`'s successor.
        live.filter_above::<LogIndex>(cut.frontier.max(self.after))
    }

    fn range(&self, cut: StableCut) -> Channel<Self::Item, Finite> {
        let replay = historical_replay(
            Arc::clone(&self.archive),
            self.stable_log.as_ref(),
            self.after,
            LogIndex::next_after(cut.frontier),
        )
        .map(|item| item.map(|(index, data)| StableLogEvent::Appended { index, data }));

        // The watermark trails the entries it covers, per the causal-order
        // contract in `api/reader.rs`. A hang-up can eat a `Watermark` event, so
        // re-emitting the current one per cycle is what stops a consumer that
        // fell behind once from sitting on a stale watermark forever.
        let watermark = match cut.watermark {
            Some(hlc) => Channel::once(Ok(StableLogEvent::Watermark(hlc))),
            None => Channel::empty(),
        };

        replay.chain(watermark)
    }
}

/// One gapless, index-ordered stream strictly after `after`, spliced out of
/// repeated cycles of *replay the durable window, then follow the live tail*:
///
/// ```text
///   ┌──────────────────────────────────────────────┐
///   │  subscribe · replay (cursor, frontier] · live │──┐
///   └──────────────────────────────────────────────┘  │
///                      ▲     hung up (fell behind)     │
///                      └───────────────────────────────┘
/// ```
///
/// **Falling behind is not an error here.** A consumer slower than its
/// subscriber queue is deep gets hung up on by the fan-out, which arrives as
/// `Aborted(ProducerGone)` *after* every event already queued — the in-band lag
/// signal. [`Resumable`] answers it by reopening after the last emitted index,
/// and only that: a cancellation or an order violation ends the stream. The
/// caller sees one seamless stream; falling behind costs latency, not data.
///
/// Two orderings inside the cycle carry the whole correctness argument
/// (`Segment::open` fixes the first; `StableSeam::cut` the second):
///
/// 1. **Subscribe before reading the frontier.** Everything at or below the
///    frontier is covered by the replay; everything above it must therefore be
///    on the new subscription. It is: had an entry above the frontier been
///    appended before the subscribe, `end_index` would already have been at
///    least its index when we read it afterwards.
/// 2. **Read `end_hlc` before `end_index`.** The watermark emitted when the
///    replay finishes must not cover an entry the replay did not emit; taking
///    it first bounds it by the frontier that follows.
fn resyncing_tail(
    archive: Arc<dyn StableArchiveReader>,
    stable_log: Arc<dyn StableLogSnapshot>,
    after: Option<LogIndex>,
) -> crate::StableStream {
    let weak_archive = Arc::downgrade(&archive);
    let weak_stable_log = Arc::downgrade(&stable_log);
    drop((archive, stable_log));

    Resumable::new::<_, Contiguous<LogIndex>, _, _>(
        "stable log stream",
        after,
        move |after, opening| {
            // Declining is the only termination a consumer ever observes, and
            // it reaches them as the *previous* segment's `Aborted` — under the
            // live fan-out's label, which points at the wrong culprit. Hence
            // the lines. Debug, not warn: the ordinary cause is the node being
            // torn down.
            let Some(archive) = weak_archive.upgrade() else {
                tracing::debug!(
                    ?after,
                    ?opening,
                    "stable tail: the archive reader has been dropped, so this \
                     subscription cannot be resumed and ends here"
                );
                return None;
            };
            let Some(stable_log) = weak_stable_log.upgrade() else {
                tracing::debug!(
                    ?after,
                    ?opening,
                    "stable tail: the stable log has been dropped, so this \
                     subscription cannot be resumed and ends here"
                );
                return None;
            };

            let seam = StableSeam {
                archive,
                stable_log,
                after,
            };
            if let Opening::AfterProducerGone { what } = opening {
                // Only a hung-up (or dead) previous cycle brings us back here.
                metrics::stable_read_resync();
                tracing::warn!(
                    after = after.map(LogIndex::raw),
                    gap = seam.stable_log.end_index().map_or(0, |f| f
                        .raw()
                        .saturating_sub(after.map_or(0, LogIndex::raw))),
                    what,
                    "stable read fell behind its subscriber queue; replaying from \
                     the durable log"
                );
            }
            tracing::debug!(
                ?opening,
                after = after.map(LogIndex::raw),
                "stable tail: opening a segment"
            );
            Some(seam)
        },
    )
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
    level: crossbeam_combinators::Level<Option<Hlc>>,
    emitted: Option<Hlc>,
}

impl WatermarkGate {
    fn admit(&mut self, step: UnstableStep) -> Option<UnstableEvent> {
        let emitted = &mut self.emitted;
        match step {
            UnstableStep::Opened(watermark) => {
                *emitted = (*emitted).max(watermark);
                emitted.map(UnstableEvent::Watermark)
            }
            UnstableStep::WatermarkMoved => {
                let level = self.level.get();
                (level > *emitted).then(|| {
                    *emitted = level;
                    level.map(UnstableEvent::Watermark)
                })?
            }
            UnstableStep::Entry(data) => {
                let watermark = (*emitted).max(self.level.get());
                watermark
                    .is_none_or(|w| data.data_id.hlc() > w)
                    .then_some(UnstableEvent::Appended(data))
            }
        }
    }
}

/// The live unstable half and the watermark it is anchored to.
struct UnstableSeam {
    local_store: Arc<dyn LocalStoreView>,
    stable_log: Arc<dyn StableLogSnapshot>,
}

/// `UnstableStep::Entry` sits at its entry's HLC; the other steps have none.
impl zslg_time_types::HasHlc for UnstableStep {
    fn hlc(&self) -> Option<Hlc> {
        match self {
            UnstableStep::Entry(data) => Some(data.data_id.hlc()),
            _ => None,
        }
    }
}

impl Seam for UnstableSeam {
    type Item = Result<UnstableStep, Error>;
    type Cut = Option<Hlc>;

    fn subscribe(&self) -> Channel<Self::Item, Infinite> {
        self.local_store
            .subscribe_unstable()
            .map(|data| Ok(UnstableStep::Entry(data)))
            .merge(
                self.stable_log
                    .watermark()
                    .changes()
                    .map(|()| Ok(UnstableStep::WatermarkMoved)),
            )
    }

    fn cut(&self) -> Self::Cut {
        self.stable_log.end_hlc()
    }

    fn above(
        &self,
        live: Channel<Self::Item, Infinite>,
        cut: Self::Cut,
    ) -> Channel<Self::Item, Infinite> {
        // Drop live entries at or below the cut: they are stable, and the
        // stable log — not the scan, which reads only above the cut — covers
        // them. `WatermarkGate` still handles the moving level; this
        // seam-local filter does not rely on it for the overlap at the cut.
        live.filter_above::<Hlc>(cut)
    }

    fn range(&self, cut: Option<Hlc>) -> Channel<Self::Item, Finite> {
        let opening = Channel::once(Ok(UnstableStep::Opened(cut)));
        let scan = self
            .local_store
            .stream_above_hlc(cut)
            .into_channel()
            .map(|item| {
                item.map(|data| {
                    metrics::read_entry(ReadEntrySource::LocalStore);
                    UnstableStep::Entry(data)
                })
            });
        opening.chain(scan)
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
) -> Channel<Result<UnstableEvent, Error>, Infinite> {
    let mut gate = WatermarkGate {
        level: stable_log.watermark(),
        emitted: None,
    };
    let weak_local_store = Arc::downgrade(&local_store);
    let weak_stable_log = Arc::downgrade(&stable_log);
    drop((local_store, stable_log));

    resync("unstable tail", move |opening| {
        // As in `resyncing_tail`: declining is the only termination a consumer
        // observes, under the previous segment's label, hence the lines.
        let Some(local_store) = weak_local_store.upgrade() else {
            tracing::debug!(
                ?opening,
                "unstable tail: the local store has been dropped, so this \
                 subscription ends here"
            );
            return None;
        };
        let Some(stable_log) = weak_stable_log.upgrade() else {
            tracing::debug!(
                ?opening,
                "unstable tail: the stable log has been dropped, so this \
                 subscription ends here"
            );
            return None;
        };

        if let Opening::AfterProducerGone { what } = opening {
            metrics::unstable_read_resync();
            tracing::warn!(
                what,
                "unstable tail subscription fell behind its queue; rescanning \
                 the unstable window"
            );
        }
        Some(UnstableSeam {
            local_store,
            stable_log,
        })
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

    fn subscribe_stable_log(&self, after: Option<LogIndex>) -> crate::StableStream {
        metrics::read_request(ReadSource::Stable);
        resyncing_tail(self.archive.clone(), self.stable_log.clone(), after).until(&self.shutdown)
    }

    fn unstable_tail(&self) -> Channel<Result<Data, Error>, Finite> {
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
            .into_channel()
            .map(|item| {
                if item.is_ok() {
                    metrics::read_entry(ReadEntrySource::LocalStore);
                }
                item
            })
            .until(&self.shutdown)
    }

    fn subscribe_unstable_tail(&self) -> Channel<Result<UnstableEvent, Error>, Infinite> {
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
    use crate::test_stream::ChannelTestExt as _;
    use std::sync::Arc;
    use std::time::Duration;
    use zslg_lifecycle_types::LifecycleError;
    use zslg_snapshot_types::SnapshotError;

    use crate::Reader;
    use crate::api::QuorumCheck;
    use crate::api::{LocalStoreMut, StableLogSnapshot};
    use crossbeam_combinators::{Cancellation, Channel, Finite, Infinite};
    use zslg_log_types::StableLogEvent;

    use crate::learner::{LocalStore, QuorumReadDebouncer, StableLog};
    use zslg_log_types::LogIndex;
    use zslg_node_types::NodeId;
    use zslg_time_types::Hlc;

    /// A node clock for the reader under test.
    fn test_clock() -> Arc<dyn crate::api::Clock> {
        Arc::new(crate::hlc::HlcClock::new(Duration::from_secs(60)))
    }
    use async_trait::async_trait;
    use futures::StreamExt;
    use zslg_log_types::Data;

    use super::ReaderImpl;
    use crate::learner::stable_archive::ObjectStoreArchiveReader;
    use zslg_consensus_types::RaftStateHandle;

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
            std::collections::HashMap<
                zslg_snapshot_types::SnapshotId,
                Vec<zslg_log_types::SnapshotEntry>,
            >,
        >,
    }

    impl ScriptedSnapshotStore {
        fn script(
            &self,
            id: zslg_snapshot_types::SnapshotId,
            entries: Vec<zslg_log_types::SnapshotEntry>,
        ) {
            self.snapshots.lock().expect("lock").insert(id, entries);
        }
    }

    #[async_trait]
    impl crate::api::SnapshotStore for ScriptedSnapshotStore {
        async fn write_snapshot(
            &self,
            _id: zslg_snapshot_types::SnapshotId,
            _entries: crossbeam_combinators::Channel<
                Result<zslg_log_types::SnapshotEntry, crate::Error>,
                Finite,
            >,
            _compression: zslg_snapshot_types::CompressionLevel,
            _reason: zslg_snapshot_types::ObjectStoreReason,
        ) -> Result<crate::api::SnapshotWrite, crate::Error> {
            unimplemented!("reader tests never write snapshots")
        }
        async fn read_snapshot(
            &self,
            id: zslg_snapshot_types::SnapshotId,
            _reason: zslg_snapshot_types::ObjectStoreReason,
        ) -> Result<
            crossbeam_combinators::Channel<
                Result<zslg_log_types::SnapshotEntry, crate::Error>,
                Finite,
            >,
            crate::Error,
        > {
            let entries = self
                .snapshots
                .lock()
                .expect("lock")
                .get(&id)
                .cloned()
                .ok_or_else(|| {
                    crate::Error::Snapshot(SnapshotError::ObjectStore(format!(
                        "no scripted snapshot {id:?}"
                    )))
                })?;
            Ok(crossbeam_combinators::Channel::from_vec(
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
            _live: std::collections::BTreeSet<zslg_snapshot_types::SnapshotId>,
            _retention: Duration,
        ) -> Result<crate::api::CleanupReport, crate::Error> {
            unimplemented!()
        }
    }

    use crate::tests::make_data;
    use crate::tests::upstream;

    /// Pre-load a closed `Channel` for the channel-based `append`.
    fn rx_of(
        items: impl IntoIterator<Item = Result<(LogIndex, Data), crate::Error>>,
    ) -> crossbeam_combinators::Channel<Result<zslg_log_types::SnapshotEntry, crate::Error>, Finite>
    {
        crossbeam_combinators::Channel::from_vec(
            items
                .into_iter()
                .map(|item| {
                    item.map(|(global_index, data)| zslg_log_types::SnapshotEntry {
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
        let (ch, closer) = crossbeam_combinators::Channel::from(rx, "test integrate");
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
            .subscribe_stable_log(None)
            .into_channel()
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_test_stream();

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
            .subscribe_stable_log(Some(LogIndex(0)))
            .into_channel()
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_test_stream();
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
        use zslg_consensus_types::RaftManifestState;
        use zslg_log_types::SnapshotEntry;
        use zslg_snapshot_types::SnapshotId;
        use zslg_snapshot_types::SnapshotMetadata;

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
            .range(None, LogIndex(5))
            .items()
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
        let mut rs = zslg_consensus_types::RaftState::empty();
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
            .subscribe_stable_log(None)
            .into_channel()
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_test_stream();
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
                .publish(zslg_log_types::StableLogEvent::Watermark(hlc));
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
                .publish(zslg_log_types::StableLogEvent::Appended { index, data });
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
            _entries: Channel<Result<zslg_log_types::SnapshotEntry, crate::Error>, Finite>,
        ) -> Result<usize, crate::Error> {
            unimplemented!("scripted stable log is read-only")
        }
        async fn record_end_hlc(&self, _end_hlc: Hlc) -> Result<(), crate::Error> {
            unimplemented!("scripted stable log is read-only")
        }
        fn subscribe_events(&self) -> Channel<zslg_log_types::StableLogEvent, Infinite> {
            self.events_registry.subscribe_infinite()
        }
        fn watermark(&self) -> crossbeam_combinators::Level<Option<Hlc>> {
            self.watermark_level.clone()
        }
        fn range(
            &self,
            after: Option<LogIndex>,
            until: LogIndex,
        ) -> Channel<Result<(LogIndex, Data), crate::Error>, Finite> {
            self.ranges
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let items: Vec<Result<(LogIndex, Data), crate::Error>> = self
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

        let mut stream = reader.subscribe_stable_log(None).into_test_stream();

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
            if let zslg_log_types::StableLogEvent::Appended { index, .. } = ev {
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

        let mut stream = reader.subscribe_stable_log(None).into_test_stream();

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
                zslg_log_types::StableLogEvent::Appended { .. } => appended += 1,
                zslg_log_types::StableLogEvent::Watermark(hlc) => watermarks.push(hlc),
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
            .subscribe_stable_log(None)
            .into_channel()
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_test_stream();
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

        let mut stream = reader.subscribe_stable_log(None).into_test_stream();

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
            if matches!(ev, zslg_log_types::StableLogEvent::Appended { .. }) {
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
            if let zslg_log_types::StableLogEvent::Appended { index, .. } = ev {
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

    fn covering_manifest(
        snap_id: zslg_snapshot_types::SnapshotId,
        end: u64,
    ) -> zslg_consensus_types::RaftState {
        use zslg_consensus_types::RaftManifestState;
        use zslg_snapshot_types::SnapshotMetadata;
        let end_hlc = Hlc(end.saturating_add(100));
        let mut rs = zslg_consensus_types::RaftState::empty();
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
        use zslg_log_types::SnapshotEntry;
        use zslg_snapshot_types::SnapshotId;

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
            .subscribe_stable_log(None)
            .into_channel()
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_test_stream();
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
        use zslg_log_types::SnapshotEntry;
        use zslg_snapshot_types::SnapshotId;

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
        let mut ch =
            reader
                .subscribe_stable_log(None)
                .into_channel()
                .filter_map(|item| match item {
                    Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                    Ok(StableLogEvent::Watermark(_)) => None,
                    Err(e) => Some(Err(e)),
                });

        // 0,1 (local) then 2 (archive) come through; then the untiled 3 ends
        // the stream: the archive's window is short, and a short window is not
        // a hang-up, so the resync loop does not reopen it.
        let (got, end) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::task::spawn_blocking(move || {
                let mut got = Vec::new();
                let end = loop {
                    match ch.recv() {
                        Ok(Ok((idx, data))) => got.push((idx.raw(), data.data_id.hlc().raw())),
                        Ok(Err(e)) => panic!("unexpected error item: {e}"),
                        Err(end) => break end,
                    }
                };
                (got, end)
            }),
        )
        .await
        .expect("the stream ends")
        .expect("drain");
        assert_eq!(got, vec![(0, 100), (1, 101), (2, 102)]);
        assert!(
            matches!(
                end,
                crossbeam_combinators::AbortReason::OrderViolation {
                    what: "archive window"
                }
            ),
            "an untiled hole ends the stream short, got: {end:?}"
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

        let mut stream = reader.unstable_tail().into_test_stream();
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

        let mut stream2 = reader.unstable_tail().into_test_stream();
        let a = stream2.next().await.expect("a").expect("ok");
        let b = stream2.next().await.expect("b").expect("ok");
        assert_eq!(a.data_id.hlc(), Hlc(200));
        assert_eq!(b.data_id.hlc(), Hlc(300));
        assert!(stream2.next().await.is_none());
    }

    // ── subscribe_unstable_tail ─────────────────────────────────────────────

    type UnstableStream = Channel<Result<zslg_log_types::UnstableEvent, crate::Error>, Infinite>;

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
    async fn next_unstable(stream: &mut UnstableStream) -> zslg_log_types::UnstableEvent {
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
            zslg_log_types::UnstableEvent::Watermark(Hlc(50))
        );
        assert_eq!(
            next_unstable(&mut stream).await,
            zslg_log_types::UnstableEvent::Appended(a)
        );
        assert_eq!(
            next_unstable(&mut stream).await,
            zslg_log_types::UnstableEvent::Appended(b)
        );

        let c = make_data(3, 300, b"c");
        integrate_one(&local, 2, c.clone()).await;
        assert_eq!(
            next_unstable(&mut stream).await,
            zslg_log_types::UnstableEvent::Appended(c)
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
            zslg_log_types::UnstableEvent::Watermark(Hlc(50))
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
                zslg_log_types::UnstableEvent::Appended(data) => {
                    seen.insert(data.data_id);
                }
                zslg_log_types::UnstableEvent::Watermark(hlc) => {
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
            zslg_log_types::UnstableEvent::Watermark(Hlc(50))
        );
        for hlc in [60, 70] {
            stable.record_end_hlc(Hlc(hlc)).await.expect("advance");
            assert_eq!(
                next_unstable(&mut stream).await,
                zslg_log_types::UnstableEvent::Watermark(Hlc(hlc))
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
            zslg_log_types::UnstableEvent::Watermark(Hlc(50))
        );

        integrate_one(&local, 0, make_data(1, 100, b"settles")).await;
        stable.record_end_hlc(Hlc(150)).await.expect("advance");

        assert_eq!(
            next_unstable(&mut stream).await,
            zslg_log_types::UnstableEvent::Watermark(Hlc(150))
        );
        assert_quiet(&mut stream).await;
    }

    #[tokio::test]
    async fn an_evicted_node_refuses_the_unstable_subscription() {
        let (_dir, _local, _stable, reader) =
            unstable_rig(16, Hlc(50), Cancellation::never()).await;
        let gated = super::GatedReader::new(
            Arc::new(reader),
            zslg_consensus_types::EvictionStatus::denied_for_test(),
            upstream(9),
        );
        let mut stream = gated.subscribe_unstable_tail();
        assert!(matches!(
            stream.recv(),
            Ok(Err(crate::Error::Lifecycle(LifecycleError::NotReady(_))))
        ));
    }

    /// Node shutdown ends a quiet unstable subscription with `AbortReason::Cancelled`.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_cancels_a_quiet_unstable_subscription() {
        let (handle, shutdown) = crossbeam_combinators::CancelHandle::new();
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
        assert_eq!(end, crossbeam_combinators::AbortReason::Cancelled);
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
        delivered: &mut std::collections::BTreeSet<zslg_log_types::DataId>,
        last_watermark: &mut Option<Hlc>,
    ) -> Result<(), proptest::test_runner::TestCaseError> {
        use proptest::prelude::*;
        while let Some(item) = stream.try_recv() {
            match item.expect("no errors") {
                zslg_log_types::UnstableEvent::Appended(data) => {
                    let current = stable.end_hlc();
                    prop_assert!(
                        current.is_none_or(|w| data.data_id.hlc() > w),
                        "{:?} emitted at or below the watermark {current:?}",
                        data.data_id.hlc()
                    );
                    delivered.insert(data.data_id);
                }
                zslg_log_types::UnstableEvent::Watermark(hlc) => {
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

                let expected: std::collections::BTreeSet<zslg_log_types::DataId> = reader
                    .unstable_tail()
                    .into_test_stream()
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

    /// `subscribe_stable_log(after)` replays history after `after` before going live.
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
        let mut stream = reader.subscribe_stable_log(None).into_test_stream();

        for expected in [0u64, 1] {
            let ev = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("recv timeout")
                .expect("item")
                .expect("ok");
            match ev {
                zslg_log_types::StableLogEvent::Appended { index, .. } => {
                    assert_eq!(index.raw(), expected);
                }
                other => panic!("expected historical Appended({expected}), got {other:?}"),
            }
        }
    }

    /// Node shutdown ends a stable-log subscription parked on a log that has
    /// gone quiet with `AbortReason::Cancelled`. Nothing else would wake it: the reader
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

        let (handle, shutdown) = crossbeam_combinators::CancelHandle::new();
        let reader = reader_shut_down_by(
            local_log,
            stable_log.clone(),
            Duration::from_millis(5),
            Arc::new(ScriptedSnapshotStore::default()),
            Arc::new(RaftStateHandle::default()),
            shutdown,
        );
        let mut tail = reader.subscribe_stable_log(None);
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
        assert_eq!(end, crossbeam_combinators::AbortReason::Cancelled);
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
        let mut stream = reader.unstable_tail().into_test_stream();
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
            .into_test_stream()
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
            .into_test_stream()
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
            .into_test_stream()
            .try_collect()
            .await
            .expect("collect");
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].data_id.hlc(), Hlc(6000));
    }
}
