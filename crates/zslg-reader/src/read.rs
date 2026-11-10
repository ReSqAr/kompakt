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
use crate::QuorumReadDebouncer;
use crate::metrics::{self, ReadEntrySource, ReadSource};
use crossbeam_combinators::{
    Cancellation, Channel, Contiguous, Finite, Gap, Infinite, NonDecreasing, Opening, Resumable,
    Seam, StrictlyIncreasing, resync,
};
use std::sync::Arc;
use zslg_consensus::EvictionStatus;
use zslg_consensus::NodeId;
use zslg_lifecycle_types::LifecycleError;
use zslg_log::LogIndex;
use zslg_log::{
    ArchiveBounds, ArchiveUpperBound, LocalStoreView, Reader, StableArchiveReader,
    StableLogSnapshot,
};
use zslg_log::{Data, StableLogEvent, UnstableEvent};
use zslg_time::Clock;
use zslg_time::Hlc;

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
pub fn can_serve_clients(eviction: &EvictionStatus) -> bool {
    !eviction.is_denied()
}

/// Wraps a [`Reader`] so client reads are refused with `Error::NotReady`
/// once a reachable peer has affirmatively denied this node's rejoin attempt
/// — a node removed from the group stays up (CLAUDE.md) but its
/// replicated-state view has stopped advancing, so serving from it would
/// return a snapshot that silently ceases to move.
///
/// `stable_watermark` is deliberately **not** gated: it has no error channel
/// and is a local read that cannot mislead a caller into thinking it saw
/// fresh data.
pub struct GatedReader {
    inner: Arc<dyn Reader>,
    eviction: EvictionStatus,
    node_id: NodeId,
}

impl GatedReader {
    #[must_use]
    pub fn new(inner: Arc<dyn Reader>, eviction: EvictionStatus, node_id: NodeId) -> Self {
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
    async fn sync(&self) -> Result<(), zslg_log::Error> {
        async {
            if !self.ready() {
                return Err(self.not_ready());
            }
            self.inner.sync().await.map_err(Into::into)
        }
        .await
        .map_err(Into::into)
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

    fn subscribe_stable_log(&self, after: Option<LogIndex>) -> zslg_log::StableStream {
        if !self.ready() {
            return (Channel::once(Err(self.not_ready()))
                .chain(Channel::aborted("gated reader"))
                .ordered(after, "stable log stream"))
            .err_into();
        }
        self.inner.subscribe_stable_log(after)
    }

    fn unstable_tail(&self) -> Channel<Result<Data, zslg_log::Error>, Finite> {
        let run = || -> Channel<Result<Data, Error>, Finite> {
            if !self.ready() {
                return Channel::once(Err(self.not_ready()));
            }
            self.inner.unstable_tail().err_into()
        };
        run().err_into()
    }

    fn subscribe_unstable_tail(&self) -> Channel<Result<UnstableEvent, zslg_log::Error>, Infinite> {
        let run = || -> Channel<Result<UnstableEvent, Error>, Infinite> {
            if !self.ready() {
                return Channel::once(Err(self.not_ready()))
                    .chain(Channel::aborted("gated reader"));
            }
            self.inner.subscribe_unstable_tail().err_into()
        };
        run().err_into()
    }
}

pub struct ReaderImpl {
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
    pub fn new(
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
    (stable_log
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
        .into_channel())
    .err_into()
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
) -> zslg_log::StableStream {
    let weak_archive = Arc::downgrade(&archive);
    let weak_stable_log = Arc::downgrade(&stable_log);
    drop((archive, stable_log));

    (Resumable::new::<_, Contiguous<LogIndex>, _, _>(
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
    ))
    .err_into()
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
impl zslg_time::HasHlc for UnstableStep {
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
        opening.chain(scan.err_into())
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
    async fn sync(&self) -> Result<(), zslg_log::Error> {
        async {
            let result = self.debouncer.sync_quorum().await;
            metrics::quorum_read(metrics_kit::OpState::from_ok(result.is_ok()));
            result
        }
        .await
        .map_err(Into::into)
    }

    fn stable_watermark(&self) -> Option<Hlc> {
        self.stable_log.end_hlc()
    }

    fn hlc_now(&self) -> Hlc {
        self.clock.now()
    }

    fn subscribe_stable_log(&self, after: Option<LogIndex>) -> zslg_log::StableStream {
        metrics::read_request(ReadSource::Stable);
        resyncing_tail(self.archive.clone(), self.stable_log.clone(), after).until(&self.shutdown)
    }

    fn unstable_tail(&self) -> Channel<Result<Data, zslg_log::Error>, Finite> {
        let out: Channel<Result<Data, Error>, Finite> = {
            metrics::read_request(ReadSource::Unstable);
            let local_store = self.local_store.clone();
            let stable_log = self.stable_log.clone();
            // The stable watermark = end_hlc. Everything at or below it has been
            // settled into the stable prefix (served by stable_tail) or has been
            // superseded and must not appear here — so non-quorum writes below the
            // watermark disappear from the unstable view by the next snapshot.
            let watermark = stable_log.end_hlc();
            (local_store
                .stream_above_hlc(watermark)
                .into_channel()
                .map(|item| {
                    if item.is_ok() {
                        metrics::read_entry(ReadEntrySource::LocalStore);
                    }
                    item
                })
                .until(&self.shutdown))
            .err_into()
        };
        out.err_into()
    }

    fn subscribe_unstable_tail(&self) -> Channel<Result<UnstableEvent, zslg_log::Error>, Infinite> {
        let run = || -> Channel<Result<UnstableEvent, Error>, Infinite> {
            metrics::read_request(ReadSource::UnstableSubscription);
            resyncing_unstable_tail(self.local_store.clone(), self.stable_log.clone())
                .until(&self.shutdown)
        };
        run().err_into()
    }
}
