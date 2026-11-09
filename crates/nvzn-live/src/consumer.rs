//! The single live consumer: the cold-start stable-log scan, seamed onto the
//! unstable tail.

use crate::StopReason;
use crate::WithWireFormat;
use crate::metrics::{self};
use crossbeam_combinators::{AbortReason, Channel, Finite, Infinite, Key, Seam};
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info};
use zslg::{Data, Hlc, LogIndex, StableLogEvent, UnstableEvent};

use super::coverage::Inner;

/// What the single live consumer sees from the two halves of the seam.
pub(super) enum FoldItem {
    Live(Data),
    Scan(Data),
    ReplayDone,
    TailFailed(zslg::LogApiError),
    ScanFailed(zslg::LogApiError),
}

/// A live entry sits at its HLC; scan items and markers have no position here.
impl Key<FoldItem> for Hlc {
    fn key(item: &FoldItem) -> Option<Self> {
        match item {
            FoldItem::Live(data) => Some(data.data_id.hlc()),
            _ => None,
        }
    }
}

/// The live subscription and the stable watermark it is anchored to.
pub(super) struct LiveSeam {
    pub(super) reader: Arc<dyn zslg::Reader>,
    pub(super) after: Option<LogIndex>,
    pub(super) cancelled: crossbeam_combinators::Cancellation,
}

impl Seam for LiveSeam {
    type Item = FoldItem;
    type Cut = Option<Hlc>;

    fn subscribe(&self) -> Channel<FoldItem, Infinite> {
        self.reader
            .subscribe_unstable_tail()
            .until(&self.cancelled)
            .filter_map(|item| match item {
                Ok(UnstableEvent::Appended(data)) => Some(FoldItem::Live(data)),
                Ok(UnstableEvent::Watermark(_)) => None,
                Err(e) => Some(FoldItem::TailFailed(e)),
            })
    }

    fn cut(&self) -> Option<Hlc> {
        self.reader.stable_watermark()
    }

    fn above(
        &self,
        live: Channel<FoldItem, Infinite>,
        cut: Option<Hlc>,
    ) -> Channel<FoldItem, Infinite> {
        // Drop live entries the scan already covered. `Held` still dedups by
        // DataId above the cut; the seam does not rely on that for the overlap.
        live.filter_above::<Hlc>(cut)
    }

    /// The stable log up to the cut, then `ReplayDone`. The subscription is
    /// infinite, so a scan that ends before the cut ends the range aborted and
    /// `ReplayDone` is never sent — no sentinel needed.
    fn range(&self, cut: Option<Hlc>) -> Channel<FoldItem, Finite> {
        let Some(until) = cut else {
            return Channel::empty();
        };
        self.reader
            .subscribe_stable_log(self.after)
            .into_channel()
            .until(&self.cancelled)
            .take_until(move |item| matches!(item, Ok(ev) if at_or_after_cut(until, ev)))
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { data, .. }) => Some(FoldItem::Scan(data)),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(FoldItem::ScanFailed(e)),
            })
            .chain(Channel::once(FoldItem::ReplayDone))
    }
}

pub(super) fn at_or_after_cut(until: Hlc, event: &StableLogEvent) -> bool {
    match event {
        StableLogEvent::Appended { data, .. } => data.data_id.hlc() > until,
        StableLogEvent::Watermark(hlc) => *hlc >= until,
    }
}

pub(super) fn scan_done<Ev: WithWireFormat>(inner: &Inner<Ev>, entries: u64, started: Instant) {
    metrics::live_scan(entries, started.elapsed());
    info!(
        tenant = inner.tenant.name(),
        entries,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "live view: cold-start scan done"
    );
    inner.finish_scan();
}

/// Wakes anyone waiting on `scanned` whenever `run` returns, for any reason.
/// `finish_scan` is idempotent; `scan_done` already calls it on the normal path.
pub(super) struct FinishScanOnDrop<'a, Ev: WithWireFormat>(&'a Inner<Ev>);

impl<Ev: WithWireFormat> Drop for FinishScanOnDrop<'_, Ev> {
    fn drop(&mut self) {
        self.0.finish_scan();
    }
}

/// The single live consumer: stable scan, then/while the unstable tail.
pub(super) fn run<Ev: WithWireFormat>(inner: &Inner<Ev>, mut stream: Channel<FoldItem, Infinite>) {
    let _finish = FinishScanOnDrop(inner);
    let started = Instant::now();
    let mut entries: u64 = 0;
    loop {
        match stream.recv() {
            Ok(FoldItem::Live(data)) => {
                inner.arrive(data);
            }
            Ok(FoldItem::Scan(data)) => {
                entries = entries.saturating_add(1);
                inner.arrive(data);
            }
            Ok(FoldItem::ReplayDone) => scan_done(inner, entries, started),
            Ok(FoldItem::TailFailed(e)) => {
                error!(
                    tenant = inner.tenant.name(),
                    error = %e,
                    "live view: the unstable tail sent an error; stopping the dispatcher"
                );
                inner.fold_state.halt(StopReason::UnstableTailEnded);
                return;
            }
            Ok(FoldItem::ScanFailed(e)) => {
                error!(error = %e, "live view: the cold-start scan read an error");
                inner.fold_state.halt(StopReason::LiveScanEnded);
                scan_done(inner, entries, started);
                return;
            }
            Err(AbortReason::Cancelled | AbortReason::CancelHandleDropped) => return,
            Err(end) => {
                // A scan and the tail are both unrecoverable here; the
                // reason's label says which source stopped.
                error!(
                    tenant = inner.tenant.name(),
                    ?end,
                    "live view: the stream ended; stopping the dispatcher"
                );
                inner.fold_state.halt(StopReason::LiveStreamEnded);
                return;
            }
        }
    }
}
