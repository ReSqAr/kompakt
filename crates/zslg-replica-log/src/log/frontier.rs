//! The frontier: the next index and the running-max HLC that keys are built from.

use zslg_log::Data;
use zslg_log::LogIndex;
use zslg_log::{Admission, Completion};
use zslg_time::Hlc;

pub struct PendingWrite {
    pub(crate) data: Data,
    /// Where to send the assigned `LogIndex` (or storage error) after the batch is
    /// durable. An `Arc<Sender>` instead of a per-write `Box<dyn FnOnce>` — the
    /// caller attaches the same sender to every write in its stream with an
    /// `Arc::clone` (a refcount bump), and the flush loop sends the raw result.
    /// Mapping to `WriteOutcome` happens upstream, at the caller's consumer.
    /// The `data_id` the caller needs to build a `WriteOutcome` downstream.
    pub(crate) completion: Completion,
    pub(crate) guard: Option<Admission>,
    /// When `submit()` enqueued this write — used to measure the channel batching
    /// wait (enqueue → flush-loop dequeue).
    pub(crate) enqueued_at: std::time::Instant,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FrontierState {
    pub(crate) index: Option<LogIndex>,
    /// Largest HLC over every entry ever appended — and, because the key's
    /// secondary is a running max, also the frontier entry's own secondary.
    /// The two used to be separate fields (`hlc` was the frontier entry's own
    /// HLC, needed by the purge clamp); under the running-max encoding they are
    /// the same number, and the clamp now speaks retirement's own coordinate.
    pub(crate) max_hlc: Option<Hlc>,
}

pub(crate) struct Frontier {
    pub(crate) inner: parking_lot::Mutex<FrontierState>,
}

impl Frontier {
    pub(crate) fn new(index: Option<LogIndex>, max_hlc: Option<Hlc>) -> Self {
        Self {
            inner: parking_lot::Mutex::new(FrontierState { index, max_hlc }),
        }
    }

    pub(crate) fn next_index(&self) -> LogIndex {
        LogIndex::next_after(self.inner.lock().index)
    }

    /// The frontier's index, if the log is non-empty.
    pub(crate) fn index(&self) -> Option<LogIndex> {
        self.inner.lock().index
    }

    pub(crate) fn max_hlc(&self) -> Option<Hlc> {
        self.inner.lock().max_hlc
    }

    /// Record a committed batch. `running_max` is the secondary written into
    /// the batch's last key, so it is already the max over the whole log — the
    /// fold happens in [`Committer::append_and_flush`], where the key needs
    /// it. Folding again here with `.max()` (rather than assigning) is
    /// belt-and-braces on top of that, not a replacement: today
    /// `Committer::append_and_flush` is the sole caller, so it is a no-op, but
    /// `max_hlc` is CLAUDE.md §8's *exact* field — the leader samples it
    /// across a quorum to build the joint-membership drain bound — so this
    /// keeps under-reporting it structurally impossible rather than merely an
    /// obligation on every future caller.
    pub(crate) fn advance(&self, index: LogIndex, running_max: Hlc) {
        let mut f = self.inner.lock();
        f.index = Some(index);
        f.max_hlc = Some(
            f.max_hlc
                .map_or(running_max, |current| current.max(running_max)),
        );
    }

    /// Clamp a purge threshold strictly below the frontier entry's secondary,
    /// so retirement can never cover the newest entry — its index is the
    /// durable record `newest_live_entry` recovers `next_index` from at open.
    pub(crate) fn clamp_threshold(&self, threshold: Hlc) -> Option<Hlc> {
        let frontier = self.inner.lock().max_hlc?;
        // checked_sub: a frontier at HLC 0 leaves nothing purgeable.
        let cap = Hlc(frontier.raw().checked_sub(1)?);
        Some(threshold.min(cap))
    }
}
