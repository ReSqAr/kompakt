//! Read access to the local store: this node's own copy of everything it has
//! integrated from upstream replicas, keyed by `(hlc, data_id)`.
//!
//! It is the *unstable* half of a learner's view — data that has arrived but is
//! not yet covered by a snapshot — and it is the source every snapshot build
//! scans. Hence the two very different read shapes:
//!
//! ```text
//!   stream_above_hlc(t)                 unbounded tail  ─> reader's unstable view
//!   stream_window_ordered(above, upto)  bounded chunk   ─> snapshot build
//! ```
//!
//! Only the second is ordered and terminating, because only a snapshot build
//! needs to know it saw the *whole* window — a truncated scan silently treated
//! as complete would advance the watermark past data never read, which
//! [`WindowItem`]'s terminals exist to make impossible (CLAUDE.md §20).

use tokio::sync::watch;

use crate::channel::Channels;
use crate::hlc::Hlc;
use crate::ids::{LogIndex, NodeId};
use crate::meta::Data;

#[async_trait::async_trait]
pub(crate) trait LocalStoreView: Send + Sync + 'static {
    #[must_use]
    fn stream_above_hlc(&self, threshold: Option<Hlc>) -> Channels<Result<Data, crate::Error>>;

    /// Stream `(above, upto]` in `(hlc, data_id)` order.
    ///
    /// `max_entries` bounds the chunk so a large backlog does not produce one
    /// unbounded snapshot object. It is a **floor**: the scan stops at the first
    /// HLC boundary at or after the cap, never inside an HLC, because the caller
    /// resumes by excluding everything at the last HLC it saw. `None` streams the
    /// whole window.
    #[must_use]
    fn stream_window_ordered(
        &self,
        above: Option<Hlc>,
        upto: Hlc,
        max_entries: Option<std::num::NonZeroUsize>,
    ) -> Channels<Result<WindowItem, crate::Error>>;

    /// "Integration happened, go look" — level-triggered, carrying nothing.
    ///
    /// A [`watch`] rather than a broadcast because every consumer of this
    /// signal re-reads state and re-tests a predicate; none of them can use an
    /// individual event, and the payload is `()` precisely because there is
    /// nothing to use. Level-triggering also means a burst nobody drained
    /// cannot cost a waiter its wakeup, so the invariant
    /// `LocalStore::apply_post_fence` calls load-bearing (`sync_quorum`'s
    /// last-job-cleared edge) no longer depends on consumers remembering to
    /// treat a lag as a wakeup.
    #[must_use]
    fn subscribe_integrated(&self) -> watch::Receiver<()>;

    /// Entries integrated for the **first time**, live from this call: each is
    /// published once its batch is written (readable, not yet fsynced) and
    /// only if it is above the stable watermark at that moment. Copies the
    /// duplicate filter drops are not news and are not published.
    ///
    /// Infinite, with a bounded queue per subscriber: one that falls behind is
    /// hung up on, which surfaces as [`End::Aborted`](crate::channel::End::Aborted)
    /// after everything already queued — the cue to rescan above the watermark
    /// and subscribe again (`learner::read`).
    #[must_use]
    fn subscribe_unstable(&self) -> Channels<Data>;

    async fn subscription_cursor(&self, upstream: NodeId)
    -> Result<Option<LogIndex>, crate::Error>;
}

/// An item of a window scan. A scan that reaches a conclusion ends with one of
/// the two terminals, so a stream that stops without one is incomplete — an
/// error to the consumer, never an empty window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WindowItem {
    Entry(Data),
    /// Terminal: the scan reached `upto`, so the whole window is covered.
    Complete,
    /// Terminal: the entry cap stopped the scan here. Everything above this HLC
    /// is uncovered. Always a *complete* HLC — the scan runs to the end of the
    /// one it lands in, so a resume excluding it skips nothing.
    TruncatedAt(Hlc),
}
