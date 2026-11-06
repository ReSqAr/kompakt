//! Read access to the local store: this node's own copy of everything it has
//! integrated from upstream replicas, keyed by `(hlc, data_id)`.
//!
//! It is the *unstable* half of a learner's view — data that has arrived but is
//! not yet covered by a snapshot — and it is the source every snapshot build
//! scans. Hence the two very different read shapes:
//!
//! ```text
//!   stream_above_hlc(t)           unbounded tail  ─> reader's unstable view
//!   stream_window(above, upto)    bounded window  ─> snapshot build
//! ```
//!
//! Both are plain ordered scans that close when they reach their end, so a
//! scan that dies reads as `Aborted`, never as a short window (CLAUDE.md §20).
//! A snapshot build bounds its chunk itself (`take_at_least`) and picks the
//! watermark from what it wrote.

use tokio::sync::watch;

use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::DataId;
use crate::ids::{LogIndex, NodeId};
use crate::meta::Data;
use crossbeam_combinators::{Channel, Finite, OrderedChannel, StrictlyIncreasing};

/// A local-store scan (above an HLC, or a bounded window): strictly increasing
/// `DataId`s.
pub(crate) type LocalStoreStream =
    OrderedChannel<Result<Data, Error>, StrictlyIncreasing<DataId>, Finite>;

#[async_trait::async_trait]
pub(crate) trait LocalStoreView: Send + Sync + 'static {
    #[must_use]
    fn stream_above_hlc(&self, threshold: Option<Hlc>) -> LocalStoreStream;

    /// Stream `(above, upto]` in `(hlc, data_id)` order; it closes at `upto`.
    #[must_use]
    fn stream_window(&self, above: Option<Hlc>, upto: Hlc) -> LocalStoreStream;

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
    /// hung up on, which surfaces as [`End::Aborted`](crossbeam_combinators::End::Aborted)
    /// after everything already queued — the cue to rescan above the watermark
    /// and subscribe again (`learner::read`).
    #[must_use]
    fn subscribe_unstable(&self) -> Channel<Data, crossbeam_combinators::Infinite>;

    async fn subscription_cursor(&self, upstream: NodeId)
    -> Result<Option<LogIndex>, crate::Error>;
}
