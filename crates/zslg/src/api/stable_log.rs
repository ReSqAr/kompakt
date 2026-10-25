//! The converged half of a node's view: entries loaded from object-store
//! snapshots, in global index order, identical on every node.
//!
//! Two frontiers, and they are not the same thing:
//!
//! ```text
//!   end_index   highest index present         advanced by append
//!   end_hlc     watermark the prefix is at    advanced by append and by
//!                                             record_end_hlc — an empty-window
//!                                             manifest moves it past the last
//!                                             entry, so it is not derivable
//!                                             from the data
//! ```
//!
//! `end_hlc` is the boundary the blended read is built on: `stable_tail` serves
//! at or below it, `unstable_tail` strictly above, so the two never leave a
//! gap and below-watermark data that never reached a snapshot drops out of the
//! view — which is how nodes converge on one history. While a load is in
//! flight they may briefly *overlap* (see [`StableLogSnapshot::end_hlc`]);
//! that is the safe direction, absorbed by dedup on `data_id`.
//!
//! Entries arrive on a bounded channel rather than a stream because the entire
//! drain → encode → commit → fsync loop runs on one blocking thread (CLAUDE.md
//! §8), fed straight from the off-reactor snapshot decode. A `blocking_send` on
//! a full channel is the backpressure that paces decode to the commit rate.

use crate::channel::Channels;

use crate::hlc::Hlc;
use crate::ids::LogIndex;
use crate::meta::{Data, SnapshotEntry, StableLogEvent};

#[async_trait::async_trait]
pub(crate) trait StableLogSnapshot: Send + Sync + 'static {
    /// Recomputed from the log itself on open, so it cannot drift: there is no
    /// separately-persisted "loaded index" marker to fall out of step with the
    /// data.
    #[must_use]
    fn end_index(&self) -> Option<LogIndex>;

    /// Every entry at or below it is present. Not always `>=` the content:
    /// entries sharing an HLC may straddle a load batch, so until a larger HLC
    /// (or `record_end_hlc`) proves the tail group complete, the watermark
    /// stays below it. Erring low only repeats an entry across the blended
    /// read's two halves (deduped by `data_id`); erring high would drop the
    /// group's unloaded rest from both.
    #[must_use]
    fn end_hlc(&self) -> Option<Hlc>;

    /// Append decoded snapshot entries (chunked commit + fsync + fan-out).
    /// Gapless-ingest fence: an index past `end + 1` is rejected
    /// (`Error::StableLogGap`) rather than leaving a hole.
    async fn append(
        &self,
        entries: Channels<Result<SnapshotEntry, crate::Error>>,
    ) -> Result<usize, crate::Error>;

    /// Record that the prefix is loaded up to the manifest watermark `end_hlc`.
    async fn record_end_hlc(&self, end_hlc: Hlc) -> Result<(), crate::Error>;

    /// Entries and watermark advances in causal order: an entry is always
    /// emitted before any `Watermark` covering its HLC.
    ///
    /// The subscription is infinite and its own queue is bounded: a consumer
    /// that falls too far behind is **hung up on** (the fan-out drops its
    /// sender), which surfaces as [`crate::channel::End::Aborted`] after every
    /// event that was already queued. That is the lag signal, and the response
    /// is to replay from your cursor and subscribe again — see
    /// `learner::fanout` and `learner::read`.
    #[must_use]
    fn subscribe_events(&self) -> Channels<StableLogEvent>;

    /// The current watermark, level-triggered — for consumers that want log
    /// *state*, not its data. No payload crosses it and it cannot lag, so a
    /// slow reader simply observes the latest value rather than a backlog.
    #[must_use]
    fn watch_watermark(&self) -> tokio::sync::watch::Receiver<Option<Hlc>>;

    /// The same level for a blocking consumer: a wake-up after each move,
    /// carrying nothing — read `end_hlc` or the watch on waking. One pending
    /// wake stands for any number of moves, so it cannot lag either. Infinite;
    /// it ends `Aborted` only when the log is dropped.
    #[must_use]
    fn watermark_moves(&self) -> Channels<()>;

    #[must_use]
    fn range(
        &self,
        from: LogIndex,
        until: LogIndex,
    ) -> Channels<Result<(LogIndex, Data), crate::Error>>;

    /// Durably delete everything at or below `hlc_threshold`: a `WriteBatch`
    /// range-delete, fenced before returning. The threshold is clamped
    /// strictly below the frontier entry's HLC so the recovery anchor
    /// (`max_key`/`end_index`) is never dropped. Driven by the stable-log
    /// purge worker (`stable_log::purge`).
    fn set_purge_threshold(&self, hlc_threshold: Hlc) -> Result<(), crate::Error>;
}
