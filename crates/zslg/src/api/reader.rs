//! The client's entire read API: a blended view of one node's history.
//!
//! The [`stable_watermark`](Reader::stable_watermark) splits it, and the two
//! halves have genuinely different guarantees:
//!
//! ```text
//!   stable_tail()      <= watermark   converged; identical on every node;
//!                                     index-ordered; lags by ~one snapshot interval
//!   unstable_tail()    >  watermark   fresh; node-local; may contain entries
//!                                     that never converge
//! ```
//!
//! Nothing is lost between the halves — `unstable_tail` starts strictly above
//! the watermark, and everything at or below it is in the stable prefix. While
//! a snapshot is loading an entry can briefly be in both (the watermark errs
//! low, never high), so a consumer blending them dedups by `data_id`. The
//! watermark is also how convergence becomes visible to a reader: an
//! entry below the watermark that never reached the stable prefix (a write that
//! never made quorum) simply drops out of the blended view at the next snapshot.
//!
//! Both halves can be followed live. `subscribe_stable_log` is gapless and
//! index-ordered, resumable from a cursor. `subscribe_unstable_tail` cannot be:
//! the unstable half has no monotone position, so it is arrival-ordered and
//! at-least-once, and carries the watermark in-band as the signal to drop what
//! has settled.
//!
//! Freshness is opt-in. A read is served from local state, so a client that
//! needs to see other writers' recent writes calls
//! [`sync_quorum`](Reader::sync) first; charging every read for that
//! round-trip would be the wrong default for the tailing consumers this API is
//! shaped around.

use crate::Error;
use crate::channel::Channels;
use crate::hlc::Hlc;
use crate::ids::LogIndex;
use crate::meta::{Data, StableLogEvent, UnstableEvent};

#[async_trait::async_trait]
pub trait Reader: Send + Sync + 'static {
    /// After `Ok(())`, an immediate read observes every entry that was on a
    /// quorum of replicas when this call started. Debounced per
    /// `quorum_read_deadtime`, so concurrent callers share one round-trip.
    async fn sync(&self) -> Result<(), Error>;

    /// The manifest watermark this node has loaded into its stable prefix.
    fn stable_watermark(&self) -> Option<Hlc>;

    /// This node's clock, read now. A write this node made before the call
    /// has an HLC at or below it (its `DataId` was minted from the same
    /// clock), so a reader that waits until the stable prefix has passed
    /// `hlc_now()` sees every such write.
    fn hlc_now(&self) -> Hlc;

    /// Entries and watermark advances in causal order: a `Watermark` never
    /// arrives before an `Appended` it covers. Replays historical entries from
    /// `from` (inclusive), then follows live events.
    ///
    /// `from` is required: the stable log is a complete history and a read of
    /// it is a position in that history, so there is no "only what is new"
    /// mode. Knowing the consumer's position is also what makes the stream
    /// recoverable — **a slow consumer never sees a broadcast lag**. When the
    /// live ring laps this subscriber, the stream silently re-enters historical
    /// replay from its own cursor and rejoins the live tail, as often as
    /// necessary. What the caller observes is one gapless, index-ordered
    /// stream; falling behind costs latency, not data.
    fn subscribe_stable_log(&self, from: LogIndex) -> Channels<Result<StableLogEvent, Error>>;

    /// One-shot scan of entries strictly above the watermark. Emitting one at or
    /// below it would duplicate `stable_tail` (CLAUDE.md §12).
    fn unstable_tail(&self) -> Channels<Result<Data, Error>>;

    /// Live entries above the watermark, in arrival (integration) order, with
    /// the watermark in-band. At-least-once: an entry may repeat, so consumers
    /// dedup by `data_id`.
    ///
    /// Every cycle opens with `Watermark(w)` (once the node has a stable
    /// watermark) and a `Watermark` follows each advance; an `Appended` at or
    /// below the watermark current when it would be emitted is never emitted.
    /// On a `Watermark(w)` a consumer drops what it holds at or below `w`.
    ///
    /// Infinite: a consumer that falls behind is resynced silently (replay
    /// everything above the watermark, then live), so any termination is
    /// [`End::Aborted`](crate::channel::End::Aborted), and node shutdown is
    /// [`End::Cancelled`](crate::channel::End::Cancelled).
    fn subscribe_unstable_tail(&self) -> Channels<Result<UnstableEvent, Error>>;
}
