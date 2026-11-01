//! Blocking channel pipelines, both ends.
//!
//! A [`Channel<T>`] is the read half: a composable pull-source over crossbeam
//! receivers, with an explicit [`End`] verdict that every read returns. The
//! write half is a [`Feed<T>`] (one producer, closed with a [`Closed`] proof),
//! a [`Fanout<T>`] (one distribution thread, many subscribers), or a
//! [`Level<T>`] (a watched value). The channel graph runs at pull time on the
//! consumer's thread; only producers and the fan-out own threads.
//!
//! ```text
//!   write half                          read half
//!   ──────────                          ─────────
//!   Channel::produce_blocking(..) ────▶ Channel<T>
//!     feed.send(item)                     recv() / try_recv() / ready_batch()
//!     feed.close() -> Closed ⇒ Complete   filter_map / map / merge / chain /
//!     panic / drop       ⇒ Aborted        take_while / take_until /
//!                                         take_at_least / filter_above /
//!                                         dedup_by_key / until
//!   broadcast(..) -> (Fanout, Registry)
//!     registry.subscribe() ────────────▶ Channel<T>  (hang up on lag)
//!   Level::new(..) -> (setter, level)
//!     level.changes() ─────────────────▶ Channel<()>
//! ```
//!
//! The verdict is the contract: `Complete` means the stream ended cleanly
//! (producer close, all phases/branches complete, or a consumer bound);
//! `Aborted` means it did not, and [`AbortReason`] says why. A consumer that
//! needs completeness verifies it against its own source of truth; the channel
//! only reports the transport outcome.
//!
//! The seam ([`Tail`], [`Ranged`], [`blend_chain`]) is the
//! subscribe-then-cut pattern: open the live source, snapshot the cut, read the
//! history up to it, then continue live.
//!
//! The `tokio` feature adds [`Channel::produce_blocking`],
//! [`Cancellation::from_token`] and [`Level::changed`]. The core is
//! crossbeam-only.

// Dev-dependencies used by feature-gated tests; keep the unused-dependency
// lint quiet when the `tokio` feature is off.
#[cfg(test)]
use proptest as _;
#[cfg(test)]
use tokio as _;
#[cfg(test)]
use tokio_util as _;

mod blend;
mod channel;
mod fanout;
mod feed;
mod level;
mod ordered;

pub use blend::{Ranged, Tail, blend_chain};
pub use channel::{AbortReason, CancelHandle, Cancellation, Channel, Closer, End, ReadyBatch};
pub use fanout::{
    Fanout, FanoutConfig, KeyedRegistry, OverflowPolicy, Registry, RegistryError, Stopped,
    TryPublishError, broadcast, keyed,
};
pub use feed::{Closed, Disconnected, Feed};
pub use level::{Level, LevelGone, LevelSetter};
pub use ordered::{
    Contiguous, Gap, Key, NonDecreasing, OrderPolicy, OrderedChannel, StrictlyIncreasing, Successor,
};
