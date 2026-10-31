//! Blocking channel pipelines, both ends.
//!
//! A [`Channels<T>`] is the read half: a composable pull-source over crossbeam
//! receivers, with an explicit [`End`] verdict. A [`Feed<T>`] or [`Fanout<T>`]
//! is the write half: the sender plus the proof of completion. The graph runs
//! at pull time, on the consumer's thread; nothing here spawns.
//!
//! ```text
//!   write half                         read half
//!   ──────────                         ─────────
//!   Feed::channel(cap, what) ────────▶ Channels<T>
//!     send(item)                         recv() / try_recv() / ready_batch()
//!     close()        ⇒ End::Complete     filter_map / map / merge / chain /
//!     drop()         ⇒ End::Aborted      resume_on_abort / take_while /
//!                                        until / into_stream
//!   Fanout<T>::subscribe() ───────────▶ Channels<T>  (hang up on lag)
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
//! history up to it, then continue live. One domain-chosen item type flows
//! through both halves; the crate never changes it and never knows what a
//! boundary means.
//!
//! Tokio and futures are optional: the `tokio` feature adds
//! [`Channels::into_stream`] and [`Cancellation::from_token`]. The core is
//! crossbeam-only.

// Dev-dependencies used by feature-gated tests; keep the unused-dependency
// lint quiet when the `tokio` feature is off.
#[cfg(test)]
use futures as _;
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
mod ordered;

pub use blend::{Ranged, Tail, blend_chain};
pub use channel::{
    AbortReason, CancelHandle, Cancellation, ChannelIter, Channels, Closer, End, ReadyBatch,
};
pub use fanout::{Fanout, Wakes};
pub use feed::Feed;
pub use ordered::{
    Gap, Gapless, Key, NonDecreasing, OrderPolicy, OrderedChannel, StrictlyMonotone, Successor,
};
