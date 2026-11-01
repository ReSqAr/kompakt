//! Blocking channel pipelines, both ends.
//!
//! A [`Channel<T>`] is the read half: a composable pull-source over crossbeam
//! receivers, with an explicit [`End`] verdict. A [`Feed<T>`] is a write half;
//! [`broadcast`]/[`keyed`] add a distribution thread plus a cloneable registry,
//! a bounded data queue, and a single [`Shutdown`] owner. The channel graph
//! itself still runs at pull time; only the fan-out owns a thread.
//!
//! ```text
//!   write half                         read half
//!   ──────────                         ─────────
//!   Feed::channel(cap, what) ────────▶ Channel<T>
//!     send(item)                         recv() / try_recv() / ready_batch()
//!     close()        ⇒ End::Complete     filter_map / map / merge / chain /
//!     drop()         ⇒ End::Aborted      resume_on_abort / take_while /
//!                                        until / into_stream
//!   broadcast(..) -> (tx, registry, shutdown)
//!     registry.subscribe() ───────────▶ Channel<Arc<T>>  (hang up on lag)
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
//! [`Channel::into_stream`] and [`Cancellation::from_token`]. The core is
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
    AbortReason, CancelHandle, Cancellation, Channel, ChannelIter, Closer, End, ReadyBatch,
};
#[cfg(feature = "tokio")]
pub use fanout::shutdown_on_token;
pub use fanout::{
    KeyedRegistry, OverflowPolicy, Registry, RegistryError, Shutdown, Wakes, broadcast, keyed,
};
pub use feed::Feed;
pub use ordered::{
    Contiguous, Gap, Key, NonDecreasing, OrderPolicy, OrderedChannel, StrictlyIncreasing, Successor,
};
