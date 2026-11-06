//! `Channel<T>` — the blocking read half of a pipeline.
//!
//! A `Channel<T>` owns a private tree of sources: a leaf (one crossbeam
//! receiver), a concurrent merge, a sequential chain, a guard raced against a
//! liveness signal or [`Cancellation`], a lazy resume loop, a bounded
//! `take_while`, and pull-time `map`/`filter_map`. The tree is invisible through
//! one blocking interface.
//!
//! `recv` blocks on `Select::ready` over all live leaves; `try_recv` drains
//! without blocking; `ready_batch` takes its first item through `recv` and the
//! rest through `try_recv`. Everything runs at pull time, on the consumer's
//! thread; the crate never spawns.
//!
//! ## Verdicts
//!
//! [`End`] is only clean or not clean:
//!
//! - [`End::Complete`]: the stream ended cleanly. A producer called
//!   [`Closer::close`], every phase/branch completed, or a
//!   [`Channel::take_while`] bound was reached.
//! - [`End::Aborted`]: the stream did not complete cleanly. The
//!   [`AbortReason`] says who or what ended it.
//!
//! A data channel disconnecting without a close is
//! [`AbortReason::ProducerGone`]; a fired cancellation is
//! [`AbortReason::Cancelled`]; a dropped unfired handle is
//! [`AbortReason::CancelHandleDropped`]. The library never decides that an
//! abort is recoverable — [`crate::Resumable`] reopens only after `ProducerGone`.
//!
//! A `take_while` bound is a clean end of the derived stream. If the source
//! ends before the bound, that is also `Complete`; bound-vs-short-read is a
//! domain completeness question, not a transport verdict.
//!
//! ## Consumers still validate
//!
//! An explicit close proves the producer *believed* it finished. Where
//! completeness is a durability matter, verify it against an independent source
//! of truth as well: a gapless-fence check, a cursor, or a manifest bound. The
//! channel reports the claim; the consumer owns the proof.
//!
//! ## Composition
//!
//! [`Channel::merge`] selects across two sources and drops a dead branch as
//! soon as it dies, releasing whatever its closures captured. [`Channel::chain`]
//! runs phases in order and condemns the chain on an abort.
//! [`crate::Resumable`] and [`crate::resync`] reopen a stream after its
//! producer went away. [`Channel::take_while`] ends at a consumer bound;
//! [`Channel::filter_map`], [`Channel::map`] and [`Channel::until`]
//! transform and bound a stream at pull time.
//!
//! The write half lives next door: [`crate::Feed`] for one producer and
//! [`crate::broadcast`]/[`crate::keyed`]/[`crate::Wakes`] for many. The seam
//! ([`crate::Seam`], [`crate::Segment`]) composes a live tail with a bounded
//! history.

use std::marker::PhantomData;

use crate::kind::{Finite, Infinite};

mod cancellation;
mod chain;
mod constructors;
mod dedup_by_key;
#[cfg(feature = "tokio")]
mod deferred;
mod filter_map;
mod guarded;
mod leaf;
mod merge;
mod read;
mod resume;
mod source;
mod take_while;
mod verdict;

pub use cancellation::{CancelHandle, Cancellation};
pub use read::ReadyBatch;
pub(crate) use source::{Poll, Source};
pub use verdict::{AbortReason, Closer, End};

/// The read half of a pipeline: see the module docs.
///
/// There is no `IntoIterator`: iterating would have to drop the verdict.
/// Read with [`recv`](Self::recv) or [`ready_batch`](Self::ready_batch),
/// which return it.
///
/// ```compile_fail
/// let ch = crossbeam_combinators::Channel::from_vec(vec![1u32]);
/// for item in ch {}
/// ```
pub struct Channel<T, K> {
    pub(crate) source: Box<dyn Source<T>>,
    _kind: PhantomData<K>,
}

impl<T, K> Channel<T, K> {
    pub(crate) fn wrap(source: Box<dyn Source<T>>) -> Self {
        Self {
            source,
            _kind: PhantomData,
        }
    }
}

impl<T: Send + 'static> Channel<T, Infinite> {
    /// View this stream as finite: the one sound widening. An infinite stream
    /// is a finite stream that never completes, so a consumer that accepts
    /// either kind (a `dyn` trait method cannot be generic) takes `Finite`
    /// and an infinite caller widens. The reverse does not exist.
    #[must_use]
    pub fn into_finite(self) -> Channel<T, Finite> {
        Channel::wrap(self.source)
    }
}

/// Test support: a finite channel holding `items` whose producer never closed
/// — `Aborted(ProducerGone { what })` after the last item.
#[cfg(test)]
pub(crate) fn aborted_of<T: Send + 'static>(
    items: Vec<T>,
    what: &'static str,
) -> Channel<T, Finite> {
    let (tx, rx) = crossbeam_channel::bounded(items.len().max(1));
    for item in items {
        let _ = tx.send(item);
    }
    drop(tx);
    let (ch, closer) = Channel::from(rx, what);
    drop(closer);
    ch
}

#[cfg(test)]
mod tests;
