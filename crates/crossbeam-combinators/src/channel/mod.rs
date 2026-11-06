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

use std::collections::VecDeque;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Select};

use crate::resume::Opening;

use std::marker::PhantomData;

use crate::kind::{Finite, Infinite, Kind, MergeKind};
use crate::ordered::Key;

mod cancellation;
mod chain;
#[cfg(feature = "tokio")]
mod deferred;
mod guarded;
mod leaf;
mod merge;
mod resume;
mod source;
mod transform;
mod verdict;

pub use cancellation::{CancelHandle, Cancellation};
pub(crate) use source::{Poll, Source};
pub use verdict::{AbortReason, Closer, End};

use chain::ChainState;
#[cfg(feature = "tokio")]
use deferred::{Deferred, DeferredState};
use guarded::Guarded;
use leaf::LeafState;
use merge::{MergeState, Side};
use resume::Resume;
use transform::{DedupByKey, FilterMap, TakeWhile};

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

impl<T: Send + 'static> Channel<T, Finite> {
    /// Wrap a **finite** receiver, handing back the producer's [`Closer`].
    ///
    /// `what` names the producer in an [`End::Aborted`] verdict, so keep it
    /// specific enough to identify the site from a log line.
    ///
    /// The `Closer` is in the return type on purpose: a construction site cannot
    /// forget that closing is its job. A stream the application expects to run
    /// forever wants [`from_infinite`](Self::from_infinite) instead — do not
    /// reach for `from(..)` and an immediate `close()`, which claims a clean
    /// finish the stream cannot vouch for.
    ///
    /// Prefer [`Feed::channel`](crate::Feed::channel) for a new producer: it
    /// pairs the sender and the closer. This low-level form is for adapters
    /// that must keep them separate — an async producer, or a fan-out that
    /// shares only the sender.
    #[must_use]
    pub fn from(rx: Receiver<T>, what: &'static str) -> (Self, Closer) {
        let (close_tx, close_rx) = crossbeam_channel::bounded(1);
        (
            Self::wrap(Box::new(LeafState::Alive {
                rx,
                close_rx: Some(close_rx),
                what,
            })),
            Closer(close_tx),
        )
    }

    /// A receiver whose producer has already declared completion.
    ///
    /// Useful when the items are already queued and the producer is gone; the
    /// stream reports [`End::Complete`] after draining. For a producer that is
    /// still running, use [`Channel::from`] and keep its [`Closer`].
    #[must_use]
    pub fn closed(rx: Receiver<T>) -> Self {
        let (ch, closer) = Self::from(rx, "closed");
        closer.close();
        ch
    }

    /// A closed, empty stream: `End::Complete` immediately.
    #[must_use]
    pub fn empty() -> Self {
        Self::from_vec(Vec::new())
    }

    /// A closed stream of exactly one item.
    #[must_use]
    pub fn once(item: T) -> Self {
        Self::from_vec(vec![item])
    }

    /// A complete, finite stream: everything is already here, so it closes
    /// itself and can never be `Aborted`.
    #[must_use]
    pub fn from_vec(items: Vec<T>) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(items.len().max(1));
        for item in items {
            let _ = tx.send(item);
        }
        drop(tx);
        let (ch, closer) = Self::from(rx, "from_vec");
        closer.close();
        ch
    }

    /// Fixed phases in order. Only the front phase is registered; when it
    /// closes cleanly the next one is pulled. An [`End::Aborted`] or
    /// [`End::Aborted { reason: AbortReason::Cancelled }`] phase ends the chain there and drops the remaining
    /// phases, exactly like [`chain`](Self::chain).
    ///
    /// An empty phase list is an empty [`End::Complete`] stream. A single phase
    /// is returned as-is.
    #[must_use]
    pub fn chain_all<I>(phases: I) -> Channel<T, Finite>
    where
        I: IntoIterator<Item = Channel<T, Finite>>,
    {
        let mut phases: VecDeque<Box<dyn Source<T>>> =
            phases.into_iter().map(|ch| ch.source).collect();
        if phases.len() == 1 {
            let source = phases.pop_front().expect("checked one phase");
            return Channel::wrap(source);
        }
        Channel::wrap(Box::new(ChainState::Active { phases }))
    }

    #[must_use]
    pub fn chain<K: Kind>(self, next: Channel<T, K>) -> Channel<T, K> {
        Channel::wrap(Box::new(ChainState::Active {
            phases: VecDeque::from([self.source, next.source]),
        }))
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

    /// A stream the application expects to **never end**: socket-backed, or an
    /// internal fan replaced wholesale on reconnect.
    ///
    /// There is deliberately no [`Closer`] — nothing may declare completion,
    /// because completion is not something that happens to this stream. Any
    /// termination is an error: [`recv`](Self::recv) reports [`End::Aborted`],
    /// which is the consumer's cue to back off and resume from its last
    /// position. Whether the peer closed politely, crashed, or the socket reset
    /// is visible in the channel lifecycle and irrelevant to the caller — the
    /// response is the same either way.
    #[must_use]
    pub fn from_infinite(rx: Receiver<T>, what: &'static str) -> Self {
        Self::wrap(Box::new(LeafState::Alive {
            rx,
            close_rx: None,
            what,
        }))
    }

    /// An aborted, empty stream: `Aborted(ProducerGone { what })` immediately.
    ///
    /// Use this when a producer cannot be started (a closed registry, a full
    /// registration queue) and the consumer contract is an infinite stream:
    /// ending `Complete` would look like a deliberate finish.
    #[must_use]
    pub fn aborted(what: &'static str) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(1);
        drop(tx);
        Self::from_infinite(rx, what)
    }

    /// The resume loop (see [`Resume`]); the public faces are
    /// [`crate::Resumable`] and [`crate::resync`].
    pub(crate) fn resume<C: Send + 'static>(
        what: &'static str,
        cursor: C,
        track: fn(&T, &mut C),
        make: impl FnMut(&C, Opening) -> Option<Channel<T, Infinite>> + Send + 'static,
    ) -> Channel<T, Infinite> {
        let mut source = Resume {
            what,
            cursor,
            track,
            make: Box::new(make),
            current: None,
            last_end: None,
        };
        // Eagerly, so `leaf_count()` is accurate before the first `register`.
        source.open(Opening::First);
        Channel::wrap(Box::new(source))
    }
}

impl<T: Send + 'static, K: Kind> Channel<T, K> {
    /// A phase whose channel comes from an async opener, started on first use.
    ///
    /// Nothing runs at construction: the first time the stream is read (or
    /// registered by an enclosing merge/chain), `open` is spawned on `handle`,
    /// and the stream then delegates to the channel it resolves to, verdict
    /// included. In a `chain_all` only the front phase is read, so deferred
    /// phases open one at a time. Dropping the stream before it resolves
    /// aborts the task; a task that ends without resolving reads as
    /// `Aborted(ProducerGone { what: "defer_async" })`.
    #[cfg(feature = "tokio")]
    #[must_use]
    pub fn defer_async<F, Fut>(handle: &tokio::runtime::Handle, open: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Channel<T, K>> + Send + 'static,
    {
        let (tx, handoff) = crossbeam_channel::bounded(1);
        let handle = handle.clone();
        let start: Box<dyn FnOnce() -> tokio::task::AbortHandle + Send> = Box::new(move || {
            handle
                .spawn(async move {
                    let _ = tx.send(open().await.source);
                })
                .abort_handle()
        });
        Self::wrap(Box::new(Deferred {
            state: DeferredState::Waiting {
                handoff,
                start: Some(start),
                task: None,
            },
        }))
    }

    #[must_use]
    pub fn merge<K2: Kind>(self, other: Channel<T, K2>) -> Channel<T, <K as MergeKind<K2>>::Out>
    where
        K: MergeKind<K2>,
    {
        Channel::wrap(Box::new(MergeState::Both {
            left: self.source,
            right: other.source,
            next: Side::Left,
        }))
    }

    /// End this stream as soon as `cancel` fires, even while `recv` is
    /// blocked waiting for an item: the stream then reports
    /// [`End::Aborted { reason: AbortReason::Cancelled }`]. Items already buffered when it fires may still be
    /// delivered first, or not: the stream stops at the first wake-up that
    /// sees the cancel.
    ///
    /// A race, not a [`merge`](Self::merge): merged with a cancellation, a
    /// finite stream that closes would leave the merge waiting on the
    /// survivor — the cancellation — and never end. Here either side ending
    /// ends the stream, and a clean [`End::Complete`] keeps its verdict.
    #[must_use]
    pub fn until(self, cancel: &Cancellation) -> Channel<T, K> {
        let Some(rx) = &cancel.rx else {
            return self;
        };
        Channel::wrap(Box::new(Guarded {
            inner: self.source,
            alive: LeafState::Alive {
                rx: rx.clone(),
                close_rx: None,
                what: "cancellation",
            },
            fired: Arc::clone(&cancel.fired),
        }))
    }

    /// Transform and filter at pull time. `f` may hold state: a channel has
    /// one consumer, so it runs on one thread at a time.
    pub fn filter_map<U, F>(self, f: F) -> Channel<U, K>
    where
        U: Send + 'static,
        F: FnMut(T) -> Option<U> + Send + 'static,
    {
        Channel::wrap(Box::new(FilterMap {
            inner: self.source,
            f: Box::new(f),
        }))
    }

    /// End the stream at the first item for which `pred` returns `false`.
    ///
    /// The declined item is consumed and dropped, like `Iterator::take_while`.
    /// The stream then ends [`End::Complete`] and the inner source is dropped
    /// immediately, releasing whatever its closures captured. If the source
    /// ends first, its own verdict is preserved.
    ///
    /// This is a consumer-imposed bound, not a producer close. The channel does
    /// not distinguish "the bound was reached" from "the source closed early";
    /// that is a domain completeness question.
    pub fn take_while<F>(self, pred: F) -> Channel<T, Finite>
    where
        F: FnMut(&T) -> bool + Send + 'static,
    {
        Channel::wrap(Box::new(TakeWhile {
            inner: Some(self.source),
            pred: Box::new(pred),
            done: None,
        }))
    }

    /// End the stream at the first item for which `pred` returns `true`.
    ///
    /// The matching item is consumed and dropped, like
    /// `take_while(|item| !pred(item))`. The stream then ends
    /// [`End::Complete`]; a source abort is preserved.
    pub fn take_until<F>(self, mut pred: F) -> Channel<T, Finite>
    where
        F: FnMut(&T) -> bool + Send + 'static,
    {
        self.take_while(move |item| !pred(item))
    }

    /// At least `n` keyed items, then the rest of the last item's key group:
    /// the stream ends at the first item whose key differs once `n` is reached
    /// (that item is dropped, as in [`take_while`](Self::take_while)).
    ///
    /// `n` counts keyed items, not groups, so the last group may push the total
    /// past `n`; a group is never split. No-key items pass and do not count.
    #[must_use]
    pub fn take_at_least<Pos: Key<T>>(self, n: std::num::NonZeroUsize) -> Channel<T, Finite> {
        let mut seen: usize = 0;
        let mut finishing: Option<Pos> = None;
        self.take_while(move |item| {
            let Some(key) = Pos::key(item) else {
                return true;
            };
            if let Some(group) = finishing {
                return key == group;
            }
            seen = seen.saturating_add(1);
            if seen >= n.get() {
                finishing = Some(key);
            }
            true
        })
    }

    /// Drop an item when its key equals the previous item's key.
    ///
    /// No-key items pass through and do not change the last key. This is a
    /// stream transform, not an order policy; pair it with a contiguous policy
    /// when exact succession is required.
    #[must_use]
    pub fn dedup_by_key<Pos: Key<T>>(self) -> Channel<T, K> {
        Channel::wrap(Box::new(DedupByKey::<T, Pos> {
            inner: Some(self.source),
            last: None,
            done: None,
        }))
    }

    /// Drop items whose key is at or below `cut`; no-key items pass. `None`
    /// keeps everything.
    #[must_use]
    pub fn filter_above<Pos: Key<T>>(self, cut: Option<Pos>) -> Channel<T, K> {
        self.filter_map(move |item| match (Pos::key(&item), cut) {
            (Some(key), Some(cut)) if key <= cut => None,
            _ => Some(item),
        })
    }

    pub fn map<U, F>(self, mut f: F) -> Channel<U, K>
    where
        U: Send + 'static,
        F: FnMut(T) -> U + Send + 'static,
    {
        self.filter_map(move |t| Some(f(t)))
    }

    /// Block for the next item; `Err` is the stream's verdict once it ended.
    pub fn recv(&mut self) -> Result<T, K::End> {
        self.recv_end().map_err(K::end)
    }

    /// `recv` with the internal verdict, before the kind narrows it.
    pub(crate) fn recv_end(&mut self) -> Result<T, End> {
        loop {
            let leaf_count = self.source.leaf_count();
            if leaf_count == 0 {
                return Err(self.source.end_reason());
            }
            let mut sel = Select::new();
            let registered = self.source.register(&mut sel);
            debug_assert_eq!(
                registered, leaf_count,
                "register pushed {registered} ops but leaf_count() is {leaf_count}"
            );
            let idx = sel.ready();
            debug_assert!(
                idx < registered,
                "ready index {idx} exceeds {registered} registered operations"
            );
            match self.source.poll(idx) {
                Poll::Got(v) => return Ok(v),
                Poll::Filtered | Poll::Empty => continue,
                Poll::Dead => {
                    debug_assert_eq!(
                        self.source.leaf_count(),
                        0,
                        "a Dead source still counts leaves; recv would spin"
                    );
                    continue;
                }
            }
        }
    }

    pub fn try_recv(&mut self) -> Option<T> {
        self.source.drain_first()
    }

    /// Why the stream finished, for a stream already known to have stopped.
    /// `recv` and `ready_batch` return the same verdict in their `Err`.
    pub fn end_reason(&mut self) -> K::End {
        K::end(self.source.end_reason())
    }

    /// Block for the first item, then non-blocking drain up to `max` items.
    /// `Err` carries the verdict when the stream has ended, exactly as
    /// [`recv`](Self::recv) does; the returned iterator yields at least one
    /// item. Each subsequent `next()` calls `try_recv()`, so items that arrive
    /// while you iterate are still captured.
    pub fn ready_batch(&mut self, max: usize) -> Result<ReadyBatch<'_, T, K>, K::End> {
        let first = self.recv()?;
        Ok(ReadyBatch {
            channel: self,
            head: Some(first),
            remaining: max.saturating_sub(1),
        })
    }
}

pub struct ReadyBatch<'a, T, K> {
    channel: &'a mut Channel<T, K>,
    head: Option<T>,
    remaining: usize,
}

impl<T: Send + 'static, K: Kind> Iterator for ReadyBatch<'_, T, K> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.head.take().or_else(|| {
            if self.remaining == 0 {
                return None;
            }
            self.remaining = self.remaining.saturating_sub(1);
            self.channel.try_recv()
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self
            .remaining
            .saturating_add(if self.head.is_some() { 1 } else { 0 });
        (0, Some(n))
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
