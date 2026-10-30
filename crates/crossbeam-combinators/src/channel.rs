//! `Channels<T>` — the blocking read half of a pipeline.
//!
//! A `Channels<T>` owns a private tree of sources: a leaf (one crossbeam
//! receiver), a concurrent merge, a sequential chain, a guard raced against a
//! liveness signal or [`Cancellation`], a lazy `unfold`, and pull-time
//! `map`/`filter_map`. The tree is invisible through one blocking interface.
//!
//! `recv` blocks on `Select::ready` over all live leaves; `try_recv` drains
//! without blocking; `ready_batch` takes its first item through `recv` and the
//! rest through `try_recv`. Everything runs at pull time, on the consumer's
//! thread; the crate never spawns.
//!
//! ## Verdicts
//!
//! Disconnection means "no more items"; it does **not** mean "the producer
//! finished its work". The two are indistinguishable at the channel level:
//!
//! - A producer that panics closes its channel exactly like one that ran to
//!   completion.
//! - A task discarded before it runs never sends at all.
//! - An infinite stream (a socket tail, a subscription) can end because the
//!   peer went away, the connection reset, or the reader loop exited. The
//!   response is the same: resume from the last position.
//!
//! For **finite** streams the producer says so: [`Channels::from`] hands back a
//! [`Closer`], and [`recv`](Channels::recv) reports [`End::Closed`] only if
//! `close()` was actually called. A producer that panicked, was discarded, or
//! forgot yields [`End::Aborted`] instead — "I forgot" and "I died" are the
//! same case, deliberately. This is a local guarantee: a reader that exits on
//! EOF will close, whether the peer finished or crashed.
//!
//! A third verdict, [`End::Cancelled`], is a stop request: a [`Cancellation`]
//! bound with [`Channels::until`] fired. It exists because a tokio token cannot
//! wake a consumer parked in `recv`. Items may be missing, but nothing went
//! wrong; the response is to stop, not to back off and resume.
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
//! [`Channels::merge`] selects across two sources and drops a dead branch as
//! soon as it dies, releasing whatever its closures captured. [`Channels::chain`]
//! runs phases in order and condemns the chain on an abort. [`Channels::unfold`]
//! produces resumable segments lazily, handing each segment's verdict to the
//! caller. [`Channels::filter_map`], [`Channels::map`] and
//! [`Channels::until`] transform and bound a stream at pull time.
//!
//! The write half lives next door: [`crate::Feed`] for one producer and
//! [`crate::Fanout`]/[`crate::Wakes`] for many. The seam
//! ([`crate::Tail`], [`crate::Ranged`], [`crate::blend_chain`],
//! [`crate::blend_merge`]) composes a live tail with a bounded history.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::{Receiver, Select, TryRecvError};

#[cfg(feature = "tokio")]
use futures::StreamExt;
#[cfg(feature = "tokio")]
use futures::stream::BoxStream;

/// Buffer capacity for the `into_stream()` mpsc bridge.
#[cfg(feature = "tokio")]
const STREAM_BRIDGE_CAP: usize = 256;

/// Why a [`Channels`] stopped yielding items.
///
/// Go's `close(ch)`: a producer that finished says so, and silence is a fault.
/// The distinction only carries weight for **finite** streams (a bounded scan, a
/// snapshot read) — infinite ones (a socket tail, a subscription) never
/// terminate and get their correctness from resuming at the last position
/// instead.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum End {
    /// Every producer called [`Closer::close`] — the stream ended deliberately.
    Closed,
    /// A [`Closer`] was dropped without `close()`: the producer panicked, was
    /// discarded before it ran, or simply forgot. **Items may be missing.**
    Aborted { what: &'static str },
    /// The stream was stopped on request: the [`Cancellation`] it was bound to
    /// with [`Channels::until`] fired. **Items may be missing** — anything still
    /// buffered when it fired may or may not have been delivered — but nothing
    /// went wrong, so the response is to stop, not to back off and resume.
    Cancelled,
}

/// The producer's half of an explicit close. Handed out by
/// [`Channels::from`] so a construction site cannot forget it exists.
///
/// Dropping this without calling [`close`](Self::close) marks the stream
/// [`End::Aborted`] — that is the whole point, so the "I forgot" case and the
/// "I panicked" case land in the same place.
pub struct Closer(crossbeam_channel::Sender<()>);

impl Closer {
    /// Declare the stream complete. Everything the producer meant to send has
    /// been sent.
    ///
    /// **Close before the last `Sender` drops.** The consumer forms its verdict
    /// the moment the data channel disconnects, so if the sender goes first the
    /// consumer can read "disconnected, not yet closed" — an abort — for a stream
    /// that was about to close cleanly.
    ///
    /// Keep the `Closer` in the same scope as the sender and this is free: the
    /// sender drops at the end of that scope, after the `close()` that precedes
    /// it. Producers here are written with a single exit for that reason —
    /// `break` out to one unconditional `close()` rather than closing on each
    /// path. Do not move the sender into a nested `async` block or closure while
    /// closing outside it; the sender would then drop first.
    pub fn close(self) {
        let _ = self.0.try_send(());
    }
}

enum Poll<T> {
    Got(T),
    Filtered,
    Empty,
    Dead,
}

trait Source<T>: Send {
    /// Register this source's live leaves with `sel`, depth-first. The index
    /// `Select::ready` later returns is the leaf's position in that order, so
    /// `poll` can route by it directly — no path is stored.
    ///
    /// **Register exactly [`leaf_count()`](Source::leaf_count) operations, in
    /// that order.** `register` and `leaf_count` are one contract split over
    /// two methods: composite sources cut the ordinal space by the counts of
    /// their children (`MergeState::poll`, `Guarded::poll`), so a source that
    /// registers a different number than it counts misroutes every ordinal to
    /// its right — including in *sibling* subtrees it cannot see.
    ///
    /// Registering under a condition `leaf_count` does not also test is the
    /// way to break it. `Guarded` is the worked case: either side gone means
    /// nothing registers **and** the count is zero, tested by the same call.
    ///
    /// The failure does not look like a routing bug. A misrouted poll lands on
    /// a live-but-empty leaf, returns `Poll::Empty`, and `recv` loops — so the
    /// symptom is a hang or a spin, never a wrong value. (Reaching the wrong
    /// leaf is harmless in itself: every leaf feeds the same output stream.
    /// What breaks is finding *nothing* there, and mistaking whose death it
    /// was.)
    ///
    /// Returns the number of operations pushed. `recv` debug-asserts it equals
    /// [`leaf_count()`](Source::leaf_count); returning it makes that equality
    /// checkable rather than assumed.
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize;
    /// Poll the leaf at `ordinal`, the depth-first leaf index from `register`.
    fn poll(&mut self, ordinal: usize) -> Poll<T>;
    fn drain_first(&mut self) -> Option<T>;
    /// How many leaves [`register`](Source::register) will register — see there,
    /// the equality is load-bearing.
    ///
    /// It must also hold *steady* from `register` until the `poll` that
    /// follows it, because the splits are computed at poll time while the
    /// registration was built before `Select::ready` blocked. Nothing
    /// threatens that today — every mutating method takes `&mut self` and the
    /// consumer holds `&mut Channels`, so no leaf can die while `ready` is
    /// parked — but a source that mutated itself from elsewhere would break
    /// routing without touching this file.
    fn leaf_count(&self) -> usize;
    /// Why this source finished. Only meaningful once `leaf_count() == 0`.
    ///
    /// Deliberately separate from `leaf_count`, which is *arithmetic*
    /// (`MergeState::poll` uses it to route an ordinal, where an error has no
    /// meaningful handling). The check still happens where deadness is decided
    /// — at the leaf.
    fn end_reason(&mut self) -> End;
}

// ── Leaf: LeafState<T> ────────────────────────────────────────────────

/// The lifecycle of a single wrapped `Receiver`. `Alive` holds the receiver and
/// the consumer's half of the explicit close (or `None` for an
/// [`Channels::from_infinite`] stream — no closer may exist, so any termination
/// is [`End::Aborted`]). When the data channel disconnects, the verdict is
/// resolved from `close_rx` and the state moves to `Resolved` — or `Pending`
/// if the closer has not spoken yet (the non-memoisation safety net, see
/// `verdict_is_not_memoised_while_the_closer_is_still_alive`).
enum LeafState<T> {
    Alive {
        rx: Receiver<T>,
        close_rx: Option<crossbeam_channel::Receiver<()>>,
        what: &'static str,
    },
    /// Data channel dead, but the `Closer` is still outstanding. Holds
    /// `close_rx` so a late `close()` can still be honoured on the next
    /// `end_reason()` read. Fail-closed in the meantime: reports `Aborted`.
    Pending {
        close_rx: crossbeam_channel::Receiver<()>,
        what: &'static str,
    },
    Resolved(End),
}

impl<T: Send + 'static> Source<T> for LeafState<T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        if let LeafState::Alive { rx, .. } = self {
            sel.recv(rx);
            1
        } else {
            0
        }
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        debug_assert_eq!(ordinal, 0, "a leaf is always polled at relative ordinal 0");
        if ordinal != 0 {
            self.poison(ordinal);
            return Poll::Dead;
        }
        match self {
            LeafState::Alive { rx, close_rx, what } => match rx.try_recv() {
                Ok(v) => Poll::Got(v),
                Err(TryRecvError::Empty) => Poll::Empty,
                Err(TryRecvError::Disconnected) => {
                    *self = Self::resolve(close_rx.take(), what);
                    Poll::Dead
                }
            },
            _ => Poll::Dead,
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        match self {
            LeafState::Alive { rx, close_rx, what } => match rx.try_recv() {
                Ok(v) => Some(v),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => {
                    *self = Self::resolve(close_rx.take(), what);
                    None
                }
            },
            _ => None,
        }
    }
    fn leaf_count(&self) -> usize {
        match self {
            LeafState::Alive { .. } => 1,
            _ => 0,
        }
    }
    fn end_reason(&mut self) -> End {
        match self {
            LeafState::Resolved(end) => *end,
            LeafState::Pending { close_rx, what } => {
                let what = *what;
                match close_rx.try_recv() {
                    Ok(()) => {
                        *self = LeafState::Resolved(End::Closed);
                        End::Closed
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        *self = LeafState::Resolved(End::Aborted { what });
                        End::Aborted { what }
                    }
                    // Still outstanding: fail closed for this read, but do not
                    // commit — a late `close()` can still be honoured next time.
                    // This IS the non-memoisation the test guards.
                    Err(crossbeam_channel::TryRecvError::Empty) => End::Aborted { what },
                }
            }
            // Called before the leaf reported Dead (defensive / fail-closed).
            // The data channel may still hold items, so this does not drop `rx`
            // on the non-committal (`Empty`/infinite) paths; a definitive
            // `Ok`/`Closed` answer resolves and drops it (the verdict is final).
            LeafState::Alive { close_rx, what, .. } => {
                let what = *what;
                let (verdict, settle) = match close_rx.as_mut() {
                    Some(rx) => match rx.try_recv() {
                        Ok(()) => (End::Closed, true),
                        Err(crossbeam_channel::TryRecvError::Disconnected) => {
                            (End::Aborted { what }, true)
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => {
                            (End::Aborted { what }, false)
                        }
                    },
                    None => (End::Aborted { what }, false),
                };
                if settle {
                    *self = LeafState::Resolved(verdict);
                }
                verdict
            }
        }
    }
}

impl<T> LeafState<T> {
    /// Fail closed on a routing invariant violation: a nonzero ordinal means a
    /// caller's split arithmetic routed into this leaf by mistake. Resolving the
    /// leaf `Aborted` makes an enclosing `Merge`/`Chain` condemn the stream
    /// (`recv` returns `Err(End::Aborted)`) instead of hanging or serving a
    /// sibling's item.
    fn poison(&mut self, ordinal: usize) {
        let what = match self {
            LeafState::Alive { what, .. } | LeafState::Pending { what, .. } => *what,
            // Already resolved: nothing left to poison.
            LeafState::Resolved(_) => return,
        };
        tracing::error!(
            ordinal,
            producer = what,
            "leaf polled at a nonzero ordinal; routing invariant violated"
        );
        *self = LeafState::Resolved(End::Aborted { what });
    }

    /// Resolve the leaf after the data channel disconnects. A `None` `close_rx`
    /// is an infinite stream — ending is always an abort.
    fn resolve(
        close_rx: Option<crossbeam_channel::Receiver<()>>,
        what: &'static str,
    ) -> LeafState<T> {
        match close_rx {
            Some(rx) => match rx.try_recv() {
                Ok(()) => LeafState::Resolved(End::Closed),
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    LeafState::Resolved(End::Aborted { what })
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    LeafState::Pending { close_rx: rx, what }
                }
            },
            None => LeafState::Resolved(End::Aborted { what }),
        }
    }
}

// ── Merge<T> ───────────────────────────────────────────────────────────

/// Which branch a `MergeState::Both` should drain from next. Alternation keeps
/// `drain_first` fair: `ready_batch` takes only its head through the fair
/// `recv`, and every subsequent item through `drain_first`, so a fixed bias
/// would drain one branch entirely before touching the other.
#[derive(Clone, Copy)]
enum Side {
    Left,
    Right,
}

/// Concurrent select across two sources.
///
/// **Dropping a dead branch is load-bearing, not an optimisation.** The moment
/// a branch reports `Dead` it is dropped — `Both` collapses to `One` (or
/// straight to `Done` on an abort). Two invariants depend on it:
///
/// 1. A source's closures may own the `Sender` for the *other* branch — a
///    request/response merge where the request filter holds the shared
///    completion sender. Only dropping the exhausted branch releases that
///    sender, letting the merged output disconnect once the last in-flight
///    request lands. Merely *pruning it from `register`* leaves the sender
///    alive and the merged output blocks in `recv()` forever.
/// 2. `Dead` is reported only when **both** branches are gone; a half-dead
///    merge reports `Filtered` so the caller retries. Returning `Dead` while
///    the survivor still holds buffered items makes an enclosing [`ChainState`]
///    pop the whole phase and silently lose them.
///
/// An `Aborted` branch **condemns the merge immediately**: the survivor is
/// discarded and the merge reports `Done(Aborted)`. This is consistent with the
/// module contract ("items may be missing" for `Aborted`) and releases any
/// captured resources (including the sibling's `Sender`) sooner than draining
/// the survivor first would.
enum MergeState<T> {
    Both {
        left: Box<dyn Source<T>>,
        right: Box<dyn Source<T>>,
        next: Side,
    },
    One {
        survivor: Box<dyn Source<T>>,
    },
    Done(End),
}

impl<T: Send + 'static> Source<T> for MergeState<T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        match self {
            // Registration order (left then right) defines the ordinals, so it
            // must match `poll`'s `left.leaf_count()` split exactly.
            MergeState::Both { left, right, .. } => {
                let left_count = left.register(sel);
                let right_count = right.register(sel);
                left_count.saturating_add(right_count)
            }
            // The survivor's leaves register in their own order and `poll`
            // passes the ordinal straight through, so its original side is moot.
            MergeState::One { survivor } => survivor.register(sel),
            MergeState::Done(_) => 0,
        }
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        // Take ownership so branches can be moved between variants.
        let owned = std::mem::replace(self, MergeState::Done(End::Closed));
        let (next, poll) = match owned {
            MergeState::Both {
                mut left,
                mut right,
                next,
            } => {
                let left_count = left.leaf_count();
                // Leaf counts cannot overflow a `usize`; `saturating_add` just
                // satisfies the no-bare-arithmetic rule.
                let total = left_count.saturating_add(right.leaf_count());
                debug_assert!(ordinal < total, "ordinal {ordinal} out of range {total}");
                if ordinal < left_count {
                    match left.poll(ordinal) {
                        Poll::Dead => {
                            let reason = left.end_reason();
                            match reason {
                                // Drop the dead branch; keep the survivor. `left`
                                // is dropped at end of arm, releasing its captures.
                                End::Closed => {
                                    (MergeState::One { survivor: right }, Poll::Filtered)
                                }
                                // Abort condemns the merge: discard the survivor.
                                aborted @ (End::Aborted { .. } | End::Cancelled) => {
                                    (MergeState::Done(aborted), Poll::Dead)
                                }
                            }
                        }
                        other => (MergeState::Both { left, right, next }, other),
                    }
                } else if ordinal < total {
                    // This branch guarantees `ordinal >= left_count`, so the
                    // subtraction cannot underflow.
                    match right.poll(ordinal.saturating_sub(left_count)) {
                        Poll::Dead => {
                            let reason = right.end_reason();
                            match reason {
                                End::Closed => (MergeState::One { survivor: left }, Poll::Filtered),
                                aborted @ (End::Aborted { .. } | End::Cancelled) => {
                                    (MergeState::Done(aborted), Poll::Dead)
                                }
                            }
                        }
                        other => (MergeState::Both { left, right, next }, other),
                    }
                } else {
                    // Defensive: `ready` never yields an out-of-range index.
                    (MergeState::Both { left, right, next }, Poll::Filtered)
                }
            }
            // No split here, so the release path does no bound-check: `One`
            // registered the survivor's leaves in their own order and hands the
            // ordinal straight back, exactly as `ChainState` and `FilterMap`
            // do. Debug builds still assert it is in range, so a misroute fails
            // at the node instead of hanging. This used to answer an
            // out-of-range ordinal with `Poll::Dead`, which was the worst
            // available guess — see invariant 2 above: `Dead` while the
            // survivor still holds buffered items makes an enclosing
            // `ChainState` pop the phase and lose them.
            MergeState::One { mut survivor } => {
                debug_assert!(
                    ordinal < survivor.leaf_count(),
                    "ordinal {ordinal} out of range for survivor with {} leaves",
                    survivor.leaf_count()
                );
                match survivor.poll(ordinal) {
                    Poll::Dead => {
                        let reason = survivor.end_reason();
                        (MergeState::Done(reason), Poll::Dead)
                    }
                    other => (MergeState::One { survivor }, other),
                }
            }
            done @ MergeState::Done(_) => (done, Poll::Dead),
        };
        *self = next;
        poll
    }
    fn drain_first(&mut self) -> Option<T> {
        let owned = std::mem::replace(self, MergeState::Done(End::Closed));
        match owned {
            MergeState::Both { left, right, next } => {
                // Alternate the start side for fairness. `first`/`second`
                // own the boxes for this iteration; reconstruct `Both` from
                // them (re-ordered back to left/right) on the non-terminal
                // paths.
                let prefer_right = matches!(next, Side::Right);
                let (mut first, mut second) = if prefer_right {
                    (right, left)
                } else {
                    (left, right)
                };
                let flipped = if prefer_right {
                    Side::Left
                } else {
                    Side::Right
                };
                // Rebuild `Both` from `first`/`second` in canonical order.
                let rebuild = |f: Box<dyn Source<T>>, s: Box<dyn Source<T>>| {
                    let (l, r) = if prefer_right { (s, f) } else { (f, s) };
                    MergeState::Both {
                        left: l,
                        right: r,
                        next: flipped,
                    }
                };
                match first.drain_first() {
                    Some(v) => {
                        *self = rebuild(first, second);
                        Some(v)
                    }
                    None => {
                        if first.leaf_count() == 0 {
                            let reason = first.end_reason();
                            match reason {
                                End::Closed => {
                                    drop(first);
                                    *self = MergeState::One { survivor: second };
                                    None
                                }
                                aborted @ (End::Aborted { .. } | End::Cancelled) => {
                                    drop(first);
                                    drop(second);
                                    *self = MergeState::Done(aborted);
                                    None
                                }
                            }
                        } else {
                            // first alive but empty: try the sibling.
                            match second.drain_first() {
                                Some(v) => {
                                    *self = rebuild(first, second);
                                    Some(v)
                                }
                                None => {
                                    if second.leaf_count() == 0 {
                                        let reason = second.end_reason();
                                        match reason {
                                            End::Closed => {
                                                drop(second);
                                                *self = MergeState::One { survivor: first };
                                                None
                                            }
                                            aborted @ (End::Aborted { .. } | End::Cancelled) => {
                                                drop(first);
                                                drop(second);
                                                *self = MergeState::Done(aborted);
                                                None
                                            }
                                        }
                                    } else {
                                        // Both alive, both empty.
                                        *self = rebuild(first, second);
                                        None
                                    }
                                }
                            }
                        }
                    }
                }
            }
            MergeState::One { mut survivor } => match survivor.drain_first() {
                Some(v) => {
                    *self = MergeState::One { survivor };
                    Some(v)
                }
                None => {
                    if survivor.leaf_count() == 0 {
                        let reason = survivor.end_reason();
                        *self = MergeState::Done(reason);
                        None
                    } else {
                        *self = MergeState::One { survivor };
                        None
                    }
                }
            },
            done @ MergeState::Done(_) => {
                *self = done;
                None
            }
        }
    }
    fn leaf_count(&self) -> usize {
        match self {
            MergeState::Both { left, right, .. } => {
                left.leaf_count().saturating_add(right.leaf_count())
            }
            MergeState::One { survivor } => survivor.leaf_count(),
            MergeState::Done(_) => 0,
        }
    }
    fn end_reason(&mut self) -> End {
        match self {
            MergeState::Done(end) => *end,
            MergeState::One { survivor } => survivor.end_reason(),
            // Both still alive: no verdict to report. Closed is the identity.
            MergeState::Both { .. } => End::Closed,
        }
    }
}

// ── Chain<T> ───────────────────────────────────────────────────────────

/// Sequential first-then-second. An aborted phase **condemns the chain
/// immediately** — remaining phases are discarded and the chain reports
/// `Done(Aborted)`. This stops a truncated historical replay from laundering
/// itself behind live items that would make the stream look complete.
enum ChainState<T> {
    Active {
        phases: VecDeque<Box<dyn Source<T>>>,
    },
    Done(End),
}

impl<T: Send + 'static> Source<T> for ChainState<T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        match self {
            ChainState::Active { phases } => match phases.front_mut() {
                Some(front) => front.register(sel),
                None => 0,
            },
            ChainState::Done(_) => 0,
        }
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        match self {
            ChainState::Active { phases } => match phases.front_mut() {
                None => {
                    *self = ChainState::Done(End::Closed);
                    Poll::Dead
                }
                Some(front) => {
                    debug_assert!(
                        ordinal < front.leaf_count(),
                        "ordinal {ordinal} out of range for chain front with {} leaves",
                        front.leaf_count()
                    );
                    match front.poll(ordinal) {
                        Poll::Dead => {
                            let reason = front.end_reason();
                            phases.pop_front();
                            match reason {
                                End::Closed if phases.is_empty() => {
                                    *self = ChainState::Done(End::Closed);
                                    Poll::Dead
                                }
                                End::Closed => Poll::Filtered,
                                aborted @ (End::Aborted { .. } | End::Cancelled) => {
                                    *self = ChainState::Done(aborted);
                                    Poll::Dead
                                }
                            }
                        }
                        other => other,
                    }
                }
            },
            ChainState::Done(_) => Poll::Dead,
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        loop {
            return match self {
                ChainState::Active { phases } => {
                    let front = match phases.front_mut() {
                        Some(f) => f,
                        None => {
                            *self = ChainState::Done(End::Closed);
                            return None;
                        }
                    };
                    match front.drain_first() {
                        Some(v) => Some(v),
                        None => {
                            if front.leaf_count() == 0 {
                                let reason = front.end_reason();
                                phases.pop_front();
                                match reason {
                                    End::Closed if phases.is_empty() => {
                                        *self = ChainState::Done(End::Closed);
                                        return None;
                                    }
                                    End::Closed => continue,
                                    aborted @ (End::Aborted { .. } | End::Cancelled) => {
                                        *self = ChainState::Done(aborted);
                                        return None;
                                    }
                                }
                            }
                            None
                        }
                    }
                }
                ChainState::Done(_) => None,
            };
        }
    }
    fn leaf_count(&self) -> usize {
        match self {
            ChainState::Active { phases } => phases.front().map(|f| f.leaf_count()).unwrap_or(0),
            ChainState::Done(_) => 0,
        }
    }
    fn end_reason(&mut self) -> End {
        match self {
            ChainState::Done(end) => *end,
            ChainState::Active { phases } => match phases.front_mut() {
                Some(front) => front.end_reason(),
                None => End::Closed,
            },
        }
    }
}

// ── Guarded<T> ─────────────────────────────────────────────────────────

/// An inner source raced against a liveness receiver: whichever ends first ends
/// the stream.
///
/// Not [`MergeState`], which keeps the survivor when one branch closes cleanly
/// — that would make a finite source never terminate. Not [`ChainState`]
/// either, which is sequential. This is "drain the inner source, but only for
/// as long as anyone is still listening", and the guard carries no items: its
/// only event is disconnecting.
///
/// Used by [`Channels::into_stream`], where the guard's sender rides with the
/// returned stream, so the drain thread stops the moment the consumer drops it
/// rather than parking until an item that may never come.
///
/// Also [`Channels::until`], where the guard is a [`Cancellation`] and `fired`
/// is `Some`. Its disconnect ends the stream the same way — whatever holds the
/// stream may be what keeps its producer alive, so the guard is the one thing
/// that can be relied on to wake the consumer — and `fired` only picks the
/// verdict: [`End::Cancelled`] for a cancel, the guard leaf's `Aborted` for a
/// [`CancelHandle`] dropped without cancelling.
struct Guarded<T> {
    inner: Box<dyn Source<T>>,
    alive: LeafState<()>,
    /// Set before the guard's sender is dropped by a deliberate cancel. `None`
    /// for a liveness guard.
    fired: Option<Arc<AtomicBool>>,
}

impl<T> Guarded<T> {
    fn fired(&self) -> bool {
        self.fired
            .as_ref()
            .is_some_and(|fired| fired.load(Ordering::SeqCst))
    }
}

impl<T: Send + 'static> Source<T> for Guarded<T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        // Either side finishing finishes the source, so once one is gone
        // nothing is registered and `recv` falls through to `end_reason`.
        // Registering the survivor instead would block forever: on the guard
        // after a finite source completed, or on the source after the consumer
        // left — the two bugs this type exists to prevent.
        if self.leaf_count() == 0 {
            return 0;
        }
        // Registration order (inner then guard) defines the ordinals, as in
        // MergeState; the guard's leaf gets the highest one.
        let inner = self.inner.register(sel);
        let alive = self.alive.register(sel);
        inner.saturating_add(alive)
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        debug_assert!(
            ordinal < self.leaf_count(),
            "ordinal {ordinal} out of range for guarded source with {} leaves",
            self.leaf_count()
        );
        if ordinal < self.inner.leaf_count() {
            self.inner.poll(ordinal)
        } else {
            debug_assert_eq!(
                ordinal,
                self.inner.leaf_count(),
                "only the guard leaf lives above the inner source"
            );
            match self.alive.poll(0) {
                // The guard never carries items, so the only thing that can
                // happen to it is dying — and that ends the stream.
                Poll::Dead => Poll::Dead,
                _ => Poll::Filtered,
            }
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        if self.leaf_count() == 0 {
            return None;
        }
        self.inner.drain_first()
    }
    /// Zero as soon as *either* side is gone — the count must match what
    /// `register` actually registers, since an enclosing source derives its
    /// ordinal split from it.
    fn leaf_count(&self) -> usize {
        let (inner, alive) = (self.inner.leaf_count(), self.alive.leaf_count());
        if inner == 0 || alive == 0 {
            return 0;
        }
        inner.saturating_add(alive)
    }
    fn end_reason(&mut self) -> End {
        // A declared completion stands whatever happened afterwards: the
        // producer closed and every item it sent was drained, so the stream is
        // whole.
        if self.inner.leaf_count() == 0 && self.inner.end_reason() == End::Closed {
            return End::Closed;
        }
        // Read the flag, not the guard leaf: the leaf learns of the disconnect
        // only when polled, and the select may have woken on the inner side
        // first. `fired` is set before anything the cancel tears down, so an
        // inner end seen once it is set is the cancel's doing.
        if self.fired() {
            return End::Cancelled;
        }
        if self.alive.leaf_count() == 0 {
            // The consumer left, or a cancel handle went away unfired.
            return self.alive.end_reason();
        }
        self.inner.end_reason()
    }
}

// ── Cancellation ───────────────────────────────────────────────────────

/// A cancellation that can end a blocked [`Channels::recv`].
///
/// A tokio [`CancellationToken`](tokio_util::sync::CancellationToken) cannot
/// wake a crossbeam `Select`, but a disconnecting receiver can, and every clone
/// of a crossbeam receiver sees the disconnect. So one [`CancelHandle`] owns
/// the only sender and every bound stream holds a receiver clone: firing is one
/// drop, however many streams are bound, and no per-stream task exists to
/// leak when a consumer walks away early.
///
/// The disconnect always ends the bound streams at once — that is the point:
/// whatever holds a stream may be what keeps its producer alive, so nothing but
/// the guard can be relied on to wake a parked consumer. `fired` only decides
/// the verdict. It is set *before* the sender drops, so a bound stream that
/// ends once it is set reports [`End::Cancelled`]. A handle dropped without
/// [`CancelHandle::cancel`] ends them `Aborted` instead: that is teardown (the
/// runtime running [`from_token`](Self::from_token)'s task shut down), not a
/// request to stop. An owner whose streams should outlive it — and deliver
/// what they hold — keeps its handle alive until it means to cancel.
#[derive(Clone)]
pub struct Cancellation {
    /// `None` for [`never`](Self::never): nothing to race against.
    rx: Option<Receiver<()>>,
    fired: Arc<AtomicBool>,
}

/// The firing half of a [`Cancellation`].
pub struct CancelHandle {
    /// `Mutex` only so `cancel` can take `&self`; never held across an
    /// `.await` (§4).
    tx: std::sync::Mutex<Option<crossbeam_channel::Sender<()>>>,
    fired: Arc<AtomicBool>,
}

impl CancelHandle {
    /// A fresh, unfired cancellation and the handle that fires it.
    #[must_use]
    pub fn new() -> (Self, Cancellation) {
        let (tx, rx) = crossbeam_channel::bounded::<()>(0);
        let fired = Arc::new(AtomicBool::new(false));
        (
            Self {
                tx: std::sync::Mutex::new(Some(tx)),
                fired: Arc::clone(&fired),
            },
            Cancellation {
                rx: Some(rx),
                fired,
            },
        )
    }

    /// Fire: every stream bound to this cancellation ends with
    /// [`End::Cancelled`]. Idempotent, and synchronous — a consumer woken after
    /// this returns sees the cancel, which is what lets a caller order it
    /// before tearing down whatever feeds those streams.
    pub fn cancel(&self) {
        // Before the drop: a guard reads the flag only after observing the
        // disconnect, so it must already be set by then.
        self.fired.store(true, Ordering::SeqCst);
        drop(
            self.tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
    }
}

impl Cancellation {
    /// A cancellation that fires when `token` is cancelled.
    ///
    /// Spawns one task, which lives until the token fires, so call it once per
    /// token and clone the result rather than once per stream. Must be called
    /// from within a Tokio runtime.
    #[cfg(feature = "tokio")]
    #[must_use]
    pub fn from_token(token: &tokio_util::sync::CancellationToken) -> Self {
        let (handle, cancellation) = CancelHandle::new();
        let token = token.clone();
        tokio::spawn(async move {
            token.cancelled().await;
            handle.cancel();
        });
        cancellation
    }

    /// A cancellation that never fires: [`Channels::until`] with it is a
    /// no-op. For a caller with nothing to cancel on.
    #[must_use]
    pub fn never() -> Self {
        Self {
            rx: None,
            fired: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether [`CancelHandle::cancel`] has run.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}

// ── Unfold<S, T> ───────────────────────────────────────────────────────

/// [`ChainState`]'s unbounded, lazy sibling: when the current segment ends,
/// `next` produces the following one from the running state.
///
/// The difference that matters is the verdict. `ChainState` lets an aborted
/// phase condemn the whole chain, so a truncated replay cannot launder itself
/// behind later items. `Unfold` cannot make that call on the caller's behalf —
/// for some streams an ending segment IS the control signal — so it **hands the
/// previous segment's [`End`] to `next`** and lets the caller decide: continue,
/// or decline by returning `None`. A resuming consumer whose fan-out hangs up
/// on an overrun subscriber uses that verdict as the cue to replay from its
/// durable cursor and resubscribe.
///
/// A segment is materialised eagerly — at construction, and again the instant
/// its predecessor dies — never lazily inside `register`, and never while a
/// registration is outstanding. An enclosing [`MergeState`] splits the ordinal
/// space by `leaf_count()` *at poll time*, against a registration built before
/// `Select::ready` blocked, so the count has to hold across that whole window,
/// not merely during registration. A segment swapped in mid-window would
/// misalign every index to its right. `Unfold` is the one source whose leaf
/// count is meant to change, which is why the rule is written down here.
struct Unfold<S, T> {
    state: S,
    #[allow(clippy::type_complexity, reason = "one boxed closure, named inline")]
    next: Box<dyn FnMut(&mut S, Option<End>) -> Option<Channels<T>> + Send>,
    current: Option<Box<dyn Source<T>>>,
    /// The end of the segment before `current`, reported once by `end_reason`
    /// when `next` finally declines.
    last_end: Option<End>,
}

impl<S: Send + 'static, T: Send + 'static> Unfold<S, T> {
    /// Pull the next segment. Idempotent once `next` has declined: it is only
    /// called while `current` is `None`, and a declining `next` is asked again
    /// on the following pull — a caller that wants to stay finished says so by
    /// declining again.
    fn advance(&mut self) {
        self.current = (self.next)(&mut self.state, self.last_end).map(|ch| ch.source);
    }
}

impl<S: Send + 'static, T: Send + 'static> Source<T> for Unfold<S, T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        self.current.as_mut().map_or(0, |src| src.register(sel))
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        match &mut self.current {
            Some(src) => {
                debug_assert!(
                    ordinal < src.leaf_count(),
                    "ordinal {ordinal} out of range for unfold segment with {} leaves",
                    src.leaf_count()
                );
                match src.poll(ordinal) {
                    Poll::Dead => {
                        self.last_end = Some(src.end_reason());
                        self.current = None;
                        self.advance();
                        // Retry rather than Dead: the new segment (if any) has
                        // not been registered yet, and an enclosing Chain/Merge
                        // must not pop this source while it can still produce.
                        match self.current {
                            Some(_) => Poll::Filtered,
                            None => Poll::Dead,
                        }
                    }
                    other => other,
                }
            }
            None => Poll::Dead,
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        loop {
            let src = self.current.as_mut()?;
            if let Some(v) = src.drain_first() {
                return Some(v);
            }
            if src.leaf_count() > 0 {
                return None; // alive, just empty
            }
            self.last_end = Some(src.end_reason());
            self.current = None;
            self.advance();
        }
    }
    fn leaf_count(&self) -> usize {
        self.current.as_ref().map_or(0, |src| src.leaf_count())
    }
    fn end_reason(&mut self) -> End {
        match &mut self.current {
            Some(src) => src.end_reason(),
            // `next` declined: the last segment's own verdict is the stream's.
            None => self.last_end.unwrap_or(End::Closed),
        }
    }
}

// ── FilterMap<S, T> ────────────────────────────────────────────────────

struct FilterMap<S, T> {
    inner: Box<dyn Source<S>>,
    f: Arc<dyn Fn(S) -> Option<T> + Send + Sync>,
}

impl<S: Send + 'static, T: Send + 'static> Source<T> for FilterMap<S, T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        self.inner.register(sel)
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        debug_assert!(
            ordinal < self.inner.leaf_count(),
            "ordinal {ordinal} out of range for filter_map inner with {} leaves",
            self.inner.leaf_count()
        );
        match self.inner.poll(ordinal) {
            Poll::Got(s) => match (self.f)(s) {
                Some(t) => Poll::Got(t),
                None => Poll::Filtered,
            },
            Poll::Filtered => Poll::Filtered,
            Poll::Empty => Poll::Empty,
            Poll::Dead => Poll::Dead,
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        loop {
            let s = self.inner.drain_first()?;
            if let Some(t) = (self.f)(s) {
                return Some(t);
            }
        }
    }
    fn leaf_count(&self) -> usize {
        self.inner.leaf_count()
    }
    fn end_reason(&mut self) -> End {
        self.inner.end_reason()
    }
}

// ── Channels<T> ────────────────────────────────────────────────────────

pub struct Channels<T> {
    source: Box<dyn Source<T>>,
}

impl<T: Send + 'static> Channels<T> {
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
            Self {
                source: Box::new(LeafState::Alive {
                    rx,
                    close_rx: Some(close_rx),
                    what,
                }),
            },
            Closer(close_tx),
        )
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
        Self {
            source: Box::new(LeafState::Alive {
                rx,
                close_rx: None,
                what,
            }),
        }
    }

    /// A receiver whose producer has already declared completion.
    ///
    /// Useful when the items are already queued and the producer is gone; the
    /// stream reports [`End::Closed`] after draining. For a producer that is
    /// still running, use [`Channels::from`] and keep its [`Closer`].
    #[must_use]
    pub fn closed(rx: Receiver<T>) -> Self {
        let (ch, closer) = Self::from(rx, "closed");
        closer.close();
        ch
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
        let (ch, closer) = Channels::from(rx, "from_vec");
        closer.close();
        ch
    }

    /// Segments produced lazily and without bound: each time the current one
    /// ends, `next` is handed the running state and that segment's [`End`], and
    /// returns the following segment — or `None` to finish the stream.
    ///
    /// This is [`chain`](Self::chain) generalised. The verdict is the reason it
    /// is a separate combinator rather than a loop over `chain`: an aborted
    /// segment condemns a chain, whereas here `next` decides, because for some
    /// streams a segment ending IS the signal to build the next one.
    pub fn unfold<S, F>(state: S, next: F) -> Channels<T>
    where
        S: Send + 'static,
        F: FnMut(&mut S, Option<End>) -> Option<Channels<T>> + Send + 'static,
    {
        let mut unfold = Unfold {
            state,
            next: Box::new(next),
            current: None,
            last_end: None,
        };
        // Eagerly, so `leaf_count()` is accurate before the first `register`.
        unfold.advance();
        Channels {
            source: Box::new(unfold),
        }
    }

    #[must_use]
    pub fn merge(self, other: Channels<T>) -> Channels<T> {
        Channels {
            source: Box::new(MergeState::Both {
                left: self.source,
                right: other.source,
                next: Side::Left,
            }),
        }
    }

    /// End this stream as soon as `cancel` fires, even while `recv` is
    /// blocked waiting for an item: the stream then reports
    /// [`End::Cancelled`]. Items already buffered when it fires may still be
    /// delivered first, or not: the stream stops at the first wake-up that
    /// sees the cancel.
    ///
    /// A race, not a [`merge`](Self::merge): merged with a cancellation, a
    /// finite stream that closes would leave the merge waiting on the
    /// survivor — the cancellation — and never end. Here either side ending
    /// ends the stream, and a clean [`End::Closed`] keeps its verdict.
    #[must_use]
    pub fn until(self, cancel: &Cancellation) -> Channels<T> {
        let Some(rx) = &cancel.rx else {
            return self;
        };
        Channels {
            source: Box::new(Guarded {
                inner: self.source,
                alive: LeafState::Alive {
                    rx: rx.clone(),
                    close_rx: None,
                    what: "cancellation",
                },
                fired: Some(Arc::clone(&cancel.fired)),
            }),
        }
    }

    #[must_use]
    pub fn chain(self, next: Channels<T>) -> Channels<T> {
        let mut phases = VecDeque::new();
        phases.push_back(self.source);
        phases.push_back(next.source);
        Channels {
            source: Box::new(ChainState::Active { phases }),
        }
    }

    pub fn filter_map<U, F>(self, f: F) -> Channels<U>
    where
        U: Send + 'static,
        F: Fn(T) -> Option<U> + Send + Sync + 'static,
    {
        Channels {
            source: Box::new(FilterMap {
                inner: self.source,
                f: Arc::new(f),
            }),
        }
    }

    pub fn map<U, F>(self, f: F) -> Channels<U>
    where
        U: Send + 'static,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        self.filter_map(move |t| Some(f(t)))
    }

    pub fn recv(&mut self) -> Result<T, End> {
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

    /// Why the stream finished. Meaningful once it has stopped yielding — i.e.
    /// after `recv` returned `Err`, or after a `ready_batch` drain loop broke.
    /// `ready_batch` returns `None` for both a clean close and an abort, so a
    /// loop that needs the distinction consults this afterwards.
    pub fn end_reason(&mut self) -> End {
        self.source.end_reason()
    }

    /// Block for the first item, then non-blocking drain up to `max` items.
    /// Returns `None` when the channel is disconnected; the returned iterator
    /// yields at least one item. Each subsequent `next()` calls `try_recv()`,
    /// so items that arrive while you iterate are still captured.
    pub fn ready_batch(&mut self, max: usize) -> Option<ReadyBatch<'_, T>> {
        let first = self.recv().ok()?;
        Some(ReadyBatch {
            channel: self,
            head: Some(first),
            remaining: max.saturating_sub(1),
        })
    }

    /// Bridge to an async `Stream`, draining on a blocking thread.
    ///
    /// The drain is woken by *either* an item or the consumer going away. The
    /// second half is not a nicety: a failing `blocking_send` is the only other
    /// way this loop learns the stream was dropped, and on an infinite source
    /// that has gone quiet no further item ever arrives to make one fail. The
    /// thread would park in `recv` forever — and `Runtime::drop` waits for
    /// started blocking tasks, so one dropped tail subscription would hang
    /// shutdown.
    ///
    /// So the source is raced against a liveness receiver (a guard): it
    /// carries no items, its sender rides with the returned stream, and
    /// dropping the stream disconnects it. A race, not a
    /// [`merge`](Self::merge) — a merge keeps the surviving branch when the
    /// other closes cleanly, which would stop a *finite* stream from ever
    /// ending.
    #[cfg(feature = "tokio")]
    #[must_use = "a stream does nothing unless polled"]
    pub fn into_stream(self) -> BoxStream<'static, T> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BRIDGE_CAP);
        let (alive_tx, alive_rx) = crossbeam_channel::bounded::<()>(1);
        let source = Channels {
            source: Box::new(Guarded {
                inner: self.source,
                alive: LeafState::Alive {
                    rx: alive_rx,
                    close_rx: None,
                    what: "into_stream consumer",
                },
                fired: None,
            }),
        };
        tokio::task::spawn_blocking(move || {
            for item in source {
                if tx.blocking_send(item).is_err() {
                    break;
                }
            }
        });
        futures::stream::unfold(
            (tokio_stream::wrappers::ReceiverStream::new(rx), alive_tx),
            |(mut rx, alive)| async move {
                // `alive` is carried in the state, so it is released exactly
                // when this stream is dropped — never sooner.
                rx.next().await.map(|item| (item, (rx, alive)))
            },
        )
        .boxed()
    }

    #[cfg(test)]
    pub(crate) fn drain_into(self, tx: &crossbeam_channel::Sender<T>) -> bool {
        for item in self {
            if tx.send(item).is_err() {
                return false;
            }
        }
        true
    }
}

pub struct ReadyBatch<'a, T> {
    channel: &'a mut Channels<T>,
    head: Option<T>,
    remaining: usize,
}

impl<T: Send + 'static> Iterator for ReadyBatch<'_, T> {
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

// ── ChannelIter<T> ─────────────────────────────────────────────────────

pub struct ChannelIter<T> {
    inner: Channels<T>,
}

impl<T: Send + 'static> Iterator for ChannelIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.inner.recv().ok()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl<T: Send + 'static> IntoIterator for Channels<T> {
    type Item = T;
    type IntoIter = ChannelIter<T>;

    fn into_iter(self) -> ChannelIter<T> {
        ChannelIter { inner: self }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Most tests care about item flow, not the verdict: wrap a receiver in a
    /// channel whose producer has already declared completion.
    fn closed<T: Send + 'static>(rx: Receiver<T>) -> Channels<T> {
        let (ch, closer) = Channels::from(rx, "test");
        closer.close();
        ch
    }
    #[test]
    fn from_and_recv_roundtrip() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let mut ch = closed(rx);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        drop(tx);
        assert_eq!(ch.recv(), Err(End::Closed));
    }

    /// `unfold` is `chain`'s unbounded sibling: segments are produced lazily,
    /// one at a time, until `next` declines.
    #[test]
    fn unfold_produces_segments_until_next_declines() {
        let mut ch = Channels::unfold(0u32, |n, _prev| {
            (*n < 3).then(|| {
                *n = n.saturating_add(1);
                Channels::from_vec(vec![*n * 10, *n * 10 + 1])
            })
        });
        let got: Vec<u32> = std::iter::from_fn(|| ch.recv().ok()).collect();
        assert_eq!(got, vec![10, 11, 20, 21, 30, 31]);
    }

    /// The load-bearing difference from `chain`, which an aborted phase
    /// condemns: `unfold` hands the verdict to `next` and lets it decide. A
    /// resuming consumer (a stable tail, where a hung-up subscription IS the
    /// lag signal) continues; anything else can decline.
    #[test]
    fn unfold_hands_the_previous_end_to_next_and_may_continue_past_an_abort() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let s = seen.clone();
        let mut ch = Channels::unfold(0u32, move |n, prev| {
            s.lock().unwrap().push(prev);
            *n = n.saturating_add(1);
            match *n {
                // An infinite segment whose sender drops: End::Aborted.
                1 => {
                    let (tx, rx) = crossbeam_channel::bounded(4);
                    tx.send(7).unwrap();
                    drop(tx);
                    Some(Channels::from_infinite(rx, "seg"))
                }
                2 => Some(Channels::from_vec(vec![8])),
                _ => None,
            }
        });
        let got: Vec<u32> = std::iter::from_fn(|| ch.recv().ok()).collect();
        assert_eq!(
            got,
            vec![7, 8],
            "an aborted segment does not condemn the unfold"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            vec![None, Some(End::Aborted { what: "seg" }), Some(End::Closed)],
            "next sees no verdict first, then each segment's own"
        );
    }

    /// `into_stream`'s bridge drains on a blocking thread, so it only notices a
    /// departed consumer when a send fails — which never happens on an infinite
    /// stream that has gone quiet. The thread then parks forever in `recv`, and
    /// `Runtime::drop` waits for started blocking tasks, so a single dropped
    /// tail subscription hangs shutdown. The stream must therefore carry a
    /// liveness signal the drain can select on.
    ///
    /// Nothing is ever sent here: the sender stays alive and idle, so the only
    /// thing that can end the drain is the consumer going away. The guard
    /// travels with the source, so it is released exactly when the drain
    /// thread drops it — i.e. when the thread ends.
    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn into_stream_stops_draining_when_the_consumer_drops_it() {
        let guard = Arc::new(());
        let alive = Arc::downgrade(&guard);
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);

        let stream = Channels::from_infinite(rx, "socket")
            .map(move |x| {
                let _held = &guard;
                x
            })
            .into_stream();
        drop(stream);

        for _ in 0..200u32 {
            if alive.strong_count() == 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the drain thread never exited: still parked in recv()");
    }

    #[test]
    fn filter_map_is_lazy_and_runs_at_pull() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = crossbeam_channel::bounded(4);
        let c = calls.clone();
        let mut ch = closed(rx).filter_map(move |x: u32| {
            c.fetch_add(1, Ordering::SeqCst);
            x.is_multiple_of(2).then_some(x * 10)
        });
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(ch.recv(), Ok(20));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn merge_selects_across_sources_then_survivor() {
        let (tx_a, rx_a) = crossbeam_channel::bounded(4);
        let (tx_b, rx_b) = crossbeam_channel::bounded(4);
        let mut ch = closed(rx_a).merge(closed(rx_b));
        tx_a.send(1).unwrap();
        tx_b.send(2).unwrap();
        let mut got = vec![ch.recv().unwrap(), ch.recv().unwrap()];
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
        drop(tx_a);
        tx_b.send(3).unwrap();
        assert_eq!(ch.recv(), Ok(3));
        drop(tx_b);
        assert_eq!(ch.recv(), Err(End::Closed));
    }

    /// Routing must survive nesting: a `Merge` whose left child is itself a
    /// `Merge` flattens to three select operations, and each ordinal must reach
    /// its own leaf. A wrong `leaf_count` split misroutes the third.
    #[test]
    fn nested_merge_reaches_every_leaf() {
        let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
        let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
        let (tx_c, rx_c) = crossbeam_channel::bounded::<u32>(1);
        tx_a.send(1).unwrap();
        tx_b.send(2).unwrap();
        tx_c.send(3).unwrap();

        let mut ch = closed(rx_a).merge(closed(rx_b)).merge(closed(rx_c));

        let mut got = vec![ch.recv().unwrap(), ch.recv().unwrap(), ch.recv().unwrap()];
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3]);
    }

    /// A transposed ordinal split still delivers every item when all three
    /// leaves are primed — a set-membership check cannot tell which leaf each
    /// ordinal reached. Prime only the middle leaf (`b`, ordinal 1) and leave
    /// `a`/`c` alive but empty, so a misroute lands on an empty leaf and fails
    /// fast instead of hanging.
    #[test]
    fn nested_merge_routes_each_ordinal_to_its_own_leaf() {
        let (_tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
        let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
        let (_tx_c, rx_c) = crossbeam_channel::bounded::<u32>(1);
        tx_b.send(2).unwrap();

        let mut ch = closed(rx_a).merge(closed(rx_b)).merge(closed(rx_c));

        let mut sel = Select::new();
        ch.source.register(&mut sel);
        let idx = sel.ready();
        match ch.source.poll(idx) {
            Poll::Got(v) => assert_eq!(v, 2),
            _ => panic!("ready index routed to the wrong leaf"),
        }
    }

    /// When one branch of a `Merge` closes cleanly the root collapses to
    /// `MergeState::One` holding the surviving subtree. Here the survivor is
    /// itself a `Merge` (leaves 0 and 1) and the dead branch `c` was ordinal 2,
    /// so the survivor's ordinals must keep routing through `One` to `a` and
    /// `b`. Polling directly by ordinal is the point: the `Select` is not
    /// rebuilt between polls.
    #[test]
    fn merge_one_routes_ordinals_into_a_multi_leaf_survivor() {
        let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
        let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
        let (tx_c, rx_c) = crossbeam_channel::bounded::<u32>(1);
        let inner = closed(rx_a).merge(closed(rx_b));
        let (ch_c, closer_c) = Channels::from(rx_c, "c");
        closer_c.close();
        drop(tx_c);
        let mut ch = inner.merge(ch_c);

        tx_a.send(1).unwrap();
        tx_b.send(2).unwrap();

        let mut sel = Select::new();
        ch.source.register(&mut sel);

        // c's ordinal (2): the branch died cleanly, so the root collapses to
        // `One` and reports `Filtered` rather than an item.
        assert!(matches!(ch.source.poll(2), Poll::Filtered));
        match ch.source.poll(0) {
            Poll::Got(v) => assert_eq!(v, 1),
            _ => panic!("ordinal 0 did not route through One to leaf a"),
        }
        match ch.source.poll(1) {
            Poll::Got(v) => assert_eq!(v, 2),
            _ => panic!("ordinal 1 did not route through One to leaf b"),
        }
    }

    /// The [`Source`] contract, checked per leaf: every ordinal must reach
    /// *its own* leaf.
    ///
    /// Fail-fast by construction — plant an item in exactly one leaf and leave
    /// the rest alive-but-empty, so a misroute finds nothing and the case
    /// fails instead of passing on a coincidence. "Did every item come out?"
    /// cannot distinguish a correct split from a transposed one.
    fn assert_ordinals_reach(
        parts: usize,
        ordinal_to_part: &[usize],
        build: impl Fn(Vec<Channels<u32>>) -> Channels<u32>,
    ) {
        for (ordinal, &part) in ordinal_to_part.iter().enumerate() {
            let planted = u32::try_from(part).unwrap().saturating_add(100);
            let mut keepalive = Vec::new();
            let mut leaves = Vec::new();
            for i in 0..parts {
                let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
                if i == part {
                    tx.send(planted).unwrap();
                }
                keepalive.push(tx);
                leaves.push(closed(rx));
            }
            let mut ch = build(leaves);

            let mut sel = Select::new();
            ch.source.register(&mut sel);
            let idx = sel.ready();
            assert_eq!(idx, ordinal, "part {part} did not register at {ordinal}");
            match ch.source.poll(idx) {
                Poll::Got(v) => assert_eq!(v, planted, "ordinal {ordinal} hit the wrong leaf"),
                _ => panic!("ordinal {ordinal} did not reach part {part}"),
            }
            drop(keepalive);
        }
    }

    /// Depth-first ordinals across a nested merge: `(a|b)|c` flattens to three
    /// registrations and each must round-trip to the leaf that owns it.
    #[test]
    fn every_ordinal_reaches_its_own_leaf_across_a_nested_merge() {
        assert_ordinals_reach(3, &[0, 1, 2], |mut leaves| {
            let c = leaves.pop().unwrap();
            let b = leaves.pop().unwrap();
            let a = leaves.pop().unwrap();
            a.merge(b).merge(c)
        });
    }

    /// A `Chain` registers only its **front** phase, and counts only that
    /// phase — so the queued phase must consume no ordinal at all. Get that
    /// wrong and everything to the chain's right shifts by one: here `c` would
    /// stop being reachable at ordinal 1.
    #[test]
    fn a_queued_chain_phase_consumes_no_ordinal() {
        assert_ordinals_reach(3, &[0, 2], |mut leaves| {
            let c = leaves.pop().unwrap();
            let b = leaves.pop().unwrap();
            let a = leaves.pop().unwrap();
            a.chain(b).merge(c)
        });
    }

    /// `Guarded` registers inner-then-guard, so the guard takes the *highest*
    /// ordinal and the inner source's own ordinals stay put. The guard carries
    /// no items by construction, so the observable claim is the inner half:
    /// its leaves are still reachable at the ordinals they had alone.
    #[test]
    fn guarded_leaves_the_inner_ordinals_alone() {
        for target in 0..2usize {
            let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
            let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
            let planted = u32::try_from(target).unwrap().saturating_add(100);
            if target == 0 {
                tx_a.send(planted).unwrap();
            } else {
                tx_b.send(planted).unwrap();
            }
            // The guard's sender must outlive the poll: a disconnected guard
            // is *ready*, and would win the select on its own.
            let (alive_tx, alive_rx) = crossbeam_channel::bounded::<()>(1);
            let inner = closed(rx_a).merge(closed(rx_b));
            let mut ch = Channels {
                source: Box::new(Guarded {
                    inner: inner.source,
                    alive: LeafState::Alive {
                        rx: alive_rx,
                        close_rx: None,
                        what: "test guard",
                    },
                    fired: None,
                }),
            };

            let mut sel = Select::new();
            ch.source.register(&mut sel);
            let idx = sel.ready();
            assert_eq!(idx, target, "the guard displaced inner leaf {target}");
            match ch.source.poll(idx) {
                Poll::Got(v) => assert_eq!(v, planted),
                _ => panic!("ordinal {idx} did not reach inner leaf {target}"),
            }
            drop((tx_a, tx_b, alive_tx));
        }
    }

    /// A `Source` that claims one leaf but registers none — the
    /// under-registration the [`Source`] contract forbids. It otherwise
    /// presents as a hang, so the debug assert must catch it.
    struct UnderRegistering;

    impl Source<u32> for UnderRegistering {
        fn register<'a>(&'a mut self, _sel: &mut Select<'a>) -> usize {
            0
        }
        fn poll(&mut self, _ordinal: usize) -> Poll<u32> {
            Poll::Empty
        }
        fn drain_first(&mut self) -> Option<u32> {
            None
        }
        fn leaf_count(&self) -> usize {
            1
        }
        fn end_reason(&mut self) -> End {
            End::Closed
        }
    }

    #[test]
    #[should_panic(expected = "register pushed 0 ops but leaf_count() is 1")]
    fn register_leaf_count_mismatch_is_caught() {
        let mut ch = Channels {
            source: Box::new(UnderRegistering),
        };
        let _ = ch.recv();
    }

    /// A leaf is only ever reached at relative ordinal 0; in a debug build a
    /// nonzero ordinal aborts, pinning the bug to its source.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "a leaf is always polled at relative ordinal 0")]
    fn leaf_poll_rejects_a_nonzero_ordinal() {
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let mut ch = closed(rx);
        let _ = ch.source.poll(1);
    }

    /// In a release build the same bad ordinal must fail closed: the leaf
    /// resolves `Aborted` and reports `Dead`, so an enclosing tree condemns the
    /// stream instead of hanging or serving a sibling's item.
    #[cfg(not(debug_assertions))]
    #[test]
    fn leaf_poll_fails_closed_in_release() {
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let mut ch = closed(rx);
        assert!(matches!(ch.source.poll(1), Poll::Dead));
        assert_eq!(ch.end_reason(), End::Aborted { what: "test" });
    }

    /// The release side of the leaf ordinal check: a bad ordinal must resolve
    /// the leaf `Aborted`, not silently poll it, so an enclosing tree can
    /// condemn the stream. Exercised directly because the debug assert fires
    /// first in a debug build.
    #[test]
    fn leaf_poison_fails_closed_on_a_bad_ordinal() {
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let mut leaf = LeafState::Alive {
            rx,
            close_rx: None,
            what: "routing test",
        };
        leaf.poison(7);
        assert_eq!(leaf.leaf_count(), 0);
        assert_eq!(
            leaf.end_reason(),
            End::Aborted {
                what: "routing test"
            }
        );
    }

    #[test]
    fn merge_of_heterogeneous_origin_types() {
        let (tx_n, rx_n) = crossbeam_channel::bounded(4);
        let (tx_s, rx_s) = crossbeam_channel::bounded(4);
        let numbers = closed(rx_n).map(|n: u32| format!("n{n}"));
        let strings = closed(rx_s);
        let mut ch = numbers.merge(strings);
        tx_n.send(7).unwrap();
        tx_s.send("s".to_owned()).unwrap();
        let mut got = vec![ch.recv().unwrap(), ch.recv().unwrap()];
        got.sort();
        assert_eq!(got, vec!["n7".to_owned(), "s".to_owned()]);
    }

    #[test]
    fn try_recv_none_when_empty_or_disconnected() {
        let (tx_a, rx_a) = crossbeam_channel::bounded(4);
        let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(4);
        let mut ch = closed(rx_a).merge(closed(rx_b));
        assert_eq!(ch.try_recv(), None);
        tx_a.send(1).unwrap();
        assert_eq!(ch.try_recv(), Some(1));
        assert_eq!(ch.try_recv(), None);
        drop(tx_a);
        drop(tx_b);
        assert_eq!(ch.try_recv(), None);
    }

    #[test]
    fn try_recv_drains_filtered_items() {
        let (tx, rx) = crossbeam_channel::bounded(8);
        let mut ch = closed(rx).filter_map(|x: u32| x.is_multiple_of(2).then_some(x));
        tx.send(1).unwrap();
        tx.send(3).unwrap();
        tx.send(4).unwrap();
        assert_eq!(ch.try_recv(), Some(4));
    }

    #[test]
    fn chain_historic_then_live() {
        let (htx, hrx) = crossbeam_channel::bounded(4);
        let (ltx, lrx) = crossbeam_channel::bounded(4);
        let mut ch = closed(hrx).chain(closed(lrx));

        htx.send("h0").unwrap();
        htx.send("h1").unwrap();
        drop(htx);

        ltx.send("L0").unwrap();

        assert_eq!(ch.recv(), Ok("h0"));
        assert_eq!(ch.recv(), Ok("h1"));
        assert_eq!(ch.recv(), Ok("L0"));
        drop(ltx);
        assert_eq!(ch.recv(), Err(End::Closed));
    }

    /// A `Merge` inside a `Chain`: one leaf dying must not discard the items the
    /// sibling still holds, nor pop the whole phase. Before `Merge` reported
    /// `Filtered` (rather than `Dead`) while one side was alive, this yielded
    /// `[1, 99]` — item `2` was silently lost.
    #[test]
    fn chain_of_merge_survives_one_leaf_dying() {
        let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(4);
        let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(4);
        let (tx_c, rx_c) = crossbeam_channel::bounded::<u32>(4);

        let mut ch = closed(rx_a).merge(closed(rx_b)).chain(closed(rx_c));

        tx_b.send(1).unwrap();
        tx_b.send(2).unwrap();
        drop(tx_a); // left leaf dies; right still holds buffered items
        tx_c.send(99).unwrap();
        drop(tx_b);
        drop(tx_c);

        let mut got: Vec<u32> = Vec::new();
        while let Ok(v) = ch.recv() {
            got.push(v);
        }
        assert_eq!(got, vec![1, 2, 99]);
    }

    /// A request/response topology: the *left* branch's closure owns the
    /// `Sender` for the *right* branch. Dropping the exhausted left branch
    /// releases that sender and lets the merged output disconnect — without it
    /// `recv()` blocks forever and the writer loop leaks its thread.
    #[test]
    fn merge_drops_dead_branch_releasing_its_captured_senders() {
        let (in_tx, in_rx) = crossbeam_channel::bounded::<u32>(8);
        let (cmt_tx, cmt_rx) = crossbeam_channel::bounded::<u32>(8);
        let tx = Arc::new(cmt_tx);

        let outcomes = closed(cmt_rx);
        let mut merged = closed(in_rx)
            .filter_map(move |d: u32| {
                // "submit": hand off with a sender clone, answered out of band.
                let _ = tx.send(d.saturating_mul(10));
                None::<u32>
            })
            .merge(outcomes);

        in_tx.send(1).unwrap();
        drop(in_tx); // input closed, one write still in flight

        assert_eq!(merged.recv(), Ok(10), "the in-flight write is answered");
        assert_eq!(
            merged.recv(),
            Err(End::Closed),
            "output must disconnect once the input is gone and nothing is in flight"
        );
    }

    // ── explicit close ────────────────────────────────────────────────

    /// The consumer forms its verdict when the *data* channel disconnects, so a
    /// producer that drops its sender before closing is briefly indistinguishable
    /// from one that died. The transient answer must not be memoised, or a
    /// correct close arriving a moment later would be ignored.
    #[test]
    fn verdict_is_not_memoised_while_the_closer_is_still_alive() {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(4);
        let (mut ch, closer) = Channels::from(rx, "racy producer");
        drop(tx); // sender gone first — the ordering hazard
        assert_eq!(
            ch.end_reason(),
            End::Aborted {
                what: "racy producer"
            },
            "not closed yet, so fail closed"
        );
        closer.close();
        assert_eq!(
            ch.end_reason(),
            End::Closed,
            "a close arriving after the disconnect must still be honoured"
        );
    }

    /// An infinite stream has no `Closer`: ending is always an error, whatever
    /// the underlying reason. That is what drives a consumer's reconnect
    /// backoff rather than letting it look like a clean finish.
    #[test]
    fn infinite_stream_end_is_always_aborted() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let mut ch = Channels::from_infinite(rx, "socket");
        tx.send(1).unwrap();
        drop(tx); // peer closed politely — still an error to the app
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Err(End::Aborted { what: "socket" }));
        // Idempotent: a drain loop may ask again after the loop breaks.
        assert_eq!(ch.end_reason(), End::Aborted { what: "socket" });
    }

    /// An infinite branch ending condemns the merge, so a connection-scoped
    /// stream cannot be mistaken for a completed one.
    #[test]
    fn merge_with_infinite_branch_reports_aborted() {
        let (tx_f, rx_f) = crossbeam_channel::bounded::<u32>(4);
        let (tx_i, rx_i) = crossbeam_channel::bounded::<u32>(4);
        let (ch_f, closer_f) = Channels::from(rx_f, "finite");
        drop(tx_f);
        closer_f.close();
        let ch_i = Channels::from_infinite(rx_i, "socket");
        drop(tx_i);
        let mut ch = ch_f.merge(ch_i);
        assert_eq!(ch.recv(), Err(End::Aborted { what: "socket" }));
    }

    #[test]
    fn closed_producer_yields_closed() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let (mut ch, closer) = Channels::from(rx, "finite scan");
        tx.send(1).unwrap();
        drop(tx);
        closer.close();
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Err(End::Closed));
    }

    /// The whole point: a producer that vanished is not a producer that
    /// finished, even though the data channel looks identical.
    #[test]
    fn unclosed_producer_yields_aborted() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let (mut ch, closer) = Channels::from(rx, "finite scan");
        tx.send(1).unwrap();
        drop(tx);
        drop(closer); // dropped without close(): panicked, or forgot
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                what: "finite scan"
            })
        );
    }

    /// A `spawn_blocking` body that panics drops its `Closer` while unwinding —
    /// the case that motivated all of this.
    #[test]
    fn panicking_producer_yields_aborted() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let (mut ch, closer) = Channels::from(rx, "scan");
        let handle = std::thread::spawn(move || {
            let _closer = closer;
            tx.send(1).unwrap();
            panic!("scan blew up");
        });
        assert!(handle.join().is_err());
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Err(End::Aborted { what: "scan" }));
    }

    /// A task discarded before its body ever runs still drops the `Closer`, so
    /// the tokio-shutdown gap is covered too.
    #[test]
    fn producer_dropped_before_running_is_aborted() {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(4);
        let (mut ch, closer) = Channels::from(rx, "never ran");
        let never_ran = move || {
            let _closer = closer;
            let _tx = tx;
        };
        drop(never_ran); // the closure is discarded, never called
        assert_eq!(ch.recv(), Err(End::Aborted { what: "never ran" }));
    }

    /// Fail closed across a merge: one aborted branch condemns the stream even
    /// though the other closed cleanly. An abort now condemns the merge
    /// *immediately* — the sibling's buffered items may or may not have been
    /// drained first, depending on which branch `Select::ready` polled. Both
    /// outcomes are valid: the contract is "items may be missing" for `Aborted`.
    #[test]
    fn merge_reports_aborted_if_either_branch_aborted() {
        let (tx_a, rx_a) = crossbeam_channel::bounded(4);
        let (tx_b, rx_b) = crossbeam_channel::bounded(4);
        let (ch_a, closer_a) = Channels::from(rx_a, "good");
        let (ch_b, closer_b) = Channels::from(rx_b, "bad");
        tx_a.send(1).unwrap();
        tx_b.send(2).unwrap();
        drop(tx_a);
        drop(tx_b);
        closer_a.close();
        drop(closer_b); // this branch died

        let mut ch = ch_a.merge(ch_b);
        let mut got: Vec<u32> = Vec::new();
        loop {
            match ch.recv() {
                Ok(v) => {
                    assert!(v == 1 || v == 2, "unexpected item {v}");
                    got.push(v);
                }
                Err(End::Aborted { what: "bad" }) => break,
                Err(e) => panic!("unexpected end: {e:?}"),
            }
        }
        // The aborted branch may surface before the sibling's buffered item is
        // drained, so 1 or 2 items is valid (never zero — both are buffered and
        // `try_recv` returns items before `Disconnected`).
        assert!((1..=2).contains(&got.len()), "got {got:?}");
    }

    #[test]
    fn merge_reports_closed_when_both_branches_closed() {
        let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(4);
        let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(4);
        let (ch_a, closer_a) = Channels::from(rx_a, "a");
        let (ch_b, closer_b) = Channels::from(rx_b, "b");
        drop(tx_a);
        drop(tx_b);
        closer_a.close();
        closer_b.close();
        let mut ch = ch_a.merge(ch_b);
        assert_eq!(ch.recv(), Err(End::Closed));
    }

    /// A phase that died mid-chain condemns it immediately — the
    /// historical→live handoff must not launder a truncated replay behind live
    /// items that would make the stream look complete. The live phase's items
    /// are discarded the moment the historical phase reports `Aborted`.
    #[test]
    fn chain_reports_aborted_from_the_phase_that_died() {
        let (tx_h, rx_h) = crossbeam_channel::bounded(4);
        let (tx_l, rx_l) = crossbeam_channel::bounded(4);
        let (ch_h, closer_h) = Channels::from(rx_h, "historical");
        let (ch_l, closer_l) = Channels::from(rx_l, "live");
        tx_h.send(1).unwrap();
        drop(tx_h);
        drop(closer_h); // historical replay died
        tx_l.send(2).unwrap();
        drop(tx_l);
        closer_l.close();

        let mut ch = ch_h.chain(ch_l);
        assert_eq!(ch.recv(), Ok(1));
        // The historical phase aborts before the live phase is ever reached.
        assert_eq!(ch.recv(), Err(End::Aborted { what: "historical" }));
    }

    /// The verdict survives a `filter_map` — the snapshot pipeline shape.
    #[test]
    fn abort_survives_filter_map() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let (ch, closer) = Channels::from(rx, "window");
        tx.send(2u32).unwrap();
        drop(tx);
        drop(closer);
        let mut ch = ch.filter_map(|v: u32| Some(v.saturating_mul(10)));
        assert_eq!(ch.recv(), Ok(20));
        assert_eq!(ch.recv(), Err(End::Aborted { what: "window" }));
    }

    #[test]
    fn from_vec_is_always_closed() {
        let mut ch = Channels::from_vec(vec![1u32]);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Err(End::Closed));
    }

    /// `drain_first` must not starve one side: `ready_batch` takes only its head
    /// through the fair `recv`, so a fixed bias would drain the whole left branch
    /// before ever touching the right.
    #[test]
    fn merge_drain_first_alternates_between_branches() {
        let (tx_l, rx_l) = crossbeam_channel::bounded::<u32>(16);
        let (tx_r, rx_r) = crossbeam_channel::bounded::<u32>(16);
        for _ in 0..6 {
            tx_l.send(1).unwrap();
            tx_r.send(2).unwrap();
        }
        let mut ch = closed(rx_l).merge(closed(rx_r));

        let batch: Vec<u32> = ch.ready_batch(8).unwrap().collect();
        assert!(
            batch.contains(&1) && batch.contains(&2),
            "both branches must appear within one batch, got {batch:?}"
        );
    }

    #[test]
    fn chain_try_recv_transitions() {
        let (htx, hrx) = crossbeam_channel::bounded(4);
        let (ltx, lrx) = crossbeam_channel::bounded(4);
        let mut ch = closed(hrx).chain(closed(lrx));

        htx.send("h").unwrap();
        drop(htx);

        assert_eq!(ch.try_recv(), Some("h"));
        ltx.send("L").unwrap();
        assert_eq!(ch.try_recv(), Some("L"));
    }

    #[test]
    fn ready_batch_drains_max_items() {
        let (tx, rx) = crossbeam_channel::bounded(16);
        let mut ch = closed(rx);
        for i in 0..10u32 {
            tx.send(i).unwrap();
        }
        let batch: Vec<u32> = ch.ready_batch(3).unwrap().collect();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch, vec![0, 1, 2]);
        let rest: Vec<u32> = ch.ready_batch(100).unwrap().collect();
        assert_eq!(rest.len(), 7);
    }

    #[test]
    fn ready_batch_yields_everything_when_max_exceeds() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let mut ch = closed(rx);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        drop(tx);
        let batch: Vec<u32> = ch.ready_batch(10).unwrap().collect();
        assert_eq!(batch, vec![1, 2]);
    }

    #[test]
    fn from_vec_drains_all_items() {
        let mut ch = Channels::from_vec(vec![10, 20, 30]);
        assert_eq!(ch.recv(), Ok(10));
        assert_eq!(ch.recv(), Ok(20));
        assert_eq!(ch.recv(), Ok(30));
        assert_eq!(ch.recv(), Err(End::Closed));
    }

    #[test]
    fn from_vec_empty_disconnects_immediately() {
        let mut ch: Channels<u32> = Channels::from_vec(vec![]);
        assert_eq!(ch.recv(), Err(End::Closed));
    }

    #[test]
    fn into_iter_collects_all_items() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let ch = closed(rx);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        drop(tx);
        let got: Vec<u32> = ch.into_iter().collect();
        assert_eq!(got, vec![1, 2, 3]);
    }

    #[test]
    fn into_iter_empty_disconnected() {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        drop(tx);
        let ch = closed(rx);
        let got: Vec<u32> = ch.into_iter().collect();
        assert!(got.is_empty());
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn into_stream_yields_items() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (tx, rx) = crossbeam_channel::bounded(4);
        tx.send(10).unwrap();
        tx.send(20).unwrap();
        drop(tx);
        let got: Vec<u32> = rt.block_on(async {
            let stream = closed(rx).into_stream();
            use futures::StreamExt;
            stream.collect().await
        });
        assert_eq!(got, vec![10, 20]);
    }

    #[test]
    fn drain_into_transfers_items() {
        let (stx, srx) = crossbeam_channel::bounded(4);
        let (dtx, drx) = crossbeam_channel::bounded(4);
        stx.send(1).unwrap();
        stx.send(2).unwrap();
        drop(stx);
        assert!(closed(srx).drain_into(&dtx));
        drop(dtx);
        let got: Vec<u32> = drx.iter().collect();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn drain_into_returns_false_when_receiver_dropped() {
        let (stx, srx) = crossbeam_channel::bounded(4);
        let (dtx, drx) = crossbeam_channel::bounded(1);
        stx.send(1).unwrap();
        stx.send(2).unwrap();
        drop(stx);
        drop(drx); // consumer drops early
        assert!(!closed(srx).drain_into(&dtx));
    }

    // ── until / Cancellation ───────────────────────────────────────────

    /// The point of `until`: a consumer parked in `recv` on a stream that has
    /// gone quiet — sender alive, nothing arriving — is woken by the cancel.
    #[test]
    fn until_wakes_a_blocked_recv_with_cancelled() {
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let (handle, cancellation) = CancelHandle::new();
        let mut ch = Channels::from_infinite(rx, "quiet").until(&cancellation);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(50));
                handle.cancel();
            });
            assert_eq!(ch.recv(), Err(End::Cancelled));
        });
        assert!(cancellation.is_cancelled());
        assert_eq!(ch.end_reason(), End::Cancelled);
    }

    /// A race, not a merge: a finite stream that closes ends the bound stream
    /// at once instead of waiting on the cancellation, and a cancel that comes
    /// later does not rewrite a declared completion.
    #[test]
    fn until_keeps_a_clean_close_after_a_later_cancel() {
        let (tx, rx) = crossbeam_channel::bounded(2);
        let (handle, cancellation) = CancelHandle::new();
        let mut ch = closed(rx).until(&cancellation);
        tx.send(1).unwrap();
        drop(tx);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Err(End::Closed));
        handle.cancel();
        assert_eq!(ch.end_reason(), End::Closed);
    }

    /// A handle that goes away without `cancel` ends the stream at once, even
    /// with its source still alive — that source may be what whoever holds the
    /// stream keeps alive — but it is not a cancel.
    #[test]
    fn until_a_handle_dropped_unfired_ends_aborted() {
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let (handle, cancellation) = CancelHandle::new();
        let mut ch = Channels::from_infinite(rx, "quiet").until(&cancellation);
        drop(handle);
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                what: "cancellation"
            })
        );
        assert!(!cancellation.is_cancelled());
    }

    /// The verdict follows the flag even when the select wakes on the inner
    /// side first: a cancel whose teardown disconnects the source is still a
    /// cancel, never that source's abort.
    #[test]
    fn until_a_cancel_that_tears_down_the_source_is_still_a_cancel() {
        for _ in 0..64 {
            let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
            let (handle, cancellation) = CancelHandle::new();
            let mut ch = Channels::from_infinite(rx, "source").until(&cancellation);
            handle.cancel();
            drop(tx);
            assert_eq!(ch.recv(), Err(End::Cancelled));
        }
    }

    /// A stream bound after the cancel has already fired ends immediately.
    #[test]
    fn until_an_already_fired_cancellation_ends_at_once() {
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let (handle, cancellation) = CancelHandle::new();
        handle.cancel();
        handle.cancel(); // idempotent
        let mut ch = Channels::from_infinite(rx, "quiet").until(&cancellation);
        assert_eq!(ch.recv(), Err(End::Cancelled));
    }

    /// `Cancelled` condemns an enclosing merge like `Aborted` does: the
    /// surviving branch is not drained as though the stream were whole.
    #[test]
    fn a_cancelled_branch_condemns_a_merge() {
        let (_tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
        let (_tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
        let (handle, cancellation) = CancelHandle::new();
        let mut ch = Channels::from_infinite(rx_a, "a")
            .until(&cancellation)
            .merge(Channels::from_infinite(rx_b, "b"));
        handle.cancel();
        assert_eq!(ch.recv(), Err(End::Cancelled));
    }

    /// `never` binds nothing: the stream and its verdict are untouched.
    #[test]
    fn never_leaves_the_stream_untouched() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let mut ch = closed(rx).until(&Cancellation::never());
        tx.send(7).unwrap();
        drop(tx);
        assert_eq!(ch.recv(), Ok(7));
        assert_eq!(ch.recv(), Err(End::Closed));
    }

    /// `from_token` is the bridge from a tokio token to a blocked crossbeam
    /// `recv` on a blocking thread.
    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread")]
    async fn from_token_wakes_a_blocking_recv() {
        let token = tokio_util::sync::CancellationToken::new();
        let cancellation = Cancellation::from_token(&token);
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let mut ch = Channels::from_infinite(rx, "quiet").until(&cancellation);
        let blocked = tokio::task::spawn_blocking(move || ch.recv());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!blocked.is_finished(), "recv returned before the cancel");
        token.cancel();
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), blocked)
            .await
            .unwrap() // the cancel must wake recv within the bound
            .unwrap();
        assert_eq!(got, Err(End::Cancelled));
        drop(tx);
    }
}
