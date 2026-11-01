//! `Channel<T>` — the blocking read half of a pipeline.
//!
//! A `Channel<T>` owns a private tree of sources: a leaf (one crossbeam
//! receiver), a concurrent merge, a sequential chain, a guard raced against a
//! liveness signal or [`Cancellation`], a lazy `resume_on_abort`, a bounded
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
//! abort is recoverable — [`Channel::resume_on_abort`] hands it to the caller.
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
//! [`Channel::resume_on_abort`] rebuilds a stream after an abort, handing
//! that abort to the caller. [`Channel::take_while`] ends at a consumer bound;
//! [`Channel::filter_map`], [`Channel::map`] and [`Channel::until`]
//! transform and bound a stream at pull time.
//!
//! The write half lives next door: [`crate::Feed`] for one producer and
//! [`crate::broadcast`]/[`crate::keyed`]/[`crate::Wakes`] for many. The seam
//! ([`crate::Tail`], [`crate::Ranged`], [`crate::blend_chain`]) composes a live
//! tail with a bounded history.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::{Receiver, Select, TryRecvError};

use std::marker::PhantomData;

use crate::kind::{Finite, Infinite, Kind, MergeKind};
use crate::ordered::Key;

/// Why a [`Channel`] stopped yielding items.
///
/// The top level is only “clean” or “not clean”. The detail lives in
/// [`AbortReason`], so a producer close, a consumer bound, and a cancellation
/// share one small verdict type without sharing one meaning.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum End {
    /// The stream ended cleanly: the producer declared completion, every
    /// phase/branch completed, or a consumer bound was reached.
    Complete,
    /// The stream did not complete cleanly. **Items may be missing** unless the
    /// reason says otherwise.
    Aborted { reason: AbortReason },
}

/// Why an [`End::Aborted`] stream stopped.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AbortReason {
    /// A data channel disconnected without a close: the producer panicked, was
    /// discarded before it ran, simply forgot, or an infinite source ended.
    ProducerGone { what: &'static str },
    /// A [`Cancellation`] bound with [`Channel::until`] fired. Items still
    /// buffered when it fired may or may not have been delivered.
    Cancelled,
    /// A cancellation handle was dropped without firing. This is teardown, not
    /// a stop request.
    CancelHandleDropped,
    /// An internal routing invariant was violated; the stream fails closed.
    RoutingViolation { what: &'static str },
    /// An ordered source saw a position that violated its policy.
    OrderViolation { what: &'static str },
}

impl AbortReason {
    /// The static label for this reason: the producer/guard/source name, or a
    /// fixed string for the reasons without one.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::ProducerGone { what }
            | Self::RoutingViolation { what }
            | Self::OrderViolation { what } => what,
            Self::Cancelled => "cancelled",
            Self::CancelHandleDropped => "cancellation handle dropped",
        }
    }
}

impl std::fmt::Display for AbortReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProducerGone { what } => write!(f, "stream {what} ended"),
            Self::Cancelled => f.write_str("stream cancelled"),
            Self::CancelHandleDropped => f.write_str("cancellation handle dropped"),
            Self::RoutingViolation { what } => write!(f, "routing violation in {what}"),
            Self::OrderViolation { what } => write!(f, "order violation in {what}"),
        }
    }
}

/// The producer's half of an explicit close. Handed out by
/// [`Channel::from`] so a construction site cannot forget it exists.
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

pub(crate) enum Poll<T> {
    Got(T),
    Filtered,
    Empty,
    Dead,
}

pub(crate) trait Source<T>: Send {
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
    /// consumer holds `&mut Channel`, so no leaf can die while `ready` is
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
/// [`Channel::from_infinite`] stream — no closer may exist, so any termination
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
                        *self = LeafState::Resolved(End::Complete);
                        End::Complete
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        let end = End::Aborted {
                            reason: AbortReason::ProducerGone { what },
                        };
                        *self = LeafState::Resolved(end);
                        end
                    }
                    // Still outstanding: fail closed for this read, but do not
                    // commit — a late `close()` can still be honoured next time.
                    // This IS the non-memoisation the test guards.
                    Err(crossbeam_channel::TryRecvError::Empty) => End::Aborted {
                        reason: AbortReason::ProducerGone { what },
                    },
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
                        Ok(()) => (End::Complete, true),
                        Err(crossbeam_channel::TryRecvError::Disconnected) => (
                            End::Aborted {
                                reason: AbortReason::ProducerGone { what },
                            },
                            true,
                        ),
                        Err(crossbeam_channel::TryRecvError::Empty) => (
                            End::Aborted {
                                reason: AbortReason::ProducerGone { what },
                            },
                            false,
                        ),
                    },
                    None => (
                        End::Aborted {
                            reason: AbortReason::ProducerGone { what },
                        },
                        false,
                    ),
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
        *self = LeafState::Resolved(End::Aborted {
            reason: AbortReason::RoutingViolation { what },
        });
    }

    /// Resolve the leaf after the data channel disconnects. A `None` `close_rx`
    /// is an infinite stream — ending is always an abort.
    fn resolve(
        close_rx: Option<crossbeam_channel::Receiver<()>>,
        what: &'static str,
    ) -> LeafState<T> {
        match close_rx {
            Some(rx) => match rx.try_recv() {
                Ok(()) => LeafState::Resolved(End::Complete),
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    LeafState::Resolved(End::Aborted {
                        reason: AbortReason::ProducerGone { what },
                    })
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    LeafState::Pending { close_rx: rx, what }
                }
            },
            None => LeafState::Resolved(End::Aborted {
                reason: AbortReason::ProducerGone { what },
            }),
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
        let owned = std::mem::replace(self, MergeState::Done(End::Complete));
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
                                End::Complete => {
                                    (MergeState::One { survivor: right }, Poll::Filtered)
                                }
                                // Abort condemns the merge: discard the survivor.
                                aborted @ End::Aborted { .. } => {
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
                                End::Complete => {
                                    (MergeState::One { survivor: left }, Poll::Filtered)
                                }
                                aborted @ End::Aborted { .. } => {
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
        let owned = std::mem::replace(self, MergeState::Done(End::Complete));
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
                                End::Complete => {
                                    drop(first);
                                    *self = MergeState::One { survivor: second };
                                    // Retry in this same call: a clean death must
                                    // not make `try_recv` report an empty batch
                                    // while the survivor still holds items.
                                    self.drain_first()
                                }
                                aborted @ End::Aborted { .. } => {
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
                                            End::Complete => {
                                                drop(second);
                                                *self = MergeState::One { survivor: first };
                                                self.drain_first()
                                            }
                                            aborted @ End::Aborted { .. } => {
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
            MergeState::Both { .. } => End::Complete,
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
                    *self = ChainState::Done(End::Complete);
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
                                End::Complete if phases.is_empty() => {
                                    *self = ChainState::Done(End::Complete);
                                    Poll::Dead
                                }
                                End::Complete => Poll::Filtered,
                                aborted @ End::Aborted { .. } => {
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
                            *self = ChainState::Done(End::Complete);
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
                                    End::Complete if phases.is_empty() => {
                                        *self = ChainState::Done(End::Complete);
                                        return None;
                                    }
                                    End::Complete => continue,
                                    aborted @ End::Aborted { .. } => {
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
                None => End::Complete,
            },
        }
    }
}

// ── Guarded<T> ─────────────────────────────────────────────────────────

/// An inner source raced against a [`Cancellation`]: whichever ends first ends
/// the stream ([`Channel::until`]).
///
/// Not [`MergeState`], which keeps the survivor when one branch closes cleanly
/// — that would make a finite source never terminate. Not [`ChainState`]
/// either, which is sequential. The guard carries no items: its only event is
/// disconnecting. Whatever holds the stream may be what keeps its producer
/// alive, so the guard is the one thing that can be relied on to wake the
/// consumer; `fired` only picks the verdict — `Cancelled` for a cancel,
/// `CancelHandleDropped` for a handle dropped without cancelling.
struct Guarded<T> {
    inner: Box<dyn Source<T>>,
    alive: LeafState<()>,
    /// Set before the guard's sender is dropped by a deliberate cancel.
    fired: Arc<AtomicBool>,
}

impl<T> Guarded<T> {
    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
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
        if self.inner.leaf_count() == 0 && self.inner.end_reason() == End::Complete {
            return End::Complete;
        }
        // Read the flag, not the guard leaf: the leaf learns of the disconnect
        // only when polled, and the select may have woken on the inner side
        // first. `fired` is set before anything the cancel tears down, so an
        // inner end seen once it is set is the cancel's doing.
        if self.fired() {
            return End::Aborted {
                reason: AbortReason::Cancelled,
            };
        }
        if self.alive.leaf_count() == 0 {
            // A cancel handle went away unfired. The guard's own disconnect is
            // the reason, not the inner source's.
            return End::Aborted {
                reason: AbortReason::CancelHandleDropped,
            };
        }
        self.inner.end_reason()
    }
}

// ── Cancellation ───────────────────────────────────────────────────────

/// A cancellation that can end a blocked [`Channel::recv`].
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
/// ends once it is set reports [`End::Aborted { reason: AbortReason::Cancelled }`]. A handle dropped without
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
    /// [`End::Aborted { reason: AbortReason::Cancelled }`]. Idempotent, and synchronous — a consumer woken after
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

    /// A cancellation that never fires: [`Channel::until`] with it is a
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

// ── ResumeOnAbort<S, T> ───────────────────────────────────────────────

/// [`ChainState`]'s resumable sibling: after an **aborted** segment, `next` may
/// produce the following one from the running state.
///
/// `Complete` is final: a cleanly ended segment is not resumed. For an
/// [`End::Aborted`], the source **hands the verdict to `next`** and lets the
/// caller decide: resume, or decline by returning `None`. A stable tail whose
/// fan-out hangs up on an overrun subscriber uses that verdict as the cue to
/// replay from its durable cursor and resubscribe.
///
/// The library does not decide that a fault is recoverable. A panic still
/// closes the producer as [`End::Aborted`], and only the closure — which knows
/// the cursor and the durable source — can decide whether to resume. A
/// `Cancellation` arrives as an abort reason and can be declined the same way.
///
/// A segment is materialised eagerly — at construction, and again the instant
/// its predecessor dies — never lazily inside `register`, and never while a
/// registration is outstanding. An enclosing [`MergeState`] splits the ordinal
/// space by `leaf_count()` *at poll time*, against a registration built before
/// `Select::ready` blocked, so the count has to hold across that whole window,
/// not merely during registration. A segment swapped in mid-window would
/// misalign every index to its right. This is the one source whose leaf count
/// is meant to change, which is why the rule is written down here.
struct ResumeOnAbort<S, T> {
    state: S,
    #[allow(clippy::type_complexity, reason = "one boxed closure, named inline")]
    next: Box<dyn FnMut(&mut S, Option<End>) -> Option<Channel<T, Infinite>> + Send>,
    current: Option<Box<dyn Source<T>>>,
    /// The end of the segment before `current`, reported once by `end_reason`
    /// when `next` finally declines.
    last_end: Option<End>,
}

impl<S: Send + 'static, T: Send + 'static> ResumeOnAbort<S, T> {
    /// Pull the first segment. The closure sees `None`.
    fn start(&mut self) {
        self.current = (self.next)(&mut self.state, None).map(|ch| ch.source);
    }

    /// Pull a segment after an abort. `Complete` never reaches this method.
    fn resume(&mut self, end: End) {
        self.last_end = Some(end);
        self.current = (self.next)(&mut self.state, Some(end)).map(|ch| ch.source);
    }
}

impl<S: Send + 'static, T: Send + 'static> Source<T> for ResumeOnAbort<S, T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        self.current.as_mut().map_or(0, |src| src.register(sel))
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        match &mut self.current {
            Some(src) => {
                debug_assert!(
                    ordinal < src.leaf_count(),
                    "ordinal {ordinal} out of range for resume-on-abort segment with {} leaves",
                    src.leaf_count()
                );
                match src.poll(ordinal) {
                    Poll::Dead => {
                        let end = src.end_reason();
                        self.last_end = Some(end);
                        self.current = None;
                        if matches!(end, End::Complete) {
                            return Poll::Dead;
                        }
                        self.resume(end);
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
            let end = src.end_reason();
            self.last_end = Some(end);
            self.current = None;
            if matches!(end, End::Complete) {
                return None;
            }
            self.resume(end);
        }
    }
    fn leaf_count(&self) -> usize {
        self.current.as_ref().map_or(0, |src| src.leaf_count())
    }
    fn end_reason(&mut self) -> End {
        match &mut self.current {
            Some(src) => src.end_reason(),
            // `next` declined: the last segment's own verdict is the stream's.
            // Declining the first open is still an abort: the stream is
            // infinite, and a clean end would claim a finish it never had.
            None => self.last_end.unwrap_or(End::Aborted {
                reason: AbortReason::ProducerGone {
                    what: "resume_on_abort",
                },
            }),
        }
    }
}

// ── FilterMap<S, T> ────────────────────────────────────────────────────

struct FilterMap<S, T> {
    inner: Box<dyn Source<S>>,
    f: Box<dyn FnMut(S) -> Option<T> + Send>,
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

// ── TakeWhile<T> ───────────────────────────────────────────────────────

/// A bounded prefix: stop at the first item `pred` declines.
///
/// The declined item is consumed and dropped, like `Iterator::take_while`. The
/// stream ends [`End::Complete`]: from the consumer's point of view the
/// requested range is done. If the source ends first, its own verdict is
/// preserved; a clean `Complete` there is also a complete (if shorter) stream.
struct TakeWhile<T> {
    inner: Option<Box<dyn Source<T>>>,
    pred: Box<dyn FnMut(&T) -> bool + Send>,
    done: Option<End>,
}

impl<T: Send + 'static> Source<T> for TakeWhile<T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        self.inner.as_mut().map_or(0, |src| src.register(sel))
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        let Some(mut inner) = self.inner.take() else {
            return Poll::Dead;
        };
        match inner.poll(ordinal) {
            Poll::Got(v) => {
                if (self.pred)(&v) {
                    self.inner = Some(inner);
                    Poll::Got(v)
                } else {
                    self.done = Some(End::Complete);
                    Poll::Dead
                }
            }
            Poll::Filtered => {
                self.inner = Some(inner);
                Poll::Filtered
            }
            Poll::Empty => {
                self.inner = Some(inner);
                Poll::Empty
            }
            Poll::Dead => {
                self.done = Some(inner.end_reason());
                Poll::Dead
            }
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        let mut inner = self.inner.take()?;
        match inner.drain_first() {
            Some(v) => {
                if (self.pred)(&v) {
                    self.inner = Some(inner);
                    Some(v)
                } else {
                    self.done = Some(End::Complete);
                    None
                }
            }
            None => {
                if inner.leaf_count() == 0 {
                    self.done = Some(inner.end_reason());
                    None
                } else {
                    self.inner = Some(inner);
                    None
                }
            }
        }
    }
    fn leaf_count(&self) -> usize {
        self.inner.as_ref().map_or(0, |src| src.leaf_count())
    }
    fn end_reason(&mut self) -> End {
        match self.done {
            Some(end) => end,
            None => self
                .inner
                .as_mut()
                .map_or(End::Complete, |src| src.end_reason()),
        }
    }
}

// ── DedupByKey<T, Pos> ─────────────────────────────────────────────────

/// Drop consecutive items whose key repeats.
///
/// No-key items (errors, watermarks) pass through and do not change the last
/// key. This is a stream transform, not an order policy: the caller can pair it
/// with `Contiguous` when exact succession is required.
struct DedupByKey<T, Pos> {
    inner: Option<Box<dyn Source<T>>>,
    last: Option<Pos>,
    done: Option<End>,
}

impl<T, Pos> Source<T> for DedupByKey<T, Pos>
where
    T: Send + 'static,
    Pos: Key<T>,
{
    fn register<'a>(&'a mut self, sel: &mut crossbeam_channel::Select<'a>) -> usize {
        self.inner.as_mut().map_or(0, |src| src.register(sel))
    }

    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        let Some(mut inner) = self.inner.take() else {
            return Poll::Dead;
        };
        match inner.poll(ordinal) {
            Poll::Got(item) => match Pos::key(&item) {
                Some(key) if self.last == Some(key) => {
                    self.inner = Some(inner);
                    Poll::Filtered
                }
                Some(key) => {
                    self.last = Some(key);
                    self.inner = Some(inner);
                    Poll::Got(item)
                }
                None => {
                    self.inner = Some(inner);
                    Poll::Got(item)
                }
            },
            Poll::Filtered => {
                self.inner = Some(inner);
                Poll::Filtered
            }
            Poll::Empty => {
                self.inner = Some(inner);
                Poll::Empty
            }
            Poll::Dead => {
                self.done = Some(inner.end_reason());
                Poll::Dead
            }
        }
    }

    fn drain_first(&mut self) -> Option<T> {
        loop {
            let mut inner = self.inner.take()?;
            match inner.drain_first() {
                Some(item) => match Pos::key(&item) {
                    Some(key) if self.last == Some(key) => {
                        self.inner = Some(inner);
                        continue;
                    }
                    Some(key) => {
                        self.last = Some(key);
                        self.inner = Some(inner);
                        return Some(item);
                    }
                    None => {
                        self.inner = Some(inner);
                        return Some(item);
                    }
                },
                None => {
                    if inner.leaf_count() == 0 {
                        self.done = Some(inner.end_reason());
                        return None;
                    }
                    self.inner = Some(inner);
                    return None;
                }
            }
        }
    }

    fn leaf_count(&self) -> usize {
        self.inner.as_ref().map_or(0, |src| src.leaf_count())
    }

    fn end_reason(&mut self) -> End {
        match self.done {
            Some(end) => end,
            None => self
                .inner
                .as_mut()
                .map_or(End::Complete, |src| src.end_reason()),
        }
    }
}

// ── Deferred<T> ────────────────────────────────────────────────────────

/// A phase whose producer starts on first use: [`Channel::defer_async`].
///
/// Until the future resolves, the source registers the hand-off receiver, so
/// it stays selectable (an enclosing merge or chain does not block). The
/// future is spawned the first time the source is registered or drained —
/// never at construction — so a `chain_all` of deferred phases opens them one
/// at a time. Dropping the source while it waits aborts the task.
#[cfg(feature = "tokio")]
struct Deferred<T> {
    state: DeferredState<T>,
}

#[cfg(feature = "tokio")]
enum DeferredState<T> {
    Waiting {
        handoff: Receiver<Box<dyn Source<T>>>,
        start: Option<Box<dyn FnOnce() -> tokio::task::AbortHandle + Send>>,
        task: Option<tokio::task::AbortHandle>,
    },
    Running(Box<dyn Source<T>>),
    Ended(End),
}

#[cfg(feature = "tokio")]
const DEFERRED: &str = "defer_async";

#[cfg(feature = "tokio")]
impl<T: Send + 'static> Deferred<T> {
    fn ensure_started(&mut self) {
        if let DeferredState::Waiting { start, task, .. } = &mut self.state
            && let Some(start) = start.take()
        {
            *task = Some(start());
        }
    }

    /// Take the resolved channel if it is there. `true` when the state moved.
    fn try_resolve(&mut self) -> bool {
        let DeferredState::Waiting { handoff, .. } = &self.state else {
            return false;
        };
        match handoff.try_recv() {
            Ok(source) => {
                self.state = DeferredState::Running(source);
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => {
                self.state = DeferredState::Ended(End::Aborted {
                    reason: AbortReason::ProducerGone { what: DEFERRED },
                });
                true
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl<T: Send + 'static> Source<T> for Deferred<T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        self.ensure_started();
        match &mut self.state {
            DeferredState::Waiting { handoff, .. } => {
                sel.recv(handoff);
                1
            }
            DeferredState::Running(src) => src.register(sel),
            DeferredState::Ended(_) => 0,
        }
    }

    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        match &mut self.state {
            DeferredState::Waiting { .. } => {
                if !self.try_resolve() {
                    return Poll::Empty;
                }
                match self.state {
                    // Resolved: the new leaves are registered on the next round.
                    DeferredState::Running(_) => Poll::Filtered,
                    _ => Poll::Dead,
                }
            }
            DeferredState::Running(src) => src.poll(ordinal),
            DeferredState::Ended(_) => Poll::Dead,
        }
    }

    fn drain_first(&mut self) -> Option<T> {
        self.ensure_started();
        self.try_resolve();
        match &mut self.state {
            DeferredState::Running(src) => src.drain_first(),
            _ => None,
        }
    }

    fn leaf_count(&self) -> usize {
        match &self.state {
            DeferredState::Waiting { .. } => 1,
            DeferredState::Running(src) => src.leaf_count(),
            DeferredState::Ended(_) => 0,
        }
    }

    fn end_reason(&mut self) -> End {
        match &mut self.state {
            // Asked before the phase ended: fail closed, as a leaf does.
            DeferredState::Waiting { .. } => End::Aborted {
                reason: AbortReason::ProducerGone { what: DEFERRED },
            },
            DeferredState::Running(src) => src.end_reason(),
            DeferredState::Ended(end) => *end,
        }
    }
}

#[cfg(feature = "tokio")]
impl<T> Drop for Deferred<T> {
    fn drop(&mut self) {
        if let DeferredState::Waiting {
            task: Some(task), ..
        } = &self.state
        {
            task.abort();
        }
    }
}

// ── Channel<T> ────────────────────────────────────────────────────────

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

    /// Rebuild the stream after an **abort**: `next` is called at start with
    /// `None`, and again with the aborted segment's [`End`] after an
    /// [`End::Aborted`] segment. It may return the next segment, or `None` to
    /// finish. A [`End::Complete`] segment ends the stream; it is not resumed.
    ///
    /// This is [`chain`](Self::chain)'s resumable sibling. A chain condemns the
    /// rest on an abort; here the caller chooses, because for a stable tail a
    /// hung-up subscription is the signal to replay from a cursor and
    /// resubscribe. The library makes no policy: a panic is still
    /// [`End::Aborted`], and `next` decides whether that fault is recoverable.
    pub fn resume_on_abort<S, F>(state: S, next: F) -> Channel<T, Infinite>
    where
        S: Send + 'static,
        F: FnMut(&mut S, Option<End>) -> Option<Channel<T, Infinite>> + Send + 'static,
    {
        let mut source = ResumeOnAbort {
            state,
            next: Box::new(next),
            current: None,
            last_end: None,
        };
        // Eagerly, so `leaf_count()` is accurate before the first `register`.
        source.start();
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

/// Test support: an infinite channel holding `items`, whose producer is then
/// gone — `Aborted(ProducerGone { what })` after the last item.
#[cfg(test)]
pub(crate) fn infinite_of<T: Send + 'static>(
    items: Vec<T>,
    what: &'static str,
) -> Channel<T, Infinite> {
    let (tx, rx) = crossbeam_channel::bounded(items.len().max(1));
    for item in items {
        let _ = tx.send(item);
    }
    drop(tx);
    Channel::from_infinite(rx, what)
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
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Most tests care about item flow, not the verdict: wrap a receiver in a
    /// channel whose producer has already declared completion.
    fn closed<T: Send + 'static>(rx: Receiver<T>) -> Channel<T, Finite> {
        let (ch, closer) = Channel::from(rx, "test");
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
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    /// The load-bearing difference from `chain`, which an aborted phase
    /// condemns: `resume_on_abort` hands the verdict to `next` and lets it
    /// decide. A resuming consumer (a stable tail, where a hung-up
    /// subscription IS the lag signal) continues; anything else can decline.
    #[test]
    fn resume_on_abort_hands_the_previous_end_to_next_and_may_continue_past_an_abort() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let s = seen.clone();
        let mut ch = Channel::resume_on_abort(0u32, move |n, prev| {
            s.lock().unwrap().push(prev);
            *n = n.saturating_add(1);
            match *n {
                // An infinite segment whose sender drops: End::Aborted.
                1 => {
                    let (tx, rx) = crossbeam_channel::bounded(4);
                    tx.send(7).unwrap();
                    drop(tx);
                    Some(Channel::from_infinite(rx, "seg"))
                }
                2 => Some(infinite_of(vec![8], "seg 2")),
                _ => None,
            }
        });
        let got: Vec<u32> = std::iter::from_fn(|| ch.recv().ok()).collect();
        assert_eq!(
            got,
            vec![7, 8],
            "an aborted segment does not end the resumable stream"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                None,
                Some(End::Aborted {
                    reason: AbortReason::ProducerGone { what: "seg" }
                }),
                Some(End::Aborted {
                    reason: AbortReason::ProducerGone { what: "seg 2" }
                })
            ],
            "next sees no verdict first, then each segment's abort"
        );
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
        assert_eq!(ch.recv(), Err(End::Complete));
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
        let (ch_c, closer_c) = Channel::from(rx_c, "c");
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
        build: impl Fn(Vec<Channel<u32, Finite>>) -> Channel<u32, Finite>,
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
            let mut ch: Channel<u32, Finite> = Channel::wrap(Box::new(Guarded {
                inner: inner.source,
                alive: LeafState::Alive {
                    rx: alive_rx,
                    close_rx: None,
                    what: "test guard",
                },
                fired: Arc::new(AtomicBool::new(false)),
            }));

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
            End::Complete
        }
    }

    #[test]
    #[should_panic(expected = "register pushed 0 ops but leaf_count() is 1")]
    fn register_leaf_count_mismatch_is_caught() {
        let mut ch: Channel<u32, Finite> = Channel::wrap(Box::new(UnderRegistering));
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
        assert_eq!(
            ch.end_reason(),
            End::Aborted {
                reason: AbortReason::ProducerGone { what: "test" }
            }
        );
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
                reason: AbortReason::RoutingViolation {
                    what: "routing test"
                }
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

    /// A clean death in one merge branch must not make `try_recv` report an
    /// empty batch while the survivor still holds items: it retries the
    /// survivor after collapsing to `One`.
    #[test]
    fn try_recv_retries_the_survivor_after_a_clean_branch_death() {
        let mut ch = Channel::from_vec(Vec::<u32>::new()).merge(Channel::from_vec(vec![1, 2]));
        assert_eq!(ch.try_recv(), Some(1));
        assert_eq!(ch.try_recv(), Some(2));
        assert_eq!(ch.try_recv(), None);
        assert_eq!(ch.end_reason(), End::Complete);
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
        assert_eq!(ch.recv(), Err(End::Complete));
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
            Err(End::Complete),
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
        let (mut ch, closer) = Channel::from(rx, "racy producer");
        drop(tx); // sender gone first — the ordering hazard
        assert_eq!(
            ch.end_reason(),
            End::Aborted {
                reason: AbortReason::ProducerGone {
                    what: "racy producer"
                }
            },
            "not closed yet, so fail closed"
        );
        closer.close();
        assert_eq!(
            ch.end_reason(),
            End::Complete,
            "a close arriving after the disconnect must still be honoured"
        );
    }

    /// An infinite stream has no `Closer`: ending is always an error, whatever
    /// the underlying reason. That is what drives a consumer's reconnect
    /// backoff rather than letting it look like a clean finish.
    #[test]
    fn infinite_stream_end_is_always_aborted() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let mut ch = Channel::from_infinite(rx, "socket");
        tx.send(1).unwrap();
        drop(tx); // peer closed politely — still an error to the app
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Err(AbortReason::ProducerGone { what: "socket" }));
        // Idempotent: a drain loop may ask again after the loop breaks.
        assert_eq!(
            ch.end_reason(),
            AbortReason::ProducerGone { what: "socket" }
        );
    }

    /// An infinite branch ending condemns the merge, so a connection-scoped
    /// stream cannot be mistaken for a completed one.
    #[test]
    fn merge_with_infinite_branch_reports_aborted() {
        let (tx_f, rx_f) = crossbeam_channel::bounded::<u32>(4);
        let (tx_i, rx_i) = crossbeam_channel::bounded::<u32>(4);
        let (ch_f, closer_f) = Channel::from(rx_f, "finite");
        drop(tx_f);
        closer_f.close();
        let ch_i = Channel::from_infinite(rx_i, "socket");
        drop(tx_i);
        let mut ch = ch_f.merge(ch_i);
        assert_eq!(ch.recv(), Err(AbortReason::ProducerGone { what: "socket" }));
    }

    #[test]
    fn closed_producer_yields_closed() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let (mut ch, closer) = Channel::from(rx, "finite scan");
        tx.send(1).unwrap();
        drop(tx);
        closer.close();
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    /// The whole point: a producer that vanished is not a producer that
    /// finished, even though the data channel looks identical.
    #[test]
    fn unclosed_producer_yields_aborted() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let (mut ch, closer) = Channel::from(rx, "finite scan");
        tx.send(1).unwrap();
        drop(tx);
        drop(closer); // dropped without close(): panicked, or forgot
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone {
                    what: "finite scan"
                }
            })
        );
    }

    /// A `spawn_blocking` body that panics drops its `Closer` while unwinding —
    /// the case that motivated all of this.
    #[test]
    fn panicking_producer_yields_aborted() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let (mut ch, closer) = Channel::from(rx, "scan");
        let handle = std::thread::spawn(move || {
            let _closer = closer;
            tx.send(1).unwrap();
            panic!("scan blew up");
        });
        assert!(handle.join().is_err());
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "scan" }
            })
        );
    }

    /// A task discarded before its body ever runs still drops the `Closer`, so
    /// the tokio-shutdown gap is covered too.
    #[test]
    fn producer_dropped_before_running_is_aborted() {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(4);
        let (mut ch, closer) = Channel::from(rx, "never ran");
        let never_ran = move || {
            let _closer = closer;
            let _tx = tx;
        };
        drop(never_ran); // the closure is discarded, never called
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "never ran" }
            })
        );
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
        let (ch_a, closer_a) = Channel::from(rx_a, "good");
        let (ch_b, closer_b) = Channel::from(rx_b, "bad");
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
                Err(End::Aborted {
                    reason: AbortReason::ProducerGone { what: "bad" },
                }) => break,
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
        let (ch_a, closer_a) = Channel::from(rx_a, "a");
        let (ch_b, closer_b) = Channel::from(rx_b, "b");
        drop(tx_a);
        drop(tx_b);
        closer_a.close();
        closer_b.close();
        let mut ch = ch_a.merge(ch_b);
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    /// A phase that died mid-chain condemns it immediately — the
    /// historical→live handoff must not launder a truncated replay behind live
    /// items that would make the stream look complete. The live phase's items
    /// are discarded the moment the historical phase reports `Aborted`.
    #[test]
    fn chain_reports_aborted_from_the_phase_that_died() {
        let (tx_h, rx_h) = crossbeam_channel::bounded(4);
        let (tx_l, rx_l) = crossbeam_channel::bounded(4);
        let (ch_h, closer_h) = Channel::from(rx_h, "historical");
        let (ch_l, closer_l) = Channel::from(rx_l, "live");
        tx_h.send(1).unwrap();
        drop(tx_h);
        drop(closer_h); // historical replay died
        tx_l.send(2).unwrap();
        drop(tx_l);
        closer_l.close();

        let mut ch = ch_h.chain(ch_l);
        assert_eq!(ch.recv(), Ok(1));
        // The historical phase aborts before the live phase is ever reached.
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "historical" }
            })
        );
    }

    /// The verdict survives a `filter_map` — the snapshot pipeline shape.
    #[test]
    fn abort_survives_filter_map() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        let (ch, closer) = Channel::from(rx, "window");
        tx.send(2u32).unwrap();
        drop(tx);
        drop(closer);
        let mut ch = ch.filter_map(|v: u32| Some(v.saturating_mul(10)));
        assert_eq!(ch.recv(), Ok(20));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "window" }
            })
        );
    }

    #[test]
    fn aborted_is_producer_gone() {
        let mut ch = Channel::<u32, Infinite>::aborted("test");
        assert_eq!(ch.recv(), Err(AbortReason::ProducerGone { what: "test" }));
    }

    #[test]
    fn empty_is_complete() {
        let mut ch = Channel::<u32, Finite>::empty();
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn once_yields_one_then_complete() {
        let mut ch = Channel::once(7u32);
        assert_eq!(ch.recv(), Ok(7));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn take_until_stops_before_the_match() {
        let mut ch = Channel::from_vec(vec![1u32, 2, 3, 4]).take_until(|v| *v == 3);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    /// Keyed by the tens digit: (10, 11) is one group. `n` counts items, so the
    /// group that reaches it is finished, and the first item of the next group
    /// ends the stream (and is dropped).
    #[test]
    fn take_at_least_finishes_the_group_that_reaches_n() {
        let n = std::num::NonZeroUsize::new(2).unwrap();
        let items = vec![
            Some(1u64),
            None,
            Some(10),
            Some(10),
            Some(10),
            Some(20),
            Some(30),
        ];
        let mut ch = Channel::from_vec(items).take_at_least::<u64>(n);
        let got: Vec<_> = std::iter::from_fn(|| ch.recv().ok()).collect();
        assert_eq!(got, vec![Some(1), None, Some(10), Some(10), Some(10)]);
        assert_eq!(ch.end_reason(), End::Complete);
    }

    #[test]
    fn take_at_least_below_n_keeps_the_source_verdict() {
        let n = std::num::NonZeroUsize::new(5).unwrap();
        let (tx, rx) = crossbeam_channel::bounded(4);
        tx.send(Some(1u64)).unwrap();
        drop(tx);
        let mut ch = Channel::from_infinite(rx, "short").take_at_least::<u64>(n);
        assert_eq!(ch.recv(), Ok(Some(1)));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "short" }
            })
        );
    }

    #[test]
    fn filter_above_drops_keys_at_or_below_the_cut() {
        let items = vec![Some(1u64), Some(2), None, Some(3), Some(4)];
        let mut ch = Channel::from_vec(items.clone()).filter_above::<u64>(Some(2));
        let got: Vec<_> = std::iter::from_fn(|| ch.recv().ok()).collect();
        assert_eq!(got, vec![None, Some(3), Some(4)]);
        let mut all = Channel::from_vec(items.clone()).filter_above::<u64>(None);
        let got: Vec<_> = std::iter::from_fn(|| all.recv().ok()).collect();
        assert_eq!(got, items);
    }

    #[test]
    fn filter_map_may_hold_state() {
        let mut seen = 0u32;
        let mut ch = Channel::from_vec(vec![5u32, 6, 7]).map(move |v| {
            seen += 1;
            (seen, v)
        });
        assert_eq!(ch.recv(), Ok((1, 5)));
        assert_eq!(ch.recv(), Ok((2, 6)));
    }

    #[test]
    fn ready_batch_returns_the_verdict() {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        drop(tx);
        let mut ch = Channel::from_infinite(rx, "gone");
        assert!(matches!(
            ch.ready_batch(4),
            Err(AbortReason::ProducerGone { what: "gone" })
        ));
    }

    #[test]
    fn dedup_by_key_drops_consecutive_duplicates() {
        let mut ch = Channel::from_vec(vec![Some(1u64), Some(1), None, Some(2), Some(2), Some(1)])
            .dedup_by_key::<u64>();
        assert_eq!(ch.recv(), Ok(Some(1)));
        assert_eq!(ch.recv(), Ok(None));
        assert_eq!(ch.recv(), Ok(Some(2)));
        assert_eq!(ch.recv(), Ok(Some(1)));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn from_vec_is_always_closed() {
        let mut ch = Channel::from_vec(vec![1u32]);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn take_while_stops_at_the_bound_with_complete() {
        let mut ch = Channel::from_vec(vec![1u32, 2, 3, 4]).take_while(|v| *v < 3);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Err(End::Complete));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn take_while_preserves_an_abort() {
        let (tx, rx) = crossbeam_channel::bounded(4);
        tx.send(1u32).unwrap();
        drop(tx);
        let mut ch = Channel::from_infinite(rx, "bounded").take_while(|_| true);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "bounded" },
            })
        );
    }

    #[test]
    fn take_while_advances_a_chain_phase() {
        let bounded = Channel::from_vec(vec![1u32, 2, 3]).take_while(|v| *v < 2);
        let mut ch = bounded.chain(Channel::from_vec(vec![9]));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(9));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn closed_constructor_drains_then_reports_closed() {
        let (tx, rx) = crossbeam_channel::bounded(2);
        tx.send(1u32).unwrap();
        tx.send(2).unwrap();
        drop(tx);
        let mut ch = Channel::closed(rx);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn chain_three_phases_keeps_order() {
        let mut ch = Channel::from_vec(vec![1u32])
            .chain(Channel::from_vec(vec![2]))
            .chain(Channel::from_vec(vec![3]));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Ok(3));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn chain_all_empty_is_a_closed_stream() {
        let mut ch: Channel<u32, Finite> = Channel::chain_all([]);
        assert_eq!(ch.recv(), Err(End::Complete));
        assert_eq!(ch.end_reason(), End::Complete);
    }

    #[test]
    fn chain_all_single_phase_is_that_stream() {
        let mut ch = Channel::chain_all([Channel::from_vec(vec![1u32, 2])]);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn chain_all_runs_phases_in_order_and_skips_empty_ones() {
        let mut ch = Channel::chain_all([
            Channel::from_vec(vec![1u32]),
            Channel::from_vec(Vec::new()),
            Channel::from_vec(vec![2]),
        ]);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    /// Like `chain`, `chain_all` condemns the rest of the phases on an abort:
    /// the third phase's item must never reach the consumer.
    #[test]
    fn chain_all_aborts_and_drops_the_remaining_phases() {
        let (tx_c, rx_c) = crossbeam_channel::bounded::<u32>(4);
        drop(tx_c);
        let mut ch = Channel::chain_all([
            Channel::from_vec(vec![1]),
            aborted_of(vec![2u32], "middle"),
            closed(rx_c),
        ]);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "middle" }
            })
        );
        assert_eq!(
            ch.end_reason(),
            End::Aborted {
                reason: AbortReason::ProducerGone { what: "middle" }
            }
        );
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
        let mut ch = Channel::from_vec(vec![10, 20, 30]);
        assert_eq!(ch.recv(), Ok(10));
        assert_eq!(ch.recv(), Ok(20));
        assert_eq!(ch.recv(), Ok(30));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn from_vec_empty_disconnects_immediately() {
        let mut ch: Channel<u32, Finite> = Channel::from_vec(vec![]);
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    // ── defer_async ────────────────────────────────────────────────────

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread")]
    async fn defer_async_starts_on_first_read_and_delegates_the_verdict() {
        let opened = Arc::new(AtomicUsize::new(0));
        let handle = tokio::runtime::Handle::current();
        let o = Arc::clone(&opened);
        let ch = Channel::defer_async(&handle, move || async move {
            o.fetch_add(1, Ordering::SeqCst);
            Channel::from_vec(vec![1u32, 2])
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            opened.load(Ordering::SeqCst),
            0,
            "nothing runs at construction"
        );
        let got = tokio::task::spawn_blocking(move || {
            let mut ch = ch;
            let items: Vec<u32> = std::iter::from_fn(|| ch.recv().ok()).collect();
            (items, ch.end_reason())
        })
        .await
        .unwrap();
        assert_eq!(got, (vec![1, 2], End::Complete));
        assert_eq!(opened.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread")]
    async fn deferred_phases_in_a_chain_open_one_at_a_time() {
        let open = Arc::new(AtomicUsize::new(0));
        let max_open = Arc::new(AtomicUsize::new(0));
        let handle = tokio::runtime::Handle::current();
        let phases: Vec<Channel<u32, Finite>> = (0..3u32)
            .map(|i| {
                let (open, max_open) = (Arc::clone(&open), Arc::clone(&max_open));
                Channel::defer_async(&handle, move || async move {
                    let now = open.fetch_add(1, Ordering::SeqCst) + 1;
                    max_open.fetch_max(now, Ordering::SeqCst);
                    let ch = Channel::from_vec(vec![i]);
                    open.fetch_sub(1, Ordering::SeqCst);
                    ch
                })
            })
            .collect();
        let got = tokio::task::spawn_blocking(move || {
            let mut ch = Channel::chain_all(phases);
            std::iter::from_fn(|| ch.recv().ok()).collect::<Vec<_>>()
        })
        .await
        .unwrap();
        assert_eq!(got, vec![0, 1, 2]);
        assert_eq!(
            max_open.load(Ordering::SeqCst),
            1,
            "never two openers at once"
        );
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_deferred_opener_that_dies_aborts_the_phase() {
        let handle = tokio::runtime::Handle::current();
        let ch: Channel<u32, Finite> =
            Channel::defer_async(&handle, || async { panic!("open blew up") });
        let end = tokio::task::spawn_blocking(move || {
            let mut ch = ch;
            ch.recv()
        })
        .await
        .unwrap();
        assert_eq!(
            end,
            Err(End::Aborted {
                reason: AbortReason::ProducerGone {
                    what: "defer_async"
                }
            })
        );
    }

    // ── until / Cancellation ───────────────────────────────────────────

    /// The point of `until`: a consumer parked in `recv` on a stream that has
    /// gone quiet — sender alive, nothing arriving — is woken by the cancel.
    #[test]
    fn until_wakes_a_blocked_recv_with_cancelled() {
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let (handle, cancellation) = CancelHandle::new();
        let mut ch = Channel::from_infinite(rx, "quiet").until(&cancellation);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(50));
                handle.cancel();
            });
            assert_eq!(ch.recv(), Err(AbortReason::Cancelled));
        });
        assert!(cancellation.is_cancelled());
        assert_eq!(ch.end_reason(), AbortReason::Cancelled);
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
        assert_eq!(ch.recv(), Err(End::Complete));
        handle.cancel();
        assert_eq!(ch.end_reason(), End::Complete);
    }

    /// A handle that goes away without `cancel` ends the stream at once, even
    /// with its source still alive — that source may be what whoever holds the
    /// stream keeps alive — but it is not a cancel.
    #[test]
    fn until_a_handle_dropped_unfired_ends_aborted() {
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let (handle, cancellation) = CancelHandle::new();
        let mut ch = Channel::from_infinite(rx, "quiet").until(&cancellation);
        drop(handle);
        assert_eq!(ch.recv(), Err(AbortReason::CancelHandleDropped));
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
            let mut ch = Channel::from_infinite(rx, "source").until(&cancellation);
            handle.cancel();
            drop(tx);
            assert_eq!(ch.recv(), Err(AbortReason::Cancelled));
        }
    }

    /// A stream bound after the cancel has already fired ends immediately.
    #[test]
    fn until_an_already_fired_cancellation_ends_at_once() {
        let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let (handle, cancellation) = CancelHandle::new();
        handle.cancel();
        handle.cancel(); // idempotent
        let mut ch = Channel::from_infinite(rx, "quiet").until(&cancellation);
        assert_eq!(ch.recv(), Err(AbortReason::Cancelled));
    }

    /// `Cancelled` condemns an enclosing merge like `Aborted` does: the
    /// surviving branch is not drained as though the stream were whole.
    #[test]
    fn a_cancelled_branch_condemns_a_merge() {
        let (_tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
        let (_tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
        let (handle, cancellation) = CancelHandle::new();
        let mut ch = Channel::from_infinite(rx_a, "a")
            .until(&cancellation)
            .merge(Channel::from_infinite(rx_b, "b"));
        handle.cancel();
        assert_eq!(ch.recv(), Err(AbortReason::Cancelled));
    }

    /// `never` binds nothing: the stream and its verdict are untouched.
    #[test]
    fn never_leaves_the_stream_untouched() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let mut ch = closed(rx).until(&Cancellation::never());
        tx.send(7).unwrap();
        drop(tx);
        assert_eq!(ch.recv(), Ok(7));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    /// `from_token` is the bridge from a tokio token to a blocked crossbeam
    /// `recv` on a blocking thread.
    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread")]
    async fn from_token_wakes_a_blocking_recv() {
        let token = tokio_util::sync::CancellationToken::new();
        let cancellation = Cancellation::from_token(&token);
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        let mut ch = Channel::from_infinite(rx, "quiet").until(&cancellation);
        let blocked = tokio::task::spawn_blocking(move || ch.recv());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!blocked.is_finished(), "recv returned before the cancel");
        token.cancel();
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), blocked)
            .await
            .unwrap() // the cancel must wake recv within the bound
            .unwrap();
        assert_eq!(got, Err(AbortReason::Cancelled));
        drop(tx);
    }
}
