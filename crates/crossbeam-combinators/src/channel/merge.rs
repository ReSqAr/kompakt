//! Concurrent merge of two sources.

use crossbeam_channel::Select;

use super::{End, Poll, Source};

/// Which branch a `MergeState::Both` should drain from next. Alternation keeps
/// `drain_first` fair: `ready_batch` takes only its head through the fair
/// `recv`, and every subsequent item through `drain_first`, so a fixed bias
/// would drain one branch entirely before touching the other.
#[derive(Clone, Copy)]
pub(super) enum Side {
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
pub(super) enum MergeState<T> {
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
