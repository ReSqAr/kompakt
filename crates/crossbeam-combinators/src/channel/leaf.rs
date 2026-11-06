//! A single wrapped receiver and its explicit close.

use crossbeam_channel::{Receiver, Select, TryRecvError};

use super::{AbortReason, End, Poll, Source};

/// The lifecycle of a single wrapped `Receiver`. `Alive` holds the receiver and
/// the consumer's half of the explicit close (or `None` for an
/// [`Channel::from_infinite`] stream — no closer may exist, so any termination
/// is [`End::Aborted`]). When the data channel disconnects, the verdict is
/// resolved from `close_rx` and the state moves to `Resolved` — or `Pending`
/// if the closer has not spoken yet (the non-memoisation safety net, see
/// `verdict_is_not_memoised_while_the_closer_is_still_alive`).
pub(super) enum LeafState<T> {
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
    pub(super) fn poison(&mut self, ordinal: usize) {
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
