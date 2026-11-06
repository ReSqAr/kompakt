//! Streams that reopen themselves: `Resumable` (with a position) and `resync`
//! (without).
//!
//! ```text
//!        ┌───────── make(after, First) ─────────┐
//!        ▼                                      │
//!   [segment open] ── item k ──> emit, after := k
//!        │
//!        ├─ Aborted(ProducerGone) ──> make(after, AfterProducerGone) ──┐
//!        │                                      ▲                      │
//!        │                                      └──── Some(seam) ──────┘
//!        ├─ make → None ──────────────> end: previous reason
//!        └─ any other abort ──────────> end: that reason
//! ```
//!
//! Each segment is `Segment::open(&seam).chain()`: the seam's history up to its
//! cut, then its live tail. The factory builds one seam per cycle, starting
//! **after** the position it is handed (always exclusive; `None` is the
//! beginning), and may decline with `None` when what it needs is gone.
//!
//! A new cycle opens only after `Aborted(ProducerGone)` — a fan-out hang-up or
//! a dead producer, which a fresh cycle repairs. A cancellation, an order
//! violation or any other end passes through unchanged, so a corrupt log
//! cannot spin the loop. There is no dedup: a factory that starts at the wrong
//! place is caught by the order policy, not repaired.

use crate::channel::Channel;
use crate::kind::Infinite;
use crate::ordered::{Key, OrderPolicy, OrderedChannel};
use crate::seam::{Seam, Segment};

/// Why a factory is being called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opening {
    /// The stream's first open.
    First,
    /// The previous cycle ended with `Aborted(ProducerGone { what })`.
    AfterProducerGone {
        /// The label of the source that went away.
        what: &'static str,
    },
}

/// A positioned stream that reopens itself after its producer goes away.
pub struct Resumable;

impl Resumable {
    /// `make(after, opening)` builds the seam for one cycle, starting after
    /// `after`: the caller's `after` at the first open, the last emitted key at
    /// a reopen. `P` checks every step, across cycles, from `after`.
    #[allow(
        clippy::new_ret_no_self,
        reason = "Resumable is a namespace for the constructor, not a value"
    )]
    #[must_use]
    pub fn new<T, P, S, F>(
        what: &'static str,
        after: Option<P::Key>,
        mut make: F,
    ) -> OrderedChannel<T, P, Infinite>
    where
        T: Send + 'static,
        P: OrderPolicy,
        P::Key: Key<T>,
        S: Seam<Item = T>,
        F: FnMut(Option<P::Key>, Opening) -> Option<S> + Send + 'static,
    {
        Channel::resume(what, after, track::<T, P::Key>, move |cursor, opening| {
            make(*cursor, opening).map(|seam| Segment::open(&seam).chain())
        })
        .ordered::<P>(after, what)
    }
}

fn track<T, Pos: Key<T> + Copy>(item: &T, cursor: &mut Option<Pos>) {
    if let Some(key) = Pos::key(item) {
        *cursor = Some(key);
    }
}

/// [`Resumable`] without a position: every cycle starts from the seam's own
/// beginning, and duplicates across cycles are the consumer's to handle.
#[must_use]
pub fn resync<S, F>(what: &'static str, mut make: F) -> Channel<S::Item, Infinite>
where
    S: Seam,
    F: FnMut(Opening) -> Option<S> + Send + 'static,
{
    Channel::resume(
        what,
        (),
        |_, ()| {},
        move |(), opening| make(opening).map(|seam| Segment::open(&seam).chain()),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::kind::Finite;
    use crate::{AbortReason, Contiguous, Successor};

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    struct Pos(u64);
    impl Key<u64> for Pos {
        fn key(v: &u64) -> Option<Self> {
            Some(Pos(*v))
        }
    }
    impl Successor for Pos {
        fn next_after(p: Option<Self>) -> Self {
            Pos(p.map_or(0, |p| p.0 + 1))
        }
    }

    type Call = (Option<Pos>, Opening);

    /// One cycle: history `range` (finite), then a live tail that hangs up
    /// (`ProducerGone`) or stays open with a cancel.
    struct Cycle {
        history: Vec<u64>,
        live: Vec<u64>,
    }

    impl Seam for Cycle {
        type Item = u64;
        type Cut = ();
        fn subscribe(&self) -> Channel<u64, Infinite> {
            let (tx, rx) = crossbeam_channel::bounded(16);
            for v in &self.live {
                tx.send(*v).unwrap();
            }
            drop(tx);
            Channel::from_infinite(rx, "live")
        }
        fn cut(&self) {}
        fn above(&self, live: Channel<u64, Infinite>, (): ()) -> Channel<u64, Infinite> {
            live
        }
        fn range(&self, (): ()) -> Channel<u64, Finite> {
            Channel::from_vec(self.history.clone())
        }
    }

    fn drain<E>(mut recv: impl FnMut() -> Result<u64, E>) -> Vec<u64> {
        std::iter::from_fn(|| recv().ok()).collect()
    }

    #[test]
    fn resumes_after_the_last_emitted_key_and_passes_the_opening() {
        let seen: Arc<Mutex<Vec<Call>>> = Arc::default();
        let log = Arc::clone(&seen);
        let mut cycles = 0;
        let mut ch = Resumable::new::<u64, Contiguous<Pos>, _, _>(
            "res",
            Some(Pos(9)),
            move |after, opening| {
                log.lock().unwrap().push((after, opening));
                cycles += 1;
                match cycles {
                    1 => Some(Cycle {
                        history: vec![10, 11],
                        live: vec![12],
                    }),
                    2 => Some(Cycle {
                        history: vec![13],
                        live: vec![14],
                    }),
                    _ => None,
                }
            },
        );
        assert_eq!(drain(|| ch.recv()), vec![10, 11, 12, 13, 14]);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                (Some(Pos(9)), Opening::First),
                (Some(Pos(12)), Opening::AfterProducerGone { what: "live" }),
                (Some(Pos(14)), Opening::AfterProducerGone { what: "live" }),
            ]
        );
        assert_eq!(
            ch.recv(),
            Err(AbortReason::ProducerGone { what: "live" }),
            "a decline after a cycle ends with that cycle's reason"
        );
    }

    #[test]
    fn declining_the_first_open_is_an_abort_under_the_label() {
        let mut ch = Resumable::new::<u64, Contiguous<Pos>, Cycle, _>("res", None, |_, _| None);
        assert_eq!(ch.recv(), Err(AbortReason::ProducerGone { what: "res" }));
    }

    #[test]
    fn a_factory_that_starts_in_the_wrong_place_is_caught_not_repaired() {
        let mut ch = Resumable::new::<u64, Contiguous<Pos>, _, _>("res", None, |_, _| {
            Some(Cycle {
                history: vec![0, 1],
                live: vec![],
            })
        });
        let mut got = Vec::new();
        let end = loop {
            match ch.recv() {
                Ok(v) => got.push(v),
                Err(e) => break e,
            }
        };
        // Cycle 1 emits 0, 1; the reopen repeats 0 and the policy ends it.
        assert_eq!(got, vec![0, 1]);
        assert!(matches!(end, AbortReason::OrderViolation { .. }), "{end:?}");
    }

    #[test]
    fn other_aborts_are_not_resumed() {
        let calls = Arc::new(Mutex::new(0));
        let c = Arc::clone(&calls);
        let (handle, cancellation) = crate::CancelHandle::new();
        let mut ch = resync("rs", move |_| {
            *c.lock().unwrap() += 1;
            Some(Cycle {
                history: vec![],
                live: vec![],
            })
        })
        .until(&cancellation);
        handle.cancel();
        assert_eq!(ch.recv(), Err(AbortReason::Cancelled));
        assert_eq!(*calls.lock().unwrap(), 1, "a cancel is not a reopen");
    }

    #[test]
    fn resync_reopens_without_a_cursor() {
        let mut cycles = 0;
        let mut ch = resync("rs", move |opening| {
            cycles += 1;
            assert_eq!(
                opening,
                if cycles == 1 {
                    Opening::First
                } else {
                    Opening::AfterProducerGone { what: "live" }
                }
            );
            (cycles <= 2).then(|| Cycle {
                history: vec![1],
                live: vec![2],
            })
        });
        assert_eq!(drain(|| ch.recv()), vec![1, 2, 1, 2]);
    }
}
