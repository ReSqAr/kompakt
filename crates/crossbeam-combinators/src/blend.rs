//! The subscribe → cut → range → live seam.
//!
//! A live subscription has to be opened **before** the cut is read; otherwise
//! an item racing the cut is both outside the history and absent from the live
//! tail — a silent gap. The two traits make that order the only one:
//!
//! ```text
//!   live = subscribe()        first
//!   cut  = snapshot()         after the subscription
//!   live = above(live, cut)   drop what the history covered
//!   history = range(cut)      finite, bounded at cut
//!   blend(history, live)      chain
//! ```
//!
//! The two halves use one item type chosen by the domain. Neither trait changes
//! it, and the blend does not inspect the cut.

use crate::channel::Channel;

/// A live tail and the cut it is anchored to.
pub trait Tail {
    /// The item flowing through both halves of the seam.
    type Item;
    /// The snapshot marker read after subscribing.
    type Cut: Copy;

    /// Open the live subscription. Called first by [`Tail::open`].
    fn subscribe(&self) -> Channel<Self::Item>;

    /// Read the cut. Called second by [`Tail::open`].
    fn cut(&self) -> Self::Cut;

    /// The live half with everything the history covered removed.
    ///
    /// A pure filter: the item type does not change. The cut is how the
    /// subscription recognises the overlap; the implementation may use its own
    /// state (a cursor, a watermark) instead.
    fn above(&self, live: Channel<Self::Item>, cut: Self::Cut) -> Channel<Self::Item>;

    /// The only ordering-safe way to obtain the live half and the cut.
    fn open(&self) -> (Channel<Self::Item>, Self::Cut) {
        let live = self.subscribe();
        let cut = self.cut();
        (self.above(live, cut), cut)
    }

    /// History up to the cut, then the live half — plus the cut, for a caller
    /// that has to log or measure the segment it just opened.
    ///
    /// This is [`blend_chain`] with the cut still in hand. Use it whenever a
    /// resuming loop needs both: opening the two halves by hand and calling
    /// `range(...).chain(live)` repeats the seam's one ordering rule outside
    /// the crate.
    fn blend<R>(&self, ranged: &R) -> (Channel<Self::Item>, Self::Cut)
    where
        Self: Sized,
        Self::Item: Send + 'static,
        R: Ranged<Self::Cut, Item = Self::Item>,
    {
        let (live, cut) = self.open();
        (ranged.range(cut).chain(live), cut)
    }
}

/// The history that covers everything up to a cut.
pub trait Ranged<C> {
    /// The item flowing through both halves of the seam.
    type Item;

    /// A finite stream covering the range through `cut`, closing only after it
    /// has been served. An abort is an incomplete range; a clean end is the
    /// domain's claim that it reached the cut.
    fn range(&self, cut: C) -> Channel<Self::Item>;
}

/// History, then live. The chain's phase change is the handoff.
pub fn blend_chain<T, R>(tail: &T, ranged: &R) -> Channel<T::Item>
where
    T: Tail,
    R: Ranged<T::Cut, Item = T::Item>,
    T::Item: Send + 'static,
{
    tail.blend(ranged).0
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::End;
    use crate::channel::AbortReason;

    /// Records the calls `Tail::open` makes, in order.
    struct Scripted {
        seen: Arc<Mutex<Vec<&'static str>>>,
        live: Vec<u32>,
        history: Vec<u32>,
    }

    impl Tail for Scripted {
        type Item = u32;
        type Cut = u32;

        fn subscribe(&self) -> Channel<u32> {
            self.seen.lock().unwrap().push("subscribe");
            Channel::from_vec(self.live.clone())
        }

        fn cut(&self) -> u32 {
            self.seen.lock().unwrap().push("cut");
            10
        }

        fn above(&self, live: Channel<u32>, cut: u32) -> Channel<u32> {
            self.seen.lock().unwrap().push("above");
            live.filter_map(move |v| (v > cut).then_some(v))
        }
    }

    impl Ranged<u32> for Scripted {
        type Item = u32;

        fn range(&self, cut: u32) -> Channel<u32> {
            assert_eq!(cut, 10, "range receives the cut that open read");
            self.seen.lock().unwrap().push("range");
            Channel::from_vec(self.history.clone())
        }
    }

    fn drain(mut ch: Channel<u32>) -> Vec<u32> {
        let mut out = Vec::new();
        while let Ok(v) = ch.recv() {
            out.push(v);
        }
        out
    }

    fn end_of(mut ch: Channel<u32>) -> End {
        loop {
            match ch.recv() {
                Ok(_) => {}
                Err(end) => return end,
            }
        }
    }

    #[test]
    fn open_calls_subscribe_then_cut_then_above() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seam = Scripted {
            seen: Arc::clone(&seen),
            live: vec![11],
            history: vec![1],
        };
        let (mut live, cut) = seam.open();
        assert_eq!(cut, 10);
        assert_eq!(
            live.try_recv(),
            Some(11),
            "above keeps the live side above cut"
        );
        assert_eq!(live.try_recv(), None);
        assert_eq!(*seen.lock().unwrap(), vec!["subscribe", "cut", "above"]);
    }

    #[test]
    fn blend_chain_is_history_then_live() {
        let seam = Scripted {
            seen: Arc::new(Mutex::new(Vec::new())),
            live: vec![9, 11, 12],
            history: vec![1, 2],
        };
        assert_eq!(drain(blend_chain(&seam, &seam)), vec![1, 2, 11, 12]);
    }

    #[test]
    fn blend_returns_the_segment_and_its_cut() {
        let seam = Scripted {
            seen: Arc::new(Mutex::new(Vec::new())),
            live: vec![11, 12],
            history: vec![1, 2],
        };
        let (segment, cut) = seam.blend(&seam);
        assert_eq!(cut, 10);
        assert_eq!(drain(segment), vec![1, 2, 11, 12]);
    }

    #[test]
    fn an_aborted_history_condemns_the_chain() {
        struct Seam {
            fail_history: bool,
            fail_live: bool,
        }
        impl Tail for Seam {
            type Item = u32;
            type Cut = ();
            fn subscribe(&self) -> Channel<u32> {
                if self.fail_live {
                    let (tx, rx) = crossbeam_channel::bounded(1);
                    drop(tx);
                    Channel::from_infinite(rx, "live")
                } else {
                    Channel::from_vec(vec![2])
                }
            }
            fn cut(&self) {}
            fn above(&self, live: Channel<u32>, _cut: ()) -> Channel<u32> {
                live
            }
        }
        impl Ranged<()> for Seam {
            type Item = u32;
            fn range(&self, _: ()) -> Channel<u32> {
                if self.fail_history {
                    let (tx, rx) = crossbeam_channel::bounded(1);
                    drop(tx);
                    Channel::from_infinite(rx, "history")
                } else {
                    Channel::from_vec(vec![1])
                }
            }
        }

        let history = Seam {
            fail_history: true,
            fail_live: false,
        };
        assert_eq!(
            end_of(blend_chain(&history, &history)),
            End::Aborted {
                reason: AbortReason::ProducerGone { what: "history" }
            }
        );
        let live = Seam {
            fail_history: false,
            fail_live: true,
        };
        assert_eq!(
            end_of(blend_chain(&live, &live)),
            End::Aborted {
                reason: AbortReason::ProducerGone { what: "live" }
            }
        );
    }

    #[test]
    fn a_domain_done_item_stays_in_the_history_half() {
        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        enum Item {
            Data(u32),
            Done,
        }

        struct Seam;
        impl Tail for Seam {
            type Item = Item;
            type Cut = ();
            fn subscribe(&self) -> Channel<Item> {
                Channel::from_vec(vec![Item::Data(2)])
            }
            fn cut(&self) {}
            fn above(&self, live: Channel<Item>, _cut: ()) -> Channel<Item> {
                live
            }
        }
        impl Ranged<()> for Seam {
            type Item = Item;
            fn range(&self, _: ()) -> Channel<Item> {
                Channel::from_vec(vec![Item::Data(1), Item::Done])
            }
        }

        let got: Vec<Item> = blend_chain(&Seam, &Seam).into_iter().collect();
        assert_eq!(got, vec![Item::Data(1), Item::Done, Item::Data(2)]);
    }
}
