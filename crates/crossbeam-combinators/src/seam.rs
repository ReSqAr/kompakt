//! History, then live: the subscribe → cut → range seam.
//!
//! ```text
//!   live  = seam.subscribe()       first: from here on, nothing is missed
//!   cut   = seam.cut()             after the subscription
//!   live  = seam.above(live, cut)  drop what the history covers
//!   range = seam.range(cut)        finite: everything up to the cut
//!
//!   Segment::chain   range, then live         (a stream that starts in the past)
//!   Segment::merge   range ∥ live             (a fold that integrates both at once)
//! ```
//!
//! The order is the seam's whole correctness argument: anything at or below the
//! cut is in `range`; anything above it was appended after the cut was read,
//! hence after the subscription existed, hence is on `live`. The [`Seam`]
//! methods are hooks that only [`Segment::open`] calls, in that order — callers
//! never assemble a seam by hand. (`cut` stays callable on its own: every seam
//! is used from one place, and sealing the trait would stop a domain crate
//! implementing it.)

use crate::channel::Channel;
use crate::kind::{Finite, Infinite};

/// A source with a live tail and a history up to a cut. See the module docs:
/// these are hooks for [`Segment::open`], not an API to call directly.
pub trait Seam {
    /// The item both halves carry.
    type Item: Send + 'static;
    /// The snapshot marker read after subscribing.
    type Cut: Copy;

    /// Open the live subscription.
    fn subscribe(&self) -> Channel<Self::Item, Infinite>;
    /// Read the cut, after the subscription exists.
    fn cut(&self) -> Self::Cut;
    /// The live half with everything the history covers removed.
    fn above(
        &self,
        live: Channel<Self::Item, Infinite>,
        cut: Self::Cut,
    ) -> Channel<Self::Item, Infinite>;
    /// The history up to and including the cut; closes when it is served.
    fn range(&self, cut: Self::Cut) -> Channel<Self::Item, Finite>;
}

/// An opened seam: the live half is subscribed and the cut is read.
pub struct Segment<'a, S: Seam> {
    seam: &'a S,
    live: Channel<S::Item, Infinite>,
    cut: S::Cut,
}

impl<'a, S: Seam> Segment<'a, S> {
    /// Subscribe, then read the cut, then drop the live overlap — the one
    /// place the seam's order is fixed.
    #[must_use]
    pub fn open(seam: &'a S) -> Self {
        let live = seam.subscribe();
        let cut = seam.cut();
        let live = seam.above(live, cut);
        Self { seam, live, cut }
    }

    /// The cut this segment was opened at.
    #[must_use]
    pub fn cut(&self) -> S::Cut {
        self.cut
    }

    /// The history up to the cut, then the live tail.
    #[must_use]
    pub fn chain(self) -> Channel<S::Item, Infinite> {
        self.seam.range(self.cut).chain(self.live)
    }

    /// The history and the live tail at once.
    #[must_use]
    pub fn merge(self) -> Channel<S::Item, Infinite> {
        self.seam.range(self.cut).merge(self.live)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::AbortReason;

    /// Live items 11, 12 (above the cut 10) and 9 (an overlap `above` drops);
    /// history 1, 2. Records the order the hooks run in.
    struct Scripted {
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Seam for Scripted {
        type Item = u32;
        type Cut = u32;

        fn subscribe(&self) -> Channel<u32, Infinite> {
            self.calls.lock().unwrap().push("subscribe");
            let (tx, rx) = crossbeam_channel::bounded(4);
            for v in [9, 11, 12] {
                tx.send(v).unwrap();
            }
            drop(tx);
            Channel::from_infinite(rx, "live")
        }
        fn cut(&self) -> u32 {
            self.calls.lock().unwrap().push("cut");
            10
        }
        fn above(&self, live: Channel<u32, Infinite>, cut: u32) -> Channel<u32, Infinite> {
            self.calls.lock().unwrap().push("above");
            live.filter_map(move |v| (v > cut).then_some(v))
        }
        fn range(&self, cut: u32) -> Channel<u32, Finite> {
            self.calls.lock().unwrap().push("range");
            assert_eq!(cut, 10);
            Channel::from_vec(vec![1, 2])
        }
    }

    fn drain(mut ch: Channel<u32, Infinite>) -> (Vec<u32>, AbortReason) {
        let mut got = Vec::new();
        let end = loop {
            match ch.recv() {
                Ok(v) => got.push(v),
                Err(end) => break end,
            }
        };
        (got, end)
    }

    #[test]
    fn open_subscribes_before_reading_the_cut() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seam = Scripted {
            calls: Arc::clone(&calls),
        };
        let segment = Segment::open(&seam);
        assert_eq!(segment.cut(), 10);
        assert_eq!(*calls.lock().unwrap(), vec!["subscribe", "cut", "above"]);
        let (got, end) = drain(segment.chain());
        assert_eq!(got, vec![1, 2, 11, 12], "history, then live above the cut");
        assert_eq!(end, AbortReason::ProducerGone { what: "live" });
        assert_eq!(
            *calls.lock().unwrap(),
            vec!["subscribe", "cut", "above", "range"]
        );
    }

    #[test]
    fn merge_serves_both_halves() {
        let seam = Scripted {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let (mut got, _) = drain(Segment::open(&seam).merge());
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 11, 12]);
    }
}
