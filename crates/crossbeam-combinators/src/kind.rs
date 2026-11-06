//! The kind of a stream: whether it may end cleanly.
//!
//! ```text
//!   Channel<T, Finite>     recv -> Result<T, End>          Complete is a clean end
//!   Channel<T, Infinite>   recv -> Result<T, AbortReason>  any end is an error
//! ```
//!
//! There is no default: every signature names its kind, so a finite stream
//! cannot stand in for a subscription (`Channel::empty()` where an infinite
//! channel is owed does not compile). The rules a combinator follows:
//!
//! ```text
//!   map, filter_map, filter_above, dedup_by_key, until    keep the kind
//!   take_while, take_until, take_at_least                → Finite (a bound is completion)
//!   Finite.chain(K)                                      → K   (Infinite has no chain)
//!   merge                                                → Infinite if either side is
//! ```
//!
//! What the types forbid:
//!
//! ```compile_fail
//! use crossbeam_combinators::{Channel, Infinite};
//! // A finite stream cannot stand in for a subscription.
//! fn subscribe() -> Channel<u32, Infinite> {
//!     Channel::empty()
//! }
//! ```
//!
//! ```compile_fail
//! use crossbeam_combinators::{Channel, Infinite};
//! // An infinite stream never reaches a `chain` argument, so it has no `chain`.
//! fn after(live: Channel<u32, Infinite>) {
//!     let _ = live.chain(Channel::from_vec(vec![1]));
//! }
//! ```
//!
//! ```compile_fail
//! # use crossbeam_combinators::{Channel, Contiguous, Infinite, Key, Successor};
//! # #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
//! # struct Pos(u64);
//! # impl Key<u64> for Pos { fn key(v: &u64) -> Option<Self> { Some(Pos(*v)) } }
//! # impl Successor for Pos {
//! #     fn next_after(p: Option<Self>) -> Self { Pos(p.map_or(0, |p| p.0 + 1)) }
//! # }
//! // `covers` checks an end, and an infinite stream has none.
//! fn window(live: Channel<u64, Infinite>) {
//!     let _ = live.ordered::<Contiguous<Pos>>(None, "w").covers(..Pos(5), "w");
//! }
//! ```
//!
//! ```
//! # use crossbeam_combinators::{Contiguous, Key, Successor};
//! # #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
//! # struct Pos(u64);
//! # impl Key<u64> for Pos { fn key(v: &u64) -> Option<Self> { Some(Pos(*v)) } }
//! # impl Successor for Pos {
//! #     fn next_after(p: Option<Self>) -> Self { Pos(p.map_or(0, |p| p.0 + 1)) }
//! # }
//! # fn window(w: crossbeam_combinators::Channel<u64, crossbeam_combinators::Finite>) {
//! #     let _ = w.ordered::<Contiguous<Pos>>(None, "w").covers(..Pos(5), "w");
//! # }
//! use crossbeam_combinators::{AbortReason, Channel, Finite, Infinite};
//! // A bound turns an infinite stream finite; its read verdicts differ by kind.
//! fn bounded(live: Channel<u32, Infinite>) -> Channel<u32, Finite> {
//!     live.take_until(|v| *v > 9)
//! }
//! let mut gone = Channel::<u32, Infinite>::aborted("gone");
//! assert_eq!(gone.recv(), Err(AbortReason::ProducerGone { what: "gone" }));
//! ```
//!
//! `Source` still reports [`End`] internally. An infinite channel maps an inner
//! `Complete` — which its sources never produce — to
//! `Aborted(ProducerGone)` behind a `debug_assert!`, so the public type never
//! lies.

use crossbeam_channel::Receiver;

use crate::channel::{AbortReason, Channel, Closer, End};
use crate::feed::Closed;

mod sealed {
    pub trait Sealed {}
}

/// A stream that may end cleanly: `Complete` is a declared finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Finite;

/// A stream that never completes: ending at all is an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Infinite;

impl sealed::Sealed for Finite {}
impl sealed::Sealed for Infinite {}

/// The closed set of stream kinds: [`Finite`] and [`Infinite`].
pub trait Kind: sealed::Sealed + Send + Sync + 'static {
    /// The verdict a read returns when the stream has ended.
    type End: Copy + std::fmt::Debug + PartialEq + Eq + Send;
    /// What a producer body returns: the close proof, or nothing to prove.
    type Proof: Send + 'static;

    #[doc(hidden)]
    fn end(end: End) -> Self::End;

    #[doc(hidden)]
    fn open<T: Send + 'static>(
        rx: Receiver<T>,
        what: &'static str,
    ) -> (Channel<T, Self>, Option<Closer>)
    where
        Self: Sized;
}

impl Kind for Finite {
    type End = End;
    type Proof = Closed;

    fn end(end: End) -> End {
        end
    }

    fn open<T: Send + 'static>(
        rx: Receiver<T>,
        what: &'static str,
    ) -> (Channel<T, Self>, Option<Closer>) {
        let (ch, closer) = Channel::from(rx, what);
        (ch, Some(closer))
    }
}

impl Kind for Infinite {
    type End = AbortReason;
    type Proof = ();

    fn end(end: End) -> AbortReason {
        match end {
            End::Aborted { reason } => reason,
            End::Complete => {
                debug_assert!(false, "an infinite stream's source reported Complete");
                AbortReason::ProducerGone {
                    what: "infinite stream",
                }
            }
        }
    }

    fn open<T: Send + 'static>(
        rx: Receiver<T>,
        what: &'static str,
    ) -> (Channel<T, Self>, Option<Closer>) {
        (Channel::from_infinite(rx, what), None)
    }
}

/// The kind of `merge`: finite only when both sides are.
pub trait MergeKind<Other: Kind>: Kind {
    type Out: Kind;
}

impl MergeKind<Finite> for Finite {
    type Out = Finite;
}
impl MergeKind<Infinite> for Finite {
    type Out = Infinite;
}
impl MergeKind<Finite> for Infinite {
    type Out = Infinite;
}
impl MergeKind<Infinite> for Infinite {
    type Out = Infinite;
}
