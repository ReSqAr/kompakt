//! Ordered channels: a position-keyed stream with an explicit policy.
//!
//! `OrderedChannel<T, P>` wraps a [`Channels<T>`] with a policy marker `P`
//! (`Gapless<K>`, `StrictlyMonotone<K>`, `NonDecreasing<K>`). `K: Key<T>`
//! extracts the position from an item; `None` means the item does not
//! participate in the order (a watermark or an error item).
//!
//! The policy is enforced in the wrapped source, so `recv`, `try_recv` and
//! `ready_batch` all see the same check. A violation ends the stream with
//! [`AbortReason::OrderViolation`]; a permitted forward gap is passed through.
//!
//! `stitch` is the one transition that strengthens a policy: it fills gaps from
//! a fallback and returns a `Gapless` channel.

use std::marker::PhantomData;

use crate::channel::{AbortReason, Channels, End, Poll, ReadyBatch, Source};

/// The position of an item in an ordered stream.
///
/// `None` means the item is not positional and passes through without advancing
/// the order. Implement this for the key type, not for the item, because the
/// same item type can be ordered by different keys.
pub trait Key<T>: Copy + Ord + Send + 'static {
    fn key(item: &T) -> Option<Self>;
}

/// The next position for a gapless key.
///
/// `Hlc` is ordered but not gapless; it implements `Key`/`Ord`, not this.
pub trait Successor: Copy {
    fn next(self) -> Self;
}

/// A gap in an ordered stream: `[expected, got)` was absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gap<K> {
    pub expected: K,
    pub got: K,
}

/// The internal state a policy advances over.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct OrderState<K> {
    pub(crate) start: Option<K>,
    pub(crate) last: Option<K>,
}

/// The closed set of order policies.
///
/// Sealed on purpose: the markers below are the whole set.
pub trait OrderPolicy: sealed::Sealed + Send + 'static {
    type Key: Copy + Ord + Send + 'static;

    #[doc(hidden)]
    const LABEL: &'static str;

    #[doc(hidden)]
    fn check(state: &mut OrderState<Self::Key>, key: Self::Key) -> bool;
}

mod sealed {
    pub trait Sealed {}
}

/// Every key must be the successor of the previous one.
pub struct Gapless<K>(PhantomData<K>);

/// Keys are strictly increasing; gaps are allowed.
pub struct StrictlyMonotone<K>(PhantomData<K>);

/// Keys are non-decreasing; gaps and duplicates are allowed.
pub struct NonDecreasing<K>(PhantomData<K>);

impl<K: Successor + Ord + Send + 'static> sealed::Sealed for Gapless<K> {}
impl<K: Ord + Copy + Send + 'static> sealed::Sealed for StrictlyMonotone<K> {}
impl<K: Ord + Copy + Send + 'static> sealed::Sealed for NonDecreasing<K> {}

impl<K: Successor + Ord + Send + 'static> OrderPolicy for Gapless<K> {
    type Key = K;
    const LABEL: &'static str = "gapless";

    fn check(state: &mut OrderState<K>, key: K) -> bool {
        let expected = state.last.map(Successor::next).or(state.start);
        if expected.is_some_and(|expected| key != expected) {
            return false;
        }
        state.last = Some(key);
        true
    }
}

impl<K: Ord + Copy + Send + 'static> OrderPolicy for StrictlyMonotone<K> {
    type Key = K;
    const LABEL: &'static str = "strictly monotone";

    fn check(state: &mut OrderState<K>, key: K) -> bool {
        match state.last {
            Some(last) if key <= last => false,
            None if state.start.is_some_and(|start| key < start) => false,
            _ => {
                state.last = Some(key);
                true
            }
        }
    }
}

impl<K: Ord + Copy + Send + 'static> OrderPolicy for NonDecreasing<K> {
    type Key = K;
    const LABEL: &'static str = "non-decreasing";

    fn check(state: &mut OrderState<K>, key: K) -> bool {
        match state.last {
            Some(last) if key < last => false,
            None if state.start.is_some_and(|start| key < start) => false,
            _ => {
                state.last = Some(key);
                true
            }
        }
    }
}

struct OrderSource<T, P>
where
    P: OrderPolicy,
{
    inner: Option<Box<dyn Source<T>>>,
    state: OrderState<P::Key>,
    done: Option<End>,
    _policy: PhantomData<P>,
}

impl<T, P> OrderSource<T, P>
where
    T: Send + 'static,
    P: OrderPolicy,
    P::Key: Key<T>,
{
    fn new(inner: Box<dyn Source<T>>, start: Option<P::Key>) -> Self {
        Self {
            inner: Some(inner),
            state: OrderState { start, last: None },
            done: None,
            _policy: PhantomData,
        }
    }

    fn violation(&mut self) -> Poll<T> {
        self.done = Some(End::Aborted {
            reason: AbortReason::OrderViolation { what: P::LABEL },
        });
        self.inner = None;
        Poll::Dead
    }
}

impl<T, P> Source<T> for OrderSource<T, P>
where
    T: Send + 'static,
    P: OrderPolicy,
    P::Key: Key<T>,
{
    fn register<'a>(&'a mut self, sel: &mut crossbeam_channel::Select<'a>) -> usize {
        self.inner.as_mut().map_or(0, |src| src.register(sel))
    }

    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        let Some(mut inner) = self.inner.take() else {
            return Poll::Dead;
        };
        match inner.poll(ordinal) {
            Poll::Got(item) => {
                if P::Key::key(&item).is_none_or(|key| P::check(&mut self.state, key)) {
                    self.inner = Some(inner);
                    Poll::Got(item)
                } else {
                    self.violation()
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
            Some(item) => {
                if P::Key::key(&item).is_none_or(|key| P::check(&mut self.state, key)) {
                    self.inner = Some(inner);
                    Some(item)
                } else {
                    self.done = Some(End::Aborted {
                        reason: AbortReason::OrderViolation { what: P::LABEL },
                    });
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

/// A [`Channels`] whose position policy is enforced and named in the type.
///
/// The read surface delegates to the inner channel, so this is a drop-in
/// replacement for `recv`/`try_recv`/`ready_batch`/`end_reason`.
pub struct OrderedChannel<T, P>
where
    P: OrderPolicy,
{
    inner: Channels<T>,
    start: Option<P::Key>,
    _policy: PhantomData<P>,
}

impl<T, P> OrderedChannel<T, P>
where
    T: Send + 'static,
    P: OrderPolicy,
    P::Key: Key<T>,
{
    /// Wrap `inner` with policy `P`. `start` is the first expected/lower-bound
    /// key; `None` means no expectation.
    #[must_use]
    pub fn new(inner: Channels<T>, start: Option<P::Key>) -> Self {
        let source = OrderSource::<T, P>::new(inner.source, start);
        Self {
            inner: Channels {
                source: Box::new(source),
            },
            start,
            _policy: PhantomData,
        }
    }

    pub fn recv(&mut self) -> Result<T, End> {
        self.inner.recv()
    }

    pub fn try_recv(&mut self) -> Option<T> {
        self.inner.try_recv()
    }

    pub fn ready_batch(&mut self, max: usize) -> Option<ReadyBatch<'_, T>> {
        self.inner.ready_batch(max)
    }

    pub fn end_reason(&mut self) -> End {
        self.inner.end_reason()
    }

    /// Give up the type-level order guarantee and keep the already-checked
    /// channel.
    #[must_use]
    pub fn into_channels(self) -> Channels<T> {
        self.inner
    }

    #[cfg(feature = "tokio")]
    #[must_use = "a stream does nothing unless polled"]
    pub fn into_stream(self) -> futures::stream::BoxStream<'static, T> {
        self.inner.into_stream()
    }
}

impl<T, K> OrderedChannel<T, StrictlyMonotone<K>>
where
    T: Send + 'static,
    K: Key<T> + Successor,
{
    /// Fill forward gaps from `fallback`, strengthening `StrictlyMonotone` to
    /// `Gapless`.
    ///
    /// The fallback covers `[gap.expected, gap.got)`; the primary item at
    /// `gap.got` is buffered and emitted after the fallback. A short, overlong,
    /// gapped, or aborted fallback fails the stitched stream.
    #[must_use]
    pub fn stitch<F>(self, fallback: F) -> OrderedChannel<T, Gapless<K>>
    where
        F: FnMut(Gap<K>) -> Channels<T> + Send + 'static,
    {
        let stitched = StitchSource {
            primary: Some(self.inner.source),
            fallback,
            fallback_source: None,
            pending: None,
            start: self.start,
            last: None,
            fallback_last: None,
            gap_expected: None,
            gap_got: None,
            done: None,
            _key: PhantomData,
        };
        let inner = Channels {
            source: Box::new(stitched),
        };
        // `StitchSource` already returns a gapless sequence. The outer
        // Gapless check is deliberate defence-in-depth: do not remove it as
        // "redundant" without preserving the same guarantee another way.
        OrderedChannel::<T, Gapless<K>>::new(inner, self.start)
    }
}

struct StitchSource<T, K, F>
where
    K: Key<T> + Successor,
{
    primary: Option<Box<dyn Source<T>>>,
    fallback: F,
    fallback_source: Option<Box<dyn Source<T>>>,
    pending: Option<T>,
    start: Option<K>,
    last: Option<K>,
    fallback_last: Option<K>,
    gap_expected: Option<K>,
    gap_got: Option<K>,
    done: Option<End>,
    _key: PhantomData<K>,
}

enum PrimaryStep<T> {
    Emit(T),
    Gap,
    Failed,
}

impl<T, K, F> StitchSource<T, K, F>
where
    T: Send + 'static,
    K: Key<T> + Successor,
    F: FnMut(Gap<K>) -> Channels<T> + Send + 'static,
{
    fn expected(&self) -> Option<K> {
        self.last.map(Successor::next).or(self.start)
    }

    fn fail(&mut self, what: &'static str) {
        self.done = Some(End::Aborted {
            reason: AbortReason::OrderViolation { what },
        });
        self.primary = None;
        self.fallback_source = None;
        self.pending = None;
    }

    fn primary_item(&mut self, item: T) -> PrimaryStep<T> {
        let Some(got) = K::key(&item) else {
            return PrimaryStep::Emit(item);
        };
        let expected = self.expected();
        if let Some(expected) = expected {
            if got < expected {
                self.fail("stitch primary");
                return PrimaryStep::Failed;
            }
            if got > expected {
                self.pending = Some(item);
                self.gap_expected = Some(expected);
                self.gap_got = Some(got);
                self.fallback_last = None;
                self.fallback_source = Some((self.fallback)(Gap { expected, got }).source);
                return PrimaryStep::Gap;
            }
        }
        self.last = Some(got);
        PrimaryStep::Emit(item)
    }

    fn fallback_item(&mut self, item: T) -> Result<T, ()> {
        let Some(got) = K::key(&item) else {
            // No-key items (errors, watermarks) pass through; they do not
            // advance coverage, so the endpoint check still has to pass.
            return Ok(item);
        };
        if self.gap_got.is_some_and(|end| got >= end) || Some(got) != self.gap_expected {
            self.fail("stitch fallback");
            return Err(());
        }
        self.fallback_last = Some(got);
        self.gap_expected = Some(got.next());
        Ok(item)
    }

    fn finish_fallback(&mut self) -> Option<T> {
        let Some(got) = self.gap_got else {
            self.fail("stitch fallback");
            return None;
        };
        let covered = self.fallback_last.is_some_and(|last| last.next() == got);
        if !covered {
            self.fail("stitch fallback");
            return None;
        }
        let item = self.pending.take();
        if item.is_none() {
            self.fail("stitch fallback");
            return None;
        }
        self.last = Some(got);
        self.gap_expected = None;
        self.gap_got = None;
        self.fallback_last = None;
        self.fallback_source = None;
        item
    }

    fn primary_end(&mut self, end: End) -> Poll<T> {
        self.done = Some(end);
        self.primary = None;
        self.pending = None;
        Poll::Dead
    }

    fn fallback_end(&mut self, end: End) -> Poll<T> {
        match end {
            End::Complete => match self.finish_fallback() {
                Some(item) => Poll::Got(item),
                None => Poll::Dead,
            },
            End::Aborted { .. } => {
                self.done = Some(end);
                self.primary = None;
                self.fallback_source = None;
                self.pending = None;
                Poll::Dead
            }
        }
    }
}

impl<T, K, F> Source<T> for StitchSource<T, K, F>
where
    T: Send + 'static,
    K: Key<T> + Successor,
    F: FnMut(Gap<K>) -> Channels<T> + Send + 'static,
{
    fn register<'a>(&'a mut self, sel: &mut crossbeam_channel::Select<'a>) -> usize {
        if let Some(fallback) = self.fallback_source.as_mut() {
            fallback.register(sel)
        } else {
            self.primary.as_mut().map_or(0, |src| src.register(sel))
        }
    }

    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        if let Some(mut fallback) = self.fallback_source.take() {
            let poll = match fallback.poll(ordinal) {
                Poll::Got(item) => match self.fallback_item(item) {
                    Ok(item) => {
                        self.fallback_source = Some(fallback);
                        Poll::Got(item)
                    }
                    Err(()) => Poll::Dead,
                },
                Poll::Filtered => {
                    self.fallback_source = Some(fallback);
                    Poll::Filtered
                }
                Poll::Empty => {
                    self.fallback_source = Some(fallback);
                    Poll::Empty
                }
                Poll::Dead => {
                    let end = fallback.end_reason();
                    self.fallback_end(end)
                }
            };
            return poll;
        }

        let Some(mut primary) = self.primary.take() else {
            return Poll::Dead;
        };
        match primary.poll(ordinal) {
            Poll::Got(item) => match self.primary_item(item) {
                PrimaryStep::Emit(item) => {
                    self.primary = Some(primary);
                    Poll::Got(item)
                }
                PrimaryStep::Gap => {
                    self.primary = Some(primary);
                    // A fallback can be born with no live leaves, e.g.
                    // `chain_all([])`. `recv` would then see leaf_count 0 and
                    // read the empty fallback's Complete as the stream's end,
                    // silently dropping the buffered item. Close it through the
                    // same path as a drained fallback so coverage is checked.
                    let Some(mut fallback) = self.fallback_source.take() else {
                        return Poll::Filtered;
                    };
                    if fallback.leaf_count() == 0 {
                        let end = fallback.end_reason();
                        self.fallback_end(end)
                    } else {
                        self.fallback_source = Some(fallback);
                        Poll::Filtered
                    }
                }
                PrimaryStep::Failed => Poll::Dead,
            },
            Poll::Filtered => {
                self.primary = Some(primary);
                Poll::Filtered
            }
            Poll::Empty => {
                self.primary = Some(primary);
                Poll::Empty
            }
            Poll::Dead => {
                let end = primary.end_reason();
                self.primary_end(end)
            }
        }
    }

    fn drain_first(&mut self) -> Option<T> {
        loop {
            if self.fallback_source.is_some() {
                let mut fallback = self.fallback_source.take()?;
                match fallback.drain_first() {
                    Some(item) => match self.fallback_item(item) {
                        Ok(item) => {
                            self.fallback_source = Some(fallback);
                            return Some(item);
                        }
                        Err(()) => return None,
                    },
                    None => {
                        if fallback.leaf_count() == 0 {
                            let end = fallback.end_reason();
                            return match self.fallback_end(end) {
                                Poll::Got(item) => Some(item),
                                _ => None,
                            };
                        }
                        self.fallback_source = Some(fallback);
                        return None;
                    }
                }
            }

            let mut primary = self.primary.take()?;
            match primary.drain_first() {
                Some(item) => match self.primary_item(item) {
                    PrimaryStep::Emit(item) => {
                        self.primary = Some(primary);
                        return Some(item);
                    }
                    PrimaryStep::Gap => {
                        self.primary = Some(primary);
                        // The fallback is now active; drain it on the next
                        // iteration rather than polling the primary past the
                        // buffered gap-triggering item. If it has no live
                        // leaves, close it now so its Complete cannot be read
                        // as the stitched stream's end.
                        let Some(mut fallback) = self.fallback_source.take() else {
                            continue;
                        };
                        if fallback.leaf_count() == 0 {
                            let end = fallback.end_reason();
                            return match self.fallback_end(end) {
                                Poll::Got(item) => Some(item),
                                _ => None,
                            };
                        }
                        self.fallback_source = Some(fallback);
                        continue;
                    }
                    PrimaryStep::Failed => return None,
                },
                None => {
                    if primary.leaf_count() == 0 {
                        let end = primary.end_reason();
                        self.primary_end(end);
                        return None;
                    }
                    self.primary = Some(primary);
                    return None;
                }
            }
        }
    }

    fn leaf_count(&self) -> usize {
        self.fallback_source
            .as_ref()
            .or(self.primary.as_ref())
            .map_or(0, |src| src.leaf_count())
    }

    fn end_reason(&mut self) -> End {
        match self.done {
            Some(end) => end,
            None => self
                .fallback_source
                .as_mut()
                .or(self.primary.as_mut())
                .map_or(End::Complete, |src| src.end_reason()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;
    use crate::Channels;

    impl Key<u64> for u64 {
        fn key(item: &u64) -> Option<Self> {
            Some(*item)
        }
    }

    impl Key<Option<u64>> for u64 {
        fn key(item: &Option<u64>) -> Option<Self> {
            *item
        }
    }

    impl Successor for u64 {
        fn next(self) -> Self {
            self.checked_add(1)
                .unwrap_or_else(|| panic!("test key exhausted"))
        }
    }

    fn strict(items: Vec<u64>, start: Option<u64>) -> OrderedChannel<u64, StrictlyMonotone<u64>> {
        OrderedChannel::new(Channels::from_vec(items), start)
    }

    #[test]
    fn strictly_monotone_accepts_forward_gaps() {
        let mut ch = strict(vec![1, 3, 4], Some(1));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(3));
        assert_eq!(ch.recv(), Ok(4));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn strictly_monotone_rejects_duplicate() {
        let mut ch = strict(vec![1, 2, 2], Some(1));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::OrderViolation {
                    what: "strictly monotone"
                }
            })
        );
    }

    #[test]
    fn strictly_monotone_rejects_leading_regression() {
        let mut ch = strict(vec![0, 1], Some(1));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::OrderViolation {
                    what: "strictly monotone"
                }
            })
        );
    }

    #[test]
    fn gapless_rejects_a_gap() {
        let mut ch =
            OrderedChannel::<_, Gapless<u64>>::new(Channels::from_vec(vec![1, 3]), Some(1));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::OrderViolation { what: "gapless" }
            })
        );
    }

    #[test]
    fn non_decreasing_allows_duplicates() {
        let mut ch = OrderedChannel::<_, NonDecreasing<u64>>::new(
            Channels::from_vec(vec![1, 1, 2]),
            Some(1),
        );
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn no_key_items_pass_without_advancing() {
        let mut ch = OrderedChannel::<_, Gapless<u64>>::new(
            Channels::from_vec(vec![Some(1), None, Some(2)]),
            Some(1),
        );
        assert_eq!(ch.recv(), Ok(Some(1)));
        assert_eq!(ch.recv(), Ok(None));
        assert_eq!(ch.recv(), Ok(Some(2)));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn batch_and_projection_keep_the_order_source() {
        let ordered = strict(vec![1, 2, 3], Some(1));
        let mut ch = ordered.into_channels();
        let batch: Vec<u64> = ch.ready_batch(2).unwrap().collect();
        assert_eq!(batch, vec![1, 2]);
        let rest: Vec<u64> = std::iter::from_fn(|| ch.recv().ok()).collect();
        assert_eq!(rest, vec![3]);
        assert_eq!(ch.end_reason(), End::Complete);
    }

    #[test]
    fn stitch_fills_a_leading_gap() {
        let mut ch = strict(vec![3, 4], Some(1)).stitch(|gap| {
            assert_eq!(
                gap,
                Gap {
                    expected: 1,
                    got: 3
                }
            );
            Channels::from_vec(vec![1, 2])
        });
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Ok(3));
        assert_eq!(ch.recv(), Ok(4));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn stitch_fills_the_gap_and_returns_gapless() {
        let mut ch = strict(vec![1, 4], Some(1)).stitch(|gap| {
            assert_eq!(
                gap,
                Gap {
                    expected: 2,
                    got: 4
                }
            );
            Channels::from_vec(vec![2, 3])
        });
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Ok(3));
        assert_eq!(ch.recv(), Ok(4));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn stitch_drains_through_ready_batch() {
        let mut ch = strict(vec![1, 4, 5], Some(1)).stitch(|_| Channels::from_vec(vec![2, 3]));
        let batch: Vec<u64> = ch.ready_batch(10).unwrap().collect();
        assert_eq!(batch, vec![1, 2, 3, 4, 5]);
        assert_eq!(ch.end_reason(), End::Complete);
    }

    #[test]
    fn stitch_passes_no_key_fallback_items() {
        let ordered = OrderedChannel::<_, StrictlyMonotone<u64>>::new(
            Channels::from_vec(vec![Some(1u64), Some(4)]),
            Some(1),
        );
        let mut ch = ordered.stitch(|_| Channels::from_vec(vec![Some(2), None, Some(3)]));
        assert_eq!(ch.recv(), Ok(Some(1)));
        assert_eq!(ch.recv(), Ok(Some(2)));
        assert_eq!(ch.recv(), Ok(None));
        assert_eq!(ch.recv(), Ok(Some(3)));
        assert_eq!(ch.recv(), Ok(Some(4)));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn stitch_with_empty_fallback_aborts_instead_of_completing() {
        let mut ch = strict(vec![1, 4, 5], Some(1))
            .stitch(|_| Channels::chain_all(Vec::<Channels<u64>>::new()));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::OrderViolation {
                    what: "stitch fallback"
                }
            })
        );
    }

    #[test]
    fn stitch_rejects_a_short_fallback() {
        let mut ch = strict(vec![1, 4], Some(1)).stitch(|_| Channels::from_vec(vec![2]));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::OrderViolation {
                    what: "stitch fallback"
                }
            })
        );
    }

    #[test]
    fn stitch_rejects_an_overlong_fallback() {
        let mut ch = strict(vec![1, 4], Some(1)).stitch(|_| Channels::from_vec(vec![2, 3, 4]));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Ok(3));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::OrderViolation {
                    what: "stitch fallback"
                }
            })
        );
    }

    #[test]
    fn stitch_propagates_a_fallback_abort() {
        let mut ch = strict(vec![1, 4], Some(1)).stitch(|_| {
            let (tx, rx) = crossbeam_channel::bounded(4);
            tx.send(2).unwrap();
            tx.send(3).unwrap();
            drop(tx);
            Channels::from_infinite(rx, "fallback")
        });
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Ok(3));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "fallback" }
            })
        );
    }
}
