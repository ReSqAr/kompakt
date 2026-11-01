//! Ordered channels: a position-keyed stream with an explicit policy.
//!
//! `OrderedChannel<T, P>` wraps a [`Channel<T>`] with a policy marker `P`
//! (`Contiguous<K>`, `StrictlyIncreasing<K>`, `NonDecreasing<K>`). `K: Key<T>`
//! extracts the position from an item; `None` means the item does not
//! participate in the order (a watermark or an error item).
//!
//! The policy is enforced in the wrapped source, so `recv`, `try_recv` and
//! `ready_batch` all see the same check. A violation ends the stream with
//! [`AbortReason::OrderViolation`]; a permitted forward gap is passed through.
//!
//! Every start is exclusive: a channel is checked from `after: Option<K>`, the
//! last position already delivered (`None` = from the beginning). `covers`
//! checks the end of a finite contiguous stream; `also` adds a second policy as
//! a runtime guard without changing the type.
//!
//! `stitch` is the one transition that strengthens a policy: it fills gaps from
//! a fallback and returns a `Contiguous` channel.

use std::marker::PhantomData;

use crate::channel::{AbortReason, Channel, End, Poll, ReadyBatch, Source};

/// The position of an item in an ordered stream.
///
/// `None` means the item is not positional and passes through without advancing
/// the order. Implement this for the key type, not for the item, because the
/// same item type can be ordered by different keys.
pub trait Key<T>: Copy + Ord + Send + 'static {
    fn key(item: &T) -> Option<Self>;
}

/// The first position after a gapless key.
///
/// `next_after(None)` is the first position of all, so an exclusive `after`
/// of `None` ("from the beginning") has a defined first key. `Hlc` is ordered
/// but not gapless; it implements `Key`/`Ord`, not this.
pub trait Successor: Copy {
    fn next_after(prev: Option<Self>) -> Self;
}

/// A gap in an ordered stream: everything in `(after, got)` was absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gap<K> {
    /// The last position delivered before the gap (`None`: none yet).
    pub after: Option<K>,
    /// The first position past the gap.
    pub got: K,
}

/// The internal state a policy advances over.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct OrderState<K> {
    pub(crate) after: Option<K>,
    pub(crate) last: Option<K>,
}

impl<K: Copy> OrderState<K> {
    /// The last position this stream has delivered, or started after.
    fn bound(&self) -> Option<K> {
        self.last.or(self.after)
    }
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
pub struct Contiguous<K>(PhantomData<K>);

/// Keys are strictly increasing; gaps are allowed.
pub struct StrictlyIncreasing<K>(PhantomData<K>);

/// Keys are non-decreasing; gaps and duplicates are allowed.
pub struct NonDecreasing<K>(PhantomData<K>);

impl<K: Successor + Ord + Send + 'static> sealed::Sealed for Contiguous<K> {}
impl<K: Ord + Copy + Send + 'static> sealed::Sealed for StrictlyIncreasing<K> {}
impl<K: Ord + Copy + Send + 'static> sealed::Sealed for NonDecreasing<K> {}

impl<K: Successor + Ord + Send + 'static> OrderPolicy for Contiguous<K> {
    type Key = K;
    const LABEL: &'static str = "contiguous";

    fn check(state: &mut OrderState<K>, key: K) -> bool {
        if key != K::next_after(state.bound()) {
            return false;
        }
        state.last = Some(key);
        true
    }
}

impl<K: Ord + Copy + Send + 'static> OrderPolicy for StrictlyIncreasing<K> {
    type Key = K;
    const LABEL: &'static str = "strictly increasing";

    fn check(state: &mut OrderState<K>, key: K) -> bool {
        if state.bound().is_some_and(|bound| key <= bound) {
            return false;
        }
        state.last = Some(key);
        true
    }
}

impl<K: Ord + Copy + Send + 'static> OrderPolicy for NonDecreasing<K> {
    type Key = K;
    const LABEL: &'static str = "non-decreasing";

    fn check(state: &mut OrderState<K>, key: K) -> bool {
        let ok = match state.last {
            // Duplicates are allowed between items...
            Some(last) => key >= last,
            // ...but the first key is still strictly after the start.
            None => state.after.is_none_or(|after| key > after),
        };
        if ok {
            state.last = Some(key);
        }
        ok
    }
}

struct OrderSource<T, P>
where
    P: OrderPolicy,
{
    inner: Option<Box<dyn Source<T>>>,
    state: OrderState<P::Key>,
    what: &'static str,
    done: Option<End>,
    _policy: PhantomData<P>,
}

impl<T, P> OrderSource<T, P>
where
    T: Send + 'static,
    P: OrderPolicy,
    P::Key: Key<T>,
{
    fn new(inner: Box<dyn Source<T>>, after: Option<P::Key>, what: &'static str) -> Self {
        Self {
            inner: Some(inner),
            state: OrderState { after, last: None },
            what,
            done: None,
            _policy: PhantomData,
        }
    }

    fn violation(&mut self) -> Poll<T> {
        self.done = Some(End::Aborted {
            reason: AbortReason::OrderViolation { what: self.what },
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
                        reason: AbortReason::OrderViolation { what: self.what },
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

/// A [`Channel`] whose position policy is enforced and named in the type.
///
/// The read surface delegates to the inner channel, so this is a drop-in
/// replacement for `recv`/`try_recv`/`ready_batch`/`end_reason`.
pub struct OrderedChannel<T, P>
where
    P: OrderPolicy,
{
    inner: Channel<T>,
    after: Option<P::Key>,
    what: &'static str,
    _policy: PhantomData<P>,
}

impl<T, P> OrderedChannel<T, P>
where
    T: Send + 'static,
    P: OrderPolicy,
    P::Key: Key<T>,
{
    /// Wrap `inner` with policy `P`, checked from `after` — the last position
    /// already delivered (exclusive; `None` = from the beginning). The
    /// violation label is the policy name; use [`named`](Self::named) at a
    /// boundary.
    #[must_use]
    pub fn new(inner: Channel<T>, after: Option<P::Key>) -> Self {
        Self::named(inner, after, P::LABEL)
    }

    /// As [`new`](Self::new), but names the boundary in an
    /// [`AbortReason::OrderViolation`].
    #[must_use]
    pub fn named(inner: Channel<T>, after: Option<P::Key>, what: &'static str) -> Self {
        let source = OrderSource::<T, P>::new(inner.source, after, what);
        Self {
            inner: Channel {
                source: Box::new(source),
            },
            after,
            what,
            _policy: PhantomData,
        }
    }

    /// Enforce a second policy `Q` at runtime, keeping `P` in the type: the
    /// primary order is what consumers rely on, the secondary is a guard
    /// (e.g. HLC monotonicity under an index order).
    #[must_use]
    pub fn also<Q>(self, after: Option<Q::Key>, what: &'static str) -> Self
    where
        Q: OrderPolicy,
        Q::Key: Key<T>,
    {
        let source = OrderSource::<T, Q>::new(self.inner.source, after, what);
        Self {
            inner: Channel {
                source: Box::new(source),
            },
            ..self
        }
    }

    pub fn recv(&mut self) -> Result<T, End> {
        self.inner.recv()
    }

    pub fn try_recv(&mut self) -> Option<T> {
        self.inner.try_recv()
    }

    /// See [`Channel::ready_batch`].
    ///
    /// # Errors
    ///
    /// The verdict once the stream has ended.
    pub fn ready_batch(&mut self, max: usize) -> Result<ReadyBatch<'_, T>, End> {
        self.inner.ready_batch(max)
    }

    pub fn end_reason(&mut self) -> End {
        self.inner.end_reason()
    }

    /// Give up the type-level order guarantee and keep the already-checked
    /// channel.
    #[must_use]
    pub fn into_channel(self) -> Channel<T> {
        self.inner
    }
}

impl<T: Send + 'static> Channel<T> {
    /// Wrap in policy `P`, checked from `after`; see [`OrderedChannel::named`].
    #[must_use]
    pub fn ordered<P>(self, after: Option<P::Key>, what: &'static str) -> OrderedChannel<T, P>
    where
        P: OrderPolicy,
        P::Key: Key<T>,
    {
        OrderedChannel::named(self, after, what)
    }
}

impl<T, K> OrderedChannel<T, Contiguous<K>>
where
    T: Send + 'static,
    K: Key<T> + Successor,
{
    /// Require the stream to reach `until` (exclusive): a clean end before it
    /// becomes `Aborted(OrderViolation { what })`.
    ///
    /// Order policies check the steps between items; this checks the end. It
    /// is complete when the last key's successor is `until`, or — for a
    /// stream with no keyed item — when `next_after(after)` is. A source that
    /// aborts keeps its own reason.
    #[must_use]
    pub fn covers(self, range: std::ops::RangeTo<K>, what: &'static str) -> Self {
        let source = CoversSource {
            inner: Some(self.inner.source),
            last: self.after,
            until: range.end,
            what,
            done: None,
        };
        Self {
            inner: Channel {
                source: Box::new(source),
            },
            ..self
        }
    }
}

/// The end check behind [`OrderedChannel::covers`].
struct CoversSource<T, K> {
    inner: Option<Box<dyn Source<T>>>,
    /// The last key seen, starting at the channel's `after`.
    last: Option<K>,
    until: K,
    what: &'static str,
    done: Option<End>,
}

impl<T, K> CoversSource<T, K>
where
    T: Send + 'static,
    K: Key<T> + Successor,
{
    fn observe(&mut self, item: &T) {
        if let Some(key) = K::key(item) {
            self.last = Some(key);
        }
    }

    /// The verdict once the inner source has ended with `end`.
    fn judge(&mut self, end: End) -> End {
        let verdict = match end {
            End::Complete if K::next_after(self.last) == self.until => End::Complete,
            End::Complete => End::Aborted {
                reason: AbortReason::OrderViolation { what: self.what },
            },
            aborted @ End::Aborted { .. } => aborted,
        };
        self.done = Some(verdict);
        self.inner = None;
        verdict
    }
}

impl<T, K> Source<T> for CoversSource<T, K>
where
    T: Send + 'static,
    K: Key<T> + Successor,
{
    fn register<'a>(&'a mut self, sel: &mut crossbeam_channel::Select<'a>) -> usize {
        self.inner.as_mut().map_or(0, |src| src.register(sel))
    }

    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Dead;
        };
        match inner.poll(ordinal) {
            Poll::Got(item) => {
                self.observe(&item);
                Poll::Got(item)
            }
            Poll::Dead => {
                let end = inner.end_reason();
                self.judge(end);
                Poll::Dead
            }
            other => other,
        }
    }

    fn drain_first(&mut self) -> Option<T> {
        let inner = self.inner.as_mut()?;
        match inner.drain_first() {
            Some(item) => {
                self.observe(&item);
                Some(item)
            }
            None => {
                if inner.leaf_count() == 0 {
                    let end = inner.end_reason();
                    self.judge(end);
                }
                None
            }
        }
    }

    fn leaf_count(&self) -> usize {
        self.inner.as_ref().map_or(0, |src| src.leaf_count())
    }

    fn end_reason(&mut self) -> End {
        match (self.done, self.inner.as_mut()) {
            (Some(end), _) => end,
            (None, Some(inner)) => inner.end_reason(),
            (None, None) => End::Complete,
        }
    }
}

impl<T, K> OrderedChannel<T, StrictlyIncreasing<K>>
where
    T: Send + 'static,
    K: Key<T> + Successor,
{
    /// Fill forward gaps from `fallback`, strengthening `StrictlyIncreasing` to
    /// `Contiguous`.
    ///
    /// The fallback covers `(gap.after, gap.got)`; the primary item at
    /// `gap.got` is buffered and emitted after the fallback. A short, overlong,
    /// gapped, or aborted fallback fails the stitched stream.
    #[must_use]
    pub fn stitch<F>(self, fallback: F) -> OrderedChannel<T, Contiguous<K>>
    where
        F: FnMut(Gap<K>) -> Channel<T> + Send + 'static,
    {
        let stitched = StitchSource {
            primary: Some(self.inner.source),
            fallback,
            fallback_source: None,
            pending: None,
            after: self.after,
            last: None,
            fallback_last: None,
            gap_next: None,
            gap_got: None,
            done: None,
            _key: PhantomData,
        };
        let inner = Channel {
            source: Box::new(stitched),
        };
        // `StitchSource` already returns a gapless sequence. The outer
        // Contiguous check is deliberate defence-in-depth: do not remove it as
        // "redundant" without preserving the same guarantee another way.
        OrderedChannel::<T, Contiguous<K>>::named(inner, self.after, self.what)
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
    after: Option<K>,
    last: Option<K>,
    fallback_last: Option<K>,
    /// The next key the active fallback owes.
    gap_next: Option<K>,
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
    F: FnMut(Gap<K>) -> Channel<T> + Send + 'static,
{
    fn expected(&self) -> K {
        K::next_after(self.last.or(self.after))
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
        if got < expected {
            self.fail("stitch primary");
            return PrimaryStep::Failed;
        }
        if got > expected {
            let after = self.last.or(self.after);
            self.pending = Some(item);
            self.gap_next = Some(expected);
            self.gap_got = Some(got);
            self.fallback_last = None;
            self.fallback_source = Some((self.fallback)(Gap { after, got }).source);
            return PrimaryStep::Gap;
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
        if self.gap_got.is_some_and(|end| got >= end) || Some(got) != self.gap_next {
            self.fail("stitch fallback");
            return Err(());
        }
        self.fallback_last = Some(got);
        self.gap_next = Some(K::next_after(Some(got)));
        Ok(item)
    }

    fn finish_fallback(&mut self) -> Option<T> {
        let Some(got) = self.gap_got else {
            self.fail("stitch fallback");
            return None;
        };
        let covered = self
            .fallback_last
            .is_some_and(|last| K::next_after(Some(last)) == got);
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
        self.gap_next = None;
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
    F: FnMut(Gap<K>) -> Channel<T> + Send + 'static,
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
    use crate::Channel;

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
        fn next_after(prev: Option<Self>) -> Self {
            prev.map_or(0, |p| {
                p.checked_add(1)
                    .unwrap_or_else(|| panic!("test key exhausted"))
            })
        }
    }

    fn strict(items: Vec<u64>, after: Option<u64>) -> OrderedChannel<u64, StrictlyIncreasing<u64>> {
        OrderedChannel::new(Channel::from_vec(items), after)
    }

    #[test]
    fn strictly_monotone_accepts_forward_gaps() {
        let mut ch = strict(vec![1, 3, 4], Some(0));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(3));
        assert_eq!(ch.recv(), Ok(4));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn strictly_monotone_rejects_duplicate() {
        let mut ch = strict(vec![1, 2, 2], Some(0));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::OrderViolation {
                    what: "strictly increasing"
                }
            })
        );
    }

    #[test]
    fn strictly_monotone_rejects_leading_regression() {
        let mut ch = strict(vec![0, 1], Some(0));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::OrderViolation {
                    what: "strictly increasing"
                }
            })
        );
    }

    #[test]
    fn gapless_rejects_a_gap() {
        let mut ch =
            OrderedChannel::<_, Contiguous<u64>>::new(Channel::from_vec(vec![1, 3]), Some(0));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::OrderViolation { what: "contiguous" }
            })
        );
    }

    #[test]
    fn non_decreasing_allows_duplicates() {
        let mut ch =
            OrderedChannel::<_, NonDecreasing<u64>>::new(Channel::from_vec(vec![1, 1, 2]), Some(0));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn no_key_items_pass_without_advancing() {
        let mut ch = OrderedChannel::<_, Contiguous<u64>>::new(
            Channel::from_vec(vec![Some(1), None, Some(2)]),
            Some(0),
        );
        assert_eq!(ch.recv(), Ok(Some(1)));
        assert_eq!(ch.recv(), Ok(None));
        assert_eq!(ch.recv(), Ok(Some(2)));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn batch_and_projection_keep_the_order_source() {
        let ordered = strict(vec![1, 2, 3], Some(0));
        let mut ch = ordered.into_channel();
        let batch: Vec<u64> = ch.ready_batch(2).unwrap().collect();
        assert_eq!(batch, vec![1, 2]);
        let rest: Vec<u64> = std::iter::from_fn(|| ch.recv().ok()).collect();
        assert_eq!(rest, vec![3]);
        assert_eq!(ch.end_reason(), End::Complete);
    }

    #[test]
    fn stitch_fills_a_leading_gap() {
        let mut ch = strict(vec![3, 4], Some(0)).stitch(|gap| {
            assert_eq!(
                gap,
                Gap {
                    after: Some(0),
                    got: 3
                }
            );
            Channel::from_vec(vec![1, 2])
        });
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Ok(3));
        assert_eq!(ch.recv(), Ok(4));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn stitch_fills_the_gap_and_returns_gapless() {
        let mut ch = strict(vec![1, 4], Some(0)).stitch(|gap| {
            assert_eq!(
                gap,
                Gap {
                    after: Some(1),
                    got: 4
                }
            );
            Channel::from_vec(vec![2, 3])
        });
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Ok(3));
        assert_eq!(ch.recv(), Ok(4));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn stitch_drains_through_ready_batch() {
        let mut ch = strict(vec![1, 4, 5], Some(0)).stitch(|_| Channel::from_vec(vec![2, 3]));
        let batch: Vec<u64> = ch.ready_batch(10).unwrap().collect();
        assert_eq!(batch, vec![1, 2, 3, 4, 5]);
        assert_eq!(ch.end_reason(), End::Complete);
    }

    #[test]
    fn stitch_passes_no_key_fallback_items() {
        let ordered = OrderedChannel::<_, StrictlyIncreasing<u64>>::new(
            Channel::from_vec(vec![Some(1u64), Some(4)]),
            Some(0),
        );
        let mut ch = ordered.stitch(|_| Channel::from_vec(vec![Some(2), None, Some(3)]));
        assert_eq!(ch.recv(), Ok(Some(1)));
        assert_eq!(ch.recv(), Ok(Some(2)));
        assert_eq!(ch.recv(), Ok(None));
        assert_eq!(ch.recv(), Ok(Some(3)));
        assert_eq!(ch.recv(), Ok(Some(4)));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn stitch_with_empty_fallback_aborts_instead_of_completing() {
        let mut ch = strict(vec![1, 4, 5], Some(0))
            .stitch(|_| Channel::chain_all(Vec::<Channel<u64>>::new()));
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
        let mut ch = strict(vec![1, 4], Some(0)).stitch(|_| Channel::from_vec(vec![2]));
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
        let mut ch = strict(vec![1, 4], Some(0)).stitch(|_| Channel::from_vec(vec![2, 3, 4]));
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
        let mut ch = strict(vec![1, 4], Some(0)).stitch(|_| {
            let (tx, rx) = crossbeam_channel::bounded(4);
            tx.send(2).unwrap();
            tx.send(3).unwrap();
            drop(tx);
            Channel::from_infinite(rx, "fallback")
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

    fn contiguous(items: Vec<u64>, after: Option<u64>) -> OrderedChannel<u64, Contiguous<u64>> {
        Channel::from_vec(items).ordered(after, "test window")
    }

    fn drain<P: OrderPolicy>(mut ch: OrderedChannel<u64, P>) -> (Vec<u64>, End)
    where
        P::Key: Key<u64>,
    {
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
    fn contiguous_from_none_starts_at_the_first_key() {
        assert_eq!(
            drain(contiguous(vec![0, 1], None)),
            (vec![0, 1], End::Complete)
        );
        let (_, end) = drain(contiguous(vec![1], None));
        assert!(matches!(end, End::Aborted { .. }), "0 was skipped");
    }

    #[test]
    fn covers_completes_only_at_the_bound() {
        let reached = contiguous(vec![3, 4], Some(2)).covers(..5, "window");
        assert_eq!(drain(reached), (vec![3, 4], End::Complete));

        let short = contiguous(vec![3], Some(2)).covers(..5, "window");
        assert_eq!(
            drain(short),
            (
                vec![3],
                End::Aborted {
                    reason: AbortReason::OrderViolation { what: "window" }
                }
            )
        );
    }

    #[test]
    fn covers_judges_an_empty_window_by_its_start() {
        let empty = contiguous(vec![], Some(4)).covers(..5, "window");
        assert_eq!(drain(empty).1, End::Complete, "(4, 5) holds nothing");
        let missing = contiguous(vec![], Some(2)).covers(..5, "window");
        assert!(
            matches!(drain(missing).1, End::Aborted { .. }),
            "(2, 5) was owed"
        );
    }

    #[test]
    fn covers_keeps_a_source_abort() {
        let (tx, rx) = crossbeam_channel::bounded(2);
        tx.send(1u64).unwrap();
        drop(tx);
        let ch = Channel::from_infinite(rx, "src")
            .ordered::<Contiguous<u64>>(Some(0), "w")
            .covers(..5, "window");
        assert_eq!(
            drain(ch).1,
            End::Aborted {
                reason: AbortReason::ProducerGone { what: "src" }
            }
        );
    }

    #[test]
    fn covers_through_ready_batch() {
        let mut ch = contiguous(vec![1, 2], Some(0)).covers(..4, "window");
        let mut got = Vec::new();
        let end = loop {
            match ch.ready_batch(8) {
                Ok(batch) => got.extend(batch),
                Err(end) => break end,
            }
        };
        assert_eq!(got, vec![1, 2]);
        assert!(matches!(end, End::Aborted { .. }), "3 was owed");
    }

    #[test]
    fn also_guards_a_second_order_without_changing_the_type() {
        // Index order holds, the secondary (descending is a violation) trips.
        let ch: OrderedChannel<u64, StrictlyIncreasing<u64>> =
            strict(vec![1, 5, 3], None).also::<NonDecreasing<u64>>(None, "secondary");
        let (got, end) = drain(ch);
        assert_eq!(got, vec![1, 5]);
        assert!(matches!(
            end,
            End::Aborted {
                reason: AbortReason::OrderViolation { .. }
            }
        ));
    }

    #[test]
    fn non_decreasing_first_key_is_strictly_after() {
        let ch = Channel::from_vec(vec![2u64, 2, 3]).ordered::<NonDecreasing<u64>>(Some(2), "nd");
        assert!(
            matches!(drain(ch).1, End::Aborted { .. }),
            "2 is not after 2"
        );
        let ch = Channel::from_vec(vec![3u64, 3, 4]).ordered::<NonDecreasing<u64>>(Some(2), "nd");
        assert_eq!(drain(ch), (vec![3, 3, 4], End::Complete));
    }
}
