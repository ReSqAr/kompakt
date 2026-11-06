//! `covers`: require a finite contiguous stream to reach its bound.

use super::{Contiguous, Key, OrderedChannel, Successor};
use crate::channel::{AbortReason, Channel, End, Poll, Source};
use crate::kind::Finite;

impl<T, Pos> OrderedChannel<T, Contiguous<Pos>, Finite>
where
    T: Send + 'static,
    Pos: Key<T> + Successor,
{
    /// Require the stream to reach `until` (exclusive): a clean end before it
    /// becomes `Aborted(OrderViolation { what })`.
    ///
    /// Order policies check the steps between items; this checks the end. It
    /// is complete when the last key's successor is `until`, or — for a
    /// stream with no keyed item — when `next_after(after)` is. A source that
    /// aborts keeps its own reason.
    #[must_use]
    pub fn covers(self, range: std::ops::RangeTo<Pos>, what: &'static str) -> Self {
        let source = CoversSource {
            inner: Some(self.inner.source),
            last: self.after,
            until: range.end,
            what,
            done: None,
        };
        Self {
            inner: Channel::wrap(Box::new(source)),
            ..self
        }
    }
}

/// The end check behind [`OrderedChannel::covers`].
pub(super) struct CoversSource<T, K> {
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
