//! `take_while` / `take_until` / `take_at_least`: bounds that end a stream cleanly.

use crossbeam_channel::Select;

use super::{Channel, End, Poll, Source};
use crate::kind::{Finite, Kind};
use crate::ordered::Key;

/// A bounded prefix: stop at the first item `pred` declines.
///
/// The declined item is consumed and dropped, like `Iterator::take_while`. The
/// stream ends [`End::Complete`]: from the consumer's point of view the
/// requested range is done. If the source ends first, its own verdict is
/// preserved; a clean `Complete` there is also a complete (if shorter) stream.
pub(super) struct TakeWhile<T> {
    pub(super) inner: Option<Box<dyn Source<T>>>,
    pub(super) pred: Box<dyn FnMut(&T) -> bool + Send>,
    pub(super) done: Option<End>,
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

impl<T: Send + 'static, K: Kind> Channel<T, K> {
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
}
