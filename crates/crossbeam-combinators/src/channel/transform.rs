//! Pull-time transforms and bounds: `filter_map`, `take_while`, `dedup_by_key`.

use crossbeam_channel::Select;

use super::{End, Poll, Source};
use crate::ordered::Key;

pub(super) struct FilterMap<S, T> {
    pub(super) inner: Box<dyn Source<S>>,
    pub(super) f: Box<dyn FnMut(S) -> Option<T> + Send>,
}

impl<S: Send + 'static, T: Send + 'static> Source<T> for FilterMap<S, T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        self.inner.register(sel)
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        debug_assert!(
            ordinal < self.inner.leaf_count(),
            "ordinal {ordinal} out of range for filter_map inner with {} leaves",
            self.inner.leaf_count()
        );
        match self.inner.poll(ordinal) {
            Poll::Got(s) => match (self.f)(s) {
                Some(t) => Poll::Got(t),
                None => Poll::Filtered,
            },
            Poll::Filtered => Poll::Filtered,
            Poll::Empty => Poll::Empty,
            Poll::Dead => Poll::Dead,
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        loop {
            let s = self.inner.drain_first()?;
            if let Some(t) = (self.f)(s) {
                return Some(t);
            }
        }
    }
    fn leaf_count(&self) -> usize {
        self.inner.leaf_count()
    }
    fn end_reason(&mut self) -> End {
        self.inner.end_reason()
    }
}

// ── TakeWhile<T> ───────────────────────────────────────────────────────

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

// ── DedupByKey<T, Pos> ─────────────────────────────────────────────────

/// Drop consecutive items whose key repeats.
///
/// No-key items (errors, watermarks) pass through and do not change the last
/// key. This is a stream transform, not an order policy: the caller can pair it
/// with `Contiguous` when exact succession is required.
pub(super) struct DedupByKey<T, Pos> {
    pub(super) inner: Option<Box<dyn Source<T>>>,
    pub(super) last: Option<Pos>,
    pub(super) done: Option<End>,
}

impl<T, Pos> Source<T> for DedupByKey<T, Pos>
where
    T: Send + 'static,
    Pos: Key<T>,
{
    fn register<'a>(&'a mut self, sel: &mut crossbeam_channel::Select<'a>) -> usize {
        self.inner.as_mut().map_or(0, |src| src.register(sel))
    }

    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        let Some(mut inner) = self.inner.take() else {
            return Poll::Dead;
        };
        match inner.poll(ordinal) {
            Poll::Got(item) => match Pos::key(&item) {
                Some(key) if self.last == Some(key) => {
                    self.inner = Some(inner);
                    Poll::Filtered
                }
                Some(key) => {
                    self.last = Some(key);
                    self.inner = Some(inner);
                    Poll::Got(item)
                }
                None => {
                    self.inner = Some(inner);
                    Poll::Got(item)
                }
            },
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
        loop {
            let mut inner = self.inner.take()?;
            match inner.drain_first() {
                Some(item) => match Pos::key(&item) {
                    Some(key) if self.last == Some(key) => {
                        self.inner = Some(inner);
                        continue;
                    }
                    Some(key) => {
                        self.last = Some(key);
                        self.inner = Some(inner);
                        return Some(item);
                    }
                    None => {
                        self.inner = Some(inner);
                        return Some(item);
                    }
                },
                None => {
                    if inner.leaf_count() == 0 {
                        self.done = Some(inner.end_reason());
                        return None;
                    }
                    self.inner = Some(inner);
                    return None;
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
