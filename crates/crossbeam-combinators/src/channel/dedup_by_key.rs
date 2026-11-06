//! `dedup_by_key`: drop consecutive items with an equal key.

use super::{Channel, End, Poll, Source};
use crate::kind::Kind;
use crate::ordered::Key;

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

impl<T: Send + 'static, K: Kind> Channel<T, K> {
    /// Drop an item when its key equals the previous item's key.
    ///
    /// No-key items pass through and do not change the last key. This is a
    /// stream transform, not an order policy; pair it with a contiguous policy
    /// when exact succession is required.
    #[must_use]
    pub fn dedup_by_key<Pos: Key<T>>(self) -> Channel<T, K> {
        Channel::wrap(Box::new(DedupByKey::<T, Pos> {
            inner: Some(self.source),
            last: None,
            done: None,
        }))
    }
}
