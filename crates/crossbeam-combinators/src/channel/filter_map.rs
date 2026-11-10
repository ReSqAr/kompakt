//! `filter_map` / `map` / `filter_above`: pull-time item transforms.

use crossbeam_channel::Select;

use super::{Channel, End, Poll, Source};
use crate::kind::Kind;
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

impl<A: Send + 'static, E: Send + 'static, K: Kind> Channel<Result<A, E>, K> {
    /// Convert the error of every `Err` item with `From`, at pull time. The
    /// seam where one crate's error type becomes the next one's.
    #[must_use]
    pub fn err_into<E2: From<E> + Send + 'static>(self) -> Channel<Result<A, E2>, K> {
        self.map(|item| item.map_err(E2::from))
    }
}

impl<T: Send + 'static, K: Kind> Channel<T, K> {
    /// Transform and filter at pull time. `f` may hold state: a channel has
    /// one consumer, so it runs on one thread at a time.
    pub fn filter_map<U, F>(self, f: F) -> Channel<U, K>
    where
        U: Send + 'static,
        F: FnMut(T) -> Option<U> + Send + 'static,
    {
        Channel::wrap(Box::new(FilterMap {
            inner: self.source,
            f: Box::new(f),
        }))
    }

    pub fn map<U, F>(self, mut f: F) -> Channel<U, K>
    where
        U: Send + 'static,
        F: FnMut(T) -> U + Send + 'static,
    {
        self.filter_map(move |t| Some(f(t)))
    }

    /// Drop items whose key is at or below `cut`; no-key items pass. `None`
    /// keeps everything.
    #[must_use]
    pub fn filter_above<Pos: Key<T>>(self, cut: Option<Pos>) -> Channel<T, K> {
        self.filter_map(move |item| match (Pos::key(&item), cut) {
            (Some(key), Some(cut)) if key <= cut => None,
            _ => Some(item),
        })
    }
}
