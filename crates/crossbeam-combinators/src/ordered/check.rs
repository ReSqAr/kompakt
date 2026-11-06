//! The source that enforces a policy as items pass.

use std::marker::PhantomData;

use super::{Key, OrderPolicy, OrderState};
use crate::channel::{AbortReason, End, Poll, Source};

pub(super) struct OrderSource<T, P>
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
    pub(super) fn new(
        inner: Box<dyn Source<T>>,
        after: Option<P::Key>,
        what: &'static str,
    ) -> Self {
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
