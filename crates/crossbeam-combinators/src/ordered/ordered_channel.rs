//! [`OrderedChannel`]: a channel whose position policy is part of its type.

use std::marker::PhantomData;

use super::check::OrderSource;
use super::{Key, OrderPolicy};
use crate::channel::{Channel, ReadyBatch};
use crate::kind::Kind;

/// A [`Channel`] whose position policy is enforced and named in the type.
///
/// The read surface delegates to the inner channel, so this is a drop-in
/// replacement for `recv`/`try_recv`/`ready_batch`/`end_reason`.
pub struct OrderedChannel<T, P, K>
where
    P: OrderPolicy,
{
    pub(super) inner: Channel<T, K>,
    pub(super) after: Option<P::Key>,
    pub(super) what: &'static str,
    pub(super) _policy: PhantomData<P>,
}

impl<T, P, K> OrderedChannel<T, P, K>
where
    T: Send + 'static,
    P: OrderPolicy,
    P::Key: Key<T>,
    K: Kind,
{
    /// Wrap `inner` with policy `P`, checked from `after` — the last position
    /// already delivered (exclusive; `None` = from the beginning). The
    /// violation label is the policy name; use [`named`](Self::named) at a
    /// boundary.
    #[must_use]
    pub fn new(inner: Channel<T, K>, after: Option<P::Key>) -> Self {
        Self::named(inner, after, P::LABEL)
    }

    /// As [`new`](Self::new), but names the boundary in an
    /// [`AbortReason::OrderViolation`](crate::AbortReason::OrderViolation).
    #[must_use]
    pub fn named(inner: Channel<T, K>, after: Option<P::Key>, what: &'static str) -> Self {
        let source = OrderSource::<T, P>::new(inner.source, after, what);
        Self {
            inner: Channel::wrap(Box::new(source)),
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
            inner: Channel::wrap(Box::new(source)),
            ..self
        }
    }

    pub fn recv(&mut self) -> Result<T, K::End> {
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
    pub fn ready_batch(&mut self, max: usize) -> Result<ReadyBatch<'_, T, K>, K::End> {
        self.inner.ready_batch(max)
    }

    pub fn end_reason(&mut self) -> K::End {
        self.inner.end_reason()
    }

    /// The order check, applied until `cancel` fires; see [`Channel::until`].
    #[must_use]
    pub fn until(self, cancel: &crate::Cancellation) -> Self {
        Self {
            inner: self.inner.until(cancel),
            ..self
        }
    }

    /// Give up the type-level order guarantee and keep the already-checked
    /// channel.
    #[must_use]
    pub fn into_channel(self) -> Channel<T, K> {
        self.inner
    }
}

impl<A, E, P, K> OrderedChannel<Result<A, E>, P, K>
where
    A: Send + 'static,
    E: Send + 'static,
    P: OrderPolicy,
    K: Kind,
{
    /// Convert the error of every `Err` item with `From`, keeping the order
    /// check that already ran on the original items. Errors carry no position,
    /// so the policy's key is unaffected by the conversion.
    #[must_use]
    pub fn err_into<E2>(self) -> OrderedChannel<Result<A, E2>, P, K>
    where
        E2: From<E> + Send + 'static,
        P::Key: Key<Result<A, E2>>,
    {
        OrderedChannel {
            inner: self.inner.err_into(),
            after: self.after,
            what: self.what,
            _policy: PhantomData,
        }
    }
}

impl<T: Send + 'static, K: Kind> Channel<T, K> {
    /// Wrap in policy `P`, checked from `after`; see [`OrderedChannel::named`].
    #[must_use]
    pub fn ordered<P>(self, after: Option<P::Key>, what: &'static str) -> OrderedChannel<T, P, K>
    where
        P: OrderPolicy,
        P::Key: Key<T>,
    {
        OrderedChannel::named(self, after, what)
    }
}
