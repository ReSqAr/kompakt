//! The subscribe side: [`Registry`] and [`KeyedRegistry`].

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crossbeam_channel::{Sender, TrySendError};

use crate::channel::Channel;
use crate::kind::Infinite;

use super::error::RegistryError;
use super::producer::State;

/// The broadcast subscribe side.
pub struct Registry<T> {
    pub(super) register: Sender<((), Sender<T>, u64)>,
    pub(super) sub_cap: NonZeroUsize,
    pub(super) what: &'static str,
    pub(super) state: Arc<State>,
}

impl<T> Clone for Registry<T> {
    fn clone(&self) -> Self {
        Self {
            register: self.register.clone(),
            sub_cap: self.sub_cap,
            what: self.what,
            state: Arc::clone(&self.state),
        }
    }
}

impl<T: Clone + Send + 'static> Registry<T> {
    /// A new subscriber stream; `Full` if the registration queue is full,
    /// `Closed` if the fan-out is gone.
    ///
    /// # Errors
    ///
    /// [`RegistryError`] when the subscriber could not be registered.
    pub fn subscribe(&self) -> Result<Channel<T, Infinite>, RegistryError> {
        subscribe(&self.register, &self.state, (), self.sub_cap, self.what)
    }

    /// A subscriber stream for a source that is **infinite by contract**.
    ///
    /// [`subscribe`](Self::subscribe) fails; this cannot, because a consumer of
    /// an infinite stream has no clean end to interpret. `Full` and `Closed`
    /// both yield a stream that ends immediately as
    /// [`Aborted(ProducerGone { what })`](crate::AbortReason::ProducerGone)
    /// carrying this fan-out's label, which is the cue to retry rather than to
    /// stop.
    #[must_use]
    pub fn subscribe_infinite(&self) -> Channel<T, Infinite> {
        self.subscribe()
            .unwrap_or_else(|_| Channel::aborted(self.what))
    }
}

/// The keyed subscribe side.
pub struct KeyedRegistry<K, T> {
    pub(super) register: Sender<(K, Sender<T>, u64)>,
    pub(super) sub_cap: NonZeroUsize,
    pub(super) what: &'static str,
    pub(super) state: Arc<State>,
}

impl<K, T> Clone for KeyedRegistry<K, T> {
    fn clone(&self) -> Self {
        Self {
            register: self.register.clone(),
            sub_cap: self.sub_cap,
            what: self.what,
            state: Arc::clone(&self.state),
        }
    }
}

impl<K: Eq + Copy + Send + 'static, T: Clone + Send + 'static> KeyedRegistry<K, T> {
    /// A new subscriber stream for `key`; `Full` if the registration queue is
    /// full, `Closed` if the fan-out is gone.
    ///
    /// # Errors
    ///
    /// [`RegistryError`] when the subscriber could not be registered.
    pub fn subscribe(&self, key: K) -> Result<Channel<T, Infinite>, RegistryError> {
        subscribe(&self.register, &self.state, key, self.sub_cap, self.what)
    }

    /// A subscriber stream for a source that is **infinite by contract**.
    ///
    /// As [`Registry::subscribe_infinite`]: `Full` and `Closed` both yield a
    /// stream that ends immediately `Aborted(ProducerGone { what })` under this
    /// fan-out's label, never `Complete`.
    #[must_use]
    pub fn subscribe_infinite(&self, key: K) -> Channel<T, Infinite> {
        self.subscribe(key)
            .unwrap_or_else(|_| Channel::aborted(self.what))
    }
}

pub(super) fn subscribe<K, T>(
    register: &Sender<(K, Sender<T>, u64)>,
    state: &State,
    key: K,
    sub_cap: NonZeroUsize,
    what: &'static str,
) -> Result<Channel<T, Infinite>, RegistryError>
where
    T: Clone + Send + 'static,
{
    let (tx, rx) = crossbeam_channel::bounded(sub_cap.get());
    let closed = state.closed.lock();
    if *closed {
        return Err(RegistryError::Closed);
    }
    // Read under the lock and before the registration queues: everything
    // numbered below this was published before this call.
    let since = state.published.load(Ordering::SeqCst);
    match register.try_send((key, tx, since)) {
        Ok(()) => Ok(Channel::from_infinite(rx, what)),
        Err(TrySendError::Full(_)) => Err(RegistryError::Full),
        Err(TrySendError::Disconnected(_)) => Err(RegistryError::Closed),
    }
}
