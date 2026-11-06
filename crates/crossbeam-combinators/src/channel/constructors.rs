//! Streams built from values rather than from a receiver.

use super::Channel;
use crate::kind::{Finite, Infinite};

impl<T: Send + 'static> Channel<T, Finite> {
    /// A closed, empty stream: `End::Complete` immediately.
    #[must_use]
    pub fn empty() -> Self {
        Self::from_vec(Vec::new())
    }

    /// A closed stream of exactly one item.
    #[must_use]
    pub fn once(item: T) -> Self {
        Self::from_vec(vec![item])
    }

    /// A complete, finite stream: everything is already here, so it closes
    /// itself and can never be `Aborted`.
    #[must_use]
    pub fn from_vec(items: Vec<T>) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(items.len().max(1));
        for item in items {
            let _ = tx.send(item);
        }
        drop(tx);
        let (ch, closer) = Self::from(rx, "from_vec");
        closer.close();
        ch
    }
}

impl<T: Send + 'static> Channel<T, Infinite> {
    /// An aborted, empty stream: `Aborted(ProducerGone { what })` immediately.
    ///
    /// Use this when a producer cannot be started (a closed registry, a full
    /// registration queue) and the consumer contract is an infinite stream:
    /// ending `Complete` would look like a deliberate finish.
    #[must_use]
    pub fn aborted(what: &'static str) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(1);
        drop(tx);
        Self::from_infinite(rx, what)
    }
}
