//! Wake-ups for a [`Level`](super::Level): the subscriber list its setter signals.

use crossbeam_channel::{Sender, TrySendError};
use parking_lot::Mutex;

use crate::channel::Channel;
use crate::kind::Infinite;

/// Wake-ups for a level the subscribers read themselves. Owned by whatever
/// moves the level ([`crate::Level`]), which calls [`wake`](Self::wake) right
/// after each move and [`close`](Self::close) when it is gone.
pub(super) struct Wakes {
    /// `None` once closed: every subscriber has been released.
    subscribers: Mutex<Option<Vec<Sender<()>>>>,
    what: &'static str,
}

impl Wakes {
    #[must_use]
    pub(super) fn new(what: &'static str) -> Self {
        Self {
            subscribers: Mutex::new(Some(Vec::new())),
            what,
        }
    }

    /// A wake-up on every later move; read the level after each. Infinite:
    /// it ends `Aborted` only when the owner closes or drops it.
    pub(super) fn subscribe(&self) -> Channel<(), Infinite> {
        let mut guard = self.subscribers.lock();
        let Some(subscribers) = guard.as_mut() else {
            return Channel::aborted(self.what);
        };
        let (tx, rx) = crossbeam_channel::bounded(1);
        subscribers.push(tx);
        Channel::from_infinite(rx, self.what)
    }

    /// Wake every subscriber without blocking; a departed one is removed here.
    pub(super) fn wake(&self) {
        if let Some(subscribers) = self.subscribers.lock().as_mut() {
            subscribers
                .retain(|tx| !matches!(tx.try_send(()), Err(TrySendError::Disconnected(()))));
        }
    }

    /// Release every subscriber: their streams end `Aborted(ProducerGone)`,
    /// and later subscriptions end at once.
    pub(super) fn close(&self) {
        self.subscribers.lock().take();
    }

    /// Test-only observation of the subscriber set.
    #[cfg(test)]
    pub(super) fn subscriber_count(&self) -> usize {
        self.subscribers.lock().as_ref().map_or(0, Vec::len)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;

    #[test]
    fn pending_wakes_collapse_and_never_hang_up() {
        let wakes = Wakes::new("wakes");
        let mut sub = wakes.subscribe();
        wakes.wake();
        wakes.wake();
        wakes.wake();
        assert_eq!(wakes.subscriber_count(), 1);
        assert_eq!(sub.try_recv(), Some(()));
        assert_eq!(sub.try_recv(), None, "three moves, one pending wake");
    }

    #[test]
    fn a_dropped_wake_subscriber_is_removed_on_the_next_move() {
        let wakes = Wakes::new("wakes");
        let _kept = wakes.subscribe();
        drop(wakes.subscribe());
        assert_eq!(wakes.subscriber_count(), 2, "not noticed until a move");

        wakes.wake();
        assert_eq!(wakes.subscriber_count(), 1);
    }
}
