//! The write half of a [`Channels`]: a sender plus the completion proof.
//!
//! A producer writes through a `Feed` and calls [`Feed::close`] on its normal
//! exit. Dropping the feed without closing — a panic, an early return, a task
//! discarded before it ran — yields [`End::Aborted`](crate::End) instead of a
//! clean finish. The caller owns the thread/executor; `Feed` never spawns.
//!
//! ```text
//!   let (feed, ch) = Feed::channel(cap, "scan");
//!   spawn(move || {
//!       for item in scan { if !feed.send(item) { break; } }
//!       feed.close();
//!   });
//!   ch
//! ```
//!
//! [`Feed::send`] returning `false` is the consumer-gone signal. `close` closes
//! before the data sender drops, so the consumer cannot see the disconnect as
//! an abort first.

use crossbeam_channel::{Sender, TrySendError};

use crate::channel::{Channels, Closer};

/// The producer's handle on one [`Channels`].
pub struct Feed<T> {
    tx: Sender<T>,
    closer: Closer,
}

impl<T: Send + 'static> Feed<T> {
    /// Create the write/read pair. The caller spawns the producer body.
    #[must_use]
    pub fn channel(cap: usize, what: &'static str) -> (Self, Channels<T>) {
        let (tx, rx) = crossbeam_channel::bounded(cap);
        let (ch, closer) = Channels::from(rx, what);
        (Self { tx, closer }, ch)
    }
}

impl<T> Feed<T> {
    /// Blocking send. `false` means the consumer is gone; stop sending.
    pub fn send(&self, item: T) -> bool {
        self.tx.send(item).is_ok()
    }

    /// Non-blocking send.
    ///
    /// # Errors
    ///
    /// Returns the item and the reason when the queue is full or the consumer
    /// is gone.
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.tx.try_send(item)
    }

    /// Declare the stream complete: everything meant to be sent has been sent.
    /// This closes before the data sender drops, so the consumer sees
    /// `End::Complete` after draining.
    pub fn close(self) {
        let Self { tx, closer } = self;
        closer.close();
        drop(tx);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;
    use crate::End;
    use crate::channel::AbortReason;

    #[test]
    fn close_yields_closed_after_draining() {
        let (feed, mut ch) = Feed::channel(4, "test");
        assert!(feed.send(1));
        assert!(feed.send(2));
        feed.close();
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn dropping_the_feed_without_close_aborts() {
        let (feed, mut ch) = Feed::channel(4, "test");
        assert!(feed.send(1));
        drop(feed);
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "test" }
            })
        );
    }

    #[test]
    fn a_panicking_producer_aborts() {
        let (feed, mut ch) = Feed::<u32>::channel(4, "scan");
        let handle = std::thread::spawn(move || {
            let _feed = feed;
            panic!("scan blew up");
        });
        assert!(handle.join().is_err());
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "scan" }
            })
        );
    }

    #[test]
    fn send_reports_a_departed_consumer() {
        let (feed, ch) = Feed::<u32>::channel(4, "test");
        drop(ch);
        assert!(!feed.send(1), "the consumer is gone");
    }

    #[test]
    fn try_send_reports_full() {
        let (feed, _ch) = Feed::channel(1, "test");
        assert!(feed.try_send(1).is_ok());
        assert!(matches!(feed.try_send(2), Err(TrySendError::Full(2))));
    }

    #[test]
    fn a_feed_discarded_before_it_runs_aborts() {
        let (feed, mut ch) = Feed::<u32>::channel(4, "never ran");
        drop(feed);
        assert_eq!(
            ch.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "never ran" }
            })
        );
    }

    /// A consumer parked in `recv` is woken by a later `Feed::send`, and the
    /// close still lands after the item.
    #[test]
    fn recv_wakes_when_a_feed_sends_later() {
        let (feed, mut ch) = Feed::channel(1, "wake");
        let producer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            assert!(feed.send(7));
            feed.close();
        });
        assert_eq!(ch.recv(), Ok(7));
        assert_eq!(ch.recv(), Err(End::Complete));
        producer.join().unwrap();
    }
}
