//! The write half of a [`Channel`]: a sender plus the completion proof.
//!
//! A producer writes through a `Feed` and ends with [`Feed::close`], which
//! returns the [`Closed`] proof. A body that must return `Closed` cannot reach
//! a normal return without closing; dropping the feed instead — a panic, a
//! task discarded before it ran — yields `Aborted(ProducerGone)`.
//!
//! ```text
//!   Channel::produce_blocking(cap, "scan", |feed| {
//!       for item in scan { if feed.send(item).is_err() { break; } }
//!       feed.close()                       // -> Closed
//!   })
//! ```
//!
//! `close` closes before the data sender drops, so the consumer cannot see the
//! disconnect as an abort first. [`Feed::send`]'s `Err` is the consumer-gone
//! signal and hands the item back.

use std::time::Duration;

use crossbeam_channel::{SendTimeoutError, Sender, TrySendError};

use crate::channel::{Channel, Closer};
use crate::kind::{Finite, Kind};

/// The producer's handle on one [`Channel`] of kind `K`. A finite feed ends
/// with [`close`](Feed::close); an infinite one has nothing to close — its
/// stream ends `Aborted(ProducerGone)` when the feed is dropped.
pub struct Feed<T, K> {
    tx: Sender<T>,
    /// Always `Some` for a finite feed, `None` for an infinite one.
    closer: Option<Closer>,
    _kind: std::marker::PhantomData<K>,
}

/// Proof that a [`Feed`] was closed: only [`Feed::close`] makes one.
#[must_use = "return the proof from the producer body"]
#[derive(Debug)]
pub struct Closed(());

/// The consumer is gone; the item is handed back.
#[derive(Debug, PartialEq, Eq)]
pub struct Disconnected<T>(pub T);

impl<T: Send + 'static, K: Kind> Feed<T, K> {
    /// Create the write/read pair. The caller spawns the producer body; prefer
    /// [`Channel::produce_blocking`] when a blocking-pool body will do.
    #[must_use]
    pub fn channel(cap: usize, what: &'static str) -> (Self, Channel<T, K>) {
        let (tx, rx) = crossbeam_channel::bounded(cap);
        let (ch, closer) = K::open(rx, what);
        (
            Self {
                tx,
                closer,
                _kind: std::marker::PhantomData,
            },
            ch,
        )
    }
}

impl<T, K> Feed<T, K> {
    /// Blocking send.
    ///
    /// # Errors
    ///
    /// [`Disconnected`] with the item when the consumer is gone; stop sending.
    pub fn send(&self, item: T) -> Result<(), Disconnected<T>> {
        self.tx.send(item).map_err(|e| Disconnected(e.into_inner()))
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

    /// Send, waiting at most `timeout` for room.
    ///
    /// # Errors
    ///
    /// Returns the item and the reason on timeout or when the consumer is gone.
    pub fn send_timeout(&self, item: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        self.tx.send_timeout(item, timeout)
    }

    /// The queue's capacity, for diagnostics.
    #[must_use]
    pub fn capacity(&self) -> Option<usize> {
        self.tx.capacity()
    }
}

impl<T> Feed<T, Finite> {
    /// Declare the stream complete: everything meant to be sent has been sent.
    /// This closes before the data sender drops, so the consumer sees
    /// `End::Complete` after draining.
    pub fn close(self) -> Closed {
        let Self { tx, closer, .. } = self;
        if let Some(closer) = closer {
            closer.close();
        }
        drop(tx);
        Closed(())
    }
}

#[cfg(feature = "tokio")]
impl<T: Send + 'static, K: Kind> Channel<T, K> {
    /// Run `body` on the blocking pool, feeding the returned channel.
    ///
    /// The body returns `K::Proof`: for a finite stream the [`Closed`] proof,
    /// so its normal return is a close; for an infinite one nothing. A panic
    /// drops the feed and the stream ends `Aborted(ProducerGone)`. Spawns at
    /// the call, so it needs a tokio runtime context there.
    pub fn produce_blocking<F>(cap: usize, what: &'static str, body: F) -> Channel<T, K>
    where
        F: FnOnce(Feed<T, K>) -> K::Proof + Send + 'static,
    {
        let (feed, ch) = Feed::channel(cap, what);
        tokio::task::spawn_blocking(move || {
            let _proof: K::Proof = body(feed);
        });
        ch
    }

    /// Run `body` as a tokio task, feeding the returned channel; the async dual
    /// of [`produce_blocking`](Self::produce_blocking), with the same proof
    /// and panic rules.
    pub fn produce_async<F, Fut>(cap: usize, what: &'static str, body: F) -> Channel<T, K>
    where
        F: FnOnce(Feed<T, K>) -> Fut,
        Fut: std::future::Future<Output = K::Proof> + Send + 'static,
    {
        let (feed, ch) = Feed::channel(cap, what);
        let task = body(feed);
        tokio::spawn(async move {
            let _proof: K::Proof = task.await;
        });
        ch
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
        let (feed, mut ch) = Feed::<_, Finite>::channel(4, "test");
        assert!(feed.send(1).is_ok());
        assert!(feed.send(2).is_ok());
        let _ = feed.close();
        assert_eq!(ch.recv(), Ok(1));
        assert_eq!(ch.recv(), Ok(2));
        assert_eq!(ch.recv(), Err(End::Complete));
    }

    #[test]
    fn dropping_the_feed_without_close_aborts() {
        let (feed, mut ch) = Feed::<_, Finite>::channel(4, "test");
        assert!(feed.send(1).is_ok());
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
        let (feed, mut ch) = Feed::<u32, Finite>::channel(4, "scan");
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
        let (feed, ch) = Feed::<u32, Finite>::channel(4, "test");
        drop(ch);
        assert_eq!(feed.send(1), Err(Disconnected(1)), "the consumer is gone");
    }

    #[test]
    fn try_send_reports_full() {
        let (feed, _ch) = Feed::<_, Finite>::channel(1, "test");
        assert!(feed.try_send(1).is_ok());
        assert!(matches!(feed.try_send(2), Err(TrySendError::Full(2))));
    }

    #[test]
    fn a_feed_discarded_before_it_runs_aborts() {
        let (feed, mut ch) = Feed::<u32, Finite>::channel(4, "never ran");
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
        let (feed, mut ch) = Feed::<_, Finite>::channel(1, "wake");
        let producer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            assert!(feed.send(7).is_ok());
            let _ = feed.close();
        });
        assert_eq!(ch.recv(), Ok(7));
        assert_eq!(ch.recv(), Err(End::Complete));
        producer.join().unwrap();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn produce_blocking_closes_on_return_and_aborts_on_panic() {
        let ch = Channel::<_, Finite>::produce_blocking(4, "ok", |feed| {
            let _ = feed.send(1u32);
            feed.close()
        });
        let got = tokio::task::spawn_blocking(move || {
            let mut ch = ch;
            (ch.recv(), ch.recv())
        })
        .await
        .unwrap();
        assert_eq!(got, (Ok(1), Err(End::Complete)));

        let ch =
            Channel::<u32, Finite>::produce_blocking(4, "boom", |_feed| panic!("scan blew up"));
        let got = tokio::task::spawn_blocking(move || {
            let mut ch = ch;
            ch.recv()
        })
        .await
        .unwrap();
        assert_eq!(
            got,
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "boom" }
            })
        );
    }
}
