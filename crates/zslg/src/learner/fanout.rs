//! Per-subscriber fan-out of a log's live events, on crossbeam — the stable
//! log's [`StableLogEvent`]s and the local store's unstable entries.
//!
//! Every subscriber gets its own bounded queue. The interesting case is the one
//! that cannot keep up: **the fan-out hangs up on it** — drops its sender — and
//! that disconnect is the whole lag protocol. It is in-band, so it is *ordered*
//! with the data: the consumer drains every event that was queued and only then
//! sees the end. An out-of-band flag would be observable while stale events
//! were still queued, and would need clearing without racing the next overrun.
//!
//! A sentinel event cannot do this job: it would need a slot at exactly the
//! moment there are none. Hanging up costs no capacity.
//!
//! For the consumer, [`End::Aborted`] on an infinite stream already means
//! "resume from your last position" (CLAUDE.md §20), which is precisely the
//! response — replay what was missed, then subscribe afresh (`learner::read`).
//! Overrun and a departed consumer are therefore the same event here, handled
//! by the same line: remove the slot.
//!
//! [`Wakes`] is the payload-free sibling, for a *level* rather than a stream:
//! one slot per subscriber, and a full slot is simply a wake-up still pending,
//! which stands for any number of further moves. Nothing can be lost to a full
//! slot, so only a departed subscriber is removed.
//!
//! [`StableLogEvent`]: crate::meta::StableLogEvent

use std::num::NonZeroUsize;

use parking_lot::Mutex;

use crate::channel::Channels;

/// The live subscriber set. Senders only: a consumer that drops its
/// [`Channels`] makes the next `try_send` fail exactly like an overrun does.
pub(in crate::learner) struct Fanout<T> {
    subscribers: Mutex<Vec<crossbeam_channel::Sender<T>>>,
    /// Per-subscriber queue depth — how far behind a consumer may fall before
    /// it is hung up on and has to replay.
    capacity: NonZeroUsize,
    /// Names the subscription in an `Aborted` verdict and in the log lines.
    what: &'static str,
}

impl<T: Clone + Send + 'static> Fanout<T> {
    pub(in crate::learner) fn new(capacity: NonZeroUsize, what: &'static str) -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
            capacity,
            what,
        }
    }

    /// A new subscription, live from this moment. Infinite by construction:
    /// completion is not something that happens to a log tail, so every
    /// termination is an [`crate::channel::End::Aborted`] telling the consumer
    /// to resume from its own cursor.
    pub(in crate::learner) fn subscribe(&self) -> Channels<T> {
        let (tx, rx) = crossbeam_channel::bounded(self.capacity.get());
        let mut subscribers = self.subscribers.lock();
        subscribers.push(tx);
        let live = subscribers.len();
        drop(subscribers);
        tracing::debug!(
            live,
            capacity = self.capacity.get(),
            what = self.what,
            "fan-out: new subscriber"
        );
        Channels::from_infinite(rx, self.what)
    }

    /// Fan one event out; see [`send_all`](Self::send_all).
    pub(in crate::learner) fn send(&self, event: &T) -> usize {
        self.send_all(std::slice::from_ref(event))
    }

    /// Fan `events` out, in order, to every subscriber. A subscriber whose
    /// queue fills, or whose consumer has gone, is dropped here — the two are
    /// the same case. Returns how many were hung up on.
    ///
    /// Synchronous throughout (a `parking_lot` critical section that never
    /// awaits, CLAUDE.md §4); called from a writer's blocking thread, and never
    /// blocks on a consumer.
    pub(in crate::learner) fn send_all(&self, events: &[T]) -> usize {
        let mut subscribers = self.subscribers.lock();
        let before = subscribers.len();
        subscribers.retain(|tx| {
            events
                .iter()
                .all(|event| tx.try_send(event.clone()).is_ok())
        });
        let dropped = before.saturating_sub(subscribers.len());
        let remaining = subscribers.len();
        drop(subscribers);

        // The consumer sees only an `Aborted`; this is the side that knows why.
        if dropped > 0 {
            tracing::debug!(
                dropped,
                remaining,
                capacity = self.capacity.get(),
                what = self.what,
                "fan-out: hung up on subscribers that were full or gone"
            );
        }
        dropped
    }

    /// Live subscriber count — subscribers that have gone are only noticed on
    /// the next [`send`](Self::send). Test-only: an observation, not a trigger,
    /// and nothing in production needs it (docs/TESTING.md).
    #[cfg(test)]
    pub(in crate::learner) fn subscriber_count(&self) -> usize {
        self.subscribers.lock().len()
    }
}

/// Wake-ups for a level the subscribers read themselves. Owned by whatever
/// moves the level, which calls [`wake`](Self::wake) right after each move.
pub(in crate::learner) struct Wakes {
    subscribers: Mutex<Vec<crossbeam_channel::Sender<()>>>,
    what: &'static str,
}

impl Wakes {
    pub(in crate::learner) fn new(what: &'static str) -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
            what,
        }
    }

    /// A wake-up on every later move; read the level after each. Infinite:
    /// it ends `Aborted` only when the owner is gone.
    pub(in crate::learner) fn subscribe(&self) -> Channels<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.subscribers.lock().push(tx);
        Channels::from_infinite(rx, self.what)
    }

    /// Wake every subscriber without blocking; a departed one is removed here.
    pub(in crate::learner) fn wake(&self) {
        self.subscribers.lock().retain(|tx| {
            !matches!(
                tx.try_send(()),
                Err(crossbeam_channel::TrySendError::Disconnected(()))
            )
        });
    }

    /// Test-only observation, as [`Fanout::subscriber_count`].
    #[cfg(test)]
    pub(in crate::learner) fn subscriber_count(&self) -> usize {
        self.subscribers.lock().len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;
    use crate::channel::End;
    use crate::hlc::Hlc;
    use crate::meta::StableLogEvent;
    use crate::unit::nonzero;

    fn watermark(hlc: u64) -> StableLogEvent {
        StableLogEvent::Watermark(Hlc(hlc))
    }

    fn hlc_of(ev: &StableLogEvent) -> u64 {
        match ev {
            StableLogEvent::Watermark(h) => h.raw(),
            StableLogEvent::Appended { .. } => panic!("only watermarks in these tests"),
        }
    }

    #[test]
    fn every_live_subscriber_gets_every_event() {
        let fanout = Fanout::new(nonzero(4), "stable log subscription");
        let mut a = fanout.subscribe();
        let mut b = fanout.subscribe();

        fanout.send(&watermark(1));
        fanout.send(&watermark(2));

        for sub in [&mut a, &mut b] {
            assert_eq!(hlc_of(&sub.recv().unwrap()), 1);
            assert_eq!(hlc_of(&sub.recv().unwrap()), 2);
        }
    }

    /// Overrun is signalled **in band**: the fan-out hangs up, so the consumer
    /// drains everything that was queued and only then sees the end. An
    /// out-of-band flag could be observed while stale items were still queued.
    #[test]
    fn an_overrun_subscriber_is_hung_up_on_after_its_queued_events() {
        let fanout = Fanout::new(nonzero(2), "stable log subscription");
        let mut sub = fanout.subscribe();

        fanout.send(&watermark(1));
        fanout.send(&watermark(2));
        fanout.send(&watermark(3)); // no room — hang up

        assert_eq!(hlc_of(&sub.recv().unwrap()), 1);
        assert_eq!(hlc_of(&sub.recv().unwrap()), 2);
        assert_eq!(
            sub.recv().err(),
            Some(End::Aborted {
                what: "stable log subscription"
            }),
            "the hang-up arrives after the queued events, never before"
        );
    }

    /// A hang-up is per subscriber: one slow consumer must not cost the others
    /// their events.
    #[test]
    fn hanging_up_on_one_subscriber_leaves_the_others_untouched() {
        let fanout = Fanout::new(nonzero(2), "stable log subscription");
        let _slow = fanout.subscribe();
        let mut fast = fanout.subscribe();

        for hlc in 1..=3 {
            fanout.send(&watermark(hlc));
            let _ = fast.recv().unwrap(); // drained every time
        }
        assert_eq!(fanout.subscriber_count(), 1, "only the slow one hung up");
    }

    #[test]
    fn a_dropped_subscriber_is_pruned_on_the_next_send() {
        let fanout = Fanout::new(nonzero(4), "stable log subscription");
        drop(fanout.subscribe());
        assert_eq!(fanout.subscriber_count(), 1, "not noticed until a send");

        fanout.send(&watermark(1));
        assert_eq!(fanout.subscriber_count(), 0);
    }

    /// A wake is a level: moves made while one is pending collapse into it,
    /// and a slow subscriber is never dropped for it.
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
