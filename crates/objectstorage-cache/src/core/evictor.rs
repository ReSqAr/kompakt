//! The evictor: the one process that chooses victims and deletes files.
//! Callers only update the bookkeeping and send it a wake or a room request.
//!
//! ```text
//!   loop {
//!     serve every queued Room { bytes }    victims (held predictions last)
//!                                          until used + reserved + bytes ≤ hard
//!     on the tick (5 s): expire predictions, the idle sweep;
//!                        every minute, the recency flush
//!     drain: while used > soft             up to 1,024 victims per lock hold,
//!                                          deleted outside it; a queued Room
//!                                          interrupts between batches
//!     wait: a Room request, a wake, the tick, or the stop
//!   }
//! ```
//!
//! Soft and hard eviction never run at once, so they cannot race. Two Room
//! requests, served one after the other, may free a little more than both
//! needed together: accepted. The evictor holds the core weakly: it ends
//! with the last handle on the core, or when told to stop.

use super::Inner;
use crate::disk::Id;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::time::Instant;

/// How often predictions expire and idle files are looked for.
const TICK: Duration = Duration::from_secs(5);
/// How often use is written back to the files' modification times.
const FLUSH_EVERY: Duration = Duration::from_secs(60);
/// Victims per lock hold in the drain.
const BATCH: usize = 1024;

pub(super) enum Ask {
    /// Make room for `bytes` more under hard; the reply says it is done
    /// (whether or not anything could go).
    Room {
        bytes: u64,
        reply: oneshot::Sender<()>,
    },
    /// Reply once nothing is queued and the drain is done (tests).
    #[cfg(test)]
    Settle(oneshot::Sender<()>),
    /// Run the tick's duties now; reply with how many idle files went (tests).
    #[cfg(test)]
    Tick(oneshot::Sender<usize>),
}

pub(super) struct Channels {
    pub(super) wake: Arc<Notify>,
    pub(super) asks: mpsc::UnboundedReceiver<Ask>,
    pub(super) stop: watch::Receiver<bool>,
}

pub(super) async fn run(inner: Weak<Inner>, mut ch: Channels) {
    // A deadline looked at every turn, not only when idle: under a steady
    // stream of wakes and room requests the loop may never wait, and the
    // tick must still come.
    let mut next_tick = next_tick_from_now();
    let mut queued: Option<Ask> = None;
    let mut since_flush = Duration::ZERO;
    #[cfg(test)]
    let mut settles: Vec<oneshot::Sender<()>> = Vec::new();
    loop {
        {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            // Room first: a download waits on it.
            while let Some(ask) = queued.take().or_else(|| ch.asks.try_recv().ok()) {
                match ask {
                    Ask::Room { bytes, reply } => {
                        let victims = inner.state.lock().room(bytes, &inner.shape);
                        inner.delete(victims).await;
                        reply.send(()).ok();
                    }
                    #[cfg(test)]
                    Ask::Settle(reply) => settles.push(reply),
                    #[cfg(test)]
                    Ask::Tick(reply) => {
                        let swept = inner.on_tick().await;
                        reply.send(swept).ok();
                    }
                }
            }
            if Instant::now() >= next_tick {
                next_tick = next_tick_from_now();
                let swept = inner.on_tick().await;
                if swept > 0 {
                    tracing::info!(swept, "idle files evicted from the cache");
                }
                since_flush = since_flush.saturating_add(TICK);
                if since_flush >= FLUSH_EVERY {
                    since_flush = Duration::ZERO;
                    let flushing = inner.clone();
                    tokio::task::spawn_blocking(move || flushing.flush_recency())
                        .await
                        .ok();
                }
            }
            // The drain, interrupted by any request.
            let interrupted = loop {
                if !ch.asks.is_empty() {
                    break true;
                }
                let victims = inner.state.lock().drain(&inner.shape, BATCH);
                if victims.is_empty() {
                    break false;
                }
                inner.delete(victims).await;
            };
            if interrupted {
                continue;
            }
            #[cfg(test)]
            for reply in settles.drain(..) {
                reply.send(()).ok();
            }
        }
        tokio::select! {
            biased;
            _ = ch.stop.changed() => return,
            ask = ch.asks.recv() => match ask {
                Some(ask) => queued = Some(ask),
                None => return,
            },
            () = ch.wake.notified() => {}
            () = tokio::time::sleep_until(next_tick) => {}
        }
    }
}

fn next_tick_from_now() -> Instant {
    let now = Instant::now();
    now.checked_add(TICK).unwrap_or(now)
}

impl Inner {
    /// Delete evicted files, outside the lock and off the runtime's
    /// threads; then tell waiting downloads there may be room.
    async fn delete(self: &Arc<Self>, victims: Vec<Id>) {
        if victims.is_empty() {
            return;
        }
        let inner = self.clone();
        tokio::task::spawn_blocking(move || {
            for id in victims {
                if let Err(e) = inner.disk.remove(&id) {
                    tracing::warn!(?e, "could not delete an evicted cache file");
                }
            }
        })
        .await
        .ok();
        self.publish();
        self.room.notify_waiters();
    }

    /// Expire predictions, then evict idle files; returns how many went.
    async fn on_tick(self: &Arc<Self>) -> usize {
        let victims = {
            let mut st = self.state.lock();
            let now = SystemTime::now();
            st.expire(now);
            st.idle(now)
        };
        let swept = victims.len();
        self.delete(victims).await;
        self.publish();
        swept
    }

    /// Wake the evictor if the state is over soft (after unlocking it).
    pub(super) fn wake_if(&self, over_soft: bool) {
        if over_soft {
            self.wake.notify_one();
        }
    }
}
