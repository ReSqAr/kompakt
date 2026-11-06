//! The producer side: [`Fanout`], its publish count and its stop handle.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender, TrySendError};
use parking_lot::Mutex;

use super::error::{Stopped, TryPublishError};

/// What the producer, the registries and the thread share: whether
/// registration is closed, and how many elements have been published.
///
/// The count orders publishes against subscribes. A publish takes the next
/// number before it queues; a subscribe records the count when it registers;
/// the thread then withholds from a subscriber every element numbered below its
/// record. Without it, a registration (which the thread prefers over queued
/// data) could hand a new subscriber elements that were published — and
/// returned — before `subscribe` was called.
pub(super) struct State {
    pub(super) closed: Mutex<bool>,
    pub(super) published: AtomicU64,
}

/// The producer side of a fan-out: the data sender and the thread's lifecycle.
///
/// Dropping it cancels the thread without joining; [`shutdown`](Self::shutdown)
/// cancels and joins. Both discard what is still queued: `publish` returns once
/// an element is *queued*, not delivered. [`drain`](Self::drain) delivers it
/// first.
pub struct Fanout<T> {
    pub(super) tx: Sender<(u64, T)>,
    pub(super) shutdown: Shutdown,
    pub(super) state: Arc<State>,
}

impl<T> Fanout<T> {
    /// Queue `item` for every matching subscriber, waiting for room.
    ///
    /// # Errors
    ///
    /// [`Stopped`] with the item when the distribution thread is gone.
    pub fn publish(&self, item: T) -> Result<(), Stopped<T>> {
        let seq = self.state.published.fetch_add(1, Ordering::SeqCst);
        self.tx
            .send((seq, item))
            .map_err(|e| Stopped(e.into_inner().1))
    }

    /// Queue `item` without waiting.
    ///
    /// # Errors
    ///
    /// The item and why: the data queue is full, or the thread is gone.
    pub fn try_publish(&self, item: T) -> Result<(), TryPublishError<T>> {
        let seq = self.state.published.fetch_add(1, Ordering::SeqCst);
        self.tx.try_send((seq, item)).map_err(|e| match e {
            TrySendError::Full((_, item)) => TryPublishError::Full(item),
            TrySendError::Disconnected((_, item)) => TryPublishError::Stopped(item),
        })
    }

    /// Finish delivering what is queued, then stop. Blocking: from async code,
    /// call it on the blocking pool.
    ///
    /// Dropping or [`shutdown`](Self::shutdown) cancel at once, and the thread
    /// prefers cancellation over queued data, so an element `publish` already
    /// returned for can be discarded undelivered. `drain` closes the data queue
    /// instead and lets the thread empty it; every subscriber then ends
    /// `ProducerGone` after its last element. A `Block` subscriber that never
    /// reads would hold that forever, so the wait is bounded by `grace`: after
    /// it the thread is cancelled as `shutdown` would.
    pub fn drain(self, grace: std::time::Duration) {
        let Self {
            tx, mut shutdown, ..
        } = self;
        drop(tx);
        if let Some(done) = shutdown.done.take() {
            // The thread drops its end when it exits, which wakes this at once.
            let _ = done.recv_timeout(grace);
        }
        shutdown.wait();
    }

    /// Stop the distribution thread and wait for it. Blocking: from async code,
    /// call it on the blocking pool.
    pub fn shutdown(self) {
        let Self { tx, shutdown, .. } = self;
        drop(tx);
        shutdown.wait();
    }
}

/// The thread's stop signal and join handle. `cancel` is idempotent; dropping
/// cancels without joining.
pub(super) struct Shutdown {
    pub(super) signal: Option<Sender<()>>,
    pub(super) join: Option<JoinHandle<()>>,
    /// Disconnects when the thread exits; never carries a value.
    pub(super) done: Option<Receiver<()>>,
}

impl Shutdown {
    fn cancel(&mut self) {
        if let Some(tx) = self.signal.take() {
            let _ = tx.try_send(());
        }
    }

    fn wait(mut self) {
        self.cancel();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for Shutdown {
    fn drop(&mut self) {
        self.cancel();
    }
}
