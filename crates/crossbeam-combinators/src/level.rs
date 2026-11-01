//! A watched value: one writer, many readers, blocking and async.
//!
//! ```text
//!   setter.set(v):   value := v, version += 1   ──then──▶  wake every reader
//!   level.get()                      the current value
//!   level.changes()                  Channel<()>: one wake-up per move (coalesced)
//!   level.changed().await            async: the next move after the last one seen
//! ```
//!
//! The value is written before anyone is woken, so a woken reader always sees
//! the move that woke it. That order is the reason this type exists: two
//! notification paths moved by hand at every writer is two places to get it
//! wrong. Dropping the setter ends `changes()` streams (`ProducerGone`) and
//! makes `changed()` return [`LevelGone`].

use std::sync::Arc;

use parking_lot::Mutex;

use crate::channel::Channel;
use crate::fanout::Wakes;

/// The writing half of a [`Level`].
pub struct LevelSetter<T> {
    shared: Arc<Shared<T>>,
}

/// A reader of a watched value. Each reader tracks the last move it saw for
/// [`changed`](Self::changed); a clone is a fresh reader that has seen the
/// current value (like subscribing to a `tokio::sync::watch`).
pub struct Level<T> {
    shared: Arc<Shared<T>>,
    seen: u64,
}

/// The setter is gone: the value will not move again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelGone;

struct Shared<T> {
    state: Mutex<State<T>>,
    wakes: Wakes,
    #[cfg(feature = "tokio")]
    notify: tokio::sync::Notify,
}

struct State<T> {
    value: T,
    version: u64,
    setter_alive: bool,
}

impl<T: Clone + Send + 'static> Level<T> {
    /// A watched value starting at `initial`. `what` labels the `changes()`
    /// streams' end.
    #[must_use]
    pub fn new(what: &'static str, initial: T) -> (LevelSetter<T>, Level<T>) {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                value: initial,
                version: 0,
                setter_alive: true,
            }),
            wakes: Wakes::new(what),
            #[cfg(feature = "tokio")]
            notify: tokio::sync::Notify::new(),
        });
        (
            LevelSetter {
                shared: Arc::clone(&shared),
            },
            Level { shared, seen: 0 },
        )
    }

    /// The current value.
    #[must_use]
    pub fn get(&self) -> T {
        self.shared.state.lock().value.clone()
    }

    /// A wake-up after every later move; read [`get`](Self::get) after each.
    /// Moves between two reads collapse into one wake-up. Infinite: it ends
    /// `Aborted(ProducerGone)` when the setter is dropped.
    #[must_use]
    pub fn changes(&self) -> Channel<()> {
        self.shared.wakes.subscribe()
    }

    /// Wait for a move this reader has not seen yet.
    ///
    /// # Errors
    ///
    /// [`LevelGone`] once the setter is dropped and every move has been seen.
    #[cfg(feature = "tokio")]
    pub async fn changed(&mut self) -> Result<(), LevelGone> {
        loop {
            // Created before the check: `notify_waiters` reaches a `Notified`
            // from the moment it exists, so a move between the check and the
            // await is not lost.
            let notified = self.shared.notify.notified();
            {
                let state = self.shared.state.lock();
                if state.version > self.seen {
                    self.seen = state.version;
                    return Ok(());
                }
                if !state.setter_alive {
                    return Err(LevelGone);
                }
            }
            notified.await;
        }
    }

    /// Wait until `pred` holds for the current value, and return that value.
    ///
    /// # Errors
    ///
    /// [`LevelGone`] if the setter is dropped before `pred` holds.
    #[cfg(feature = "tokio")]
    pub async fn wait_for(&mut self, mut pred: impl FnMut(&T) -> bool) -> Result<T, LevelGone> {
        loop {
            let value = self.get();
            if pred(&value) {
                return Ok(value);
            }
            self.changed().await?;
        }
    }
}

impl<T> Clone for Level<T> {
    fn clone(&self) -> Self {
        let seen = self.shared.state.lock().version;
        Self {
            shared: Arc::clone(&self.shared),
            seen,
        }
    }
}

impl<T: Clone + Send + 'static> LevelSetter<T> {
    /// Move the value, then wake every reader.
    pub fn set(&self, value: T) {
        {
            let mut state = self.shared.state.lock();
            state.value = value;
            state.version = state.version.wrapping_add(1);
        }
        self.shared.wakes.wake();
        #[cfg(feature = "tokio")]
        self.shared.notify.notify_waiters();
    }

    /// The current value, as a reader would see it.
    #[must_use]
    pub fn get(&self) -> T {
        self.shared.state.lock().value.clone()
    }
}

impl<T> Drop for LevelSetter<T> {
    fn drop(&mut self) {
        self.shared.state.lock().setter_alive = false;
        self.shared.wakes.close();
        #[cfg(feature = "tokio")]
        self.shared.notify.notify_waiters();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;
    use crate::{AbortReason, End};

    #[test]
    fn set_moves_the_value_then_wakes() {
        let (setter, level) = Level::new("level", 1u32);
        let mut changes = level.changes();
        setter.set(2);
        assert_eq!(changes.recv(), Ok(()));
        assert_eq!(level.get(), 2, "a woken reader sees the move that woke it");
    }

    #[test]
    fn moves_between_reads_collapse() {
        let (setter, level) = Level::new("level", 0u32);
        let mut changes = level.changes();
        setter.set(1);
        setter.set(2);
        assert_eq!(changes.recv(), Ok(()));
        assert_eq!(changes.try_recv(), None);
        assert_eq!(level.get(), 2);
    }

    #[test]
    fn dropping_the_setter_ends_changes() {
        let (setter, level) = Level::new("level", 0u32);
        let mut changes = level.changes();
        drop(setter);
        assert_eq!(
            changes.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "level" }
            })
        );
        let mut late = level.changes();
        assert!(
            late.recv().is_err(),
            "subscribing after the setter is gone ends at once"
        );
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn changed_and_wait_for() {
        let (setter, mut level) = Level::new("level", 0u32);
        let mut waiter = level.clone();
        let task = tokio::spawn(async move { waiter.wait_for(|v| *v >= 3).await });
        for v in 1..=3 {
            setter.set(v);
            tokio::task::yield_now().await;
        }
        assert_eq!(task.await.unwrap(), Ok(3));
        assert_eq!(
            level.changed().await,
            Ok(()),
            "the original reader has not seen the moves"
        );
        let mut fresh = level.clone();
        setter.set(4);
        assert_eq!(fresh.changed().await, Ok(()));
        drop(setter);
        assert_eq!(
            level.changed().await,
            Ok(()),
            "the move to 4 is still unseen"
        );
        assert_eq!(level.changed().await, Err(LevelGone));
    }
}
