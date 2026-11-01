//! Threaded fan-out: a broadcast or keyed registry with one distribution thread.
//!
//! Construction returns the three sides of the same fan-out:
//!
//! ```text
//!   (tx, registry, shutdown)
//!     │      │          │
//!     │      │          └── one owner stops and joins the thread
//!     │      └───────────── cloneable subscribe side
//!     └──────────────────── raw bounded data sender
//! ```
//!
//! The thread owns the subscriber set, so routing, registration and
//! cancellation are serialized without a lock. It receives one element at a
//! time (phase 1) and sends that element to every matching subscriber (phase 2).
//! The slowest subscriber paces a `Block` fan-out; `Shed` drops a subscriber
//! whose queue is full. In both policies the shutdown receiver is in the select,
//! so a full subscriber cannot hide cancellation.
//!
//! A subscriber stream is infinite by construction: it ends with
//! [`AbortReason::ProducerGone`](crate::AbortReason::ProducerGone) when the
//! thread stops (explicit shutdown, data sender dropped, or producer gone). A
//! caller that wants `Cancelled` binds its own
//! [`Cancellation`](crate::Cancellation) with `until`.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Select, Sender, TrySendError};
use parking_lot::Mutex;

use crate::channel::Channel;

/// What the distribution thread does when a subscriber's queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Wait for the slowest subscriber. The data queue backpressures the
    /// producer while the element is delivered.
    Block,
    /// Drop a subscriber whose queue is full or whose consumer is gone.
    Shed,
}

/// A failed `Registry::subscribe`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryError {
    /// The bounded registration queue is full.
    Full,
    /// The fan-out is gone.
    Closed,
}

/// A fan-out's single lifecycle owner.
///
/// It owns the one-shot shutdown sender and the thread's join handle.
/// `cancel` is idempotent: the first call sends without blocking, later calls
/// see `None`. `wait` cancels and joins. Dropping it cancels but does not join.
pub struct Shutdown {
    signal: Option<Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl Shutdown {
    /// Signal the distribution thread to stop. The first call cannot block.
    pub fn cancel(&mut self) {
        if let Some(tx) = self.signal.take() {
            let _ = tx.try_send(());
        }
    }

    /// Cancel (if still needed) and join the distribution thread.
    ///
    /// Blocking. In async code call it from `spawn_blocking`, or use
    /// [`shutdown_on_token`] under the `tokio` feature.
    pub fn wait(mut self) {
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

/// The broadcast subscribe side.
pub struct Registry<T> {
    register: Sender<((), Sender<T>)>,
    sub_cap: NonZeroUsize,
    what: &'static str,
    state: Arc<Mutex<bool>>,
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
    pub fn subscribe(&self) -> Result<Channel<T>, RegistryError> {
        subscribe(&self.register, &self.state, (), self.sub_cap, self.what)
    }
}

/// The keyed subscribe side.
pub struct KeyedRegistry<K, T> {
    register: Sender<(K, Sender<T>)>,
    sub_cap: NonZeroUsize,
    what: &'static str,
    state: Arc<Mutex<bool>>,
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
    pub fn subscribe(&self, key: K) -> Result<Channel<T>, RegistryError> {
        subscribe(&self.register, &self.state, key, self.sub_cap, self.what)
    }
}

fn subscribe<K, T>(
    register: &Sender<(K, Sender<T>)>,
    state: &Mutex<bool>,
    key: K,
    sub_cap: NonZeroUsize,
    what: &'static str,
) -> Result<Channel<T>, RegistryError>
where
    T: Clone + Send + 'static,
{
    let (tx, rx) = crossbeam_channel::bounded(sub_cap.get());
    let closed = state.lock();
    if *closed {
        return Err(RegistryError::Closed);
    }
    match register.try_send((key, tx)) {
        Ok(()) => Ok(Channel::from_infinite(rx, what)),
        Err(TrySendError::Full(_)) => Err(RegistryError::Full),
        Err(TrySendError::Disconnected(_)) => Err(RegistryError::Closed),
    }
}

struct Subscriber<K, T> {
    key: K,
    tx: Sender<T>,
}

/// Spawn a broadcast fan-out.
///
/// `route` is implicit (`K = ()`): every subscriber receives every element.
#[must_use]
pub fn broadcast<T>(
    what: &'static str,
    data_cap: NonZeroUsize,
    register_cap: NonZeroUsize,
    sub_cap: NonZeroUsize,
    overflow: OverflowPolicy,
) -> (Sender<T>, Registry<T>, Shutdown)
where
    T: Clone + Send + 'static,
{
    let (data_tx, data_rx) = crossbeam_channel::bounded(data_cap.get());
    let (register_tx, register_rx) = crossbeam_channel::bounded(register_cap.get());
    let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded(1);
    let state = Arc::new(Mutex::new(false));
    let join = spawn(
        data_rx,
        register_rx,
        shutdown_rx,
        |_: &T| Some(()),
        overflow,
        Arc::clone(&state),
    );
    let registry = Registry {
        register: register_tx,
        sub_cap,
        what,
        state,
    };
    let shutdown = Shutdown {
        signal: Some(shutdown_tx),
        join: Some(join),
    };
    (data_tx, registry, shutdown)
}

/// Spawn a keyed fan-out. `route` returns the subscriber key for an element;
/// `None` means no subscriber receives it.
#[must_use]
pub fn keyed<K, T, F>(
    what: &'static str,
    route: F,
    data_cap: NonZeroUsize,
    register_cap: NonZeroUsize,
    sub_cap: NonZeroUsize,
    overflow: OverflowPolicy,
) -> (Sender<T>, KeyedRegistry<K, T>, Shutdown)
where
    K: Eq + Copy + Send + 'static,
    T: Clone + Send + 'static,
    F: Fn(&T) -> Option<K> + Send + 'static,
{
    let (data_tx, data_rx) = crossbeam_channel::bounded(data_cap.get());
    let (register_tx, register_rx) = crossbeam_channel::bounded(register_cap.get());
    let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded(1);
    let state = Arc::new(Mutex::new(false));
    let join = spawn(
        data_rx,
        register_rx,
        shutdown_rx,
        route,
        overflow,
        Arc::clone(&state),
    );
    let registry = KeyedRegistry {
        register: register_tx,
        sub_cap,
        what,
        state,
    };
    let shutdown = Shutdown {
        signal: Some(shutdown_tx),
        join: Some(join),
    };
    (data_tx, registry, shutdown)
}

fn spawn<K, T, F>(
    data_rx: Receiver<T>,
    register_rx: Receiver<(K, Sender<T>)>,
    shutdown_rx: Receiver<()>,
    route: F,
    overflow: OverflowPolicy,
    state: Arc<Mutex<bool>>,
) -> JoinHandle<()>
where
    K: Eq + Copy + Send + 'static,
    T: Clone + Send + 'static,
    F: Fn(&T) -> Option<K> + Send + 'static,
{
    thread::Builder::new()
        .name("fanout".to_owned())
        .spawn(move || run(data_rx, register_rx, shutdown_rx, route, overflow, state))
        .expect("spawn fan-out thread")
}

fn run<K, T, F>(
    data_rx: Receiver<T>,
    register_rx: Receiver<(K, Sender<T>)>,
    shutdown_rx: Receiver<()>,
    route: F,
    overflow: OverflowPolicy,
    state: Arc<Mutex<bool>>,
) where
    K: Eq + Copy + Send + 'static,
    T: Clone + Send + 'static,
    F: Fn(&T) -> Option<K> + Send + 'static,
{
    let mut subscribers: Vec<Subscriber<K, T>> = Vec::new();
    let mut register_open = true;

    'fanout: loop {
        // Phase 1: one element, a registration, or shutdown.
        let mut sel = Select::new_biased();
        let shutdown_op = sel.recv(&shutdown_rx);
        let register_op = register_open.then(|| sel.recv(&register_rx));
        let data_op = sel.recv(&data_rx);
        let op = sel.select();

        if op.index() == shutdown_op {
            let _ = op.recv(&shutdown_rx);
            break 'fanout;
        }
        if Some(op.index()) == register_op {
            match op.recv(&register_rx) {
                Ok((key, tx)) => subscribers.push(Subscriber { key, tx }),
                Err(_) => register_open = false,
            }
            continue;
        }
        debug_assert_eq!(op.index(), data_op);
        let element = match op.recv(&data_rx) {
            Ok(element) => element,
            Err(_) => break 'fanout,
        };

        // Phase 2: the current element's recipient set is fixed.
        let key = route(&element);
        let matches = |subscriber: &Subscriber<K, T>| key == Some(subscriber.key);

        match overflow {
            OverflowPolicy::Shed => {
                let mut dead = Vec::new();
                for (i, subscriber) in subscribers.iter().enumerate() {
                    if !matches(subscriber) {
                        continue;
                    }
                    if subscriber.tx.try_send(element.clone()).is_err() {
                        dead.push(i);
                    }
                }
                remove_descending(&mut subscribers, dead);
            }
            OverflowPolicy::Block => {
                let mut remaining: Vec<(usize, Sender<T>)> = subscribers
                    .iter()
                    .enumerate()
                    .filter(|(_, subscriber)| matches(subscriber))
                    .map(|(i, subscriber)| (i, subscriber.tx.clone()))
                    .collect();
                let mut dead = Vec::new();

                while !remaining.is_empty() {
                    let senders: Vec<Sender<T>> =
                        remaining.iter().map(|(_, tx)| tx.clone()).collect();
                    let mut sel = Select::new_biased();
                    let shutdown_op = sel.recv(&shutdown_rx);
                    let register_op = register_open.then(|| sel.recv(&register_rx));
                    let send_ops: Vec<usize> = senders.iter().map(|tx| sel.send(tx)).collect();
                    let op = sel.select();

                    if op.index() == shutdown_op {
                        let _ = op.recv(&shutdown_rx);
                        break 'fanout;
                    }
                    if Some(op.index()) == register_op {
                        match op.recv(&register_rx) {
                            Ok((key, tx)) => subscribers.push(Subscriber { key, tx }),
                            Err(_) => register_open = false,
                        }
                        continue;
                    }

                    let j = send_ops
                        .iter()
                        .position(|&i| i == op.index())
                        .expect("selected a send op");
                    let sent = op.send(&senders[j], element.clone());
                    let (subscriber_index, _) = remaining.swap_remove(j);
                    if sent.is_err() {
                        dead.push(subscriber_index);
                    }
                }
                remove_descending(&mut subscribers, dead);
            }
        }
    }

    // Close registration first, then drain queued registrations so their
    // subscriber senders drop even if the registry handle is still alive.
    let mut closed = state.lock();
    *closed = true;
    while register_rx.try_recv().is_ok() {}
    drop(closed);
}

fn remove_descending<K, T>(subscribers: &mut Vec<Subscriber<K, T>>, mut dead: Vec<usize>) {
    dead.sort_unstable();
    for index in dead.into_iter().rev() {
        subscribers.remove(index);
    }
}

/// Wake-ups for a level the subscribers read themselves. Owned by whatever
/// moves the level, which calls [`wake`](Self::wake) right after each move.
pub struct Wakes {
    subscribers: Mutex<Vec<Sender<()>>>,
    what: &'static str,
}

impl Wakes {
    #[must_use]
    pub fn new(what: &'static str) -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
            what,
        }
    }

    /// A wake-up on every later move; read the level after each. Infinite:
    /// it ends `Aborted` only when the owner is gone.
    pub fn subscribe(&self) -> Channel<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.subscribers.lock().push(tx);
        Channel::from_infinite(rx, self.what)
    }

    /// Wake every subscriber without blocking; a departed one is removed here.
    pub fn wake(&self) {
        self.subscribers
            .lock()
            .retain(|tx| !matches!(tx.try_send(()), Err(TrySendError::Disconnected(()))));
    }

    /// Test-only observation of the subscriber set.
    #[cfg(test)]
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.lock().len()
    }
}

/// Bridge a `CancellationToken` to a fan-out shutdown under the `tokio` feature.
///
/// Takes the `Shutdown` by value: the task owns the shutdown path, cancels on
/// the token, then waits on the blocking pool so the task does not complete
/// before the distribution thread has joined.
#[cfg(feature = "tokio")]
#[must_use]
pub fn shutdown_on_token(
    mut shutdown: Shutdown,
    token: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        token.cancelled().await;
        shutdown.cancel();
        let _ = tokio::task::spawn_blocking(move || shutdown.wait()).await;
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;
    use crate::channel::{AbortReason, End};

    fn cap(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[test]
    fn broadcast_delivers_to_every_subscriber() {
        let (tx, registry, mut shutdown) =
            broadcast("test", cap(4), cap(4), cap(4), OverflowPolicy::Shed);
        let mut a = registry.subscribe().unwrap();
        let mut b = registry.subscribe().unwrap();

        tx.send(1u32).unwrap();
        tx.send(2u32).unwrap();

        assert_eq!(a.recv().unwrap(), 1);
        assert_eq!(a.recv().unwrap(), 2);
        assert_eq!(b.recv().unwrap(), 1);
        assert_eq!(b.recv().unwrap(), 2);
        shutdown.cancel();
        shutdown.wait();
    }

    #[test]
    fn keyed_routes_only_matching_subscribers() {
        let (tx, registry, mut shutdown) = keyed(
            "test",
            |n: &u32| Some(n % 2),
            cap(4),
            cap(4),
            cap(4),
            OverflowPolicy::Shed,
        );
        let mut even = registry.subscribe(0).unwrap();
        let mut odd = registry.subscribe(1).unwrap();

        tx.send(2u32).unwrap();
        tx.send(3u32).unwrap();

        assert_eq!(even.recv().unwrap(), 2);
        assert_eq!(odd.recv().unwrap(), 3);
        shutdown.cancel();
        shutdown.wait();
    }

    #[test]
    fn shed_removes_a_full_subscriber() {
        let (tx, registry, mut shutdown) =
            broadcast("test", cap(4), cap(4), cap(1), OverflowPolicy::Shed);
        let mut sub = registry.subscribe().unwrap();

        tx.send(1u32).unwrap();
        tx.send(2u32).unwrap();

        assert_eq!(sub.recv().unwrap(), 1);
        assert!(matches!(
            sub.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "test" }
            })
        ));
        shutdown.cancel();
        shutdown.wait();
    }

    #[test]
    fn block_waits_for_the_slowest_subscriber() {
        let (tx, registry, mut shutdown) =
            broadcast("test", cap(4), cap(4), cap(1), OverflowPolicy::Block);
        let mut sub = registry.subscribe().unwrap();

        tx.send(1u32).unwrap();
        tx.send(2u32).unwrap();

        assert_eq!(sub.recv().unwrap(), 1);
        assert_eq!(sub.recv().unwrap(), 2);
        shutdown.cancel();
        shutdown.wait();
    }

    #[test]
    fn shutdown_ends_subscribers_as_producer_gone() {
        let (tx, registry, mut shutdown) =
            broadcast::<u32>("test", cap(4), cap(4), cap(4), OverflowPolicy::Shed);
        let mut sub = registry.subscribe().unwrap();

        shutdown.cancel();
        shutdown.wait();

        assert!(matches!(
            sub.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "test" }
            })
        ));
        drop(tx);
    }

    #[test]
    fn dropping_the_data_sender_drains_then_ends() {
        let (tx, registry, mut shutdown) =
            broadcast("test", cap(4), cap(4), cap(4), OverflowPolicy::Shed);
        let mut sub = registry.subscribe().unwrap();
        tx.send(7u32).unwrap();
        drop(tx);

        assert_eq!(sub.recv().unwrap(), 7);
        assert!(matches!(
            sub.recv(),
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "test" }
            })
        ));
        shutdown.cancel();
        shutdown.wait();
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn token_bridge_cancels_and_waits() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (tx, registry, shutdown) =
                broadcast::<u32>("bridge", cap(4), cap(4), cap(4), OverflowPolicy::Shed);
            let mut sub = registry.subscribe().unwrap();
            let token = tokio_util::sync::CancellationToken::new();
            let task = shutdown_on_token(shutdown, token.clone());
            token.cancel();
            task.await.unwrap();
            assert!(matches!(
                sub.recv(),
                Err(End::Aborted {
                    reason: AbortReason::ProducerGone { what: "bridge" }
                })
            ));
            drop(tx);
        });
    }

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
