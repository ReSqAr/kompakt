//! Threaded fan-out: a broadcast or keyed registry with one distribution thread.
//!
//! Construction returns the two sides of the same fan-out:
//!
//! ```text
//!   (fanout, registry)
//!     │        └── cloneable subscribe side
//!     └─────────── the producer: publish / try_publish / shutdown
//!                  (owns the data sender and the thread; Drop cancels)
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
//! thread stops (explicit shutdown, the [`Fanout`] dropped, or a panic). A
//! departed subscriber never reaches the producer: the thread sheds or skips
//! it. A caller that wants `Cancelled` binds its own
//! [`Cancellation`](crate::Cancellation) with `until`.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Select, Sender, TrySendError};
use parking_lot::Mutex;

use crate::channel::Channel;
use crate::kind::Infinite;

/// What the distribution thread does when a subscriber's queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Wait for the slowest subscriber. The data queue backpressures the
    /// producer while the element is delivered.
    Block,
    /// Drop a subscriber whose queue is full or whose consumer is gone.
    Shed,
}

/// The three queue bounds of a fan-out. The overflow policy is not here: it is
/// a semantic choice, not sizing, and every constructor takes it explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanoutConfig {
    /// Elements queued between the producer and the distribution thread.
    pub data_cap: NonZeroUsize,
    /// Registrations queued before `subscribe` reports `Full`.
    pub register_cap: NonZeroUsize,
    /// Per-subscriber queue depth: the lag a `Shed` subscriber may build up.
    pub sub_cap: NonZeroUsize,
}

impl FanoutConfig {
    const DEFAULT_CAP: NonZeroUsize = NonZeroUsize::new(1024).expect("nonzero");

    /// The default data and registration bounds with the given subscriber depth.
    #[must_use]
    pub const fn with_sub_cap(sub_cap: NonZeroUsize) -> Self {
        Self {
            data_cap: Self::DEFAULT_CAP,
            register_cap: Self::DEFAULT_CAP,
            sub_cap,
        }
    }
}

impl Default for FanoutConfig {
    fn default() -> Self {
        Self::with_sub_cap(Self::DEFAULT_CAP)
    }
}

/// A failed `Registry::subscribe`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryError {
    /// The bounded registration queue is full.
    Full,
    /// The fan-out is gone.
    Closed,
}

/// The distribution thread is gone (shut down or panicked); the item is
/// handed back.
#[derive(Debug, PartialEq, Eq)]
pub struct Stopped<T>(pub T);

/// A failed [`Fanout::try_publish`]; the item is handed back.
#[derive(Debug, PartialEq, Eq)]
pub enum TryPublishError<T> {
    /// The data queue is full.
    Full(T),
    /// The distribution thread is gone.
    Stopped(T),
}

/// The producer side of a fan-out: the data sender and the thread's lifecycle.
///
/// Dropping it cancels the thread without joining; [`shutdown`](Self::shutdown)
/// cancels and joins.
pub struct Fanout<T> {
    tx: Sender<T>,
    shutdown: Shutdown,
}

impl<T> Fanout<T> {
    /// Queue `item` for every matching subscriber, waiting for room.
    ///
    /// # Errors
    ///
    /// [`Stopped`] with the item when the distribution thread is gone.
    pub fn publish(&self, item: T) -> Result<(), Stopped<T>> {
        self.tx.send(item).map_err(|e| Stopped(e.into_inner()))
    }

    /// Queue `item` without waiting.
    ///
    /// # Errors
    ///
    /// The item and why: the data queue is full, or the thread is gone.
    pub fn try_publish(&self, item: T) -> Result<(), TryPublishError<T>> {
        self.tx.try_send(item).map_err(|e| match e {
            TrySendError::Full(item) => TryPublishError::Full(item),
            TrySendError::Disconnected(item) => TryPublishError::Stopped(item),
        })
    }

    /// Stop the distribution thread and wait for it. Blocking: from async code,
    /// call it on the blocking pool.
    pub fn shutdown(self) {
        let Self { tx, shutdown } = self;
        drop(tx);
        shutdown.wait();
    }
}

/// The thread's stop signal and join handle. `cancel` is idempotent; dropping
/// cancels without joining.
struct Shutdown {
    signal: Option<Sender<()>>,
    join: Option<JoinHandle<()>>,
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

fn subscribe<K, T>(
    register: &Sender<(K, Sender<T>)>,
    state: &Mutex<bool>,
    key: K,
    sub_cap: NonZeroUsize,
    what: &'static str,
) -> Result<Channel<T, Infinite>, RegistryError>
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

/// Spawn a broadcast fan-out: every subscriber receives every element.
#[must_use]
pub fn broadcast<T>(
    what: &'static str,
    config: FanoutConfig,
    overflow: OverflowPolicy,
) -> (Fanout<T>, Registry<T>)
where
    T: Clone + Send + 'static,
{
    let (fanout, register, state) = start(config, |_: &T| Some(()), overflow);
    let registry = Registry {
        register,
        sub_cap: config.sub_cap,
        what,
        state,
    };
    (fanout, registry)
}

/// Spawn a keyed fan-out. `route` returns the subscriber key for an element;
/// `None` means no subscriber receives it.
#[must_use]
pub fn keyed<K, T, F>(
    what: &'static str,
    route: F,
    config: FanoutConfig,
    overflow: OverflowPolicy,
) -> (Fanout<T>, KeyedRegistry<K, T>)
where
    K: Eq + Copy + Send + 'static,
    T: Clone + Send + 'static,
    F: Fn(&T) -> Option<K> + Send + 'static,
{
    let (fanout, register, state) = start(config, route, overflow);
    let registry = KeyedRegistry {
        register,
        sub_cap: config.sub_cap,
        what,
        state,
    };
    (fanout, registry)
}

#[allow(
    clippy::type_complexity,
    reason = "the three parts of one fan-out, unpacked by both constructors"
)]
fn start<K, T, F>(
    config: FanoutConfig,
    route: F,
    overflow: OverflowPolicy,
) -> (Fanout<T>, Sender<(K, Sender<T>)>, Arc<Mutex<bool>>)
where
    K: Eq + Copy + Send + 'static,
    T: Clone + Send + 'static,
    F: Fn(&T) -> Option<K> + Send + 'static,
{
    let (data_tx, data_rx) = crossbeam_channel::bounded(config.data_cap.get());
    let (register_tx, register_rx) = crossbeam_channel::bounded(config.register_cap.get());
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
    let fanout = Fanout {
        tx: data_tx,
        shutdown: Shutdown {
            signal: Some(shutdown_tx),
            join: Some(join),
        },
    };
    (fanout, register_tx, state)
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
/// moves the level ([`crate::Level`]), which calls [`wake`](Self::wake) right
/// after each move and [`close`](Self::close) when it is gone.
pub(crate) struct Wakes {
    /// `None` once closed: every subscriber has been released.
    subscribers: Mutex<Option<Vec<Sender<()>>>>,
    what: &'static str,
}

impl Wakes {
    #[must_use]
    pub(crate) fn new(what: &'static str) -> Self {
        Self {
            subscribers: Mutex::new(Some(Vec::new())),
            what,
        }
    }

    /// A wake-up on every later move; read the level after each. Infinite:
    /// it ends `Aborted` only when the owner closes or drops it.
    pub(crate) fn subscribe(&self) -> Channel<(), Infinite> {
        let mut guard = self.subscribers.lock();
        let Some(subscribers) = guard.as_mut() else {
            return Channel::aborted(self.what);
        };
        let (tx, rx) = crossbeam_channel::bounded(1);
        subscribers.push(tx);
        Channel::from_infinite(rx, self.what)
    }

    /// Wake every subscriber without blocking; a departed one is removed here.
    pub(crate) fn wake(&self) {
        if let Some(subscribers) = self.subscribers.lock().as_mut() {
            subscribers
                .retain(|tx| !matches!(tx.try_send(()), Err(TrySendError::Disconnected(()))));
        }
    }

    /// Release every subscriber: their streams end `Aborted(ProducerGone)`,
    /// and later subscriptions end at once.
    pub(crate) fn close(&self) {
        self.subscribers.lock().take();
    }

    /// Test-only observation of the subscriber set.
    #[cfg(test)]
    pub(crate) fn subscriber_count(&self) -> usize {
        self.subscribers.lock().as_ref().map_or(0, Vec::len)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;
    use crate::channel::AbortReason;

    fn cap(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    fn config(data: usize, register: usize, sub: usize) -> FanoutConfig {
        FanoutConfig {
            data_cap: cap(data),
            register_cap: cap(register),
            sub_cap: cap(sub),
        }
    }

    #[test]
    fn broadcast_delivers_to_every_subscriber() {
        let (fanout, registry) = broadcast("test", config(4, 4, 4), OverflowPolicy::Shed);
        let mut a = registry.subscribe().unwrap();
        let mut b = registry.subscribe().unwrap();

        fanout.publish(1u32).unwrap();
        fanout.publish(2u32).unwrap();

        assert_eq!(a.recv().unwrap(), 1);
        assert_eq!(a.recv().unwrap(), 2);
        assert_eq!(b.recv().unwrap(), 1);
        assert_eq!(b.recv().unwrap(), 2);
        fanout.shutdown();
    }

    #[test]
    fn keyed_routes_only_matching_subscribers() {
        let (fanout, registry) = keyed(
            "test",
            |n: &u32| Some(n % 2),
            config(4, 4, 4),
            OverflowPolicy::Shed,
        );
        let mut even = registry.subscribe(0).unwrap();
        let mut odd = registry.subscribe(1).unwrap();

        fanout.publish(2u32).unwrap();
        fanout.publish(3u32).unwrap();

        assert_eq!(even.recv().unwrap(), 2);
        assert_eq!(odd.recv().unwrap(), 3);
        fanout.shutdown();
    }

    #[test]
    fn shed_removes_a_full_subscriber() {
        let (fanout, registry) = broadcast("test", config(4, 4, 1), OverflowPolicy::Shed);
        let mut sub = registry.subscribe().unwrap();

        fanout.publish(1u32).unwrap();
        fanout.publish(2u32).unwrap();

        assert_eq!(sub.recv().unwrap(), 1);
        assert!(matches!(
            sub.recv(),
            Err(AbortReason::ProducerGone { what: "test" })
        ));
        fanout.shutdown();
    }

    #[test]
    fn block_waits_for_the_slowest_subscriber() {
        let (fanout, registry) = broadcast("test", config(4, 4, 1), OverflowPolicy::Block);
        let mut sub = registry.subscribe().unwrap();

        fanout.publish(1u32).unwrap();
        fanout.publish(2u32).unwrap();

        assert_eq!(sub.recv().unwrap(), 1);
        assert_eq!(sub.recv().unwrap(), 2);
        fanout.shutdown();
    }

    #[test]
    fn shutdown_ends_subscribers_as_producer_gone() {
        let (fanout, registry) = broadcast::<u32>("test", config(4, 4, 4), OverflowPolicy::Shed);
        let mut sub = registry.subscribe().unwrap();

        fanout.shutdown();

        assert!(matches!(
            sub.recv(),
            Err(AbortReason::ProducerGone { what: "test" })
        ));
    }

    #[test]
    fn subscribe_infinite_aborts_instead_of_completing() {
        let (fanout, registry) =
            broadcast::<u32>("infinite", config(4, 4, 4), OverflowPolicy::Shed);
        fanout.shutdown();

        let mut sub = registry.subscribe_infinite();
        assert!(matches!(
            sub.recv(),
            Err(AbortReason::ProducerGone { what: "infinite" })
        ));
    }

    #[test]
    fn keyed_subscribe_infinite_uses_the_fanout_label() {
        let (fanout, registry) = keyed(
            "keyed infinite",
            |n: &u32| Some(n % 2),
            config(4, 4, 4),
            OverflowPolicy::Shed,
        );
        fanout.shutdown();

        let mut sub = registry.subscribe_infinite(0);
        assert!(matches!(
            sub.recv(),
            Err(AbortReason::ProducerGone {
                what: "keyed infinite"
            })
        ));
    }

    #[test]
    fn dropping_the_fanout_ends_subscribers_after_delivered_items() {
        let (fanout, registry) = broadcast("test", config(4, 4, 4), OverflowPolicy::Shed);
        let mut sub = registry.subscribe().unwrap();
        fanout.publish(7u32).unwrap();
        assert_eq!(sub.recv().unwrap(), 7);
        drop(fanout);
        assert!(matches!(
            sub.recv(),
            Err(AbortReason::ProducerGone { what: "test" })
        ));
    }

    #[test]
    fn publish_after_shutdown_hands_the_item_back() {
        let (fanout, registry) = broadcast::<u32>("test", config(4, 4, 4), OverflowPolicy::Shed);
        let probe = registry.subscribe().unwrap();
        let tx = fanout.tx.clone();
        fanout.shutdown();
        drop(probe);
        assert!(tx.send(1).is_err(), "the thread dropped its receiver");
    }

    #[test]
    fn try_publish_reports_full_and_stopped() {
        let (fanout, registry) = broadcast::<u32>("test", config(1, 4, 1), OverflowPolicy::Block);
        let _slow = registry.subscribe().unwrap();
        // The thread takes 1 and blocks delivering 2 to the full subscriber;
        // 3 fills the data queue; 4 finds it full.
        let mut full = None;
        for v in 1..=50u32 {
            if let Err(e) = fanout.try_publish(v) {
                full = Some(e);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(matches!(full, Some(TryPublishError::Full(_))));
        let tx = fanout.tx.clone();
        fanout.shutdown();
        assert!(tx.try_send(9).is_err());
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
