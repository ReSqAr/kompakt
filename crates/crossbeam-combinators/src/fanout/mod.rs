//! Threaded fan-out: a broadcast or keyed registry with one distribution thread.
//!
//! Construction returns the two sides of the same fan-out:
//!
//! ```text
//!   (fanout, registry)
//!     │        └── cloneable subscribe side
//!     └─────────── the producer: publish / try_publish / shutdown
//!                  (owns the data sender and the thread; Drop cancels,
//!                  drain delivers what is queued first)
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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crossbeam_channel::Sender;
use parking_lot::Mutex;

use producer::{Shutdown, State};
use thread::spawn;

mod config;
mod error;
mod producer;
mod registry;
mod thread;

pub use config::{FanoutConfig, OverflowPolicy};
pub use error::{RegistryError, Stopped, TryPublishError};
pub use producer::Fanout;
pub use registry::{KeyedRegistry, Registry};

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
) -> (Fanout<T>, Sender<(K, Sender<T>, u64)>, Arc<State>)
where
    K: Eq + Copy + Send + 'static,
    T: Clone + Send + 'static,
    F: Fn(&T) -> Option<K> + Send + 'static,
{
    let (data_tx, data_rx) = crossbeam_channel::bounded(config.data_cap.get());
    let (register_tx, register_rx) = crossbeam_channel::bounded(config.register_cap.get());
    let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded(1);
    let state = Arc::new(State {
        closed: Mutex::new(false),
        published: AtomicU64::new(0),
    });
    let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(0);
    let join = spawn(
        data_rx,
        register_rx,
        shutdown_rx,
        route,
        overflow,
        Arc::clone(&state),
        done_tx,
    );
    let fanout = Fanout {
        tx: data_tx,
        shutdown: Shutdown {
            signal: Some(shutdown_tx),
            join: Some(join),
            done: Some(done_rx),
        },
        state: Arc::clone(&state),
    };
    (fanout, register_tx, state)
}

#[cfg(test)]
mod tests;
