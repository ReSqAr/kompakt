//! A disk cache and fetch scheduler for object stores, shared by apps.
//!
//! The crate knows apps, kinds and object stores; it knows nothing of what
//! the bytes are. Four layers, each calling the one below through a few
//! functions, closures going down:
//!
//! ```text
//!   store      CachedStore   per object store: open / stream / put / predict
//!   scheduler  Scheduler     per store: slots, deadlines, hedging, breaker
//!   core       Core          per process: entries, LRU + weights, pins, singleflight
//!   disk       Disk          one directory: marker, lock, layout, scan
//! ```
//!
//! The scheduler and the core are siblings: neither sees the other, and the
//! store composes them. A process owns one [`Cache`]; each app gets an
//! [`AppCache`] from it, registers its kinds ([`KindHandle`]) and builds one
//! [`CachedStore`] per object store.

mod cache;
mod core;
mod disk;
mod error;
mod handles;
mod histograms;
mod scheduler;
mod shape;
mod store;
mod ticket;
mod version;

#[cfg(test)]
mod tests;

pub use cache::{Builder, Cache, CacheConfig, KindReport, Report};
pub use core::{CachedFile, Limits, Streamed};
pub use error::Error;
pub use handles::{AppCache, KindHandle, Namespace};
pub use histograms::HISTOGRAMS;
pub use scheduler::Urgency;
pub use store::{CachedStore, ObjectMeta};
pub use ticket::Ticket;
pub use version::Version;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

/// A body still being produced: its announced size and its chunks.
pub(crate) type Download = (u64, BoxStream<'static, Result<Bytes, Error>>);

/// What the core asks for on a miss: one read of the object, started when
/// called. The store builds it from an object-store read run through the
/// scheduler.
pub(crate) trait Fetch:
    FnOnce() -> BoxFuture<'static, Result<Download, Error>> + Send + 'static
{
}

impl<F> Fetch for F where F: FnOnce() -> BoxFuture<'static, Result<Download, Error>> + Send + 'static
{}
