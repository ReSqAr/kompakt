//! The crate's one error type.

use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object {0} not found")]
    NotFound(String),
    #[error("{0:?} is not a valid object key")]
    InvalidKey(String),
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
    #[error("cache io: {0}")]
    Io(#[from] std::io::Error),
    #[error("download of {key} failed: {reason}")]
    Download { key: String, reason: Arc<str> },
    #[error("the store did not answer in time: waiting for {0}")]
    Deadline(&'static str),
    #[error("the store is failing (circuit breaker open); try again shortly")]
    Unavailable,
    /// A prediction found no spare room (it never evicts another's file).
    #[error("no room for a prediction")]
    NoRoom,
    /// A prediction was dropped before it started (withdrawn, stale, the
    /// breaker): a later request is simply a miss.
    #[error("prediction dropped: {0}")]
    Dropped(&'static str),
    #[error("background task crashed: {0}")]
    Join(#[from] tokio::task::JoinError),
    /// The cache is configured in a way that cannot work: stops the boot.
    #[error("cache configuration: {0}")]
    Config(String),
    /// A read before the cache was activated (a programming error: nothing
    /// may read while the app is still being constructed).
    #[error("the cache is not active yet: reads start after the boot")]
    NotActive,
}
