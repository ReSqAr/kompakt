//! What `subscribe` and `publish` report.

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

/// A failed [`Fanout::try_publish`](super::Fanout::try_publish); the item is handed back.
#[derive(Debug, PartialEq, Eq)]
pub enum TryPublishError<T> {
    /// The data queue is full.
    Full(T),
    /// The distribution thread is gone.
    Stopped(T),
}
