//! Stopping: the request, the proof, and the jobs that honour it.

use crate::Error;
use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;

/// The runtime's request to stop: SIGTERM, or the fold stopping.
#[derive(Clone, Default)]
pub struct Shutdown(CancellationToken);

/// Proof that shutdown was requested. A background job returns it, so a job
/// cannot end with `Ok` before shutdown: there is no way to build one.
#[derive(Debug)]
pub struct Stopped(());

impl Shutdown {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Shares `token`'s state: cancelling either cancels both.
    #[must_use]
    pub const fn from_token(token: CancellationToken) -> Self {
        Self(token)
    }

    /// The underlying token, for code that waits on a `CancellationToken`.
    #[must_use]
    pub fn token(&self) -> CancellationToken {
        self.0.clone()
    }

    pub fn cancel(&self) {
        self.0.cancel();
    }

    #[must_use]
    pub fn is_stopping(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Resolves once shutdown was requested.
    pub async fn wait(&self) -> Stopped {
        self.0.cancelled().await;
        Stopped(())
    }
}

/// A background job: ends only when told to (or fails).
pub type Job = BoxFuture<'static, Result<Stopped, Error>>;
