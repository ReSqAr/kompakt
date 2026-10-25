//! Supervising the nvzn fold: the projection read model is only as good as the
//! fold that advances it.
//!
//! ```text
//!   catch_up: Dispatcher::finalise() ──> JoinHandle<Result<(), Error>>
//!            │                          │
//!            │  returns once the fold   │ resolves when the fold ends,
//!            │  has replayed the log    │ for good
//!            v                          v
//!   FoldHealth::mark_caught_up   FoldHealth::mark_stopped
//!   /ready 200, pages served
//!                                      │
//!                     /health 503 · btrb_fold_up 0 · ERROR
//!                                      │
//!                     until_stop: drain FOLD_EXIT_GRACE, then exit non-zero
//! ```
//!
//! nvzn freezes projections the instant its fold stops: reads fail, `pending`
//! finds nothing, and the process looks idle while the log moves on. The stop
//! is only dangerous because it is invisible, so the fold is treated as a
//! critical task — its end is an error, [`FoldHealth`] carries it to `/health`
//! and metrics, and the process leaves so an orchestrator restarts it.
//!
//! Healthy and ready differ while the log is replayed (a changed projection
//! config refolds it from the start: minutes for a large log). The process
//! is healthy — alive, its node joined, nothing to restart — but not ready:
//! its projections are half-filled, so it serves nothing that reads them.
//!
//! ```text
//!                  joining   replaying   caught up   fold stopped
//!   /health          —         200          200          503
//!   /ready, pages    —         503          200          503
//! ```

use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// The process's fold. There is one dispatcher per process, so its health is a
/// process property, not request state — `/health` reads it from here rather
/// than the router carrying it.
static CURRENT: OnceLock<FoldHealth> = OnceLock::new();

/// Publish the process's fold health. Idempotent; the first install wins.
pub(crate) fn install(health: FoldHealth) {
    let _ = CURRENT.set(health);
}

/// The installed fold health, if one has been installed.
#[must_use]
pub(crate) fn current() -> Option<&'static FoldHealth> {
    CURRENT.get()
}

/// How long an unhealthy process keeps serving after the fold stops, so health
/// checks and scrapes observe the failure before it leaves.
const FOLD_EXIT_GRACE: Duration = Duration::from_secs(10);

/// Why the projection fold is no longer advancing.
#[derive(Clone, Debug)]
pub(crate) enum FoldStop {
    /// The fold returned cleanly while the process was still running.
    Ended,
    /// The fold returned an error.
    Failed(String),
}

impl FoldStop {
    /// A sentence for a log line or a health body.
    #[must_use]
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Ended => "the projection fold ended".to_owned(),
            Self::Failed(e) => format!("the projection fold failed: {e}"),
        }
    }
}

/// Shared, observable fold state: `None` while the fold advances; whether
/// it has replayed the log yet.
#[derive(Clone)]
pub(crate) struct FoldHealth {
    tx: Arc<watch::Sender<Option<FoldStop>>>,
    caught_up: Arc<watch::Sender<bool>>,
}

impl FoldHealth {
    #[must_use]
    pub(crate) fn new() -> Self {
        metrics::gauge!("btrb_fold_up").set(1.0);
        metrics::gauge!("btrb_fold_caught_up").set(0.0);
        Self {
            tx: Arc::new(watch::Sender::new(None)),
            caught_up: Arc::new(watch::Sender::new(false)),
        }
    }

    /// Whether the fold has replayed the log and still advances: the
    /// projections may be read.
    #[must_use]
    pub(crate) fn is_ready(&self) -> bool {
        *self.caught_up.borrow() && self.stop_reason().is_none()
    }

    pub(super) fn mark_caught_up(&self) {
        metrics::gauge!("btrb_fold_caught_up").set(1.0);
        self.caught_up.send_replace(true);
    }

    /// Resolve once the fold has replayed the log. Never resolves if it stops
    /// first: pair it with the shutdown it leads to.
    pub(crate) async fn caught_up(&self) {
        let mut rx = self.caught_up.subscribe();
        // `self` holds the sender, so the channel cannot close while we wait.
        let _ = rx.wait_for(|up| *up).await;
    }

    /// Why the fold stopped, once it has.
    #[must_use]
    pub(crate) fn stop_reason(&self) -> Option<FoldStop> {
        self.tx.borrow().clone()
    }

    fn mark_stopped(&self, stop: FoldStop) {
        metrics::gauge!("btrb_fold_up").set(0.0);
        self.tx.send_replace(Some(stop));
    }

    /// Resolve once the fold has stopped (at once if it already has), with why.
    pub(crate) async fn stopped(&self) -> FoldStop {
        let mut rx = self.tx.subscribe();
        // `self` holds the sender, so the channel cannot close while we wait.
        let stop = rx
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|s| s.clone());
        stop.unwrap_or(FoldStop::Ended)
    }
}

impl Default for FoldHealth {
    fn default() -> Self {
        Self::new()
    }
}

/// Wait until the process should stop: a signal (`None`), or the fold
/// stopping (`Some`). A fold stop is held for [`FOLD_EXIT_GRACE`] first, so
/// health checks and scrapes see it; a signal cuts the grace short.
pub(crate) async fn until_stop(health: &FoldHealth) -> Option<FoldStop> {
    let stop = tokio::select! {
        () = super::signals::shutdown_signal() => return None,
        stop = health.stopped() => stop,
    };
    tracing::info!(
        "draining for {}s before exiting on the stopped fold",
        FOLD_EXIT_GRACE.as_secs()
    );
    tokio::select! {
        () = super::signals::shutdown_signal() => {}
        () = tokio::time::sleep(FOLD_EXIT_GRACE) => {}
    }
    Some(stop)
}

/// Replay the log in the background, then supervise the fold. The process
/// listens meanwhile — healthy, not ready (see the module docs).
pub(crate) fn catch_up<F>(finalise: F, health: FoldHealth, shutting_down: CancellationToken)
where
    F: Future<Output = Result<JoinHandle<Result<(), nvzn::Error>>, nvzn::Error>> + Send + 'static,
{
    tokio::spawn(async move {
        match finalise.await {
            Ok(handle) => {
                tracing::info!("caught up with the log: serving");
                supervise(handle, health, shutting_down);
            }
            Err(e) if shutting_down.is_cancelled() => {
                tracing::debug!(error = %e, "the replay stopped during shutdown");
            }
            Err(e) => {
                tracing::error!(error = %e, "the log replay failed; projections are frozen");
                health.mark_stopped(FoldStop::Failed(e.to_string()));
            }
        }
    });
}

/// Watch a fold to its end and record an unrequested stop as a fault. Its
/// `handle` comes from a finished `finalise`: the log is replayed, so the
/// fold is caught up from here (every process, not only the server's).
///
/// `shutting_down` distinguishes the fold stopping because we asked from the
/// fold stopping on its own: only the latter is marked unhealthy.
pub(crate) fn supervise(
    handle: JoinHandle<Result<(), nvzn::Error>>,
    health: FoldHealth,
    shutting_down: CancellationToken,
) {
    health.mark_caught_up();
    tokio::spawn(async move {
        let outcome = handle.await;
        if shutting_down.is_cancelled() {
            tracing::debug!(?outcome, "the nvzn fold stopped during shutdown");
            return;
        }
        let stop = match outcome {
            Ok(Ok(())) => {
                tracing::error!("the nvzn fold ended; projections are frozen");
                FoldStop::Ended
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, "the nvzn fold stopped; projections are frozen");
                FoldStop::Failed(e.to_string())
            }
            Err(e) => {
                tracing::error!(error = %e, "the nvzn fold task died; projections are frozen");
                FoldStop::Failed(e.to_string())
            }
        };
        health.mark_stopped(stop);
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_fold_is_healthy() {
        assert!(FoldHealth::new().stop_reason().is_none());
    }

    /// Healthy while replaying, ready only once caught up, neither once stopped.
    #[test]
    fn ready_waits_for_the_replay_and_ends_with_the_fold() {
        let fold = FoldHealth::new();
        assert!(!fold.is_ready(), "replaying");
        fold.mark_caught_up();
        assert!(fold.is_ready());
        fold.mark_stopped(FoldStop::Ended);
        assert!(!fold.is_ready());
        assert!(fold.stop_reason().is_some());
    }

    #[tokio::test]
    async fn caught_up_resolves_once_the_replay_is_done() {
        let fold = FoldHealth::new();
        let waiting = tokio::spawn({
            let fold = fold.clone();
            async move { fold.caught_up().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished(), "replaying");
        fold.mark_caught_up();
        waiting.await.unwrap();
    }

    #[test]
    fn a_stopped_fold_is_reported_and_wakes_waiters() {
        let health = FoldHealth::new();
        health.mark_stopped(FoldStop::Failed("boom".to_owned()));
        // Already stopped: `stopped` resolves without a further wake.
        let stop = futures::executor::block_on(health.stopped());
        assert!(matches!(stop, FoldStop::Failed(_)));
        assert!(matches!(health.stop_reason(), Some(FoldStop::Failed(_))));
    }
}
