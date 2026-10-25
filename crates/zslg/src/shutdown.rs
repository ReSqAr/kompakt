//! The node's supervision tree: every background task, and the rule that any one
//! of them exiting stops the node.
//!
//! ```text
//!   first task exits ──> was the token already cancelled?
//!                          yes ─> graceful           Ok(())
//!                          no  ─> unexpected         Err
//!                        then: cancel, drain, hard-deadline
//! ```
//!
//! That question is the whole design. Every background task here is a
//! run-forever loop, so *how* it ended — error, panic, or even a clean `Ok` —
//! says less than whether anyone asked it to. An unrequested exit means the node
//! has silently lost a capability, and fail-stopping beats running on degraded.
//!
//! Each task is named at spawn purely so the log can say which one stopped the
//! node. A node that stops must never be a mystery.

use std::collections::HashMap;
use std::time::Duration;

use tokio::task::{Id, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::error::Error;

pub(crate) struct Shutdown {
    cancel: CancellationToken,
    workers: JoinSet<Result<(), Error>>,
    names: HashMap<Id, &'static str>,
}

impl Shutdown {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
            workers: JoinSet::new(),
            names: HashMap::new(),
        }
    }

    #[must_use]
    pub(crate) fn token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub(crate) fn spawn<F>(&mut self, name: &'static str, fut: F)
    where
        F: Future<Output = Result<(), Error>> + Send + 'static,
    {
        let handle = self.workers.spawn(fut);
        self.names.insert(handle.id(), name);
    }

    /// The caller-supplied name for a task id, for the exit log.
    fn name(&self, id: Id) -> &'static str {
        self.names.get(&id).copied().unwrap_or("unknown")
    }

    /// Runs until the first task exits, then cancels and drains the rest.
    ///
    /// An already-cancelled task is the one exit that is skipped rather than
    /// acted on — it is the drain working as intended.
    pub(crate) async fn run(mut self, drain_timeout: Duration) -> Result<(), Error> {
        // Logged at `error` so the reason survives `RUST_LOG=warn`.
        let outcome = loop {
            match self.workers.join_next_with_id().await {
                Some(Ok((id, Ok(())))) => {
                    let name = self.name(id);
                    if self.cancel.is_cancelled() {
                        tracing::debug!(task = %name, "worker finished during graceful shutdown");
                        break Ok(());
                    }
                    tracing::error!(
                        task = %name,
                        "background task exited unexpectedly (returned Ok — a run-forever \
                         loop ended); shutting the node down"
                    );
                    break Err(Error::Shutdown);
                }
                Some(Ok((id, Err(e)))) => {
                    let name = self.name(id);
                    if self.cancel.is_cancelled() {
                        tracing::debug!(task = %name, error = %e, "worker errored during graceful shutdown");
                        break Ok(());
                    }
                    tracing::error!(task = %name, error = %e, "background task failed; shutting the node down");
                    break Err(e);
                }
                Some(Err(je)) if je.is_cancelled() => continue,
                Some(Err(je)) => {
                    // The panic hook already logged message, location and
                    // backtrace; this adds which task it was.
                    let name = self.name(je.id());
                    tracing::error!(task = %name, error = %je, "background task panicked; shutting the node down");
                    break self.fatal_or_ok(None);
                }
                None => {
                    tracing::warn!(
                        "all background tasks have exited; node has nothing left to run"
                    );
                    break Ok(());
                }
            }
        };

        // Phase 2: tell remaining tasks to stop.
        self.cancel.cancel();

        // Phase 3: drain the rest.
        let drain = async {
            while let Some(res) = self.workers.join_next().await {
                match res {
                    Ok(Ok(())) | Ok(Err(_)) => {}
                    Err(je) if je.is_cancelled() => {}
                    Err(je) => {
                        tracing::warn!(error = %je, "worker exited abnormally during drain");
                    }
                }
            }
        };

        if tokio::time::timeout(drain_timeout, drain).await.is_err() {
            tracing::warn!(
                ?drain_timeout,
                "shutdown drain exceeded timeout; aborting workers"
            );
            self.workers.abort_all();
        }

        outcome
    }

    /// If the cancellation token was already fired, the exit was requested
    /// externally → `Ok(())`. Otherwise the task exited on its own → `Err`.
    fn fatal_or_ok(&self, error: Option<Error>) -> Result<(), Error> {
        if self.cancel.is_cancelled() {
            Ok(())
        } else {
            Err(error.unwrap_or(Error::Shutdown))
        }
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Shutdown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shutdown")
            .field("workers", &self.workers.len())
            .field("cancelled", &self.cancel.is_cancelled())
            .finish()
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn run_with_cancel_returns_ok() {
        let mut shutdown = Shutdown::new();
        let token = shutdown.token();
        let counter = Arc::new(AtomicU32::new(0));

        for _ in 0..4 {
            let t = token.clone();
            let c = counter.clone();
            shutdown.spawn("test", async move {
                t.cancelled().await;
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
        }

        // Cancel externally, then run — should return Ok(())
        token.cancel();
        let result = shutdown.run(Duration::from_secs(1)).await;
        assert!(result.is_ok(), "expected Ok(()), got {result:?}");
        assert_eq!(counter.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn run_without_cancel_returns_error() {
        let mut shutdown = Shutdown::new();
        let token = shutdown.token();

        // Spawn a task that returns an error without cancellation.
        shutdown.spawn("test", async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err(Error::MembershipDenied {
                node_id: crate::ids::NodeId::new_v4(),
            })
        });

        // Another task that waits for cancellation.
        let t2 = token.clone();
        shutdown.spawn("test", async move {
            t2.cancelled().await;
            Ok(())
        });

        let result = shutdown.run(Duration::from_secs(5)).await;
        assert!(
            matches!(result, Err(Error::MembershipDenied { .. })),
            "expected MembershipDenied, got {result:?}"
        );
    }

    #[tokio::test]
    async fn run_clean_exit_without_cancel_is_error() {
        let mut shutdown = Shutdown::new();
        let token = shutdown.token();

        // A task that completes cleanly (Ok(())) without being cancelled.
        shutdown.spawn("test", async { Ok(()) });

        // A task that waits for cancellation.
        shutdown.spawn("test", async move {
            token.cancelled().await;
            Ok(())
        });

        let result = shutdown.run(Duration::from_secs(5)).await;
        assert!(
            result.is_err(),
            "clean exit without cancel must be an error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn run_aborts_workers_that_ignore_cancel() {
        let mut shutdown = Shutdown::new();
        let token = shutdown.token();

        // A task that checks the token and exits promptly — this satisfies
        // Phase 1 (wait for first exit) so we reach Phase 3 (timed drain).
        let t = token.clone();
        shutdown.spawn("test", async move {
            t.cancelled().await;
            Ok(())
        });

        // A task that ignores cancellation — Phase 3 drain timeout will abort it.
        shutdown.spawn("test", async move {
            std::future::pending::<()>().await;
            Ok(())
        });

        // Cancel token so run() returns Ok (not a fatal error).
        token.cancel();

        let started = tokio::time::Instant::now();
        shutdown.run(Duration::from_millis(50)).await.expect("ok");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn run_with_zero_workers_returns_ok() {
        let shutdown = Shutdown::new();
        let result = shutdown.run(Duration::from_millis(10)).await;
        assert!(result.is_ok());
    }

    #[test]
    fn debug_output_includes_worker_count() {
        let s = Shutdown::new();
        let dbg = format!("{s:?}");
        assert!(dbg.contains("Shutdown"));
        assert!(dbg.contains("workers"));
    }
}
