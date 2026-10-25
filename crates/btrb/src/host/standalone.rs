//! The standalone role: the server and the worker in one process, on one
//! replica node — one fold, one cache, one metrics registry.
//!
//! ```text
//!   standalone_from_env ─▶ metrics ─▶ replica node ─▶ boot ─┬─ app.standalone ─▶ (router, jobs)
//!                                                           └─ app.admin ─────────▶ CLI socket
//!   router ─▶ assemble ─▶ serve_http  ┐
//!   jobs (after the replay) + socket  ┴─▶ run until `stop`
//! ```
//!
//! The server listens while the log replays (healthy, not ready); the jobs wait
//! for the replay, as the worker's do, so no stage runs on half-filled
//! projections. `stop` is one token: a signal, the fold stopping, a failed job
//! or a failed listener all lead to it, and the pieces drain the same way.

use futures::FutureExt as _;
use std::error::Error;
use tokio_util::sync::CancellationToken;

use super::fold::{FoldHealth, catch_up, until_stop};
use super::serve::{assemble, boot, serve_http};
use super::settings::standalone_from_env;
use crate::{Job, Shutdown};

/// A job that starts once the fold has replayed the log, and ends with `stop`
/// if the fold never gets there.
fn after_replay(fold: FoldHealth, stop: Shutdown, job: Job) -> Job {
    Box::pin(async move {
        tokio::select! {
            () = fold.caught_up() => job.await,
            stopped = stop.wait() => Ok(stopped),
        }
    })
}

#[allow(
    clippy::large_stack_frames,
    reason = "one-shot startup: the frame is the sum of the setup futures and is gone before serving"
)]
pub(super) async fn run_standalone(entries: &[crate::Entry]) -> Result<(), Box<dyn Error>> {
    let mut standalone = standalone_from_env(entries)?;
    let settings = &mut standalone.server;
    super::metrics::start(settings.metrics_listen_addr, entries)?;

    let (mut dispatcher, node) = super::db::setup_db(&settings.zslg, &settings.nvzn).await?;
    let fold = FoldHealth::new();
    super::fold::install(fold.clone());

    let Some(cache_setup) = settings.cache.take() else {
        return Err("the standalone settings carry no cache".into());
    };
    let (booted, cache) = boot(
        std::mem::take(&mut settings.apps),
        cache_setup,
        &mut dispatcher,
    )?;
    let calls: Vec<_> = booted.iter().filter_map(|b| b.app.admin()).collect();

    // One way to stop: a signal, or the fold stopping (after its grace).
    let shutdown = CancellationToken::new();
    let stop = Shutdown::from_token(shutdown.clone());
    let stopping = tokio::spawn({
        let (shutdown, fold) = (shutdown.clone(), fold.clone());
        async move {
            let stop = tokio::select! {
                stop = until_stop(&fold) => stop,
                () = shutdown.cancelled() => None,
            };
            tracing::info!("stopping: draining the listener and the background work");
            shutdown.cancel();
            stop
        }
    });

    let mut parts = Vec::new();
    let mut jobs: Vec<Job> = Vec::new();
    for b in booted {
        let (routes, app_jobs) = b.app.standalone(&b.base, stop.clone())?;
        parts.push((b.base, routes));
        jobs.extend(
            app_jobs
                .into_iter()
                .map(|job| after_replay(fold.clone(), stop.clone(), job)),
        );
    }
    let router = assemble(parts, settings.cookies);
    // Booted: the CLI may ask from here on. The socket is one more job, so a
    // path held by a live process stops this one the way any failed job does.
    jobs.push(super::work::socket_job(
        standalone.cli_socket.clone(),
        calls,
        stop.clone(),
    ));

    catch_up(dispatcher.finalise(), fold.clone(), node.token());

    let serving = serve_http(settings.http_listen_addr, router, {
        let shutdown = shutdown.clone();
        async move { shutdown.cancelled().await }
    })
    .inspect(|served| {
        if served.is_err() {
            shutdown.cancel();
        }
    });
    let working = crate::run_background(&stop, jobs);
    let (served, worked) = tokio::join!(serving.boxed(), working.boxed());
    shutdown.cancel();
    if let Err(e) = &worked {
        tracing::error!(%e, "an app's background work failed");
    }
    let fold_stop = stopping.await?;

    // In-flight requests and jobs are done by here: the node can drain.
    tracing::info!("listener and background work drained, stopping the node");
    node.shutdown().await;
    cache.shutdown().await;

    // Leave non-zero so the orchestrator restarts us; the read model cannot
    // recover without a new dispatcher.
    if let Some(stop) = fold_stop {
        return Err(std::io::Error::other(stop.describe()).into());
    }
    served?;
    worked?;
    tracing::info!("shutting down");
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn marking(ran: &Arc<AtomicBool>, stop: &Shutdown) -> Job {
        let (ran, stop) = (ran.clone(), stop.clone());
        Box::pin(async move {
            ran.store(true, Ordering::SeqCst);
            Ok(stop.wait().await)
        })
    }

    async fn settle() {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn a_job_starts_only_after_the_replay() {
        let (fold, stop, ran) = (FoldHealth::new(), Shutdown::new(), Arc::default());
        let job = tokio::spawn(after_replay(
            fold.clone(),
            stop.clone(),
            marking(&ran, &stop),
        ));
        settle().await;
        assert!(!ran.load(Ordering::SeqCst), "the log is still replaying");

        fold.mark_caught_up();
        settle().await;
        assert!(ran.load(Ordering::SeqCst));

        stop.cancel();
        job.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_job_whose_replay_never_ends_ends_with_the_stop() {
        let (fold, stop, ran) = (FoldHealth::new(), Shutdown::new(), Arc::default());
        let job = tokio::spawn(after_replay(fold, stop.clone(), marking(&ran, &stop)));
        settle().await;
        stop.cancel();
        job.await.unwrap().unwrap();
        assert!(!ran.load(Ordering::SeqCst), "it never started");
    }
}
