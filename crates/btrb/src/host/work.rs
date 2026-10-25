//! The worker role: a follower on persistent directories (a restart resumes
//! the log and the projections instead of rebuilding them), each app's
//! background work, and the socket the CLI reaches the apps through, until
//! SIGTERM.
//!
//! ```text
//!   worker_from_env ─▶ FollowerNode + Dispatcher ─┬─ each app's background ─┐
//!                                                 └─ each app's admin ─> AdminSocket ─┘─> run(shutdown)
//! ```

use futures::FutureExt as _;
use std::error::Error;
use tokio_util::sync::CancellationToken;

use super::fold::{FoldHealth, supervise, until_stop};
use super::settings::{CacheSetup, worker_from_env};

#[allow(
    clippy::large_stack_frames,
    reason = "one-shot startup: the frame is the sum of the setup futures and is gone before work starts"
)]
pub(super) async fn run_work(entries: &[crate::Entry]) -> Result<(), Box<dyn Error>> {
    let mut settings = worker_from_env(entries)?;
    super::metrics::start(settings.metrics_listen_addr, entries)?;

    let (mut dispatcher, node) =
        super::db::setup_follower_db(&settings.zslg, &settings.nvzn).await?;

    let (booted, cache) = super::serve::boot(
        std::mem::take(&mut settings.apps),
        std::mem::replace(&mut settings.cache, CacheSetup::Throwaway),
        &mut dispatcher,
    )?;

    let fold_health = FoldHealth::new();
    let fold = dispatcher.finalise().boxed().await?;
    supervise(fold, fold_health.clone(), node.clone());

    let calls: Vec<_> = booted.iter().filter_map(|b| b.app.admin()).collect();

    // One way to stop: a signal, or the fold stopping (after its grace).
    // Either cancels `shutdown`, and the work drains the same way.
    let shutdown = CancellationToken::new();
    let stopping = tokio::spawn({
        let (shutdown, fold) = (shutdown.clone(), fold_health.clone());
        async move {
            let stop = tokio::select! {
                stop = until_stop(&fold) => stop,
                () = shutdown.cancelled() => None,
            };
            tracing::info!("stopping: draining the background work");
            shutdown.cancel();
            stop
        }
    });

    // The first job to fail, in any app, stops the rest (and the process).
    let stop = crate::Shutdown::from_token(shutdown.clone());
    let mut jobs: Vec<_> = booted
        .into_iter()
        .flat_map(|b| b.app.background(stop.clone()))
        .collect();
    // Booted: the CLI may ask from here on. The socket is one more job, so a
    // path held by a live worker, or by something that is not a socket, stops
    // the worker the way any failed job does (drained, non-zero).
    jobs.push(socket_job(settings.cli_socket.clone(), calls, stop.clone()));
    let apps = crate::run_background(&stop, jobs).boxed().await;
    shutdown.cancel();
    if let Err(e) = &apps {
        tracing::error!(%e, "an app's background work failed");
    }
    let fold_stop = stopping.await?;

    tracing::info!("background work drained, stopping the node");
    node.cancel();
    cache.shutdown().await;
    drop(settings);

    // Leave non-zero so the orchestrator restarts us; the read model cannot
    // recover without a new dispatcher.
    if let Some(stop) = fold_stop {
        return Err(std::io::Error::other(stop.describe()).into());
    }
    apps.map_err(Into::into)
}

/// The CLI's socket, served until `stop`.
pub(super) fn socket_job(
    path: std::path::PathBuf,
    calls: Vec<crate::admin::Served>,
    stop: crate::Shutdown,
) -> crate::Job {
    Box::pin(async move { crate::AdminSocket::bind(&path)?.serve(calls, stop).await })
}
