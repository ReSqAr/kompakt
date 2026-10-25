//! Process lifecycle shared by the binaries.

/// Resolves when the process is asked to stop: Ctrl-C or SIGTERM.
///
/// SIGTERM is the one a container runtime or `systemctl stop` sends, so watching
/// only Ctrl-C would mean every restart kills the process mid-request. An
/// installation failure waits forever rather than completing: the other signal
/// still stops the server, and a broken handler must not read as a stop request.
pub(crate) async fn shutdown_signal() {
    tokio::select! {
        () = interrupt() => {}
        () = terminate() => {}
    }
}

async fn interrupt() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::error!(?e, "failed to install the Ctrl-C handler");
        std::future::pending::<()>().await;
    }
}

#[cfg(unix)]
async fn terminate() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut stream) => {
            stream.recv().await;
        }
        Err(e) => {
            tracing::error!(?e, "failed to install the SIGTERM handler");
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(not(unix))]
async fn terminate() {
    std::future::pending::<()>().await;
}
