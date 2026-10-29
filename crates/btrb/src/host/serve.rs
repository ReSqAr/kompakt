//! What the roles that listen share, and the server role itself.
//!
//! ```text
//!   boot      the apps, in list order, on one dispatcher and one cache
//!   assemble  each app's routes at its mount, then the layers
//!   serve_http  bind (no Nagle), serve with connect info, drain on a signal
//!
//!   serve role: from_env ─▶ metrics ─▶ replica node ─▶ boot ─▶ assemble ─▶ serve_http
//!                                                         └─ the fold catches up while it listens
//! ```
//!
//! `standalone` assembles the same pieces with the worker's jobs beside them.

use axum::Router;
use axum::serve::{Listener, ListenerExt};
use nvzn::Dispatcher;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;

use super::fold::{FoldHealth, catch_up, until_stop};
use super::settings::CacheSetup;
use crate::{BasePath, Booted, Cache, CookiePolicy, Ready, with_base_href};

/// Build every app, in list order, on `dispatcher` and the process's cache.
/// The cache is activated once every app has registered its kinds: the
/// directory is scanned, what nobody registered is removed, and reads may
/// start — before the first router is built, since an app's routes may report
/// what the cache holds.
pub(super) fn boot(
    apps: Vec<Ready>,
    cache: CacheSetup,
    dispatcher: &mut Dispatcher,
) -> Result<(Vec<Booted>, Cache), Box<dyn Error>> {
    let mut cache = cache.builder()?;
    let mut booted = Vec::new();
    for ready in apps {
        booted.push(ready.boot(dispatcher, &mut cache)?);
    }
    Ok((booted, cache.activate()?))
}

/// Mount each app's routes at its base and wrap the whole in the layers.
///
/// An app's root is a directory (`/app/`), every other path a name: decided
/// before routing (`paths`). The layers wrap it, so a redirect carries the
/// security headers and is logged like any answer; they wrap whatever is on
/// the router, so they come after the last app is nested — see
/// `routes::with_tracing`. It stays a `Router`, so the listener can still
/// attach connect info for the request log.
pub(super) fn assemble(parts: Vec<(BasePath, Router)>, cookies: CookiePolicy) -> Router {
    let mut router = super::routes::setup_router();
    // The mounts' prefixes: each is a directory (`/app/`). Leaked once, at
    // start-up, for the path rule's `'static` table.
    let mut roots: Vec<&'static str> = Vec::new();
    for (base, routes) in parts {
        if let Some(at) = base.nest_at() {
            roots.push(Box::leak(at.into_boxed_str()));
        }
        let routes = with_base_href(routes, base);
        router = match base.nest_at() {
            Some(at) => router.nest(&at, routes),
            None => router.merge(routes),
        };
    }
    let router = super::routes::with_http_metrics(router);
    let router = super::paths::canonical(router, Box::leak(roots.into_boxed_slice()));
    let router = super::security::with_security_headers(router, cookies);
    let router = super::routes::with_limits(router);
    let router = super::routes::with_startup_gate(router);
    super::routes::with_tracing(router)
}

/// Listen on `addr` and serve `router` until `shutdown` resolves, then let the
/// requests in flight finish.
pub(super) async fn serve_http(
    addr: SocketAddr,
    router: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn Error>> {
    // No Nagle: a response is written as headers and then body, and on a
    // kept-alive connection the second write otherwise waits for the client's
    // delayed ACK (~40 ms, every other request).
    let listener = tokio::net::TcpListener::bind(addr).await?.tap_io(|tcp| {
        if let Err(e) = tcp.set_nodelay(true) {
            tracing::debug!(?e, "could not set TCP_NODELAY");
        }
    });

    tracing::info!("listening on {}", listener.local_addr()?);
    // Connect info is what the request log falls back to when no proxy header
    // names the visitor.
    let app = router.into_make_service_with_connect_info::<SocketAddr>();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

/// Serve `apps` until a signal or the fold stops: what `host serve` does.
///
/// `main` builds the zslg node, the nvzn dispatcher and the router one after
/// another, and `#[tokio::main]` wraps the lot in a single async block — so the
/// frame is the sum of every startup future, which is what the lint measures.
/// This runs once and nothing is hot here: the frame is gone before the first
/// request arrives.
#[allow(
    clippy::large_stack_frames,
    reason = "one-shot startup: the frame is the sum of the setup futures and is gone before serving"
)]
pub(super) async fn run_serve(apps: &[crate::Entry]) -> Result<(), Box<dyn Error>> {
    let mut settings = super::settings::from_env(apps)?;
    super::metrics::start(settings.metrics_listen_addr, apps)?;

    let (mut dispatcher, node) = super::db::setup_db(&settings.zslg, &settings.nvzn).await?;
    let fold = FoldHealth::new();
    super::fold::install(fold.clone());

    let Some(cache_setup) = settings.cache.take() else {
        return Err("the server's settings carry no cache".into());
    };
    let (booted, cache) = boot(
        std::mem::take(&mut settings.apps),
        cache_setup,
        &mut dispatcher,
    )?;
    let parts = booted
        .into_iter()
        .map(|b| {
            let routes = b.app.router(&b.base)?;
            Ok((b.base, crate::Booted::wrap(b.read_after_write, routes)))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let router = assemble(parts, settings.cookies);

    // The log is replayed while the server already listens: healthy, so a
    // long refold does not read as a dead process; not ready, so nothing is
    // served from half-filled projections (fold.rs).
    catch_up(dispatcher.finalise(), fold.clone(), node.token());

    let stop = fold.clone();
    serve_http(settings.http_listen_addr, router, async move {
        until_stop(&stop).await;
    })
    .await?;

    // In-flight requests are done by here, so nothing is left to serve a write
    // to: the node can drain without racing the listener that fed it.
    tracing::info!("listener drained, stopping the node");
    node.shutdown().await;
    // What was used since the last flush is written down: a restart then
    // evicts by use.
    cache.shutdown().await;

    // Leave non-zero so the orchestrator restarts us; the read model cannot
    // recover without a new dispatcher.
    if let Some(stop) = fold.stop_reason() {
        return Err(stop.describe().into());
    }

    tracing::info!("shutting down");

    Ok(())
}
