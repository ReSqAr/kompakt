use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};

/// The platform's own routes (`/health`, `/ready`), with no tracing layer on
/// them yet.
///
/// Tracing is deliberately *not* applied here. `Router::layer` wraps only the
/// routes already on the router, and every app is nested onto this one after
/// this function returns. Layering here would wrap the routes below and none
/// of theirs. [`with_tracing`] is the last call in the chain for that reason.
pub(crate) fn setup_router() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
}

/// Answer 503 for everything but `/health` and `/ready` until the fold has
/// replayed the log (`fold.rs`): the projections behind every page are
/// half-filled until then. Like [`with_tracing`], call it after the last app
/// is nested.
pub(crate) fn with_startup_gate(router: Router) -> Router {
    router.layer(axum::middleware::from_fn(startup_gate))
}

/// Seconds a client is asked to wait before trying again while starting.
const STARTING_RETRY_SECONDS: &str = "5";

const STARTING_HTML: &str = "<!doctype html><meta charset=utf-8>\
<meta http-equiv=refresh content=5><title>Starting</title>\
<p style=\"font: 15px system-ui; margin: 3em auto; max-width: 30em\">\
host is starting: it is catching up with its log. This page reloads by itself.";

async fn startup_gate(request: axum::http::Request<Body>, next: Next) -> Response {
    let ready = super::fold::current().is_none_or(super::fold::FoldHealth::is_ready);
    if ready || matches!(request.uri().path(), "/health" | "/ready") {
        return next.run(request).await;
    }
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [
            (header::RETRY_AFTER, STARTING_RETRY_SECONDS),
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        STARTING_HTML,
    )
        .into_response()
}

/// Liveness that includes the projection fold: a process whose fold has stopped
/// still answers HTTP, but nothing it reads is advancing, so it must not be
/// routed to.
/// Alive: the node has joined (the server listens only after) and the fold
/// has not stopped. A replay of the log is healthy (`fold.rs`).
async fn health() -> (StatusCode, String) {
    match super::fold::current().and_then(super::fold::FoldHealth::stop_reason) {
        Some(stop) => (StatusCode::SERVICE_UNAVAILABLE, stop.describe()),
        None => (StatusCode::OK, "ok".to_owned()),
    }
}

/// Ready: healthy, and the log replayed — pages can be served.
async fn ready() -> (StatusCode, String) {
    let Some(fold) = super::fold::current() else {
        return (StatusCode::OK, "ready".to_owned());
    };
    match fold.stop_reason() {
        Some(stop) => (StatusCode::SERVICE_UNAVAILABLE, stop.describe()),
        None if fold.is_ready() => (StatusCode::OK, "ready".to_owned()),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "catching up with the log".to_owned(),
        ),
    }
}
