use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use std::time::Instant;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::Level;

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

/// The most a request body may carry: every form and JSON body any app takes
/// is far smaller (the largest, a batch of sensor readings, is a few KiB).
const BODY_LIMIT: usize = 64 * 1024;

/// How long a handler may take to produce its response head (408 after). A
/// streamed body — an original, a video — is not cut: the timer stops when
/// the head is ready.
const HANDLER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Cap request bodies and handler time over the whole surface. Like
/// [`with_tracing`], call it after the last app is nested.
pub(crate) fn with_limits(router: Router) -> Router {
    router
        .layer(axum::extract::DefaultBodyLimit::max(BODY_LIMIT))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            HANDLER_TIMEOUT,
        ))
}

/// Wrap every route registered so far in HTTP tracing.
///
/// Call this once, after the last app has been nested. `Router::layer` applies
/// to the routes present when it is called and not to any added afterwards, so
/// this being last is what makes the span cover the whole surface rather than
/// whichever prefix happened to be registered first.
///
/// One INFO line per request: the span carries the method, path and client
/// address, the response event the status and latency. Headers, cookies and
/// bodies are never logged. `RUST_LOG` tunes it like any other target.
pub(crate) fn with_tracing(router: Router) -> Router {
    router.layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &axum::http::Request<axum::body::Body>| {
                tracing::info_span!(
                    "request",
                    method = %request.method(),
                    path = %request.uri().path(),
                    client = %client_addr(request),
                )
            })
            .on_response(DefaultOnResponse::new().level(Level::INFO)),
    )
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

/// Record one request counter and one duration sample per request, labelled by
/// method, the *matched route template* (e.g. `/app/p/{asset}`; `<unmatched>`
/// for a path that reached no route) and the exact status code. Like
/// [`with_tracing`], call it after the last app is nested so it covers the whole
/// surface.
pub(crate) fn with_http_metrics(router: Router) -> Router {
    router.layer(axum::middleware::from_fn(record_http))
}

async fn record_http(request: axum::http::Request<Body>, next: Next) -> Response {
    let method = request.method().as_str().to_owned();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "<unmatched>".to_owned(), |path| path.as_str().to_owned());

    let started = Instant::now();
    let response = next.run(request).await;
    let status = response.status().as_u16().to_string();

    metrics::counter!(
        "btrb_http_requests_total",
        "method" => method.clone(),
        "route" => route.clone(),
        "status" => status,
    )
    .increment(1);
    metrics::histogram!(
        "btrb_http_request_duration_seconds",
        "method" => method,
        "route" => route,
    )
    .record(started.elapsed().as_secs_f64());

    response
}

/// The visitor's address for the log: Cloudflare's `CF-Connecting-IP`, then
/// the first `X-Forwarded-For` entry, then `X-Real-IP`, then the socket the
/// request arrived on. The headers are taken on trust — the only way in is
/// cloudflared, Tailscale or localhost — and are for logging only, not for
/// authorization.
fn client_addr(request: &axum::http::Request<axum::body::Body>) -> String {
    let header = |name: &str| {
        request
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    if let Some(ip) = header("cf-connecting-ip") {
        return ip.to_owned();
    }
    if let Some(ip) = header("x-forwarded-for")
        .and_then(|list| list.split(',').next())
        .map(str::trim)
    {
        return ip.to_owned();
    }
    if let Some(ip) = header("x-real-ip") {
        return ip.to_owned();
    }
    if let Some(axum::extract::ConnectInfo(addr)) = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        return addr.ip().to_string();
    }
    "-".to_owned()
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

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::post;
    use tower::ServiceExt;

    /// A body past the limit is refused before any handler reads it.
    #[tokio::test]
    async fn an_oversized_body_is_refused() {
        let app = with_limits(Router::new().route(
            "/f",
            post(|body: String| async move { body.len().to_string() }),
        ));
        let send = |n: usize| {
            app.clone()
                .oneshot(Request::post("/f").body(Body::from("x".repeat(n))).unwrap())
        };
        assert_eq!(send(1_024).await.unwrap().status(), StatusCode::OK);
        assert_eq!(
            send(BODY_LIMIT + 1).await.unwrap().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    /// The metrics layer records the request (see the live-server test for the
    /// labels) but must not change the response.
    #[tokio::test]
    async fn the_metrics_layer_is_transparent() {
        let app =
            with_http_metrics(Router::new().route("/f", get(|| async { StatusCode::CREATED })));
        let response = app
            .oneshot(Request::get("/f").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }
}
