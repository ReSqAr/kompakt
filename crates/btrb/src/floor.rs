//! Auth failures answer on a 500 ms step, so their timing says nothing.
//!
//! ```text
//!   t0 = request start
//!   handler marks the response AuthFailure ─▶ wait until t0 + ceil(elapsed / STEP) · STEP
//!                                              (at least one STEP)
//! ```
//!
//! What counts as an auth failure is each app's call (a malformed, unknown,
//! expired or wrong-secret session; a rejected login; a bad CSRF or API
//! token — not "no credentials at all", which carries no secret). The app
//! marks the response with [`mark`]; [`layer`] does the waiting, so the
//! timing lives in one place. Rounding up rather than a fixed sleep: a failure
//! whose work took 620 ms (a slow hash) answers at 1 000 ms, so the work's
//! duration never shows.

use axum::Router;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use std::time::Duration;
use tokio::time::Instant;

/// The step an auth failure's answer is rounded up to.
pub const STEP: Duration = Duration::from_millis(500);

/// The response marker: this answer is an auth failure.
#[derive(Clone, Copy, Debug)]
pub struct AuthFailure;

/// Mark `response` as an auth failure (it then answers on the step).
#[must_use]
pub fn mark(mut response: Response) -> Response {
    response.extensions_mut().insert(AuthFailure);
    response
}

/// Whether `response` was marked.
#[must_use]
pub fn is_marked(response: &Response) -> bool {
    response.extensions().get::<AuthFailure>().is_some()
}

/// Apply the floor to every route of `router`.
pub fn layer<S: Clone + Send + Sync + 'static>(router: Router<S>) -> Router<S> {
    router.layer(axum::middleware::from_fn(floor))
}

async fn floor(request: Request, next: Next) -> Response {
    let started = Instant::now();
    let response = next.run(request).await;
    if is_marked(&response) {
        tokio::time::sleep_until(deadline(started, Instant::now())).await;
    }
    response
}

/// `started` plus the elapsed time rounded up to whole steps, at least one.
fn deadline(started: Instant, now: Instant) -> Instant {
    let elapsed = now.saturating_duration_since(started).as_nanos();
    let step = STEP.as_nanos();
    let steps = elapsed.div_ceil(step).max(1);
    let wait = u64::try_from(steps.saturating_mul(step)).unwrap_or(u64::MAX);
    started
        .checked_add(Duration::from_nanos(wait))
        .unwrap_or(started)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use tower::ServiceExt;

    #[test]
    fn the_deadline_rounds_up_to_whole_steps() {
        let t0 = Instant::now();
        let at = |ms| deadline(t0, t0 + Duration::from_millis(ms)) - t0;
        assert_eq!(at(0), STEP);
        assert_eq!(at(120), STEP);
        assert_eq!(at(500), STEP);
        assert_eq!(at(501), STEP * 2);
        assert_eq!(at(620), STEP * 2);
    }

    /// A marked answer waits for the step; an unmarked one does not.
    #[tokio::test(start_paused = true)]
    async fn only_marked_answers_wait() {
        let app = layer(
            Router::new()
                .route("/fail", get(|| async { mark("no".into_response()) }))
                .route("/ok", get(|| async { "yes" })),
        );
        for (path, waits) in [("/fail", true), ("/ok", false)] {
            let started = Instant::now();
            let request = Request::get(path).body(Body::empty()).unwrap();
            let _ = app.clone().oneshot(request).await.unwrap();
            assert_eq!(started.elapsed() >= STEP, waits, "{path}");
        }
    }
}
