//! Canonical paths: an app's root is a directory, every other path is a name.
//!
//! ```text
//!   /app        308 ▶ /app/        the app's root is its directory
//!   /app/       served (as the root the app registered)
//!   /app/jobs/  308 ▶ /app/jobs    a named page never ends in /
//!   /             served
//! ```
//!
//! Decided before routing (the router would otherwise match or 404 first):
//! the rule wraps the whole router as its fallback. The query string rides
//! along on every redirect. An app mounts its root at `/` inside its nest,
//! which axum answers at the bare prefix — so `/app/` is handed to it as
//! `/app`.

use axum::Router;
use axum::extract::Request;
use axum::http::Uri;
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};

/// What to do with a request path.
#[derive(Debug, PartialEq, Eq)]
enum Plan {
    Serve,
    /// Serve it as this path (an app root, without its slash).
    ServeAs(String),
    /// 308 to this path.
    Redirect(String),
}

fn plan(path: &str, roots: &[&str]) -> Plan {
    if roots.contains(&path) {
        return Plan::Redirect(format!("{path}/"));
    }
    if path == "/" {
        return Plan::Serve;
    }
    match path.strip_suffix('/') {
        Some(root) if roots.contains(&root) => Plan::ServeAs(root.to_owned()),
        Some(_) => {
            let name = path.trim_end_matches('/');
            Plan::Redirect(if name.is_empty() {
                "/".to_owned()
            } else {
                name.to_owned()
            })
        }
        None => Plan::Serve,
    }
}

/// Apply the rule to every request `router` will see. `roots` are the apps'
/// prefixes (`/app`, `/other`).
pub(crate) fn canonical(router: Router, roots: &'static [&'static str]) -> Router {
    Router::new()
        .fallback_service(router)
        .layer(axum::middleware::from_fn(move |request, next| {
            canonicalise(request, next, roots)
        }))
}

async fn canonicalise(
    mut request: Request,
    next: Next,
    roots: &'static [&'static str],
) -> Response {
    let query = request
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    match plan(request.uri().path(), roots) {
        Plan::Serve => next.run(request).await,
        Plan::Redirect(path) => Redirect::permanent(&format!("{path}{query}")).into_response(),
        Plan::ServeAs(path) => {
            if let Ok(uri) = format!("{path}{query}").parse::<Uri>() {
                *request.uri_mut() = uri;
            }
            next.run(request).await
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{StatusCode, header};
    use axum::routing::get;
    use tower::ServiceExt;

    const ROOTS: &[&str] = &["/app", "/other"];

    #[test]
    fn roots_are_directories_and_names_are_not() {
        assert_eq!(plan("/app", ROOTS), Plan::Redirect("/app/".into()));
        assert_eq!(plan("/app/", ROOTS), Plan::ServeAs("/app".into()));
        assert_eq!(plan("/app/jobs", ROOTS), Plan::Serve);
        assert_eq!(
            plan("/app/jobs/", ROOTS),
            Plan::Redirect("/app/jobs".into())
        );
        assert_eq!(
            plan("/app/jobs//", ROOTS),
            Plan::Redirect("/app/jobs".into())
        );
        assert_eq!(plan("/", ROOTS), Plan::Serve);
        assert_eq!(plan("//", ROOTS), Plan::Redirect("/".into()));
        assert_eq!(plan("/health/", ROOTS), Plan::Redirect("/health".into()));
    }

    /// Through a real router: the rule runs before routing, and the query
    /// string survives a redirect.
    #[tokio::test]
    async fn the_rule_runs_before_routing() {
        let inner = Router::new().nest(
            "/app",
            Router::new()
                .route("/", get(|| async { "timeline" }))
                .route("/jobs", get(|| async { "jobs" })),
        );
        let app = canonical(inner, ROOTS);
        let get = |uri: &'static str| {
            app.clone()
                .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        };

        let root = get("/app/").await.unwrap();
        assert_eq!(root.status(), StatusCode::OK);
        let bare = get("/app?x=1").await.unwrap();
        assert_eq!(bare.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(bare.headers()[header::LOCATION], "/app/?x=1");
        let named = get("/app/jobs/?y=2").await.unwrap();
        assert_eq!(named.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(named.headers()[header::LOCATION], "/app/jobs?y=2");
        assert_eq!(get("/app/jobs").await.unwrap().status(), StatusCode::OK);
    }
}
