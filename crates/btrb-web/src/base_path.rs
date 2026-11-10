//! Where an app lives in the URL space, as an absolute path that always ends
//! in `/`.
//!
//! ```text
//!   BasePath::ROOT            /
//!   BasePath::under("app")  /app/
//!   base.join("a/7/p/x")      /app/a/7/p/x        (server-built URLs only)
//! ```
//!
//! Templates never need it: the page skeleton writes `<base href>` once and
//! every link is relative. A `BasePath` is for what must be absolute — a
//! redirect's `Location`, a cookie's `Path`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BasePath {
    Root,
    Under(&'static str),
}

impl BasePath {
    pub const ROOT: Self = Self::Root;

    /// A single segment `[a-z0-9-]+`; anything else fails at compile time when
    /// used in a `const`/`static` list.
    #[must_use]
    #[allow(
        clippy::panic,
        reason = "a const assertion: fails the build, never a request"
    )]
    pub const fn under(segment: &'static str) -> Self {
        let bytes = segment.as_bytes();
        assert!(!bytes.is_empty(), "a base path segment is not empty");
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            assert!(
                b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-',
                "a base path segment is [a-z0-9-]+"
            );
            i = i.saturating_add(1);
        }
        Self::Under(segment)
    }

    /// `/` or `/segment/`.
    #[must_use]
    pub fn path(&self) -> String {
        match self {
            Self::Root => "/".to_owned(),
            Self::Under(segment) => format!("/{segment}/"),
        }
    }

    /// An absolute URL path for `relative` (a leading `/` is ignored).
    #[must_use]
    pub fn join(&self, relative: &str) -> String {
        let relative = relative.trim_start_matches("./").trim_start_matches('/');
        format!("{}{relative}", self.path())
    }

    /// The `nest` argument for axum: no trailing slash, or `None` at the root.
    #[must_use]
    pub fn nest_at(&self) -> Option<String> {
        match self {
            Self::Root => None,
            Self::Under(segment) => Some(format!("/{segment}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_always_ends_in_a_slash_and_joins_relative() {
        assert_eq!(BasePath::ROOT.path(), "/");
        assert_eq!(BasePath::under("app").path(), "/app/");
        assert_eq!(BasePath::ROOT.join("albums"), "/albums");
        assert_eq!(BasePath::under("app").join("a/7"), "/app/a/7");
        assert_eq!(BasePath::under("app").join("/a"), "/app/a");
    }

    #[test]
    fn the_root_nests_nowhere() {
        assert_eq!(BasePath::ROOT.nest_at(), None);
        assert_eq!(BasePath::under("x").nest_at().as_deref(), Some("/x"));
    }
}

// ── what the runtime does for an app at its mount ────────────────────────────

/// Two things, so an app is written as if it lived at its own root:
///
/// ```text
///   full HTML page   <head>  ──▶  <head><base href="{base}">   links stay relative
///   3xx Location     login   ──▶  /app/login                 redirects are relative
/// ```
///
/// Fragments (no `<head>`) and everything that is not HTML pass through
/// untouched, as does a `Location` that is already absolute or a full URL.
pub fn with_base_href(router: axum::Router, base: BasePath) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| async move {
            let response = next.run(request).await;
            insert_base(response, base).await
        },
    ))
}

/// The most of a page this will hold in memory to place the tag.
const PAGE_LIMIT: usize = 8 * 1024 * 1024;

async fn insert_base(
    mut response: axum::response::Response,
    base: BasePath,
) -> axum::response::Response {
    use axum::http::header;
    if let Some(target) = response
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.starts_with('/') && !v.contains("://"))
        .and_then(|v| axum::http::HeaderValue::from_str(&base.join(v)).ok())
    {
        response.headers_mut().insert(header::LOCATION, target);
    }
    let is_html = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/html"));
    if !is_html {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, PAGE_LIMIT).await else {
        return axum::http::Response::builder()
            .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::empty())
            .unwrap_or_default();
    };
    let tag = format!("<base href=\"{}\">", base.path());
    let page = match std::str::from_utf8(&bytes).ok().and_then(|html| {
        let at = html.find("<head>")?.checked_add("<head>".len())?;
        (!html.contains("<base ")).then(|| {
            let (before, after) = html.split_at(at);
            format!("{before}{tag}{after}")
        })
    }) {
        Some(page) => axum::body::Bytes::from(page),
        None => bytes,
    };
    parts.headers.remove(header::CONTENT_LENGTH);
    axum::response::Response::from_parts(parts, axum::body::Body::from(page))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod base_href_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use axum::response::Html;
    use axum::routing::get;
    use tower::ServiceExt;

    async fn fetch(router: axum::Router, path: &str) -> String {
        let response = router
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn router() -> axum::Router {
        with_base_href(
            axum::Router::new()
                .route(
                    "/page",
                    get(|| async { Html("<html><head><title>x</title></head></html>") }),
                )
                .route("/fragment", get(|| async { Html("<div>a</div>") }))
                .route("/plain", get(|| async { "<head>" })),
            BasePath::under("app"),
        )
    }

    #[tokio::test]
    async fn a_page_gets_the_base_first_in_its_head() {
        assert_eq!(
            fetch(router(), "/page").await,
            "<html><head><base href=\"/app/\"><title>x</title></head></html>"
        );
    }

    #[tokio::test]
    async fn a_relative_redirect_becomes_absolute_under_the_mount() {
        let router = with_base_href(
            axum::Router::new()
                .route(
                    "/rel",
                    get(|| async { axum::response::Redirect::to("login") }),
                )
                .route(
                    "/home",
                    get(|| async { axum::response::Redirect::to("./") }),
                )
                .route(
                    "/abs",
                    get(|| async { axum::response::Redirect::to("/elsewhere") }),
                ),
            BasePath::under("app"),
        );
        let location = |path: &'static str| {
            let router = router.clone();
            async move {
                let response = router
                    .oneshot(Request::get(path).body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                response.headers()["location"].to_str().unwrap().to_owned()
            }
        };
        assert_eq!(location("/rel").await, "/app/login");
        assert_eq!(location("/home").await, "/app/");
        assert_eq!(location("/abs").await, "/elsewhere");
    }

    #[tokio::test]
    async fn fragments_and_other_types_are_left_alone() {
        assert_eq!(fetch(router(), "/fragment").await, "<div>a</div>");
        assert_eq!(fetch(router(), "/plain").await, "<head>");
    }
}
