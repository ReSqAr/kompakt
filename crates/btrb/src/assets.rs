//! Fingerprinted static assets: an embedded file is served under a URL that
//! carries a hash of its bytes, and never revalidated.
//!
//! ```text
//!   /app/static/app.css   ─▶ /app/static/app.3f9a1c2b7d4e.css
//!   current fingerprint ─▶ 200, public, max-age=1 year, immutable
//!   stale or bare URL   ─▶ 404, no-store (never cached)
//! ```
//!
//! The HTML is `no-store`, so it always names the current fingerprint: after
//! a deploy the first page load fetches the new bytes and a page never mixes
//! versions. The fingerprints are a pure function of bytes compiled into the
//! binary, computed once when [`Assets::new`] runs. The scheme is
//! `docs/logs/2026-10-04_design_asset_cache_busting.md` (metric-duck's, copied
//! here for the host apps).

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

/// How much of the blake3 hex to keep: 48 bits is collision-negligible for an
/// asset scheme and keeps the URLs short.
const FINGERPRINT_LEN: usize = 12;

const IMMUTABLE: HeaderValue = HeaderValue::from_static("public, max-age=31536000, immutable");
const NO_STORE: HeaderValue = HeaderValue::from_static("no-store");

/// One embedded file: its name as the templates know it (`app.css`), its
/// bytes and its media type.
pub struct Asset {
    pub name: &'static str,
    pub body: &'static [u8],
    pub content_type: &'static str,
}

/// An app's assets, under its URL prefix (`/app/static`).
pub struct Assets {
    prefix: &'static str,
    /// Each asset with its fingerprinted file name (`app.3f9a1c2b7d4e.css`).
    files: Vec<(&'static Asset, String)>,
}

impl Assets {
    #[must_use]
    pub fn new(prefix: &'static str, assets: &'static [Asset]) -> Self {
        let files = assets
            .iter()
            .map(|asset| (asset, fingerprinted(asset)))
            .collect();
        Self { prefix, files }
    }

    /// The URL of `name` (`app.css`). An unknown name is a programming
    /// error; it yields the bare URL, which is refused, so a test catches it.
    #[must_use]
    pub fn url(&self, name: &str) -> String {
        let file = self
            .files
            .iter()
            .find(|(asset, _)| asset.name == name)
            .map_or(name, |(_, file)| file.as_str());
        format!("{}/{file}", self.prefix)
    }

    /// Serve `file` (the last path segment): only the current fingerprint.
    #[must_use]
    pub fn serve(&self, file: &str) -> Response {
        match self.files.iter().find(|(_, current)| current == file) {
            Some((asset, _)) => (
                [
                    (
                        header::CONTENT_TYPE,
                        HeaderValue::from_static(asset.content_type),
                    ),
                    (header::CACHE_CONTROL, IMMUTABLE),
                ],
                asset.body,
            )
                .into_response(),
            None => (StatusCode::NOT_FOUND, [(header::CACHE_CONTROL, NO_STORE)]).into_response(),
        }
    }
}

/// `app.css` ▶ `app.<fingerprint>.css` (the extension stays last).
fn fingerprinted(asset: &Asset) -> String {
    let hex = blake3::hash(asset.body).to_hex();
    let hash = hex.get(..FINGERPRINT_LEN).unwrap_or(hex.as_str());
    match asset.name.rsplit_once('.') {
        Some((base, ext)) => format!("{base}.{hash}.{ext}"),
        None => format!("{}.{hash}", asset.name),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    static FILES: &[Asset] = &[Asset {
        name: "app.css",
        body: b"body { color: red }",
        content_type: "text/css; charset=utf-8",
    }];

    #[test]
    fn the_url_carries_the_fingerprint_and_only_it_is_served() {
        let assets = Assets::new("/x/static", FILES);
        let url = assets.url("app.css");
        let file = url.strip_prefix("/x/static/").unwrap();
        assert!(
            file.starts_with("app.")
                && file.ends_with(".css")
                && file.len() == "app..css".len() + FINGERPRINT_LEN
        );

        let served = assets.serve(file);
        assert_eq!(served.status(), StatusCode::OK);
        assert_eq!(served.headers()[header::CACHE_CONTROL], IMMUTABLE);

        for refused in ["app.css", "app.000000000000.css", "nope.css"] {
            let r = assets.serve(refused);
            assert_eq!(r.status(), StatusCode::NOT_FOUND, "{refused}");
            assert_eq!(r.headers()[header::CACHE_CONTROL], NO_STORE);
        }
    }

    #[test]
    fn new_bytes_get_a_new_url() {
        static OTHER: &[Asset] = &[Asset {
            name: "app.css",
            body: b"body { color: blue }",
            content_type: "text/css; charset=utf-8",
        }];
        assert_ne!(
            Assets::new("/x", FILES).url("app.css"),
            Assets::new("/x", OTHER).url("app.css")
        );
    }
}
