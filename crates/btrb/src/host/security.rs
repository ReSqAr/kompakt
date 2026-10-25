//! Response headers that constrain what a browser will do with our pages.
//!
//! These are cheap, static, and only work if they are on *every* response, so
//! they are applied once at the top rather than per route. Each one closes a
//! class of attack that the application code cannot close by itself:
//!
//! ```text
//!   CSP                      what may load and run at all
//!   X-Content-Type-Options   stop a sniffed type overriding ours
//!   X-Frame-Options          legacy clickjacking (CSP frame-ancestors is the modern one)
//!   Referrer-Policy          stop URLs leaking to third parties
//!   HSTS                     no first plain-HTTP request to downgrade
//! ```
//!
//! HSTS is the one that depends on deployment: it is a promise the browser
//! remembers for months, so it is sent only under [`CookiePolicy::RequireHttps`]
//! — pinning a developer's `localhost` to HTTPS is a lasting annoyance to undo.

use crate::CookiePolicy;
use axum::Router;
use axum::http::{HeaderName, HeaderValue, header};
use tower_http::set_header::SetResponseHeaderLayer;

/// What may load, and from where.
///
/// `script-src 'self'` with no `unsafe-inline` is the one that matters, and it
/// holds: every template loads its scripts with `src=` and none carries an
/// inline `<script>`. So an injected `<script>` does not run even if escaping
/// fails — which makes this a real second line behind askama's auto-escaping
/// rather than a restatement of it.
///
/// `style-src` does allow `'unsafe-inline'`, because templates still set
/// `style=` attributes (per-swatch colors, the login column width). Inline
/// *style* is a far smaller lever than inline script, but it is not nothing.
/// Closing it means replacing those attributes with a fixed set of palette
/// classes; a nonce does not help, since nonces do not apply to `style=`
/// attributes.
///
/// `object-src`, `base-uri` and `form-action` are the three that are easy to
/// forget and that undo the rest: a plugin, a rewritten relative-URL base, and
/// a form retargeted at another origin. `base-uri 'self'` rather than `'none'`:
/// the runtime writes one `<base href>` into every page (apps link relative to
/// it), which `'none'` would drop. A base can still only name this origin.
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; \
     script-src 'self'; \
     style-src 'self' 'unsafe-inline'; \
     img-src 'self' data:; \
     font-src 'self'; \
     connect-src 'self'; \
     object-src 'none'; \
     base-uri 'self'; \
     form-action 'self'; \
     frame-ancestors 'none'";

/// Two years, with subdomains, and `preload`-eligible.
///
/// Long because a short max-age is most of the cost with little of the
/// benefit; `includeSubDomains` because the CSRF design in other assumes a
/// sibling subdomain cannot quietly become a plain-HTTP cookie-writing foothold.
const STRICT_TRANSPORT_SECURITY: &str = "max-age=63072000; includeSubDomains; preload";

const X_CONTENT_TYPE_OPTIONS: HeaderName = HeaderName::from_static("x-content-type-options");
const X_FRAME_OPTIONS: HeaderName = HeaderName::from_static("x-frame-options");

/// Wrap `router` so every response it produces carries the header set.
///
/// Like tracing, this has to be applied after the last app is nested:
/// `Router::layer` only wraps the routes already present.
pub(crate) fn with_security_headers(router: Router, cookies: CookiePolicy) -> Router {
    let hsts = match cookies {
        CookiePolicy::RequireHttps => Some(HeaderValue::from_static(STRICT_TRANSPORT_SECURITY)),
        CookiePolicy::AllowPlainHttp => None,
    };

    router
        .layer(SetResponseHeaderLayer::overriding(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(CONTENT_SECURITY_POLICY),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            X_FRAME_OPTIONS,
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::REFERRER_POLICY,
            HeaderValue::from_static("no-referrer"),
        ))
        // `overriding` with `None` removes the header, which is what we want
        // under AllowPlainHttp: never send HSTS, rather than send it empty.
        .layer(SetResponseHeaderLayer::overriding(
            header::STRICT_TRANSPORT_SECURITY,
            hsts,
        ))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::{CONTENT_SECURITY_POLICY, STRICT_TRANSPORT_SECURITY};

    /// The directives that are easy to leave out and that undo the others.
    #[test]
    fn the_policy_closes_the_usual_gaps() {
        for directive in [
            "default-src 'self'",
            "script-src 'self'",
            "object-src 'none'",
            "base-uri 'self'",
            "form-action 'self'",
            "frame-ancestors 'none'",
        ] {
            assert!(
                CONTENT_SECURITY_POLICY.contains(directive),
                "missing {directive}"
            );
        }
    }

    /// Inline *script* is the one that makes a CSP worth having. If a template
    /// ever needs it, that is a decision to take deliberately, not by relaxing
    /// this constant to make a page work again.
    #[test]
    fn scripts_may_not_be_inline() {
        let script_src = CONTENT_SECURITY_POLICY
            .split(';')
            .map(str::trim)
            .find(|d| d.starts_with("script-src"))
            .expect("a script-src directive");
        assert!(
            !script_src.contains("unsafe-inline"),
            "script-src allows inline script: {script_src}"
        );
        assert!(
            !script_src.contains("unsafe-eval"),
            "script-src allows eval: {script_src}"
        );
    }

    #[test]
    fn hsts_covers_subdomains() {
        assert!(STRICT_TRANSPORT_SECURITY.contains("includeSubDomains"));
        assert!(STRICT_TRANSPORT_SECURITY.contains("max-age=63072000"));
    }
}
