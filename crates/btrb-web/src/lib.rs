//! The web capability: what a request passes through, and what an app is given to answer it with.
//!
//! ```text
//!   floor      auth failures answer on a fixed step        buckets   failed attempts, rate-limited per key
//!   assets     fingerprinted, immutable static files       base_path an app mounts at a base, links stay relative
//!   layers     tracing · body and time limits · request metrics
//!   security   CSP, nosniff, frame, referrer, HSTS         paths     /app → /app/, a named page never ends in /
//!   AppConfig  the deployment facts an app is built with (cookie policy, the signing key)
//! ```
//!
//! The platform's own routes (`/health`, `/ready`) and its startup gate belong to the host, which knows
//! the fold's health; this crate knows nothing of the db capability.

pub mod assets;
pub mod base_path;
pub mod buckets;
pub mod floor;
pub mod layers;
pub mod paths;
pub mod security;
pub mod settings;

pub use base_path::{BasePath, with_base_href};
pub use btrb_types::SigningKey;
pub use layers::{HISTOGRAMS, describe_metrics};

/// Deployment facts every mounted app is given at construction.
///
/// Built once by `host` from the validated environment and passed down
/// by value. An app must not go behind it to `std::env` — that is the habit
/// this type replaces, and it is how a security-relevant switch ends up in two
/// places disagreeing with each other.
#[derive(Clone)]
pub struct AppConfig {
    cookies: CookiePolicy,
    signing_key: SigningKey,
}

impl AppConfig {
    #[must_use]
    pub const fn new(cookies: CookiePolicy, signing_key: SigningKey) -> Self {
        Self {
            cookies,
            signing_key,
        }
    }

    #[must_use]
    pub const fn cookies(&self) -> CookiePolicy {
        self.cookies
    }

    /// The process-wide key apps sign short-lived tokens with.
    ///
    /// One key for all apps because it is one deployment secret to manage.
    /// Apps must domain-separate their own uses of it — mix a constant
    /// identifying the purpose into whatever they sign — so a token minted for
    /// one purpose cannot be presented as another.
    #[must_use]
    pub const fn signing_key(&self) -> &SigningKey {
        &self.signing_key
    }
}

impl core::fmt::Debug for AppConfig {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AppConfig")
            .field("cookies", &self.cookies)
            .field("signing_key", &self.signing_key)
            .finish()
    }
}

/// Whether cookies this deployment sets may carry the `Secure` attribute.
///
/// An enum rather than a `bool` because the two values are not symmetric and a
/// bare `insecure: bool` reads the same whichever way it is wired. Getting this
/// backwards means either sessions that silently do not work over TLS, or
/// session cookies a plain-HTTP hop can read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CookiePolicy {
    /// Set `Secure`: the browser will only send these cookies over HTTPS.
    ///
    /// The correct setting for anything reachable from a network you do not
    /// control, including behind a TLS-terminating proxy.
    RequireHttps,
    /// Omit `Secure`, so a session survives a plain-HTTP `localhost` run.
    ///
    /// Development only. Anything that sets this is one misconfiguration away
    /// from handing session cookies to the first hop that asks.
    AllowPlainHttp,
}

impl CookiePolicy {
    /// The value for the cookie's `Secure` attribute.
    #[must_use]
    pub const fn secure_attribute(self) -> bool {
        match self {
            Self::RequireHttps => true,
            Self::AllowPlainHttp => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AppConfig, CookiePolicy, SigningKey};

    #[test]
    fn the_secure_attribute_follows_the_policy() {
        assert!(CookiePolicy::RequireHttps.secure_attribute());
        assert!(!CookiePolicy::AllowPlainHttp.secure_attribute());
    }

    #[test]
    fn config_carries_its_parts_through() {
        let config = AppConfig::new(CookiePolicy::RequireHttps, SigningKey::from_bytes([7; 32]));
        assert_eq!(config.cookies(), CookiePolicy::RequireHttps);
        assert_eq!(config.signing_key().expose(), &[7u8; 32]);
    }

    /// A secret that prints itself ends up in a log line eventually.
    #[test]
    fn the_signing_key_does_not_print_itself() {
        let key = SigningKey::from_bytes([0xAB; 32]);
        let shown = format!("{key:?}");
        assert_eq!(shown, "SigningKey(<redacted>)");
        assert!(!shown.contains("ab"), "the key leaked into Debug");

        let config = AppConfig::new(CookiePolicy::AllowPlainHttp, key);
        assert!(!format!("{config:?}").contains("ab"));
    }
}
