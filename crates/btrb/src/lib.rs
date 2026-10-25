//! What the platform hands every app it mounts.
//!
//! Apps (`other` today, more later) are nested onto the platform's
//! router, and each needs a handful of deployment facts the platform is the
//! only place that knows: is there TLS in front of us, what secret do we sign
//! with, and so on. Those facts belong in one validated place — the platform's
//! `config` module — not rediscovered by each app from the environment.
//!
//! This crate exists so that can happen without a dependency cycle: the
//! platform depends on its apps, so the shared type cannot live in either. It
//! deliberately holds only the contract; nothing here reads the environment.
//!
//! It also holds the few HTTP mechanisms every app must apply the same way:
//! [`floor`] (auth failures answer on a fixed step), [`buckets`] (failed
//! attempts rate-limited per key) and [`assets`] (fingerprinted, immutable
//! static files).

pub mod admin;
pub mod app;
pub mod assets;
pub mod base_path;
pub mod buckets;
pub mod config;
pub mod console;
pub mod error;
pub mod floor;
pub mod host;
pub mod metrics;

pub use admin::{AdminSocket, Handler, NoCalls, Remote, Served, fingerprint};
pub use app::{
    App, Booted, Entry, Global, Job, Mounted, NoEvents, Nothing, Nvzn, Ready, Shutdown, Stopped,
    configure_all, env_prefixes, run_background,
};
pub use base_path::{BasePath, with_base_href};
pub use console::{Console, ScriptedConsole, StdConsole};
pub use error::Error;
pub use metrics::{Histogram, describe_metrics, histograms};
pub use objectstorage_cache::{
    AppCache, Builder as CacheBuilder, Cache, CacheConfig, Limits as CacheLimits,
};

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

/// A 32-byte secret shared by everything this process signs with.
///
/// Not `Debug`-derived, not comparable, not `Copy`: the derives are how a
/// secret ends up in a log line or in a timing-variable comparison. Reading the
/// bytes is deliberately a named call.
#[derive(Clone)]
pub struct SigningKey([u8; SigningKey::BYTES]);

impl SigningKey {
    pub const BYTES: usize = 32;

    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::BYTES]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn expose(&self) -> &[u8; Self::BYTES] {
        &self.0
    }
}

impl core::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("SigningKey(<redacted>)")
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
