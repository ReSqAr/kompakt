//! The btrb platform, as one dependency.
//!
//! btrb runs apps on a replicated log. This crate holds no logic: it re-exports
//! what the crates below it own, so that an app, or the binary that lists the
//! apps, names `btrb::…` and nothing else.
//!
//! ```text
//!   btrb ─┬─ btrb-host     the roles: serve, work, standalone, cli      host
//!         ├─ btrb-app      the seam: App, Entry, boot, the metrics list  app, metrics
//!         ├─ btrb-live     read-after-write                              live
//!         └─ the capabilities, each its own crate
//!              btrb-db  btrb-web  btrb-background  btrb-cli  btrb-metrics   (+ btrb-config, btrb-types)
//! ```
//!
//! The split is internal: every path an app used before the split still
//! resolves here.

pub use btrb_config as config;
pub use btrb_host as host;
pub use btrb_live as live;

/// The seam an app is written against.
pub mod app {
    pub use btrb_app::{
        App, Booted, Entry, Global, Handles, Job, Mounted, NoEvents, Nothing, Nvzn, Ready,
        Shutdown, Stopped, configure_all, env_prefixes, run_background,
    };
}

/// What an app says about its metrics, as data.
pub mod metrics {
    pub use btrb_app::{Histogram, describe_metrics, histograms};
}

/// The one error every btrb surface returns.
pub mod error {
    pub use btrb_app::Error;
}

pub use btrb_app::{
    AdminSocket, App, AppCache, AppConfig, BasePath, Booted, Cache, CacheBuilder, CacheConfig,
    CacheLimits, Console, CookiePolicy, Entry, Error, Global, Handler, Handles, Job, Mounted,
    NoCalls, NoEvents, Nothing, Nvzn, Ready, Remote, ScriptedConsole, Served, Shutdown, SigningKey,
    StdConsole, Stopped, admin, assets, base_path, buckets, configure_all, console, env_prefixes,
    fingerprint, floor, run_background, with_base_href,
};
pub use btrb_live::{Live, LiveSettings, ReadAfterWrite, Written};
pub use metrics::{Histogram, describe_metrics, histograms};

// The integration tests in `tests/` use these; the lib's own test target has no
// test of its own.
#[cfg(test)]
use {nvzn as _, serde as _, tokio as _};
