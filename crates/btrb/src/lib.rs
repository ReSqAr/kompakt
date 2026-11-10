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

pub mod app;
pub use btrb_config as config;
pub mod error {
    pub use btrb_types::Error;
}
pub mod host;
pub mod live;
pub mod metrics;

pub use app::{
    App, Booted, Entry, Global, Handles, Job, Mounted, NoEvents, Nothing, Nvzn, Ready, Shutdown,
    Stopped, configure_all, env_prefixes, run_background,
};
pub use btrb_cli::{
    AdminSocket, Console, Handler, NoCalls, Remote, ScriptedConsole, Served, StdConsole, admin,
    console, fingerprint,
};
pub use btrb_db::{AppCache, Cache, CacheBuilder, CacheConfig, CacheLimits};
pub use btrb_types::{Error, SigningKey};
pub use btrb_web::{
    AppConfig, BasePath, CookiePolicy, assets, base_path, buckets, floor, with_base_href,
};
pub use live::{Live, LiveSettings, ReadAfterWrite, Written};
pub use metrics::{Histogram, describe_metrics, histograms};
