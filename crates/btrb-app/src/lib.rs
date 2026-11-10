//! The seam between the platform and an app, and the process-wide facts an
//! app list adds up to.
//!
//! ```text
//!   app      App, Entry, Ready, Booted, Nvzn, Handles: what an author implements
//!            and what the platform makes of it (see its header)
//!   metrics  the histograms and help text of the whole list: the apps', then the cache's
//! ```
//!
//! An assembly: it joins the capabilities (db handles, web mechanisms, CLI
//! transport, read-after-write) into the one contract an app is written against.
//! It reads no environment and runs no process; `btrb-host` does that.

mod app;
mod metrics;

pub use app::{
    App, Booted, Entry, Global, Handles, Job, Mounted, NoEvents, Nothing, Nvzn, Ready, Shutdown,
    Stopped, configure_all, env_prefixes, run_background,
};
// What an app is written against besides the seam itself: the capabilities'
// vocabulary, so the facade reaches it through one assembly.
pub use btrb_cli::{
    AdminSocket, Console, Handler, NoCalls, Remote, ScriptedConsole, Served, StdConsole, admin,
    console, fingerprint,
};
pub use btrb_db::{AppCache, Cache, CacheBuilder, CacheConfig, CacheLimits};
pub use btrb_types::Error;
pub use btrb_web::{
    AppConfig, BasePath, CookiePolicy, SigningKey, assets, base_path, buckets, floor,
    with_base_href,
};
pub use metrics::{Histogram, describe_metrics, histograms};
