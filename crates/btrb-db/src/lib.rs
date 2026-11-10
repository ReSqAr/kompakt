//! The db capability: the two stores a process owns, the fold that fills one of them, and the cache.
//!
//! ```text
//!   settings ─┬─> zslg::ReplicaNode | FollowerNode ─> Writer + Reader ─┐
//!             └─> nvzn::StoreConfig ───────────────────────────────────┴─> nvzn::Dispatcher
//!
//!   catch_up: Dispatcher::finalise() ─> FoldHealth::mark_caught_up      supervise: its end is an error
//!   FoldHealth  ready │ caught up │ stopped    the handle a host reads for /health and /ready
//! ```
//!
//! The order is forced: the dispatcher's projections derive from the zslg log, so the node has to be
//! serving before the dispatcher can be built on its `Writer`/`Reader`. The fold is a critical task:
//! nvzn freezes projections the instant it stops, so its end is an error a host turns into `/health`,
//! a gauge and an exit. This crate knows nothing of HTTP or of what asks for the health.

mod db;
mod fold;
pub mod settings;

pub use db::{NodeShutdown, setup_db, setup_follower_db};
pub use fold::{FoldHealth, FoldStop, catch_up, describe_metrics, supervise, until_stop};
pub use objectstorage_cache::{
    AppCache, Builder as CacheBuilder, Cache, CacheConfig, Limits as CacheLimits,
};
