//! What the runtime holds: the cache of a process.
//!
//! ```text
//!   Cache::builder(config)    claim the directory (marker, lock) or fail
//!     builder.app(NAME)       one AppCache per app → App::new registers kinds
//!     builder.activate()      check weights, scan, delete what nobody
//!                             registered (counted in the log), start the
//!                             evictor (drain to soft, idle sweep, recency)
//!   cache.shutdown()          evictor stopped, recency written, lock released
//! ```
//!
//! Every inconsistency fails `activate` (a boot), with a message that names
//! what to change: an app that registered kinds and is missing from the
//! weights, a name in the weights that registered nothing.

use crate::Error;
use crate::core::{Core, Limits};
use crate::disk::Disk;
use crate::handles::{AppCache, Shared};
use crate::scheduler::Tuning;
use crate::shape::Shape;
use std::path::PathBuf;
use std::sync::Arc;

pub struct CacheConfig {
    pub dir: PathBuf,
    pub limits: Limits,
    /// `app → weight`. `None`: equal weights. `Some`: exactly the apps that
    /// register kinds.
    pub weights: Option<Vec<(String, u32)>>,
}

pub struct Builder {
    disk: Disk,
    /// Held so that a temporary directory lives as long as the cache.
    temp: Option<tempfile::TempDir>,
    limits: Limits,
    weights: Option<Vec<(String, u32)>>,
    shared: Arc<Shared>,
}

pub struct Cache {
    core: Core,
    shape: Arc<Shape>,
    shared: Arc<Shared>,
    _temp: Option<tempfile::TempDir>,
}

/// What the cache holds, per app and kind, for a status page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub soft: u64,
    pub hard: u64,
    pub used: u64,
    pub kinds: Vec<KindReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindReport {
    pub app: &'static str,
    pub kind: &'static str,
    pub app_weight: u32,
    pub kind_weight: u32,
    pub bytes: u64,
    pub files: u64,
    pub target: u64,
}

impl Cache {
    /// A cache over `config.dir`. Fails if the directory is not empty and not
    /// a cache directory, or if another process holds it.
    pub fn builder(config: CacheConfig) -> Result<Builder, Error> {
        let disk = Disk::claim(&config.dir)?;
        Ok(Builder::new(disk, None, config.limits, config.weights))
    }

    /// A throwaway cache in a temporary directory, removed at drop: for a
    /// command that must not touch the process's real one.
    pub fn temporary(limits: Limits) -> Result<Builder, Error> {
        let dir = tempfile::tempdir()?;
        let disk = Disk::claim(dir.path())?;
        Ok(Builder::new(disk, Some(dir), limits, None))
    }

    #[must_use]
    pub fn report(&self) -> Report {
        report(&self.core, &self.shape, None)
    }

    #[cfg(test)]
    pub(crate) fn core(&self) -> &Core {
        &self.core
    }

    /// Stop the evictor and write the recency of used files, so a restart
    /// evicts by use.
    pub async fn shutdown(self) {
        // Handles held by apps answer `NotActive` from here on, and the
        // directory lock goes with the last read still in flight.
        self.shared.core.write().take();
        self.core.shutdown().await;
        let core = self.core;
        tokio::task::spawn_blocking(move || core.flush_recency())
            .await
            .ok();
    }
}

/// What the cache holds: every app's kinds, or only `app`'s.
pub(crate) fn report(core: &Core, shape: &Shape, app: Option<&str>) -> Report {
    let limits = core.limits();
    Report {
        soft: limits.soft,
        hard: limits.hard,
        used: core.used_total(),
        kinds: core
            .held()
            .into_iter()
            .filter(|h| app.is_none_or(|a| h.group.app == a))
            .map(|h| KindReport {
                app: h.group.app,
                kind: h.group.kind,
                app_weight: shape.app_weight(h.group.app),
                kind_weight: shape.kind_weight(h.group.app, h.group.kind),
                bytes: h.bytes,
                files: h.files,
                target: h.target,
            })
            .collect(),
    }
}

impl Builder {
    fn new(
        disk: Disk,
        temp: Option<tempfile::TempDir>,
        limits: Limits,
        weights: Option<Vec<(String, u32)>>,
    ) -> Self {
        Self {
            disk,
            temp,
            limits,
            weights,
            shared: Arc::new(Shared {
                shape: Arc::new(Shape::default()),
                core: parking_lot::RwLock::new(None),
            }),
        }
    }

    /// The handle `App::new` receives. One per app.
    pub fn app(&mut self, name: &'static str) -> AppCache {
        AppCache {
            app: name,
            shared: self.shared.clone(),
            tuning: Arc::new(Tuning::default()),
        }
    }

    /// After every app is constructed: check, scan, evict, start.
    pub fn activate(self) -> Result<Cache, Error> {
        let Builder {
            disk,
            temp,
            limits,
            weights,
            shared,
        } = self;
        if limits.hard < limits.soft {
            return Err(Error::Config(format!(
                "the hard limit ({} bytes) is below the soft limit ({} bytes)",
                limits.hard, limits.soft
            )));
        }
        apply_weights(&shared.shape, weights.as_deref())?;
        let scan = disk.scan(&shared.shape.groups())?;
        if scan.removed.files > 0 {
            tracing::info!(
                files = scan.removed.files,
                bytes = scan.removed.bytes,
                "cache: removed leftovers and files of apps or kinds that did not register"
            );
        }
        let core = Core::new(disk, scan, limits, shared.shape.clone());
        {
            let mut slot = shared.core.write();
            if slot.is_some() {
                return Err(Error::Config("the cache was activated twice".to_owned()));
            }
            *slot = Some(core.clone());
        }
        Ok(Cache {
            core,
            shape: shared.shape.clone(),
            shared,
            _temp: temp,
        })
    }
}

/// Set the apps' weights from the environment's map, which must name exactly
/// the apps that registered kinds.
fn apply_weights(shape: &Shape, weights: Option<&[(String, u32)]>) -> Result<(), Error> {
    let registered = shape.apps();
    let Some(weights) = weights else {
        return Ok(());
    };
    let missing: Vec<&str> = registered
        .iter()
        .copied()
        .filter(|app| !weights.iter().any(|(name, _)| name == app))
        .collect();
    let unknown: Vec<&str> = weights
        .iter()
        .map(|(name, _)| name.as_str())
        .filter(|name| !registered.contains(name))
        .collect();
    if !missing.is_empty() || !unknown.is_empty() {
        return Err(Error::Config(format!(
            "BTRB_CACHE_WEIGHTS must name exactly the apps that use the cache ({}): \
             missing {missing:?}, unknown {unknown:?}",
            registered.join(", ")
        )));
    }
    for (name, weight) in weights {
        shape.set_app_weight(name, *weight);
    }
    Ok(())
}
