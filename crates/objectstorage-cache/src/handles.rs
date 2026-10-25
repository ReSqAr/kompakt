//! What an app holds: its [`AppCache`], its kinds, and the stores it builds.
//!
//! The runtime creates the cache and hands each app an `AppCache` (an app
//! cannot choose its own name or weight). Reads before the cache is active
//! return [`Error::NotActive`]: the app is still being constructed.

use crate::core::Core;
use crate::scheduler::{Scheduler, Tuning};
use crate::shape::Shape;
use crate::store::CachedStore;
use crate::{Error, Urgency};
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// What the app handles and the cache share.
pub(crate) struct Shared {
    pub(crate) shape: Arc<Shape>,
    pub(crate) core: parking_lot::RwLock<Option<Core>>,
}

impl Shared {
    /// The core, while the cache is active (from `activate` to `shutdown`).
    pub(crate) fn core(&self) -> Result<Core, Error> {
        self.core.read().clone().ok_or(Error::NotActive)
    }
}

/// One app's cache: its kinds, its stores, its policy.
#[derive(Clone)]
pub struct AppCache {
    pub(crate) app: &'static str,
    pub(crate) shared: Arc<Shared>,
    pub(crate) tuning: Arc<Tuning>,
}

/// An object store's identity inside the app: `id` is what its files are
/// hashed under (stable across configuration edits, such as a moved bucket),
/// `label` is the metrics' low-cardinality name for it.
#[derive(Debug, Clone)]
pub struct Namespace {
    pub(crate) id: Arc<str>,
    pub(crate) label: &'static str,
    /// A fixed number of requests at once, instead of the app's setting (a
    /// public tile server's policy asks for two).
    pub(crate) slots: Option<usize>,
}

impl Namespace {
    pub fn new(id: impl Into<Arc<str>>, label: &'static str) -> Self {
        Namespace {
            id: id.into(),
            label,
            slots: None,
        }
    }

    /// This store makes at most `slots` requests at once, whatever the
    /// app's tuning says.
    #[must_use]
    pub fn with_slots(mut self, slots: usize) -> Self {
        self.slots = Some(slots);
        self
    }
}

impl AppCache {
    /// Register a kind of file: `name` is `[a-z]+`, once per name; `weight`
    /// is its target share among the app's kinds (changeable with
    /// [`KindHandle::set_weight`]).
    pub fn kind(&self, name: &'static str, weight: u32) -> Result<KindHandle, Error> {
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_lowercase()) {
            return Err(Error::Config(format!(
                "cache kind {name:?} of {} is not [a-z]+",
                self.app
            )));
        }
        if self.shared.core.read().is_some() {
            return Err(Error::Config(format!(
                "cache kind {name} of {} registered after the boot",
                self.app
            )));
        }
        if !self.shared.shape.add_kind(self.app, name, weight) {
            return Err(Error::Config(format!(
                "cache kind {name} of {} registered twice",
                self.app
            )));
        }
        Ok(KindHandle {
            app: self.app,
            name,
            hedge: false,
            shared: self.shared.clone(),
        })
    }

    /// A cached view of one object store. Cheap to clone; owns its
    /// scheduler (slots, deadlines, breaker) and lives as long as the app
    /// keeps it.
    pub fn store(&self, origin: Arc<dyn ObjectStore>, namespace: Namespace) -> CachedStore {
        let tuning = match namespace.slots {
            Some(slots) => Arc::new(Tuning::new(slots, 0)),
            None => self.tuning.clone(),
        };
        let scheduler = Scheduler::new(self.app, namespace.label, tuning);
        CachedStore::new(
            self.app,
            origin,
            namespace,
            self.shared.clone(),
            scheduler,
            Urgency::Background,
        )
    }

    /// Unpinned files of this app unused for longer than `idle` are evicted
    /// (none: only room decides).
    pub fn set_max_idle(&self, idle: Option<Duration>) {
        if let Ok(core) = self.shared.core() {
            core.set_max_idle(self.app, idle);
        }
    }

    /// What this app's kinds hold, and the process's limits (empty before
    /// the boot has activated the cache).
    #[must_use]
    pub fn report(&self) -> crate::Report {
        match self.shared.core() {
            Ok(core) => crate::cache::report(&core, &self.shared.shape, Some(self.app)),
            Err(_) => crate::Report {
                soft: 0,
                hard: 0,
                used: 0,
                kinds: Vec::new(),
            },
        }
    }

    /// How many requests this app's stores make at once, and how many slots
    /// predictions leave free.
    pub fn set_store_tuning(&self, slots: usize, reserve: usize) {
        self.tuning.set(slots, reserve);
    }
}

/// A kind of file of an app: given at every read, weighted in the mix.
#[derive(Clone)]
pub struct KindHandle {
    pub(crate) app: &'static str,
    pub(crate) name: &'static str,
    /// A person's demand fetch of this kind may be duplicated when slow
    /// (small files; never a large video).
    pub(crate) hedge: bool,
    pub(crate) shared: Arc<Shared>,
}

impl KindHandle {
    /// A demand fetch of this kind that is slow to its first byte is tried a
    /// second time (within a budget): for small files someone waits for.
    #[must_use]
    pub fn hedged(mut self) -> Self {
        self.hedge = true;
        self
    }

    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub fn weight(&self) -> u32 {
        self.shared.shape.kind_weight(self.app, self.name)
    }

    /// The target share among the app's kinds. Takes effect at the next
    /// eviction decision, and evicts toward the new mix now if the cache is
    /// over its soft limit.
    pub fn set_weight(&self, weight: u32) {
        self.shared
            .shape
            .set_kind_weight(self.app, self.name, weight);
        if let Ok(core) = self.shared.core() {
            core.reweigh();
        }
    }
}
