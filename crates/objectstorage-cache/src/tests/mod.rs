//! Tests of the crate through its public surface (and the core directly,
//! where a test needs a body it controls).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]

mod boot;
mod core_direct;
mod eviction;
mod evictor;
mod reading;

use crate::{AppCache, Cache, CacheConfig, CachedStore, KindHandle, Limits, Namespace, Urgency};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;

pub(super) fn limits(soft: u64, hard: u64) -> Limits {
    Limits { soft, hard }
}

pub(super) async fn objects(objects: &[(&str, usize)]) -> Arc<dyn ObjectStore> {
    let store = InMemory::new();
    for (key, size) in objects {
        store
            .put(
                &object_store::path::Path::from(*key),
                vec![7u8; *size].into(),
            )
            .await
            .unwrap();
    }
    Arc::new(store)
}

/// A cache with one app (`app`) and the given kinds.
pub(super) struct Rig {
    pub(super) cache: Cache,
    pub(super) app: AppCache,
    pub(super) dir: tempfile::TempDir,
    pub(super) kinds: Vec<KindHandle>,
}

impl Rig {
    pub(super) fn new(soft: u64, hard: u64, kinds: &[(&'static str, u32)]) -> Rig {
        let dir = tempfile::tempdir().unwrap();
        Self::on(dir, soft, hard, kinds)
    }

    pub(super) fn on(
        dir: tempfile::TempDir,
        soft: u64,
        hard: u64,
        kinds: &[(&'static str, u32)],
    ) -> Rig {
        let mut builder = Cache::builder(CacheConfig {
            dir: dir.path().to_owned(),
            limits: limits(soft, hard),
            weights: None,
        })
        .unwrap();
        let app = builder.app("app");
        let kinds = kinds
            .iter()
            .map(|(name, weight)| app.kind(name, *weight).unwrap())
            .collect();
        let cache = builder.activate().unwrap();
        Rig {
            cache,
            app,
            dir,
            kinds,
        }
    }

    pub(super) fn kind(&self, name: &str) -> &KindHandle {
        self.kinds.iter().find(|k| k.name() == name).unwrap()
    }

    pub(super) fn store(&self, origin: Arc<dyn ObjectStore>) -> CachedStore {
        self.app
            .store(origin, Namespace::new("o", "originals"))
            .with_urgency(Urgency::Demand)
    }
}
