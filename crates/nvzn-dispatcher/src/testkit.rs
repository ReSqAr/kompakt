//! Test fixtures under one path: the shared events and projections, the store
//! and reader doubles, and the in-process log.
#![allow(
    unreachable_pub,
    reason = "crate-internal fixtures; `pub` items are consumed within the crate"
)]

use nvzn_store::ArcStore;
use std::sync::Arc;

pub use crate::FLUSH_INTERVAL;
pub use nvzn_fixtures::{
    FailingSyncReader, NewTodoV1, NewUserV3, ObservingStore, PingedV1, ScriptedReader, Todo,
    TodoDeletedV1, TodoId, UnfenceableStore, User, UserId, calls, commit_row, event, hlc, payload,
    recorded, tenant, tenant_id,
};
pub use zslg::testing::{TestLog, data_id};

pub fn inmemory() -> ArcStore {
    Arc::new(nvzn_store_fake::InMemoryStore::default())
}

pub fn rocksdb_store(dir: &std::path::Path) -> nvzn_store_rocksdb::RocksDBStore {
    let mut config = nvzn_store::StoreConfig::new(dir);
    config.memory = zslg::ByteSize::from_mib(16);
    config.flush_interval = FLUSH_INTERVAL;
    nvzn_store_rocksdb::RocksDBStore::new(&config).expect("open rocksdb")
}
