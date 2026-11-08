//! Re-exports of the shared fixtures, under the path the crate's tests use.
#![allow(
    unreachable_pub,
    reason = "crate-internal fixtures; `pub` items are consumed within the crate"
)]

pub use crate::testing::{FLUSH_INTERVAL, TestLog, data_id, inmemory, rocksdb, rocksdb_store};
pub use nvzn_fixtures::{
    FailingSyncReader, NewTodoV1, NewUserV1, NewUserV2, NewUserV3, ObservingStore, PingedV1,
    ScriptedReader, Todo, TodoDeletedV1, TodoId, UnfenceableStore, User, UserDeletedV1, UserId,
    calls, commit_row, event, hlc, payload, recorded, tenant, tenant_id,
};
