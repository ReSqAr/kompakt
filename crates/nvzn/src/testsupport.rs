//! Re-exports of the shared fixtures, under the path the crate's tests use.
#![allow(
    unreachable_pub,
    reason = "crate-internal fixtures; `pub` items are consumed within the crate"
)]

pub use crate::testing::{FLUSH_INTERVAL, TestLog, data_id, inmemory, rocksdb};
pub use nvzn_fixtures::{
    NewTodoV1, NewUserV1, NewUserV2, NewUserV3, PingedV1, Todo, TodoDeletedV1, TodoId, User,
    UserDeletedV1, UserId, commit_row, event, hlc, tenant, tenant_id,
};
