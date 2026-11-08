//! Shared rig of the result-set tests.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use crate::{
    Dispatcher, Index, IndexId, Projection, ProjectionId, Reader, Ref, Store, Watermark,
    testkit::{
        FLUSH_INTERVAL, TestLog, Todo, TodoId, User, UserId, commit_row, event, inmemory, tenant,
        tenant_id,
    },
};
use nvzn_store::WriteTransaction;
use std::sync::Arc;

const IID: IndexId = IndexId::from_u32(1);
const BY_USER: Index<Todo, User> = Todo::BY_USER;

pub(crate) fn todo(id: u128, user_id: u128, title: &str) -> Todo {
    Todo {
        id: TodoId(id),
        user_id: UserId(user_id),
        title: title.to_string(),
    }
}

pub(crate) fn user(id: u128, name: &str) -> User {
    User::new(id, name)
}

pub(crate) fn seed(store: &Arc<dyn Store>) {
    commit_row(
        store,
        tenant_id(),
        Ref::new(TodoId(1).into()),
        &todo(1, 100, "one"),
    );
    commit_row(
        store,
        tenant_id(),
        Ref::new(TodoId(2).into()),
        &todo(2, 100, "two"),
    );
    commit_row(
        store,
        tenant_id(),
        Ref::new(TodoId(3).into()),
        &todo(3, 200, "three"),
    );
    commit_row(
        store,
        tenant_id(),
        Ref::new(UserId(100).into()),
        &user(100, "holden"),
    );

    for (t, u) in [(1, 100), (2, 100), (3, 200)] {
        let mut txn = store.write().unwrap();
        txn.add_index(
            tenant_id(),
            Todo::TYPE_ID,
            IID,
            ProjectionId::from_u128(t),
            ProjectionId::from_u128(u),
        )
        .unwrap();
        txn.commit(Watermark::default()).unwrap();
    }
}

pub(crate) async fn rig() -> (Arc<dyn Store>, Reader<event::Event>, Index<Todo, User>) {
    let store = inmemory();
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<Todo>().unwrap();
    let (_writer, reader) = manager.finalise().unwrap();
    (store, reader, BY_USER)
}
