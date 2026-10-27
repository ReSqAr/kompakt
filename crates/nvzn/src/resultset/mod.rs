use crate::{Error, ReadTransaction, Rows, TenantId};

mod filter_map;
mod join_on;
mod join_pk;
mod map;

pub use join_on::{IndexJoin, LazyRows};
pub use join_pk::Lazy;

#[must_use = "a result set reads nothing until it is iterated"]
pub struct ResultSet<'a, T> {
    tenant_id: TenantId,
    read_txn: &'a dyn ReadTransaction,
    rows: Rows<'a, T>,
}

impl<'a, T: 'a> ResultSet<'a, T> {
    pub(crate) fn new(
        tenant_id: TenantId,
        read_txn: &'a dyn ReadTransaction,
        rows: Rows<'a, T>,
    ) -> Self {
        Self {
            tenant_id,
            read_txn,
            rows,
        }
    }

    #[must_use]
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    pub fn collect(self) -> Result<Vec<T>, Error> {
        self.rows.collect()
    }

    fn with_rows<S: 'a>(self, f: impl FnOnce(Rows<'a, T>) -> Rows<'a, S>) -> ResultSet<'a, S> {
        ResultSet {
            tenant_id: self.tenant_id,
            read_txn: self.read_txn,
            rows: f(self.rows),
        }
    }
}

impl<'a, T> IntoIterator for ResultSet<'a, T> {
    type Item = Result<T, Error>;
    type IntoIter = Rows<'a, T>;

    fn into_iter(self) -> Rows<'a, T> {
        self.rows
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod test_support {
    use crate::{
        Dispatcher, Index, IndexId, Projection, ProjectionId, Reader, Ref, Store, Watermark,
        WriteTransaction,
        testsupport::{
            FLUSH_INTERVAL, TestLog, Todo, TodoId, User, UserId, commit_row, event, inmemory,
            tenant, tenant_id,
        },
    };
    use std::sync::Arc;

    const IID: IndexId = IndexId::from_u32(1);
    const BY_USER: Index<Todo, User> = Todo::BY_USER;

    pub(super) fn todo(id: u128, user_id: u128, title: &str) -> Todo {
        Todo {
            id: TodoId(id),
            user_id: UserId(user_id),
            title: title.to_string(),
        }
    }

    pub(super) fn user(id: u128, name: &str) -> User {
        User::new(id, name)
    }

    pub(super) fn seed(store: &Arc<dyn Store>) {
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

    pub(super) async fn rig() -> (Arc<dyn Store>, Reader<event::Event>, Index<Todo, User>) {
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
}
