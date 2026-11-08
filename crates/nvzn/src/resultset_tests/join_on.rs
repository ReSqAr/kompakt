//! Result-set tests: join_on.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use crate::{
    Store, WriteTransaction,
    resultset_tests::test_support::{rig, seed, user},
    testsupport::{Todo, TodoId, User, UserId, commit_row, tenant_id},
};
use nvzn_projection::*;
use std::sync::Arc;

fn dangle(store: &Arc<dyn Store>, idx: &Index<Todo, User>) {
    let mut txn = store.write().unwrap();
    txn.add_index(
        tenant_id(),
        Todo::TYPE_ID,
        idx.id(),
        TodoId(9).into(),
        UserId(100).into(),
    )
    .unwrap();
    txn.commit(crate::Watermark::default()).unwrap();
}

fn own_bucket(u: &User) -> Ref<User> {
    Ref::new(u.id.into())
}

#[tokio::test]
async fn a_join_key_can_capture() {
    let (store, reader, idx) = rig().await;
    seed(&store);

    let everything_under = UserId(100);
    let snapshot = reader.snapshot().unwrap();
    let grouped = snapshot
        .select::<User>()
        .join_idx(idx)
        .on(move |_: &User| Ref::<User>::new(everything_under.into()))
        .collect()
        .unwrap();

    // Every row joins against the same captured bucket.
    assert!(!grouped.is_empty());
    for (_, todos) in grouped {
        let todos = todos.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(todos.len(), 2, "the captured bucket holds 100's two todos");
    }
}

#[tokio::test]
async fn join_idx_groups_by_index_bucket() {
    let (store, reader, idx) = rig().await;
    seed(&store);

    commit_row(
        &store,
        tenant_id(),
        Ref::new(UserId(300).into()),
        &user(300, "aavasaksa"),
    );

    let groups: Vec<(u128, usize)> = reader
        .snapshot()
        .unwrap()
        .select::<User>()
        .join_idx(idx)
        .on(own_bucket)
        .collect()
        .unwrap()
        .into_iter()
        .map(|(user, todos)| {
            let ok = todos.collect::<Result<Vec<_>, _>>().unwrap().len();
            (user.id.0, ok)
        })
        .collect();
    assert_eq!(groups, vec![(100, 2), (300, 0)]);
}

#[tokio::test]
async fn on_collected_groups_like_on() {
    let (store, reader, idx) = rig().await;
    seed(&store);

    let groups: Vec<(u128, Vec<u128>)> = reader
        .snapshot()
        .unwrap()
        .select::<User>()
        .join_idx(idx)
        .on_collected(own_bucket)
        .collect()
        .unwrap()
        .into_iter()
        .map(|(user, todos)| (user.id.0, todos.iter().map(|t| t.id.0).collect()))
        .collect();
    assert_eq!(groups, vec![(100, vec![1, 2])]);
}

#[tokio::test]
async fn a_missing_bucket_row_is_an_inner_error_for_on() {
    let (store, reader, idx) = rig().await;
    seed(&store);
    dangle(&store, &idx);

    let snapshot = reader.snapshot().unwrap();
    let rows = snapshot
        .select::<User>()
        .join_idx(idx)
        .on(own_bucket)
        .collect()
        .expect("the outer rows are fine; the scan itself succeeded");
    let (_, todos) = rows.into_iter().next().unwrap();
    let todos: Vec<_> = todos.collect();
    assert_eq!(todos.len(), 3);
    assert!(
        matches!(todos[2], Err(Error::ReferenceNotFound { .. })),
        "{todos:?}"
    );
}

#[tokio::test]
async fn a_missing_bucket_row_is_the_row_error_for_on_collected() {
    let (store, reader, idx) = rig().await;
    seed(&store);
    dangle(&store, &idx);

    let snapshot = reader.snapshot().unwrap();
    let err = snapshot
        .select::<User>()
        .join_idx(idx)
        .on_collected(own_bucket)
        .collect()
        .unwrap_err();
    assert!(matches!(err, Error::ReferenceNotFound { .. }), "{err:?}");
}

#[tokio::test]
async fn groups_outlive_their_result_set_and_read_its_snapshot() {
    let (store, reader, idx) = rig().await;
    seed(&store);

    let snapshot = reader.snapshot().unwrap();
    let rows = snapshot
        .select::<User>()
        .join_idx(idx)
        .on(own_bucket)
        .collect()
        .unwrap();
    // The result set is gone; a todo lands in user 100's bucket after it.
    commit_row(
        &store,
        tenant_id(),
        Ref::new(TodoId(4).into()),
        &crate::resultset_tests::test_support::todo(4, 100, "late"),
    );
    dangle(&store, &idx);

    let (_, todos) = rows.into_iter().next().unwrap();
    let ids: Vec<u128> = todos.map(|t| t.unwrap().id.0).collect();
    assert_eq!(ids, vec![1, 2], "the group reads the snapshot it came from");
}

#[tokio::test]
async fn on_lazy_defers_the_reads_and_reads_its_snapshot() {
    let (store, reader, idx) = rig().await;
    seed(&store);

    let snapshot = reader.snapshot().unwrap();
    let rows = snapshot
        .select::<User>()
        .join_idx(idx)
        .on_lazy(own_bucket)
        .collect()
        .unwrap();
    // Committed after the snapshot: a late todo and a dangling entry.
    commit_row(
        &store,
        tenant_id(),
        Ref::new(TodoId(4).into()),
        &crate::resultset_tests::test_support::todo(4, 100, "late"),
    );
    dangle(&store, &idx);

    let (user, todos) = rows.into_iter().next().unwrap();
    assert_eq!(user.id.0, 100);
    let todos: Vec<_> = todos.map(Result::unwrap).collect();
    let refs: Vec<_> = todos.iter().map(|t| t.reference().id()).collect();
    assert_eq!(refs, vec![TodoId(1).into(), TodoId(2).into()]);
    for _ in 0..2 {
        let ids: Vec<u128> = todos.iter().map(|t| t.load().unwrap().id.0).collect();
        assert_eq!(ids, vec![1, 2], "each load reads the snapshot it came from");
    }
}

#[tokio::test]
async fn a_missing_bucket_row_is_an_error_only_on_load_for_on_lazy() {
    let (store, reader, idx) = rig().await;
    seed(&store);
    dangle(&store, &idx);

    let snapshot = reader.snapshot().unwrap();
    let rows = snapshot
        .select::<User>()
        .join_idx(idx)
        .on_lazy(own_bucket)
        .collect()
        .unwrap();
    let (_, todos) = rows.into_iter().next().unwrap();
    let todos: Vec<_> = todos
        .collect::<Result<_, _>>()
        .expect("the scan reads no todo, so a dangling entry is not yet an error");
    assert_eq!(todos.len(), 3);
    assert!(todos[0].load().is_ok());
    assert!(
        matches!(todos[2].load(), Err(Error::ReferenceNotFound { .. })),
        "a dangling entry errs when loaded"
    );
}

#[tokio::test]
async fn lookup_reads_one_bucket() {
    let (store, reader, idx) = rig().await;
    seed(&store);

    let ids: Vec<u128> = reader
        .snapshot()
        .unwrap()
        .lookup(idx, Ref::<User>::new(UserId(100).into()))
        .into_iter()
        .map(|t| t.unwrap().id.0)
        .collect();
    assert_eq!(ids, vec![1, 2]);
}
