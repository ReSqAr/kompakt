//! Result-set tests: join_pk.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use crate::{
    resultset_tests::test_support::{rig, seed, todo, user},
    testing::{Todo, TodoId, UserId, commit_row, tenant_id},
};
use nvzn_projection::*;

#[tokio::test]
async fn join_pk_resolves_and_errors_on_missing() {
    let (store, reader, _idx) = rig().await;
    seed(&store);
    commit_row(
        &store,
        tenant_id(),
        Ref::new(UserId(200).into()),
        &user(200, "bobb"),
    );

    let resolved: Vec<(u128, u128)> = reader
        .snapshot()
        .unwrap()
        .select::<Todo>()
        .join_pk(Todo::user)
        .collect()
        .unwrap()
        .into_iter()
        .map(|(t, u)| (t.id.0, u.id.0))
        .collect();
    assert_eq!(resolved, vec![(1, 100), (2, 100), (3, 200)]);

    commit_row(
        &store,
        tenant_id(),
        Ref::new(TodoId(9).into()),
        &todo(9, 999, "dangling"),
    );
    let snapshot = reader.snapshot().unwrap();
    let err = snapshot
        .select::<Todo>()
        .join_pk(Todo::user)
        .collect()
        .expect_err("dangling reference must err");
    assert!(matches!(err, crate::Error::ReferenceNotFound { .. }));
}

#[tokio::test]
async fn join_pk_lazy_defers_the_read_and_errors_only_on_load() {
    let (store, reader, _idx) = rig().await;
    seed(&store);
    commit_row(
        &store,
        tenant_id(),
        Ref::new(TodoId(9).into()),
        &todo(9, 999, "dangling"),
    );

    // The dangling todo does not fail the result set: nothing is read yet.
    let snapshot = reader.snapshot().unwrap();
    let rows = snapshot
        .select::<Todo>()
        .join_pk_lazy(Todo::user)
        .collect()
        .expect("a lazy join reads no target");
    assert_eq!(rows.len(), 4);

    // User 200 lands after the snapshot; the lazy load must not see it.
    commit_row(
        &store,
        tenant_id(),
        Ref::new(UserId(200).into()),
        &user(200, "bobb"),
    );

    let loaded: Vec<(u128, Option<u128>)> = rows
        .iter()
        .map(|(t, u)| {
            assert_eq!(u.reference(), Todo::user(t));
            match u.load() {
                Ok(u) => (t.id.0, Some(u.id.0)),
                Err(crate::Error::ReferenceNotFound { .. }) => (t.id.0, None),
                Err(e) => panic!("unexpected error {e:?}"),
            }
        })
        .collect();
    assert_eq!(
        loaded,
        vec![(1, Some(100)), (2, Some(100)), (3, None), (9, None)],
        "loads read the snapshot the join came from"
    );
}
