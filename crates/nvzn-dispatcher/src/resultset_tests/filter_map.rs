//! Result-set tests: filter_map.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use crate::{
    resultset_tests::test_support::{rig, seed},
    testkit::{Todo, UserId},
};

#[tokio::test]
async fn filter_map_drops_none_rows() {
    let (store, reader, _idx) = rig().await;
    seed(&store);

    let mut filtered = reader
        .snapshot()
        .unwrap()
        .select::<Todo>()
        .filter_map(|t: Todo| (t.user_id.0 == 100).then_some(t.title))
        .collect()
        .unwrap();
    filtered.sort();
    assert_eq!(filtered, vec!["one", "two"]);
}

#[tokio::test]
async fn filter_map_can_capture() {
    let (store, reader, _idx) = rig().await;
    seed(&store);

    let wanted = UserId(100);
    let mut filtered = reader
        .snapshot()
        .unwrap()
        .select::<Todo>()
        .filter_map(move |t: Todo| (t.user_id == wanted).then_some(t.title))
        .collect()
        .unwrap();
    filtered.sort();
    assert_eq!(filtered, vec!["one", "two"]);
}
