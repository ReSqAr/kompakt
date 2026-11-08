//! Store-trait and overlay semantics, exercised through the in-memory store with
//! the test projections.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use bytes::Bytes;
use nvzn_projection::*;
use nvzn_store::*;
use zslg::{Hlc, LogIndex};

use crate::{
    InMemoryStore, Projection, ReadTransactionExt, Ref, WriteTransactionExt,
    testsupport::{self, Todo, TodoId, UserId, tenant_id},
};
use proptest::prelude::*;
use std::sync::Arc;

const IID: IndexId = IndexId::from_u32(1);
const PTID: ProjectionTypeId = Todo::TYPE_ID;

fn todo(id: u128, user_id: u128, title: &str) -> Todo {
    Todo {
        id: TodoId(id),
        user_id: UserId(user_id),
        title: title.to_string(),
    }
}

fn inmemory_store() -> Arc<dyn Store> {
    Arc::new(InMemoryStore::default())
}

/// What a transaction stages, as the fold reads it to cut a commit: a row
/// counts once, at its last payload; an index entry counts once.
#[test]
fn staged_bytes_follow_what_a_transaction_holds() {
    let store = inmemory_store();
    let mut txn = store.write().unwrap();
    assert_eq!(txn.staged_bytes(), 0);
    let pid = |n: u8| ProjectionId::from_slice([n; 16]);
    let big = Some(Bytes::from(vec![0u8; 1_000]));
    txn.set_raw(tenant_id(), PTID, pid(1), big.clone()).unwrap();
    let one = txn.staged_bytes();
    assert!(one >= 1_000, "{one}");
    txn.set_raw(tenant_id(), PTID, pid(1), Some(Bytes::from(vec![0u8; 10])))
        .unwrap();
    assert_eq!(txn.staged_bytes(), one - 990, "the last payload, not both");
    txn.set_raw(tenant_id(), PTID, pid(2), big).unwrap();
    assert_eq!(txn.staged_bytes(), 2 * one - 990);
    let rows = txn.staged_bytes();
    txn.add_index(tenant_id(), PTID, IID, pid(1), pid(9))
        .unwrap();
    let indexed = txn.staged_bytes();
    assert!(indexed > rows);
    txn.remove_index(tenant_id(), PTID, IID, pid(1), pid(9))
        .unwrap();
    assert_eq!(
        txn.staged_bytes(),
        indexed,
        "the same entry, another action"
    );
}

fn set_todo(store: &Arc<dyn Store>, t: &Todo) {
    let mut txn = store.write().unwrap();
    WriteTransactionExt::set(
        &mut txn,
        tenant_id(),
        Ref::<Todo>::new(t.id.into()),
        Some(t),
    )
    .unwrap();
    txn.commit(Watermark::default()).unwrap();
}

fn get_todo(store: &Arc<dyn Store>, id: u128) -> Option<Todo> {
    let txn = store.read().unwrap();
    ReadTransactionExt::get(&*txn, tenant_id(), Ref::new(TodoId(id).into())).unwrap()
}

fn todo_titles(store: &Arc<dyn Store>) -> Vec<String> {
    let txn = store.read().unwrap();
    let mut titles: Vec<String> = ReadTransactionExt::scan::<Todo>(&*txn, tenant_id())
        .map(|r| r.unwrap().title)
        .collect();
    titles.sort();
    titles
}

#[test]
fn store_commit_applies_overlay() {
    let dir = tempfile::TempDir::new().unwrap();
    for store in [inmemory_store(), testsupport::rocksdb(dir.path())] {
        set_todo(&store, &todo(1, 0, "one"));
        set_todo(&store, &todo(2, 0, "two"));

        let mut txn = store.write().unwrap();
        WriteTransactionExt::set::<Todo>(&mut txn, tenant_id(), Ref::new(TodoId(1).into()), None)
            .unwrap();
        let two = todo(2, 0, "two!");
        WriteTransactionExt::set(
            &mut txn,
            tenant_id(),
            Ref::new(TodoId(2).into()),
            Some(&two),
        )
        .unwrap();

        assert!(
            ReadTransactionExt::get::<Todo>(&txn, tenant_id(), Ref::new(TodoId(1).into()))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            ReadTransactionExt::get::<Todo>(&txn, tenant_id(), Ref::new(TodoId(2).into()))
                .unwrap()
                .unwrap()
                .title,
            "two!"
        );

        txn.commit(Watermark::default()).unwrap();
        assert!(get_todo(&store, 1).is_none());
        assert_eq!(get_todo(&store, 2).unwrap().title, "two!");
    }
}

#[test]
fn store_uncommitted_txn_leaves_store_unchanged() {
    let dir = tempfile::TempDir::new().unwrap();
    for store in [inmemory_store(), testsupport::rocksdb(dir.path())] {
        let t = todo(1, 0, "one");
        let mut txn = store.write().unwrap();
        WriteTransactionExt::set(&mut txn, tenant_id(), Ref::new(TodoId(1).into()), Some(&t))
            .unwrap();
        drop(txn);
        assert!(get_todo(&store, 1).is_none());
    }
}

#[test]
fn store_commit_updates_watermark() {
    let dir = tempfile::TempDir::new().unwrap();
    for store in [inmemory_store(), testsupport::rocksdb(dir.path())] {
        let mut txn = store.write().unwrap();
        txn.add_index(
            tenant_id(),
            PTID,
            IID,
            ProjectionId::from_u128(1),
            ProjectionId::from_u128(2),
        )
        .unwrap();
        let wm = Watermark {
            index: Some(LogIndex::from_bytes(9u64.to_be_bytes())),
            data_id: Some(testsupport::data_id(9, 9)),
            hlc: Some(Hlc::from_bytes(9u64.to_be_bytes())),
        };
        txn.commit(wm.clone()).unwrap();
        assert_eq!(store.read().unwrap().watermark().unwrap(), wm);
    }
}

#[test]
fn store_scan_merges_base_and_overlay() {
    let dir = tempfile::TempDir::new().unwrap();
    for store in [inmemory_store(), testsupport::rocksdb(dir.path())] {
        set_todo(&store, &todo(1, 0, "keep"));
        set_todo(&store, &todo(2, 0, "delete"));
        set_todo(&store, &todo(3, 0, "update"));

        let mut txn = store.write().unwrap();
        WriteTransactionExt::set::<Todo>(&mut txn, tenant_id(), Ref::new(TodoId(2).into()), None)
            .unwrap();
        let updated = todo(3, 0, "updated");
        WriteTransactionExt::set(
            &mut txn,
            tenant_id(),
            Ref::new(TodoId(3).into()),
            Some(&updated),
        )
        .unwrap();
        let fresh = todo(4, 0, "fresh");
        WriteTransactionExt::set(
            &mut txn,
            tenant_id(),
            Ref::new(TodoId(4).into()),
            Some(&fresh),
        )
        .unwrap();

        let mut titles: Vec<String> = ReadTransactionExt::scan::<Todo>(&txn, tenant_id())
            .map(|r| r.unwrap().title)
            .collect();
        titles.sort();
        assert_eq!(titles, vec!["fresh", "keep", "updated"]);

        txn.commit(Watermark::default()).unwrap();
        assert_eq!(todo_titles(&store), vec!["fresh", "keep", "updated"]);
    }
}

#[test]
fn store_scan_index_applies_add_remove() {
    let dir = tempfile::TempDir::new().unwrap();
    for store in [inmemory_store(), testsupport::rocksdb(dir.path())] {
        let p0 = ProjectionId::from_u128(10);
        let p1 = ProjectionId::from_u128(11);
        let q = ProjectionId::from_u128(100);

        let names = |store: &Arc<dyn Store>, q: ProjectionId| {
            let txn = store.read().unwrap();
            txn.scan_index(tenant_id(), PTID, IID, q)
                .map(|r| r.unwrap())
                .collect::<Vec<_>>()
        };

        let mut txn = store.write().unwrap();
        txn.add_index(tenant_id(), PTID, IID, p0, q).unwrap();
        txn.commit(Watermark::default()).unwrap();
        assert_eq!(names(&store, q), vec![p0]);

        // re-key: remove under old q, add under new
        let mut txn = store.write().unwrap();
        txn.remove_index(tenant_id(), PTID, IID, p0, q).unwrap();
        txn.add_index(tenant_id(), PTID, IID, p0, ProjectionId::from_u128(101))
            .unwrap();
        txn.commit(Watermark::default()).unwrap();
        assert_eq!(names(&store, q), Vec::<ProjectionId>::new());

        // add-then-remove in one txn nets to nothing
        let mut txn = store.write().unwrap();
        txn.add_index(tenant_id(), PTID, IID, p1, q).unwrap();
        txn.remove_index(tenant_id(), PTID, IID, p1, q).unwrap();
        txn.commit(Watermark::default()).unwrap();
        assert_eq!(names(&store, q), Vec::<ProjectionId>::new());

        // remove-then-add in one txn nets to present
        let mut txn = store.write().unwrap();
        txn.remove_index(tenant_id(), PTID, IID, p0, ProjectionId::from_u128(101))
            .unwrap();
        txn.add_index(tenant_id(), PTID, IID, p0, ProjectionId::from_u128(102))
            .unwrap();
        txn.commit(Watermark::default()).unwrap();
        assert_eq!(names(&store, ProjectionId::from_u128(102)), vec![p0]);
    }
}

#[test]
fn store_index_accumulates_into_set() {
    let dir = tempfile::TempDir::new().unwrap();
    for store in [inmemory_store(), testsupport::rocksdb(dir.path())] {
        let q = ProjectionId::from_u128(5);
        let mut txn = store.write().unwrap();
        txn.add_index(tenant_id(), PTID, IID, ProjectionId::from_u128(1), q)
            .unwrap();
        txn.add_index(tenant_id(), PTID, IID, ProjectionId::from_u128(2), q)
            .unwrap();
        txn.add_index(tenant_id(), PTID, IID, ProjectionId::from_u128(3), q)
            .unwrap();
        txn.commit(Watermark::default()).unwrap();

        let txn = store.read().unwrap();
        let got: Vec<ProjectionId> = txn
            .clone()
            .scan_index(tenant_id(), PTID, IID, q)
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            got,
            vec![
                ProjectionId::from_u128(1),
                ProjectionId::from_u128(2),
                ProjectionId::from_u128(3)
            ]
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Add(u8, u8, u8),
    Remove(u8, u8, u8),
}

fn arb_ops() -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        (0u8..2, 0u8..3, 0u8..4).prop_map(|(i, q, p)| Op::Add(i, q, p)),
        (0u8..2, 0u8..3, 0u8..4).prop_map(|(i, q, p)| Op::Remove(i, q, p)),
    ];
    proptest::collection::vec(op, 0..32)
}

fn id8(raw: u8) -> ProjectionId {
    ProjectionId::from_u128(u128::from(raw))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn store_scan_index_matches_model(ops in arb_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        for store in [inmemory_store(), testsupport::rocksdb(dir.path())] {
            let mut model: std::collections::BTreeSet<(u8, u8, u8)> =
                std::collections::BTreeSet::new();
            let mut touched: std::collections::BTreeSet<(u8, u8)> =
                std::collections::BTreeSet::new();
            let mut txn = store.write().unwrap();
            for op in &ops {
                match *op {
                    Op::Add(i, q, p) => {
                        model.insert((i, q, p));
                        touched.insert((i, q));
                        txn.add_index(tenant_id(), PTID, IndexId::from_u32(u32::from(i)), id8(p), id8(q))
                            .unwrap();
                    }
                    Op::Remove(i, q, p) => {
                        model.remove(&(i, q, p));
                        touched.insert((i, q));
                        txn.remove_index(tenant_id(), PTID, IndexId::from_u32(u32::from(i)), id8(p), id8(q))
                            .unwrap();
                    }
                }
            }
            txn.commit(Watermark::default()).unwrap();

            let txn = store.read().unwrap();
            for (i, q) in touched {
                let expected: std::collections::BTreeSet<ProjectionId> = model
                    .iter()
                    .filter(|(mi, mq, _)| *mi == i && *mq == q)
                    .map(|(_, _, mp)| id8(*mp))
                    .collect();
                let got: std::collections::BTreeSet<ProjectionId> = txn.clone()
                    .scan_index(tenant_id(), PTID, IndexId::from_u32(u32::from(i)), id8(q))
                    .map(|r| r.unwrap())
                    .collect();
                assert_eq!(got, expected, "index {i} query {q} after {ops:?}");
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RowOp {
    Set(u8, u8, u8),
    Delete(u8, u8, u8),
}

fn arb_row_ops() -> impl Strategy<Value = Vec<RowOp>> {
    let op = prop_oneof![
        (0u8..2, 0u8..2, 0u8..4).prop_map(|(a, t, i)| RowOp::Set(a, t, i)),
        (0u8..2, 0u8..2, 0u8..4).prop_map(|(a, t, i)| RowOp::Delete(a, t, i)),
    ];
    proptest::collection::vec(op, 0..32)
}

fn tenant_n(n: u8) -> TenantId {
    TenantId::from_u128(u128::from(n))
}

fn ptid_n(n: u8) -> ProjectionTypeId {
    ProjectionTypeId::from_u32(u32::from(n))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn store_scan_raw_matches_model(ops in arb_row_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        for store in [inmemory_store(), testsupport::rocksdb(dir.path())] {
            let mut model: std::collections::BTreeMap<(u8, u8, u8), Vec<u8>> =
                std::collections::BTreeMap::new();

            let mut txn = store.write().unwrap();
            for op in &ops {
                match *op {
                    RowOp::Set(a, t, i) => {
                        let payload = vec![a, t, i];
                        model.insert((a, t, i), payload.clone());
                        txn.set_raw(tenant_n(a), ptid_n(t), id8(i), Some(Bytes::from(payload.clone()))).unwrap();
                    }
                    RowOp::Delete(a, t, i) => {
                        model.remove(&(a, t, i));
                        txn.set_raw(tenant_n(a), ptid_n(t), id8(i), None).unwrap();
                    }
                }
            }
            txn.commit(Watermark::default()).unwrap();

            let txn = store.read().unwrap();
            for a in 0u8..2 {
                for t in 0u8..2 {
                    let expected: Vec<(ProjectionId, Vec<u8>)> = model
                        .iter()
                        .filter(|((ma, mt, _), _)| *ma == a && *mt == t)
                        .map(|((_, _, mi), v)| (id8(*mi), v.clone()))
                        .collect();
                    let got: Vec<(ProjectionId, Vec<u8>)> = txn.clone()
                        .scan_raw(tenant_n(a), ptid_n(t))
                        .map(|r| {
                            let (_, pid, b) = r.unwrap();
                            (pid, b.to_vec())
                        })
                        .collect();
                    prop_assert_eq!(got, expected, "tenant {} type {} after {:?}", a, t, ops);
                }
            }
        }
    }
}

#[test]
fn watermark_covers_everything_at_or_below_its_hlc_with_one_tie() {
    use testsupport::{data_id, hlc};

    let empty = Watermark::default();
    assert!(
        !empty.covers(data_id(1, 1)),
        "an empty store covers nothing"
    );

    // Raised past the last entry by a `Watermark` event: settled through
    // 50 inclusive, so a tail entry at or below it is either folded or a
    // write that never made quorum.
    let by_event = Watermark {
        index: None,
        data_id: Some(data_id(10, 1)),
        hlc: Some(hlc(50)),
    };
    assert!(by_event.covers(data_id(10, 1)), "the folded entry itself");
    assert!(by_event.covers(data_id(30, 2)), "below the watermark");
    assert!(
        by_event.covers(data_id(50, 3)),
        "at a watermark event's HLC"
    );
    assert!(!by_event.covers(data_id(51, 4)), "above the watermark");

    // Raised only by the last entry's own HLC: a later stable entry may
    // share it, so at the tie only `data_id` decides.
    let by_entry = Watermark {
        index: None,
        data_id: Some(data_id(50, 5)),
        hlc: Some(hlc(50)),
    };
    assert!(by_entry.covers(data_id(49, 9)), "below the tie");
    assert!(
        by_entry.covers(data_id(50, 4)),
        "at the tie, up to the folded id"
    );
    assert!(
        !by_entry.covers(data_id(50, 6)),
        "at the tie, past the folded id: may be a stable entry not folded yet"
    );

    let event_only = Watermark {
        index: None,
        data_id: None,
        hlc: Some(hlc(50)),
    };
    assert!(event_only.covers(data_id(50, 1)));
    assert!(!event_only.covers(data_id(51, 1)));
}
