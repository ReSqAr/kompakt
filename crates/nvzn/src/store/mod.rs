mod config;
mod inmemory;
mod overlay;
mod rocksdb;

use crate::ids::ProjectionId;
use crate::{Error, IndexId, Projection, ProjectionTypeId, Ref, TenantId};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zslg::{DataId, Hlc, LogIndex};

pub(crate) use config::DEFAULT_FOLD_COMMIT;
pub use config::StoreConfig;
pub use inmemory::InMemoryStore;
pub(crate) use overlay::IndexAction;
pub use overlay::{CommitBatch, WriteTransactionImpl};
pub use rocksdb::RocksDBStore;

pub type Row<T> = Result<T, Error>;
pub type Rows<'a, T> = Box<dyn Iterator<Item = Row<T>> + 'a>;

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    pub index: Option<LogIndex>,
    pub data_id: Option<DataId>,
    pub hlc: Option<Hlc>,
}

impl Watermark {
    /// Whether a store folded up to this watermark already accounts for the
    /// entry `data_id`: either it was folded, or it sits at or below the
    /// settled prefix without being part of it — a write that never made
    /// quorum, which the blended view must not show.
    ///
    /// Everything at or below `hlc` is covered, with one tie. The stable log
    /// is HLC-ordered, so every stable entry below `hlc` has been folded; at
    /// `hlc` itself that holds only when `hlc` came from a `Watermark` event
    /// (which settles everything at or below it). When it came from an
    /// entry's own HLC instead — `hlc` equals `data_id.hlc()` — a later stable
    /// entry may share it, so at the tie only `data_id` itself decides.
    pub(crate) fn covers(&self, data_id: DataId) -> bool {
        if self.data_id.is_some_and(|folded| data_id <= folded) {
            return true;
        }
        let Some(folded_hlc) = self.hlc else {
            return false;
        };
        let hlc = data_id.hlc();
        hlc < folded_hlc || (hlc == folded_hlc && self.data_id.is_none_or(|d| d.hlc() < folded_hlc))
    }
}

pub trait ReadTransaction: Send + Sync {
    fn watermark(&self) -> Result<Watermark, Error>;
    fn get_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        pid: ProjectionId,
    ) -> Result<Option<Bytes>, Error>;
    fn scan_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
    ) -> Rows<'_, (ProjectionTypeId, ProjectionId, Bytes)>;
    fn scan_index(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        q: ProjectionId,
    ) -> Rows<'_, ProjectionId>;
}

pub type ArcReadTransaction<'a> = Arc<dyn ReadTransaction + 'a>;

pub trait ReadTransactionExt: ReadTransaction {
    fn get<P: Projection>(&self, tenant_id: TenantId, r: Ref<P>) -> Result<Option<P>, Error> {
        self.get_raw(tenant_id, r.type_id(), r.id())?
            .map(|b| P::decode(&b))
            .transpose()
    }

    fn resolve<P: Projection>(&self, tenant_id: TenantId, r: Ref<P>) -> Result<P, Error> {
        self.get(tenant_id, r)?
            .ok_or_else(|| Error::reference_not_found(r))
    }

    fn scan<P: Projection>(&self, tenant_id: TenantId) -> Rows<'_, P> {
        Box::new(
            self.scan_raw(tenant_id, P::TYPE_ID)
                .map(|r| r.and_then(|(_, _, b)| P::decode(&b))),
        )
    }
}
impl<T: ReadTransaction + ?Sized> ReadTransactionExt for T {}

pub trait WriteTransaction: ReadTransaction {
    /// Whether the rows it stages are counted (`nvzn_projection_rows_total`):
    /// not for a live view's scratch transaction, which is never committed and
    /// would count each held event once per snapshot.
    fn counts_rows(&self) -> bool {
        true
    }

    /// Stage a row, or stage its removal with `None`.
    ///
    /// Takes an owned [`Bytes`] rather than a slice so the overlay can adopt
    /// the buffer the caller already encoded into. A `&[u8]` here would force
    /// every implementation to copy, once per row per event, purely to satisfy
    /// the signature.
    fn set_raw(
        &mut self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        pid: ProjectionId,
        payload: Option<Bytes>,
    ) -> Result<(), Error>;

    fn add_index(
        &mut self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        p: ProjectionId,
        q: ProjectionId,
    ) -> Result<(), Error>;
    fn remove_index(
        &mut self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        p: ProjectionId,
        q: ProjectionId,
    ) -> Result<(), Error>;
}

pub trait WriteTransactionExt: WriteTransaction {
    fn set<P: Projection>(
        &mut self,
        tenant_id: TenantId,
        r: Ref<P>,
        p: Option<&P>,
    ) -> Result<(), Error> {
        // `Bytes::from(Vec<u8>)` adopts the allocation `encode` just made; it
        // does not copy. The encoded row travels to the overlay untouched.
        let encoded = p.map(P::encode).transpose()?.map(Bytes::from);
        self.set_raw(tenant_id, r.type_id(), r.id(), encoded)
    }
}
impl<T: WriteTransaction + ?Sized> WriteTransactionExt for T {}

pub type ConfigHash = [u8; 32];

pub trait Store: Send + Sync + 'static {
    fn config_hash(&self) -> Result<Option<ConfigHash>, Error>;
    fn set_config_hash(&self, hash: &ConfigHash) -> Result<(), Error>;
    fn read(&self) -> Result<ArcReadTransaction<'_>, Error>;
    fn write(&self) -> Result<WriteTransactionImpl<'_>, Error>;
    fn commit_batch(&self, batch: CommitBatch, watermark: Watermark) -> Result<(), Error>;
    fn flush(&self) -> Result<(), Error>;
    fn reset(&self) -> Result<(), Error>;
}

pub type ArcStore = Arc<dyn Store>;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
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
            WriteTransactionExt::set::<Todo>(
                &mut txn,
                tenant_id(),
                Ref::new(TodoId(1).into()),
                None,
            )
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
            WriteTransactionExt::set::<Todo>(
                &mut txn,
                tenant_id(),
                Ref::new(TodoId(2).into()),
                None,
            )
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
}
