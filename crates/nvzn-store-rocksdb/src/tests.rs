//! White-box tests of the RocksDB store against the test projections.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use super::*;
use nvzn_fixtures::{Todo, TodoId, UserId, tenant_id};
use nvzn_store::{
    ArcStore, Projection, ReadTransactionExt, Ref, WriteTransaction, WriteTransactionExt,
};
use nvzn_types::ProjectionTypeId;
use proptest::prelude::*;

/// Long enough that the fence never fires mid-test.
const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

fn store_config(dir: &std::path::Path) -> StoreConfig {
    let mut config = StoreConfig::new(dir);
    config.memory = zslg::ByteSize::from_mib(16);
    config.flush_interval = FLUSH_INTERVAL;
    config
}

fn rocksdb(dir: &std::path::Path) -> ArcStore {
    Arc::new(RocksDBStore::new(&store_config(dir)).expect("open rocksdb"))
}

use rust_rocksdb::{ColumnFamilyDescriptor, DB, Options};
use std::sync::Arc;

fn write_todo(store: &ArcStore, id: u128, title: &str) {
    let t = Todo {
        id: TodoId(id),
        user_id: UserId(0),
        title: title.to_string(),
    };
    let mut txn = store.write().unwrap();
    WriteTransactionExt::set(
        &mut txn,
        tenant_id(),
        Ref::<Todo>::new(TodoId(id).into()),
        Some(&t),
    )
    .unwrap();
    txn.commit(Watermark::default()).unwrap();
}

#[test]
fn rocksdb_reopen_durability() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let store = rocksdb(&path);
        write_todo(&store, 1, "durable");
        store.flush().unwrap();
    }

    let store = rocksdb(&path);
    let txn = store.read().unwrap();
    let got =
        ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into())).unwrap();
    assert_eq!(got.unwrap().title, "durable");
}

#[test]
fn rocksdb_read_isolation() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = rocksdb(dir.path());

    let held = store.read().unwrap();
    assert!(
        ReadTransactionExt::get::<Todo>(&*held, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap()
            .is_none()
    );

    write_todo(&store, 1, "after");

    assert!(
        ReadTransactionExt::get::<Todo>(&*held, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap()
            .is_none()
    );
    drop(held);

    let fresh = store.read().unwrap();
    assert_eq!(
        ReadTransactionExt::get::<Todo>(&*fresh, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap()
            .unwrap()
            .title,
        "after"
    );
}

#[test]
fn rocksdb_watermark_defaults_empty() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = rocksdb(dir.path());
    assert_eq!(
        store.read().unwrap().watermark().unwrap(),
        Watermark::default()
    );
}

#[test]
fn rocksdb_missing_cf_errors() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = [ColumnFamilyDescriptor::new("watermark", Options::default())];
    let db = Arc::new(DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap());
    let store = RocksDBStore::from_db(db);

    let err = store.read().err().expect("missing CF must error");
    assert!(
        matches!(
            &err,
            Error::RocksDBColumnFamilyMissing(name) if name == "store" || name == "index"
        ),
        "unexpected error: {err:?}"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn store_key_order_matches_tuple_order(
        tenant_a in any::<u128>(),
        ptid_a in any::<u32>(),
        pid_a in any::<u128>(),
        tenant_b in any::<u128>(),
        ptid_b in any::<u32>(),
        pid_b in any::<u128>(),
    ) {
        let tup_a = (
            TenantId::from_u128(tenant_a),
            ProjectionTypeId::from_u32(ptid_a),
            ProjectionId::from_u128(pid_a),
        );
        let tup_b = (
            TenantId::from_u128(tenant_b),
            ProjectionTypeId::from_u32(ptid_b),
            ProjectionId::from_u128(pid_b),
        );
        let key_a = encode_store_key(tup_a.0, tup_a.1, tup_a.2);
        let key_b = encode_store_key(tup_b.0, tup_b.1, tup_b.2);
        prop_assert_eq!(tup_a.cmp(&tup_b), key_a.cmp(&key_b));
    }

    #[test]
    fn index_key_order_and_suffix_roundtrip(
        tenant_a in any::<u128>(),
        ptid_a in any::<u32>(),
        iid_a in any::<u32>(),
        qid_a in any::<u128>(),
        pid_a in any::<u128>(),
        tenant_b in any::<u128>(),
        ptid_b in any::<u32>(),
        iid_b in any::<u32>(),
        qid_b in any::<u128>(),
        pid_b in any::<u128>(),
    ) {
        let tup_a = (
            TenantId::from_u128(tenant_a),
            ProjectionTypeId::from_u32(ptid_a),
            IndexId::from_u32(iid_a),
            ProjectionId::from_u128(qid_a),
            ProjectionId::from_u128(pid_a),
        );
        let tup_b = (
            TenantId::from_u128(tenant_b),
            ProjectionTypeId::from_u32(ptid_b),
            IndexId::from_u32(iid_b),
            ProjectionId::from_u128(qid_b),
            ProjectionId::from_u128(pid_b),
        );
        let key_a = encode_index_key(tup_a.0, tup_a.1, tup_a.2, tup_a.3, tup_a.4);
        let key_b = encode_index_key(tup_b.0, tup_b.1, tup_b.2, tup_b.3, tup_b.4);
        prop_assert_eq!(tup_a.cmp(&tup_b), key_a.cmp(&key_b));
        prop_assert_eq!(index_key_suffix(&key_a), Some(tup_a.4));
        prop_assert_eq!(index_key_suffix(&key_b), Some(tup_b.4));
    }
}

fn temp_store(dir: &tempfile::TempDir) -> RocksDBStore {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = ["config_hash", "watermark", "store", "index"]
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
    let db = DB::open_cf_descriptors(&opts, dir.path(), cfs).expect("open");
    RocksDBStore::from_db(Arc::new(db))
}

#[test]
fn rocksdb_flush_is_a_noop_when_clean() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = temp_store(&dir);
    assert!(!store.is_dirty(), "a fresh store owes no fsync");
    store.flush().unwrap();
    assert!(!store.is_dirty());

    let txn = store.write().unwrap();
    txn.commit(Watermark::default()).unwrap();
    assert!(store.is_dirty(), "a commit owes an fsync");

    store.flush().unwrap();
    assert!(!store.is_dirty(), "the fence consumed it");
    store.flush().unwrap();
    assert!(!store.is_dirty());
}

/// Every path that writes must arm the fence, not just `commit_batch`.
///
/// `set_config_hash` used to write straight through `db.write` and arm
/// nothing, so the config-hash record — the thing that decides whether the
/// next start refolds the whole log — sat in an unsynced WAL that `flush`
/// would skip. Routing every write through `write_armed` is what makes this
/// hold for paths added later too, so the test walks all three.
#[test]
fn rocksdb_every_write_path_arms_the_fence() {
    #[derive(Debug)]
    enum Path {
        SetConfigHash,
        CommitBatch,
        Reset,
    }

    for path in [Path::SetConfigHash, Path::CommitBatch, Path::Reset] {
        let dir = tempfile::TempDir::new().unwrap();
        let store = temp_store(&dir);
        store.flush().unwrap();
        assert!(!store.is_dirty(), "{path:?}: the fence starts clear");

        match path {
            Path::SetConfigHash => store.set_config_hash(&[7u8; 32]).unwrap(),
            Path::CommitBatch => {
                store.write().unwrap().commit(Watermark::default()).unwrap();
            }
            Path::Reset => store.reset().unwrap(),
        }

        assert!(store.is_dirty(), "{path:?} left the fence unarmed");
    }
}

/// Compression works, and the SST-size gauge is what shows it:
/// identical compressible rows land in measurably smaller `store` SSTs
/// than with compression off. Also pins that `flush` publishes the gauge
/// for every column family, even with nothing dirty.
#[test]
fn rocksdb_compresses_by_default_and_reports_sst_sizes() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let sst_bytes = |compression: bool| -> (f64, usize) {
        let dir = tempfile::TempDir::new().unwrap();
        let mut config = store_config(dir.path());
        config.compression = compression;
        let store = RocksDBStore::new(&config).unwrap();
        let arc: ArcStore = Arc::new(store.clone());
        let title = "a very compressible title ".repeat(40);
        for id in 0..200 {
            write_todo(&arc, id, &title);
        }
        store.db.flush_cf(store.store_cf().unwrap()).unwrap();

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        ::metrics::with_local_recorder(&recorder, || store.flush().unwrap());
        let gauges: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(k, ..)| k.key().name() == metrics::STORE_LIVE_SST_FILES_SIZE_BYTES)
            .collect();
        let store_bytes = gauges
            .iter()
            .find(|(k, ..)| k.key().labels().any(|l| l.value() == STORE_CF))
            .map(|(.., v)| match v {
                DebugValue::Gauge(g) => g.into_inner(),
                other => panic!("not a gauge: {other:?}"),
            })
            .expect("store family reported");
        (store_bytes, gauges.len())
    };

    let (compressed, families) = sst_bytes(true);
    let (plain, _) = sst_bytes(false);
    assert_eq!(families, COLUMN_FAMILIES.len(), "one gauge per family");
    assert!(compressed > 0.0, "the flushed rows are in an SST");
    assert!(
        compressed < plain,
        "zstd must shrink compressible rows: {compressed} vs {plain}"
    );
}

#[test]
fn rocksdb_clones_share_the_durability_fence() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = temp_store(&dir);
    let worker = store.clone();

    let txn = store.write().unwrap();
    txn.commit(Watermark::default()).unwrap();
    assert!(worker.is_dirty(), "the clone must see the commit");

    worker.flush().unwrap();
    assert!(
        !store.is_dirty(),
        "the clone's fence must clear it here too"
    );
}

/// A scan reads its transaction's snapshot — not the DB as of the scan's
/// own creation — and keeps doing so across newer writes and a flush.
#[test]
fn rocksdb_a_scan_reads_its_transactions_snapshot() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = temp_store(&dir);
    let iid = IndexId::from_u32(1);
    let q = ProjectionId::from_u128(7);
    let add = |p: u128| {
        let mut txn = store.write().unwrap();
        txn.add_index(
            tenant_id(),
            Todo::TYPE_ID,
            iid,
            ProjectionId::from_u128(p),
            q,
        )
        .unwrap();
        txn.commit(Watermark::default()).unwrap();
    };
    add(0);
    add(1);

    let txn = store.read().unwrap();
    add(2);
    // Opened after the write: it must read the transaction's snapshot, not
    // "now".
    let scan = txn.scan_index(tenant_id(), Todo::TYPE_ID, iid, q);
    add(3);
    for cf in COLUMN_FAMILIES {
        store.db.flush_cf(store.db.cf_handle(cf).unwrap()).unwrap();
    }

    let got: Vec<ProjectionId> = scan.map(|r| r.unwrap()).collect();
    assert_eq!(
        got,
        vec![ProjectionId::from_u128(0), ProjectionId::from_u128(1)]
    );
}

/// A prefix scan returns every row under its prefix and no other.
///
/// The filter cannot be what loses a row — a bloom has false positives and
/// never false negatives. Prefix *mode* can: a seek that disagrees with the
/// extractor about where the prefix ends truncates the scan and reports
/// success. Eight prefixes are interleaved across 16 L0 files here, so the
/// ranges overlap and nothing is excluded by key range alone.
#[test]
fn rocksdb_prefix_scans_are_exact_across_many_ssts() {
    const QS: u128 = 8;
    const PER_Q: u128 = 16;

    let dir = tempfile::TempDir::new().unwrap();
    let store = temp_store(&dir);
    let iid = IndexId::from_u32(1);

    for p in 0..PER_Q {
        let mut txn = store.write().unwrap();
        for q in 0..QS {
            txn.add_index(
                tenant_id(),
                Todo::TYPE_ID,
                iid,
                ProjectionId::from_u128(p),
                ProjectionId::from_u128(q),
            )
            .unwrap();
        }
        txn.commit(Watermark::default()).unwrap();
        for cf in COLUMN_FAMILIES {
            store.db.flush_cf(store.db.cf_handle(cf).unwrap()).unwrap();
        }
    }

    let txn = store.read().unwrap();
    let want: Vec<ProjectionId> = (0..PER_Q).map(ProjectionId::from_u128).collect();
    for q in 0..QS {
        let got: Vec<ProjectionId> = txn
            .clone()
            .scan_index(tenant_id(), Todo::TYPE_ID, iid, ProjectionId::from_u128(q))
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(got, want, "scan for q={q} lost or gained rows");
    }

    assert_eq!(
        txn.clone()
            .scan_index(
                tenant_id(),
                Todo::TYPE_ID,
                iid,
                ProjectionId::from_u128(QS + 1)
            )
            .count(),
        0,
        "a q with no rows must not spill into a neighbouring prefix"
    );
}

/// `reset` walks each family with `seek_to_first` / `seek_to_last`, which
/// prefix mode does not support. It has to ask for total order, and the
/// failure if it stops doing so is a wipe that silently leaves rows behind.
#[test]
fn rocksdb_reset_clears_an_index_spread_over_many_ssts() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = temp_store(&dir);
    let iid = IndexId::from_u32(1);

    for p in 0..8u128 {
        let mut txn = store.write().unwrap();
        for q in 0..4u128 {
            txn.add_index(
                tenant_id(),
                Todo::TYPE_ID,
                iid,
                ProjectionId::from_u128(p),
                ProjectionId::from_u128(q),
            )
            .unwrap();
        }
        txn.commit(Watermark::default()).unwrap();
        for cf in COLUMN_FAMILIES {
            store.db.flush_cf(store.db.cf_handle(cf).unwrap()).unwrap();
        }
    }

    store.reset().unwrap();

    let txn = store.read().unwrap();
    for q in 0..4u128 {
        assert_eq!(
            txn.clone()
                .scan_index(tenant_id(), Todo::TYPE_ID, iid, ProjectionId::from_u128(q))
                .count(),
            0,
            "reset left rows under q={q}"
        );
    }
}

#[test]
fn rocksdb_reset_clears_every_column_family() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = temp_store(&dir);

    store.set_config_hash(&[7u8; 32]).unwrap();
    let mut txn = store.write().unwrap();
    WriteTransactionExt::set(
        &mut txn,
        tenant_id(),
        Ref::<Todo>::new(TodoId(1).into()),
        Some(&Todo {
            id: TodoId(1),
            user_id: UserId(0),
            title: "t".to_string(),
        }),
    )
    .unwrap();
    txn.add_index(
        tenant_id(),
        Todo::TYPE_ID,
        IndexId::from_u32(1),
        ProjectionId::from_u128(1),
        ProjectionId::from_u128(2),
    )
    .unwrap();
    txn.commit(Watermark {
        index: Some(zslg::LogIndex::from_bytes(3u64.to_be_bytes())),
        data_id: None,
        hlc: None,
    })
    .unwrap();

    store.reset().unwrap();

    assert_eq!(store.config_hash().unwrap(), None);
    let txn = store.read().unwrap();
    assert_eq!(txn.watermark().unwrap(), Watermark::default());
    assert!(
        ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        txn.clone()
            .scan_index(
                tenant_id(),
                Todo::TYPE_ID,
                IndexId::from_u32(1),
                ProjectionId::from_u128(2)
            )
            .count(),
        0
    );
}
