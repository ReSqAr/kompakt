use crate::ids::{APP_ID_LEN, INDEX_ID_LEN, PROJECTION_ID_LEN, PROJECTION_TYPE_ID_LEN};
use crate::metrics;
use crate::store::IndexAction;
use crate::store::config::StoreConfig;
use crate::store::overlay::WriteTransactionImpl;
use crate::{
    ArcReadTransaction, CommitBatch, ConfigHash, Error, IndexId, ProjectionId, ProjectionTypeId,
    ReadTransaction, Rows, Store, TenantId, Watermark,
};
use bytes::Bytes;
use rust_rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB,
    DBCompressionType, DBRecoveryMode, Direction, IteratorMode, Options, ReadOptions,
    SliceTransform, Snapshot, WriteBatch, WriteBufferManager,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const CONFIG_HASH_CF: &str = "config_hash";
const WATERMARK_CF: &str = "watermark";
const STORE_CF: &str = "store";
const INDEX_CF: &str = "index";

const COLUMN_FAMILIES: [&str; 4] = [CONFIG_HASH_CF, WATERMARK_CF, STORE_CF, INDEX_CF];

#[derive(Clone)]
pub struct RocksDBStore {
    db: Arc<DB>,
    dirty: Arc<AtomicBool>,
}

impl RocksDBStore {
    pub fn new(config: &StoreConfig) -> Result<Self, Error> {
        config.validate()?;
        std::fs::create_dir_all(&config.path)
            .map_err(|e| Error::Io(format!("create store root {}: {e}", config.path.display())))?;

        let (db_opts, cache) = db_options(config)?;
        let descriptors = COLUMN_FAMILIES.map(|name| {
            // `watermark` and `config_hash` are one tiny row each: nothing to
            // compress.
            let compress = config.compression && matches!(name, STORE_CF | INDEX_CF);
            let opts = cf_options(
                config,
                COLUMN_FAMILIES.len(),
                &cache,
                Access::of(name),
                compress,
            );
            ColumnFamilyDescriptor::new(name, opts)
        });

        let db = DB::open_cf_descriptors(&db_opts, &config.path, descriptors)?;
        Ok(Self::from_db(Arc::new(db)))
    }

    #[must_use]
    pub(crate) fn from_db(db: Arc<DB>) -> Self {
        Self {
            db,
            dirty: Arc::new(AtomicBool::new(false)),
        }
    }

    fn config_hash_cf(&self) -> Result<&ColumnFamily, Error> {
        self.db
            .cf_handle(CONFIG_HASH_CF)
            .ok_or_else(|| Error::RocksDBColumnFamilyMissing(CONFIG_HASH_CF.to_string()))
    }

    fn watermark_cf(&self) -> Result<&ColumnFamily, Error> {
        self.db
            .cf_handle(WATERMARK_CF)
            .ok_or_else(|| Error::RocksDBColumnFamilyMissing(WATERMARK_CF.to_string()))
    }

    fn store_cf(&self) -> Result<&ColumnFamily, Error> {
        self.db
            .cf_handle(STORE_CF)
            .ok_or_else(|| Error::RocksDBColumnFamilyMissing(STORE_CF.to_string()))
    }

    fn index_cf(&self) -> Result<&ColumnFamily, Error> {
        self.db
            .cf_handle(INDEX_CF)
            .ok_or_else(|| Error::RocksDBColumnFamilyMissing(INDEX_CF.to_string()))
    }

    /// Publish `rocksdb.live-sst-files-size` for every column family. A
    /// family whose property is missing or unparsable publishes nothing,
    /// never a fabricated 0.
    fn emit_sst_sizes(&self) {
        for name in COLUMN_FAMILIES {
            let Some(cf) = self.db.cf_handle(name) else {
                continue;
            };
            if let Ok(Some(bytes)) = self
                .db
                .property_int_value_cf(cf, "rocksdb.live-sst-files-size")
            {
                metrics::store_live_sst_files_size_bytes(name, bytes);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Write a batch and arm the fence flag, in that order.
    ///
    /// The order is the whole of the correctness argument, and it is the
    /// opposite of what it looks like it should be. The flag means "the WAL may
    /// hold bytes nobody has fsynced yet", so the tempting reading is that it
    /// must be set before those bytes can exist. Arming first is unsafe:
    ///
    /// ```text
    ///   W: dirty.store(true)
    ///   F: dirty.swap(false)   observes true, clears it
    ///   F: flush_wal(true)     B is not in the WAL yet; syncs nothing of it
    ///   W: db.write(B)         B enters the WAL, unsynced
    ///                          dirty is false, and nothing re-arms it
    /// ```
    ///
    /// B then sits unsynced until some unrelated later write happens to arm the
    /// flag again — a flush that is skipped rather than merely early.
    ///
    /// Arming afterwards cannot lose a write. Every batch in the WAL was
    /// followed by a `store(true)`, so any `swap(false)` that happened before
    /// that store is answered by the store; and any later `swap(false)` is
    /// followed by a `flush_wal` that begins after `db.write` returned, and so
    /// covers the batch.
    ///
    /// What this ordering gives up is exact: a `flush` racing an in-flight
    /// write may return `Ok` without covering it. That is not a durability
    /// hole — the write's own `store(true)` still forces a later sync — and no
    /// flush can promise anything about a write that had not finished when it
    /// was called.
    ///
    /// Note `rocksdb_every_write_path_arms_the_fence` passes under *both*
    /// orderings: it observes the flag after the write has returned, which is
    /// true either way. It is there to catch a path that arms nothing at all,
    /// not to pin this order. The order rests on the argument above.
    fn write_armed(&self, batch: &WriteBatch) -> Result<(), Error> {
        let result = self.db.write(batch);
        // Armed whatever the outcome. A failed write may still have reached the
        // WAL, and one redundant fsync is cheaper than reasoning about which
        // failure modes did.
        self.dirty.store(true, Ordering::Release);
        result?;
        Ok(())
    }

    fn clear_into(&self, batch: &mut WriteBatch, cf: &ColumnFamily) {
        let mut iter = self.db.raw_iterator_cf_opt(cf, total_order_opts());
        iter.seek_to_first();
        let Some(first) = iter.key().map(<[u8]>::to_vec) else {
            return;
        };
        iter.seek_to_last();
        let Some(last) = iter.key().map(<[u8]>::to_vec) else {
            return;
        };

        let mut end = last;
        end.push(0u8);
        batch.delete_range_cf(cf, &first, &end);
    }
}

fn db_options(config: &StoreConfig) -> Result<(Options, Cache), Error> {
    let cache = Cache::new_lru_cache(config.memory.as_usize()?);
    let wbm = WriteBufferManager::new_write_buffer_manager_with_cache(
        usize::try_from(config.memtable_budget()).unwrap_or(usize::MAX),
        true,
        cache.clone(),
    );

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_write_buffer_manager(&wbm);
    opts.set_max_background_jobs(config.background_jobs());
    opts.set_max_total_wal_size(config.max_total_wal_bytes());
    opts.set_max_open_files(config.open_files());

    opts.set_atomic_flush(true);
    opts.set_wal_recovery_mode(DBRecoveryMode::TolerateCorruptedTailRecords);
    opts.set_paranoid_checks(true);
    Ok((opts, cache))
}

/// How a column family is read.
///
/// ```text
///   store       get_raw by whole key, scan_raw by (tenant, ptid)
///   index       scan_index only; nothing looks a key up in it
///   watermark   one key, the empty slice
///   config_hash one key, the empty slice
/// ```
///
/// A bloom filter is consulted on a point lookup and ignored by an iterator, so
/// on `index` every whole key in the filter block answers a question no reader
/// asks — cache it holds is cache the `store` family cannot use.
///
/// A *prefix* filter is what an iterator can use. L0 files hold overlapping key
/// ranges, so a seek has to be positioned in every one of them; a filter over
/// the distinct prefixes a file contains lets the ones holding nothing for this
/// scan be skipped without reading their index or data blocks.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    PointLookups,
    /// `prefix` is where a key stops identifying the set being scanned and
    /// starts identifying a member of it.
    ScansOnly {
        prefix: usize,
    },
}

impl Access {
    fn of(cf: &str) -> Self {
        match cf {
            INDEX_CF => Self::ScansOnly {
                prefix: INDEX_PREFIX_WIDTH,
            },
            _ => Self::PointLookups,
        }
    }
}

/// Share of each memtable spent on its prefix bloom, charged against the write
/// buffer rather than added to it. A just-folded batch is still in a memtable,
/// so a scan that skips every SST still has to get past one.
const MEMTABLE_PREFIX_BLOOM_RATIO: f64 = 0.1;

fn cf_options(
    config: &StoreConfig,
    cf_count: usize,
    cache: &Cache,
    access: Access,
    compress: bool,
) -> Options {
    let mut bb = BlockBasedOptions::default();
    bb.set_block_cache(cache);
    bb.set_cache_index_and_filter_blocks(true);
    bb.set_cache_index_and_filter_blocks_with_high_priority(true);
    bb.set_pin_l0_filter_and_index_blocks_in_cache(true);
    bb.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    bb.set_partition_filters(true);
    bb.set_metadata_block_size(4096);

    bb.set_bloom_filter(10.0, false);
    if let Access::ScansOnly { .. } = access {
        bb.set_whole_key_filtering(false);
    }

    let mut o = Options::default();
    o.set_block_based_table_factory(&bb);
    o.set_compression_type(if compress {
        DBCompressionType::Zstd
    } else {
        DBCompressionType::None
    });

    // Sound at a fixed width because `encode_index_key` lays the id fields down
    // first at constant widths, so the cut always lands on a field boundary.
    if let Access::ScansOnly { prefix } = access {
        o.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix));
        o.set_memtable_prefix_bloom_ratio(MEMTABLE_PREFIX_BLOOM_RATIO);
    }

    let write_buffer = config.write_buffer_bytes(cf_count);
    o.set_write_buffer_size(usize::try_from(write_buffer).unwrap_or(usize::MAX));
    o.set_max_write_buffer_number(2);
    let target_file = config.target_file_size_bytes(cf_count);
    o.set_target_file_size_base(target_file);

    o.set_max_bytes_for_level_base(target_file.saturating_mul(8));

    o.set_periodic_compaction_seconds(config.retire_sweep_secs());
    o
}

/// Read options for a scan that stays inside one prefix.
///
/// `prefix_same_as_start` is the caller promising not to ask for anything
/// outside the seek key's prefix, and that promise is what lets the seek
/// consult the prefix filter at all. A scan needing to cross a prefix boundary
/// must not use these — it would stop at the boundary and report success.
fn prefix_scan_opts() -> ReadOptions {
    let mut opts = ReadOptions::default();
    opts.set_prefix_same_as_start(true);
    opts
}

/// Read options for walking a whole column family.
///
/// A configured prefix extractor puts iterators in prefix mode, where
/// `seek_to_first` and `seek_to_last` are unsupported. Anything walking a
/// family end to end has to opt back out.
fn total_order_opts() -> ReadOptions {
    let mut opts = ReadOptions::default();
    opts.set_total_order_seek(true);
    opts
}

const STORE_KEY_WIDTH: usize = APP_ID_LEN + PROJECTION_TYPE_ID_LEN + PROJECTION_ID_LEN;

fn encode_store_key(
    tenant_id: TenantId,
    ptid: ProjectionTypeId,
    pid: ProjectionId,
) -> [u8; STORE_KEY_WIDTH] {
    let mut k = [0u8; STORE_KEY_WIDTH];
    k[..APP_ID_LEN].copy_from_slice(&tenant_id.as_slice());
    k[APP_ID_LEN..APP_ID_LEN + PROJECTION_TYPE_ID_LEN].copy_from_slice(&ptid.as_slice());
    k[APP_ID_LEN + PROJECTION_TYPE_ID_LEN..].copy_from_slice(&pid.as_slice());
    k
}

const STORE_KEY_PREFIX_WIDTH: usize = APP_ID_LEN + PROJECTION_TYPE_ID_LEN;

fn store_key_suffix(k: &[u8]) -> Option<ProjectionId> {
    let b: [u8; STORE_KEY_WIDTH] = k.try_into().ok()?;
    let mut pid = [0u8; PROJECTION_ID_LEN];
    pid.copy_from_slice(&b[STORE_KEY_PREFIX_WIDTH..]);
    Some(ProjectionId::from_slice(pid))
}

const STORE_PREFIX_WIDTH: usize = APP_ID_LEN + PROJECTION_TYPE_ID_LEN;

fn store_prefix(tenant_id: TenantId, ptid: ProjectionTypeId) -> [u8; STORE_PREFIX_WIDTH] {
    let mut k = [0u8; STORE_PREFIX_WIDTH];
    k[..APP_ID_LEN].copy_from_slice(&tenant_id.as_slice());
    k[APP_ID_LEN..].copy_from_slice(&ptid.as_slice());
    k
}

const INDEX_KEY_WIDTH: usize =
    APP_ID_LEN + PROJECTION_TYPE_ID_LEN + INDEX_ID_LEN + PROJECTION_ID_LEN + PROJECTION_ID_LEN;
const INDEX_PREFIX_WIDTH: usize =
    APP_ID_LEN + PROJECTION_TYPE_ID_LEN + INDEX_ID_LEN + PROJECTION_ID_LEN;

fn encode_index_key(
    tenant_id: TenantId,
    ptid: ProjectionTypeId,
    iid: IndexId,
    qid: ProjectionId,
    pid: ProjectionId,
) -> [u8; INDEX_KEY_WIDTH] {
    let mut k = [0u8; INDEX_KEY_WIDTH];
    k[..INDEX_PREFIX_WIDTH].copy_from_slice(&index_prefix(tenant_id, ptid, iid, qid));
    k[INDEX_PREFIX_WIDTH..].copy_from_slice(&pid.as_slice());
    k
}

const INDEX_PTID_AT: usize = APP_ID_LEN;
const INDEX_IID_AT: usize = INDEX_PTID_AT + PROJECTION_TYPE_ID_LEN;
const INDEX_QID_AT: usize = INDEX_IID_AT + INDEX_ID_LEN;

fn index_prefix(
    tenant_id: TenantId,
    ptid: ProjectionTypeId,
    iid: IndexId,
    qid: ProjectionId,
) -> [u8; INDEX_PREFIX_WIDTH] {
    let mut k = [0u8; INDEX_PREFIX_WIDTH];
    k[..INDEX_PTID_AT].copy_from_slice(&tenant_id.as_slice());
    k[INDEX_PTID_AT..INDEX_IID_AT].copy_from_slice(&ptid.as_slice());
    k[INDEX_IID_AT..INDEX_QID_AT].copy_from_slice(&iid.as_slice());
    k[INDEX_QID_AT..].copy_from_slice(&qid.as_slice());
    k
}

fn index_key_suffix(k: &[u8]) -> Option<ProjectionId> {
    let b: [u8; INDEX_KEY_WIDTH] = k.try_into().ok()?;
    let mut pid = [0u8; PROJECTION_ID_LEN];
    pid.copy_from_slice(&b[INDEX_PREFIX_WIDTH..]);
    Some(ProjectionId::from_slice(pid))
}

impl Store for RocksDBStore {
    fn config_hash(&self) -> Result<Option<ConfigHash>, Error> {
        let config_hash_cf = self.config_hash_cf()?;
        let value = self.db.get_cf(config_hash_cf, [])?;
        let Some(value) = value else { return Ok(None) };
        let l = value.len();
        let x = value.try_into().map_err(|_| Error::ConfigTooShort(l))?;
        Ok(Some(x))
    }

    fn set_config_hash(&self, hash: &ConfigHash) -> Result<(), Error> {
        let config_hash_cf = self
            .db
            .cf_handle(CONFIG_HASH_CF)
            .ok_or_else(|| Error::RocksDBColumnFamilyMissing(CONFIG_HASH_CF.to_string()))?;

        let mut batch = WriteBatch::new();
        batch.put_cf(config_hash_cf, [], hash);
        self.write_armed(&batch)?;
        Ok(())
    }

    fn read(&self) -> Result<ArcReadTransaction<'_>, Error> {
        Ok(Arc::new(RocksDBReadTransaction::new(&self.db)?))
    }

    fn write(&self) -> Result<WriteTransactionImpl<'_>, Error> {
        let read_txn = self.read()?;

        Ok(WriteTransactionImpl::<'_>::new(read_txn, self))
    }

    fn commit_batch(&self, batch: CommitBatch, watermark: Watermark) -> Result<(), Error> {
        let CommitBatch {
            overlay_store,
            overlay_index,
        } = batch;

        let watermark_cf = self.watermark_cf()?;
        let store_cf = self.store_cf()?;
        let index_cf = self.index_cf()?;

        let mut batch = WriteBatch::default();

        batch.put_cf(watermark_cf, [], postcard::to_stdvec(&watermark)?);

        for ((tenant_id, ptid, pid), v) in overlay_store.into_iter() {
            match v {
                Some(v) => {
                    batch.put_cf(store_cf, encode_store_key(tenant_id, ptid, pid), &v);
                }
                None => {
                    batch.delete_cf(store_cf, encode_store_key(tenant_id, ptid, pid));
                }
            }
        }

        for ((tenant_id, ptid, iid, qid), v) in overlay_index.into_iter() {
            for (pid, a) in v.into_iter() {
                let key = encode_index_key(tenant_id, ptid, iid, qid, pid);
                match a {
                    IndexAction::Add => batch.put_cf(index_cf, key, []),
                    IndexAction::Remove => batch.delete_cf(index_cf, key),
                }
            }
        }

        self.write_armed(&batch)?;

        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
        // Sampled before the dirty check: compaction moves SST sizes with no
        // write in sight, and this is the store's one periodic tick.
        self.emit_sst_sizes();
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        self.db.flush_wal(true).inspect_err(|_| {
            self.dirty.store(true, Ordering::Release);
        })?;
        Ok(())
    }

    fn reset(&self) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        self.clear_into(&mut batch, self.config_hash_cf()?);
        self.clear_into(&mut batch, self.watermark_cf()?);
        self.clear_into(&mut batch, self.store_cf()?);
        self.clear_into(&mut batch, self.index_cf()?);
        self.write_armed(&batch)?;
        Ok(())
    }
}

struct RocksDBReadTransaction<'a> {
    db: &'a DB,
    snapshot: Snapshot<'a>,
    store_cf: &'a ColumnFamily,
    index_cf: &'a ColumnFamily,
    watermark_cf: &'a ColumnFamily,
}

impl<'a> RocksDBReadTransaction<'a> {
    fn new(db: &'a DB) -> Result<Self, Error> {
        let snapshot = db.snapshot();
        let watermark_cf = db
            .cf_handle(WATERMARK_CF)
            .ok_or_else(|| Error::RocksDBColumnFamilyMissing(WATERMARK_CF.to_string()))?;
        let store_cf = db
            .cf_handle(STORE_CF)
            .ok_or_else(|| Error::RocksDBColumnFamilyMissing(STORE_CF.to_string()))?;
        let index_cf = db
            .cf_handle(INDEX_CF)
            .ok_or_else(|| Error::RocksDBColumnFamilyMissing(INDEX_CF.to_string()))?;
        Ok(Self {
            db,
            snapshot,
            watermark_cf,
            store_cf,
            index_cf,
        })
    }
}

impl<'a> RocksDBReadTransaction<'a> {
    fn pinned(&self, mut opts: ReadOptions) -> ReadOptions {
        opts.set_snapshot(&self.snapshot);
        opts
    }
}

impl<'a> ReadTransaction for RocksDBReadTransaction<'a> {
    fn watermark(&self) -> Result<Watermark, Error> {
        let Some(b) = self.snapshot.get_cf(self.watermark_cf, [])? else {
            return Ok(Watermark::default());
        };
        Ok(postcard::from_bytes(&b)?)
    }

    fn get_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        pid: ProjectionId,
    ) -> Result<Option<Bytes>, Error> {
        let b = self
            .snapshot
            .get_cf(self.store_cf, encode_store_key(tenant_id, ptid, pid))?;
        let b = b.map(Bytes::from);
        Ok(b)
    }

    fn scan_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
    ) -> Rows<'_, (ProjectionTypeId, ProjectionId, Bytes)> {
        let prefix = store_prefix(tenant_id, ptid);
        let start = encode_store_key(tenant_id, ptid, ProjectionId::MIN);
        let it = self.db.iterator_cf_opt(
            self.store_cf,
            self.pinned(ReadOptions::default()),
            IteratorMode::From(&start, Direction::Forward),
        );

        let it = it.scan(false, move |stop, r| {
            if *stop {
                return None;
            }
            match r {
                Ok((k, v)) => k.starts_with(&prefix).then(|| match store_key_suffix(&k) {
                    Some(pid) => Ok((ptid, pid, Bytes::from(v))),
                    None => {
                        *stop = true;
                        Err(Error::Decode(format!(
                            "store key is {} bytes, expected {STORE_KEY_WIDTH}",
                            k.len()
                        )))
                    }
                }),
                Err(e) => {
                    *stop = true;
                    Some(Err(e.into()))
                }
            }
        });
        Box::new(it)
    }

    fn scan_index(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        q: ProjectionId,
    ) -> Rows<'_, ProjectionId> {
        let prefix = index_prefix(tenant_id, ptid, index_id, q);
        let start = encode_index_key(tenant_id, ptid, index_id, q, ProjectionId::MIN);
        let it = self.db.iterator_cf_opt(
            self.index_cf,
            self.pinned(prefix_scan_opts()),
            IteratorMode::From(&start, Direction::Forward),
        );

        let it = it.scan(false, move |stop, r| {
            if *stop {
                return None;
            }
            match r {
                Ok((k, _)) if k.starts_with(&prefix) => match index_key_suffix(&k) {
                    Some(pid) => Some(Ok(pid)),
                    None => {
                        *stop = true;
                        Some(Err(Error::Decode(format!(
                            "index key is {} bytes, expected {INDEX_KEY_WIDTH}",
                            k.len()
                        ))))
                    }
                },
                Ok(_) => None,
                Err(e) => {
                    *stop = true;
                    Some(Err(e.into()))
                }
            }
        });
        Box::new(it)
    }
}

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
        ArcStore, Projection, ReadTransactionExt, Ref, WriteTransaction, WriteTransactionExt,
        testsupport::{self, Todo, TodoId, UserId, tenant_id},
    };
    use proptest::prelude::*;
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
            let store = testsupport::rocksdb(&path);
            write_todo(&store, 1, "durable");
            store.flush().unwrap();
        }

        let store = testsupport::rocksdb(&path);
        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert_eq!(got.unwrap().title, "durable");
    }

    #[test]
    fn rocksdb_read_isolation() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = testsupport::rocksdb(dir.path());

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
        let store = testsupport::rocksdb(dir.path());
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
            let mut config = crate::testing::store_config(dir.path());
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
}
