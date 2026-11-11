//! The RocksDB `Store`, over the kit's one typed `Storage`.
//!
//! ```text
//!   config_hash  watermark  store     Lookup                  point reads by key, bloom filter
//!   index                             Prefixed<INDEX_PREFIX>   scans of one (tenant, type, index, query) set
//! ```
//!
//! The kit owns the engine: nothing here names `rust_rocksdb`. This store owns
//! what the kit leaves to its callers, which is the durability policy: a dirty
//! flag armed after every write, and a `flush` that fences only when it is set.

pub mod metrics;

#[cfg(test)]
mod store_tests;

use bytes::Bytes;
use nvzn_projection::{ArcReadTransaction, ReadTransaction, Rows, Watermark};
use nvzn_store::{CommitBatch, ConfigHash, IndexAction, Store, StoreConfig, WriteTransactionImpl};
use nvzn_types::{
    APP_ID_LEN, Error, INDEX_ID_LEN, IndexId, PROJECTION_ID_LEN, PROJECTION_TYPE_ID_LEN,
    ProjectionId, ProjectionTypeId, TenantId,
};
use rocksdb_kit::{
    Access, Batch, Cf, CfSpec, ColumnFamily, Lookup, Prefixed, Scan, Snapshot, Storage,
    StorageConfig,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const CONFIG_HASH_CF: ColumnFamily<Lookup> = ColumnFamily::new("config_hash");
const WATERMARK_CF: ColumnFamily<Lookup> = ColumnFamily::new("watermark");
const STORE_CF: ColumnFamily<Lookup> = ColumnFamily::new("store");
/// Only ever scanned, never looked up: a prefix filter, not a bloom on whole keys.
const INDEX_CF: ColumnFamily<Prefixed<{ INDEX_PREFIX_WIDTH }>> = ColumnFamily::new("index");

#[derive(Clone)]
pub struct RocksDBStore {
    storage: Arc<Storage>,
    dirty: Arc<AtomicBool>,
}

/// The kit's engine tuning from this store's configuration: the budget and the
/// six optional tunables are the same fields, so the arithmetic lives once, in
/// the kit.
fn storage_config(config: &StoreConfig) -> StorageConfig {
    let mut tuning = StorageConfig::new(config.memory);
    tuning.write_buffer = config.write_buffer;
    tuning.target_file_size = config.target_file_size;
    tuning.max_total_wal = config.max_total_wal;
    tuning.max_background_jobs = config.max_background_jobs;
    tuning.max_open_files = config.max_open_files;
    tuning.retire_sweep = config.retire_sweep;
    tuning
}

impl RocksDBStore {
    pub fn new(config: &StoreConfig) -> Result<Self, Error> {
        config.validate()?;
        // `watermark` and `config_hash` are one tiny row each: nothing to
        // compress.
        let families = [
            CfSpec::new(CONFIG_HASH_CF, false),
            CfSpec::new(WATERMARK_CF, false),
            CfSpec::new(STORE_CF, config.compression),
            CfSpec::new(INDEX_CF, config.compression),
        ];
        let storage = Storage::open(&config.path, storage_config(config), &families)?;
        Ok(Self {
            storage,
            dirty: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Publish `rocksdb.live-sst-files-size` for every column family. A
    /// family whose property is missing or unparsable publishes nothing,
    /// never a fabricated 0.
    fn emit_sst_sizes(&self) {
        for family in self.storage.column_families() {
            if let Some(bytes) = self
                .storage
                .property_u64(family, "rocksdb.live-sst-files-size")
            {
                metrics::store_live_sst_files_size_bytes(family.name(), bytes);
            }
        }
    }

    /// Whether anything was committed since the last fence.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
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
    ///   F: fence               B is not in the WAL yet; syncs nothing of it
    ///   W: storage.write(B)    B enters the WAL, unsynced
    ///                          dirty is false, and nothing re-arms it
    /// ```
    ///
    /// B then sits unsynced until some unrelated later write happens to arm the
    /// flag again — a flush that is skipped rather than merely early.
    ///
    /// Arming afterwards cannot lose a write. Every batch in the WAL was
    /// followed by a `store(true)`, so any `swap(false)` that happened before
    /// that store is answered by the store; and any later `swap(false)` is
    /// followed by a fence that begins after the write returned, and so
    /// covers the batch.
    ///
    /// What this ordering gives up is exact: a `flush` racing an in-flight
    /// write may return `Ok` without covering it. That is not a durability
    /// hole — the write's own `store(true)` still forces a later sync — and no
    /// flush can promise anything about a write that had not finished when it
    /// was called. This is a timer, not a commit fence.
    ///
    /// Note `rocksdb_every_write_path_arms_the_fence` passes under *both*
    /// orderings: it observes the flag after the write has returned, which is
    /// true either way. It is there to catch a path that arms nothing at all,
    /// not to pin this order. The order rests on the argument above.
    fn write_armed(&self, batch: Batch) -> Result<(), Error> {
        let result = self.storage.write(batch);
        // Armed whatever the outcome. A failed write may still have reached the
        // WAL, and one redundant fsync is cheaper than reasoning about which
        // failure modes did.
        self.dirty.store(true, Ordering::Release);
        result?;
        Ok(())
    }

    /// Delete everything in one family, as one range delete over its first and
    /// last key.
    fn clear_into<A: Access>(&self, batch: &mut Batch, cf: &Cf<'_, A>) {
        let mut scan = self.storage.scan(cf);
        scan.seek_to_first();
        let Some(first) = scan.key().map(<[u8]>::to_vec) else {
            return;
        };
        scan.seek_to_last();
        let Some(last) = scan.key().map(<[u8]>::to_vec) else {
            return;
        };

        let mut end = last;
        end.push(0u8);
        batch.delete_range(cf, first.as_slice(), end.as_slice());
    }
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
        let cf = self.storage.cf(CONFIG_HASH_CF)?;
        let Some(value) = self.storage.get_pinned(&cf, [])? else {
            return Ok(None);
        };
        let hash = ConfigHash::try_from(&*value).map_err(|_| Error::ConfigTooShort(value.len()))?;
        Ok(Some(hash))
    }

    fn set_config_hash(&self, hash: &ConfigHash) -> Result<(), Error> {
        let cf = self.storage.cf(CONFIG_HASH_CF)?;
        let mut batch = Batch::new();
        batch.put(&cf, [], hash);
        self.write_armed(batch)
    }

    fn read(&self) -> Result<ArcReadTransaction<'_>, Error> {
        Ok(Arc::new(RocksDBReadTransaction::new(&self.storage)?))
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

        let watermark_cf = self.storage.cf(WATERMARK_CF)?;
        let store_cf = self.storage.cf(STORE_CF)?;
        let index_cf = self.storage.cf(INDEX_CF)?;

        let mut batch = Batch::new();

        batch.put(&watermark_cf, [], postcard::to_stdvec(&watermark)?);

        for ((tenant_id, ptid, pid), v) in overlay_store.into_iter() {
            match v {
                Some(v) => {
                    batch.put(&store_cf, encode_store_key(tenant_id, ptid, pid), &v);
                }
                None => {
                    batch.delete(&store_cf, encode_store_key(tenant_id, ptid, pid));
                }
            }
        }

        for ((tenant_id, ptid, iid, qid), v) in overlay_index.into_iter() {
            for (pid, a) in v.into_iter() {
                let key = encode_index_key(tenant_id, ptid, iid, qid, pid);
                match a {
                    IndexAction::Add => batch.put(&index_cf, key, []),
                    IndexAction::Remove => batch.delete(&index_cf, key),
                }
            }
        }

        self.write_armed(batch)
    }

    fn flush(&self) -> Result<(), Error> {
        // Sampled before the dirty check: compaction moves SST sizes with no
        // write in sight, and this is the store's one periodic tick.
        self.emit_sst_sizes();
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        self.storage.fence().inspect_err(|_| {
            self.dirty.store(true, Ordering::Release);
        })?;
        Ok(())
    }

    fn reset(&self) -> Result<(), Error> {
        let mut batch = Batch::new();
        self.clear_into(&mut batch, &self.storage.cf(CONFIG_HASH_CF)?);
        self.clear_into(&mut batch, &self.storage.cf(WATERMARK_CF)?);
        self.clear_into(&mut batch, &self.storage.cf(STORE_CF)?);
        self.clear_into(&mut batch, &self.storage.cf(INDEX_CF)?);
        self.write_armed(batch)
    }
}

/// One read view: a snapshot of the store, and the families it reads, opened
/// once.
struct RocksDBReadTransaction<'a> {
    snapshot: Snapshot<'a>,
    store_cf: Cf<'a, Lookup>,
    index_cf: Cf<'a, Prefixed<{ INDEX_PREFIX_WIDTH }>>,
    watermark_cf: Cf<'a, Lookup>,
}

impl<'a> RocksDBReadTransaction<'a> {
    fn new(storage: &'a Storage) -> Result<Self, Error> {
        Ok(Self {
            snapshot: storage.snapshot(),
            watermark_cf: storage.cf(WATERMARK_CF)?,
            store_cf: storage.cf(STORE_CF)?,
            index_cf: storage.cf(INDEX_CF)?,
        })
    }
}

/// The rows of a scan that stay under a prefix: each row decoded from its key
/// and value, the first decode or engine failure yielded once and ending the
/// scan.
struct PrefixRows<'a, T, F: FnMut(&[u8], &[u8]) -> Result<T, Error>> {
    scan: Scan<'a>,
    prefix: Vec<u8>,
    decode: F,
    done: bool,
}

impl<T, F: FnMut(&[u8], &[u8]) -> Result<T, Error>> Iterator for PrefixRows<'_, T, F> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let Some(key) = self.scan.key() else {
            self.done = true;
            return self.scan.status().err().map(|e| Err(e.into()));
        };
        if !key.starts_with(&self.prefix) {
            self.done = true;
            return None;
        }
        let row = (self.decode)(key, self.scan.value().unwrap_or_default());
        self.done = row.is_err();
        self.scan.next();
        Some(row)
    }
}

impl ReadTransaction for RocksDBReadTransaction<'_> {
    fn watermark(&self) -> Result<Watermark, Error> {
        let Some(b) = self.snapshot.get_pinned(&self.watermark_cf, [])? else {
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
            .get_pinned(&self.store_cf, encode_store_key(tenant_id, ptid, pid))?;
        Ok(b.map(|b| Bytes::copy_from_slice(&b)))
    }

    fn scan_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
    ) -> Rows<'_, (ProjectionTypeId, ProjectionId, Bytes)> {
        let start = encode_store_key(tenant_id, ptid, ProjectionId::MIN);
        let mut scan = self.snapshot.scan(&self.store_cf);
        scan.seek(start);
        Box::new(PrefixRows {
            scan,
            prefix: store_prefix(tenant_id, ptid).to_vec(),
            decode: move |k: &[u8], v: &[u8]| match store_key_suffix(k) {
                Some(pid) => Ok((ptid, pid, Bytes::copy_from_slice(v))),
                None => Err(Error::Decode(format!(
                    "store key is {} bytes, expected {STORE_KEY_WIDTH}",
                    k.len()
                ))),
            },
            done: false,
        })
    }

    fn scan_index(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        q: ProjectionId,
    ) -> Rows<'_, ProjectionId> {
        let prefix = index_prefix(tenant_id, ptid, index_id, q);
        Box::new(PrefixRows {
            scan: self.snapshot.scan_prefix(&self.index_cf, &prefix),
            prefix: prefix.to_vec(),
            decode: |k: &[u8], _: &[u8]| {
                index_key_suffix(k).ok_or_else(|| {
                    Error::Decode(format!(
                        "index key is {} bytes, expected {INDEX_KEY_WIDTH}",
                        k.len()
                    ))
                })
            },
            done: false,
        })
    }
}

#[cfg(test)]
mod tests;
