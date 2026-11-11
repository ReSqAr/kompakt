//! The node's one RocksDB: opened once, with every column family named up
//! front, under one memory budget.

use std::path::Path;
use std::sync::Arc;

use bytesize::ByteSize;
use rust_rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType,
    DBRecoveryMode, DBWithThreadMode, MultiThreaded, Options, ReadOptions, SliceTransform,
    WriteBufferManager,
};

use crate::config::{StorageConfig, WRITE_BUFFERS_PER_CF, memtable_budget};
use crate::error::StorageError;
use crate::handles::{
    Access, AccessKind, AnyFamily, Batch, Cf, ColumnFamily, Pinned, Prefixed, Scan,
};

/// One column family to create: which store owns it, how it is read (carried
/// as data, from the family's type), and the one knob the owning store gets to
/// choose.
#[derive(Debug, Clone, Copy)]
pub struct CfSpec {
    pub cf: AnyFamily,
    /// zstd, or none for the high-volume rebuildable stores.
    pub compress: bool,
}

impl CfSpec {
    #[must_use]
    pub fn new<A: Access>(cf: ColumnFamily<A>, compress: bool) -> Self {
        Self {
            cf: cf.into(),
            compress,
        }
    }
}

/// Share of each memtable spent on a prefix bloom, charged against the write
/// buffer rather than added to it: a just-written batch is still in a memtable,
/// so a prefix scan that skips every SST still has to get past one.
const MEMTABLE_PREFIX_BLOOM_RATIO: f64 = 0.1;

/// Create the storage root and fsync its *parent*, so the root's own directory
/// entry is durable before any store opens beneath it. Without that, a crash can
/// take the root with it and orphan everything the stores below believe they
/// persisted. Called once per node, at boot.
pub async fn create_storage_root(root: &Path) -> Result<(), StorageError> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
        std::fs::create_dir_all(&root)
            .map_err(|e| StorageError::Storage(format!("create storage root: {e}")))?;
        if let Some(parent) = root.parent() {
            std::fs::File::open(parent)
                .and_then(|d| d.sync_all())
                .map_err(|e| StorageError::Storage(format!("fsync storage-root parent: {e}")))?;
        }
        Ok(())
    })
    .await
    .map_err(|e| StorageError::Storage(format!("storage-root creation panicked: {e}")))?
}

/// DB-wide options plus the cache everything is charged to.
fn db_options(cfg: StorageConfig) -> Result<(Options, Cache), StorageError> {
    let budget = cfg.memory;
    let cache = Cache::new_lru_cache(budget.as_usize()?);
    // Memtables charged to the SAME cache. Without this they are a separate
    // pool of write_buffer_size × max_write_buffer_number × num_cfs on top of
    // the budget — the term most commonly missed.
    let wbm = WriteBufferManager::new_write_buffer_manager_with_cache(
        ByteSize::from_bytes(memtable_budget(budget)).as_usize()?,
        true,
        cache.clone(),
    );

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_write_buffer_manager(&wbm);
    opts.set_max_background_jobs(cfg.background_jobs());
    // Bounded WAL retention. Without this the oldest WAL is pinned until every
    // family has flushed past it, so the least active family (raft_meta,
    // cursors) governs retention for all of them — measured at 92 WAL files and
    // 9.7 GB in five seconds when flush atomicity was disabled.
    opts.set_max_total_wal_size(cfg.max_total_wal_bytes());
    // Table readers for open SSTs live outside the block cache, so an unbounded
    // count quietly defeats the budget as the dataset grows.
    opts.set_max_open_files(cfg.open_files());
    // Cross-CF WriteBatches are atomic in the WAL; atomic_flush extends that to
    // memtable flushes, so recovery can never keep one CF's half of a batch.
    opts.set_atomic_flush(true);
    // NOT set: `enable_pipelined_write`. RocksDB rejects the combination
    // ("atomic_flush is incompatible with enable_pipelined_write"), so WAL
    // pipelining is mutually exclusive with cross-CF flush atomicity. Atomicity
    // wins: it is a correctness property (a cross-CF batch must not survive
    // half-applied), pipelining is a throughput one.
    //
    // CLAUDE.md §8: genuine corruption of committed state refuses to open; the
    // one documented exception is a torn *un-fsynced* tail, which is exactly
    // what this mode tolerates and nothing more. PointInTimeRecovery and
    // SkipAnyCorruptedRecord are self-heal-by-discard and are rejected.
    opts.set_wal_recovery_mode(DBRecoveryMode::TolerateCorruptedTailRecords);
    opts.set_paranoid_checks(true);
    Ok((opts, cache))
}

/// Per-family options.
fn cf_options(
    cfg: StorageConfig,
    cf_count: usize,
    cache: &Cache,
    access: AccessKind,
    compress: bool,
) -> Result<Options, StorageError> {
    let mut bb = BlockBasedOptions::default();
    bb.set_block_cache(cache);
    // Index + filter blocks inside the budget rather than growing without
    // bound with the dataset.
    bb.set_cache_index_and_filter_blocks(true);
    bb.set_cache_index_and_filter_blocks_with_high_priority(true);
    bb.set_pin_l0_filter_and_index_blocks_in_cache(true);
    // Partitioned metadata: only the top-level index stays resident. This is
    // what makes a SMALL cache survivable — without it a small cache evicts
    // index blocks and every point read pays extra IO.
    bb.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    bb.set_partition_filters(true);
    bb.set_metadata_block_size(4096);

    // A bloom filter answers a point lookup and is ignored by an iterator, so
    // only the modes that are read that way get one; a prefix-scanned family
    // filters by prefix instead and drops whole-key filtering.
    if matches!(access, AccessKind::Lookup | AccessKind::Prefixed(_)) {
        bb.set_bloom_filter(10.0, false);
    }
    if matches!(access, AccessKind::Prefixed(_)) {
        bb.set_whole_key_filtering(false);
    }

    let mut o = Options::default();
    o.set_block_based_table_factory(&bb);
    o.set_compression_type(if compress {
        DBCompressionType::Zstd
    } else {
        DBCompressionType::None
    });

    // Sound at a fixed width only because the owning store lays the id fields
    // down first at constant widths, so the cut lands on a field boundary.
    if let AccessKind::Prefixed(width) = access {
        o.set_prefix_extractor(SliceTransform::create_fixed_prefix(width));
        o.set_memtable_prefix_bloom_ratio(MEMTABLE_PREFIX_BLOOM_RATIO);
    }

    let write_buffer = cfg.write_buffer_bytes(cf_count);
    o.set_write_buffer_size(ByteSize::from_bytes(write_buffer).as_usize()?);
    o.set_max_write_buffer_number(i32::try_from(WRITE_BUFFERS_PER_CF).unwrap_or(2));
    let target_file = cfg.target_file_size_bytes(cf_count);
    o.set_target_file_size_base(target_file);
    // Derived from the target file size: eight files per level-1. Never
    // measured on its own, so it stays a constant multiple rather than a knob.
    //
    // saturating: heuristic shape, not a quantity to preserve exactly.
    o.set_max_bytes_for_level_base(target_file.saturating_mul(8));
    // Purge is a durable DeleteRange, so correctness never waits on
    // compaction. This is what reclaims the SPACE: a range tombstone and the
    // keys under it only go when the covering SSTs are next rewritten, and
    // RocksDB's own cadence for that is 30 days. Nothing in production drives
    // a compaction by hand.
    o.set_periodic_compaction_seconds(cfg.retire_sweep_secs());
    // NOT set: `set_compaction_style`. RocksDB's default (level) is what every
    // measurement in this migration ran under; universal would trade a very
    // different space-amplification profile for write amplification with no
    // number behind it. Per-family *compression* is the caller's `compress`
    // flag above, which does have one (the stable log keeps zstd, the
    // high-volume rebuildable families do not).
    Ok(o)
}

/// The node's storage: one RocksDB, one block cache, one column family per
/// store.
pub struct Storage {
    db: DBWithThreadMode<MultiThreaded>,
    cache: Cache,
    /// Every column family this storage has open, in the order [`open`]
    /// created them — the node's runtime schema, so a metrics loop reads
    /// back what was actually opened instead of re-deriving it.
    ///
    /// [`open`]: Self::open
    families: Vec<AnyFamily>,
    /// Fault injection for the fail-stop tests. The claim under test — an
    /// append failure fail-stops the replica's flush loop and never continues —
    /// is worth pinning, but RocksDB writes into an already-open WAL and buffers
    /// in the memtable, so no filesystem trick fails a write deterministically.
    /// Hence an explicit seam. See [`Self::fail_writes_from_now_on`].
    #[cfg(any(test, feature = "testing"))]
    fail_writes: std::sync::atomic::AtomicBool,
    /// The same seam for the fence. Separate from
    /// [`fail_writes`](Self::fail_writes) because the claims differ: a failed
    /// *write* must leave the batch unapplied, a failed *fence* must take the
    /// node down. See [`Self::fail_fences_from_now_on`].
    #[cfg(any(test, feature = "testing"))]
    fail_fences: std::sync::atomic::AtomicBool,
}

impl Storage {
    /// Open the node's one RocksDB.
    ///
    /// `cfs` is the complete schema: every family that will ever be used is
    /// named here, so there is no lazy creation and no reopen path that has to
    /// guess. That is what makes the live family count exact for sizing.
    pub fn open(
        root: &Path,
        cfg: StorageConfig,
        cfs: &[CfSpec],
    ) -> Result<Arc<Self>, StorageError> {
        cfg.validate()?;
        std::fs::create_dir_all(root)
            .map_err(|e| StorageError::Storage(format!("create rocksdb root: {e}")))?;
        let (db_opts, cache) = db_options(cfg)?;

        let families: Vec<AnyFamily> = cfs.iter().map(|spec| spec.cf).collect();

        // In a multi-node-in-one-process test, a killed node's `Arc<Storage>`
        // clones may still be held by aborted tokio tasks whose drop is
        // asynchronous. The RocksDB LOCK file is released when the last clone
        // drops, which can be a few milliseconds after `drop(node)`. Retry with
        // a short backoff so the open succeeds once the old holder cleans up,
        // rather than failing the relaunch. In production each node is its own
        // process, so the OS releases the lock on exit and the retry never
        // fires.
        const LOCK_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(10);
        const LOCK_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

        let db = {
            let deadline = std::time::Instant::now()
                .checked_add(LOCK_RETRY_MAX)
                .unwrap_or(std::time::Instant::now());
            loop {
                let mut descriptors = Vec::with_capacity(cfs.len());
                for spec in cfs {
                    let opts = cf_options(cfg, cfs.len(), &cache, spec.cf.access(), spec.compress)?;
                    descriptors.push(ColumnFamilyDescriptor::new(spec.cf.name(), opts));
                }
                match DBWithThreadMode::open_cf_descriptors(&db_opts, root, descriptors) {
                    Ok(db) => break db,
                    Err(e) => {
                        let msg = format!("{e}");
                        if msg.contains("lock hold by current process")
                            && std::time::Instant::now() < deadline
                        {
                            std::thread::sleep(LOCK_RETRY_INTERVAL);
                            continue;
                        }
                        return Err(StorageError::Storage(format!(
                            "open rocksdb at {}: {e}",
                            root.display()
                        )));
                    }
                }
            }
        };

        let storage = Arc::new(Self {
            db,
            cache,
            families,
            #[cfg(any(test, feature = "testing"))]
            fail_writes: std::sync::atomic::AtomicBool::new(false),
            #[cfg(any(test, feature = "testing"))]
            fail_fences: std::sync::atomic::AtomicBool::new(false),
        });
        Ok(storage)
    }

    /// The column-family handle. Missing means the store was not named in the
    /// `CfSpec` list at open, which is a boot-wiring bug, not a runtime state;
    /// so is opening it as a different access mode than the list named.
    pub fn cf<A: Access>(&self, family: ColumnFamily<A>) -> Result<Cf<'_, A>, StorageError> {
        let listed = self.families.iter().find(|f| f.name() == family.name());
        match listed {
            Some(f) if f.access() != A::KIND => Err(StorageError::Storage(format!(
                "column family {} was opened as {:?} but asked for as {:?}",
                family.name(),
                f.access(),
                A::KIND
            ))),
            _ => self.handle(family.name()).map(Cf::new),
        }
    }

    fn handle(&self, name: &str) -> Result<Arc<rust_rocksdb::BoundColumnFamily<'_>>, StorageError> {
        self.db.cf_handle(name).ok_or_else(|| {
            StorageError::Storage(format!(
                "column family {name} was not opened (not in the node's CfSpec list)"
            ))
        })
    }

    /// One point read. The returned value pins its block in the cache, so it
    /// must not cross an `.await` or a batch commit.
    pub fn get_pinned<A: Access>(
        &self,
        cf: &Cf<'_, A>,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Pinned<'_>>, StorageError> {
        self.db
            .get_pinned_cf(&cf.0, key)
            .map(|found| found.map(Pinned))
            .map_err(|e| StorageError::Storage(e.to_string()))
    }

    /// An ordered scan over one column family, unpositioned: seek before reading.
    ///
    /// Over the whole family, in total order: on a [`Prefixed`] family that
    /// opts out of prefix mode, so `seek_to_first` and `seek_to_last` work. To
    /// read one prefix's set, use [`scan_prefix`](Self::scan_prefix).
    #[must_use]
    pub fn scan<A: Access>(&self, cf: &Cf<'_, A>) -> Scan<'_> {
        match A::KIND {
            AccessKind::Prefixed(_) => Scan(self.db.raw_iterator_cf_opt(&cf.0, total_order_opts())),
            AccessKind::Plain | AccessKind::Lookup => Scan(self.db.raw_iterator_cf(&cf.0)),
        }
    }

    /// A scan of the keys that start with `prefix`, positioned at the first of
    /// them. It ends when the prefix does: the caller never has to check, and
    /// the prefix filter lets the engine skip files holding nothing for it.
    #[must_use]
    pub fn scan_prefix<const N: usize>(
        &self,
        cf: &Cf<'_, Prefixed<N>>,
        prefix: &[u8; N],
    ) -> Scan<'_> {
        let mut scan = Scan(self.db.raw_iterator_cf_opt(&cf.0, prefix_scan_opts()));
        scan.seek(prefix);
        scan
    }

    /// A consistent view of every family as of now: reads through it do not see
    /// writes made after it was taken. It borrows the storage, so it cannot
    /// outlive it.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            snapshot: self.db.snapshot(),
        }
    }

    /// Atomic across the batch, and across column families. Not durable until
    /// [`fence`](Self::fence).
    pub fn write(&self, batch: Batch) -> Result<(), StorageError> {
        #[cfg(any(test, feature = "testing"))]
        if self.fail_writes.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(StorageError::Storage("injected write failure".into()));
        }
        self.db.write(&batch.0).map_err(|e| {
            StorageError::Storage(format!("rocksdb write (batch of {}): {e}", batch.len()))
        })
    }

    /// The commit fence of CLAUDE.md §8: everything written before this returns
    /// is durable when it returns. Blocking — call it from `spawn_blocking`.
    ///
    /// **Call it after `write` has returned, never before.** A caller that
    /// fenced before its own `write` returned could be told "durable" about
    /// data that was not yet in the WAL. Every call site in the crate is
    /// `write(batch)?` then `fence()?`, on one thread, and must stay that way.
    ///
    /// A cross-store fsync coalescer sat here at one point (folding concurrent
    /// fencers of the one shared WAL into a single `flush_wal(true)`); it was
    /// reverted (docs/ROCKSDB_MIGRATION.md §7.2) after measurement showed no
    /// benefit at the load it targeted and a worse one at higher load. This
    /// calls [`fence_now`](Self::fence_now) directly.
    pub fn fence(&self) -> Result<(), StorageError> {
        self.fence_now()
    }

    /// [`fence`](Self::fence) for an async caller — the local store's timer
    /// flush, which has no blocking body of its own to sit in. Same ordering
    /// contract: the writes this covers committed long before it is called.
    /// Off the runtime worker (CLAUDE.md §19): the fsync runs on a blocking
    /// thread.
    pub async fn fence_async(self: &Arc<Self>) -> Result<(), StorageError> {
        let storage = Arc::clone(self);
        tokio::task::spawn_blocking(move || storage.fence_now())
            .await
            .map_err(|e| StorageError::Storage(format!("fence join: {e}")))?
    }

    /// The durability call: one `flush_wal(true)` per [`fence`](Self::fence).
    pub fn fence_now(&self) -> Result<(), StorageError> {
        #[cfg(any(test, feature = "testing"))]
        if self.fail_fences.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(StorageError::Storage("injected fence failure".into()));
        }
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::Storage(format!("rocksdb flush_wal: {e}")))
    }

    /// Make every subsequent [`write`](Self::write) fail. See
    /// `fail_writes`.
    #[cfg(any(test, feature = "testing"))]
    pub fn fail_writes_from_now_on(&self) {
        self.fail_writes
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Make every subsequent [`fence_now`](Self::fence_now) fail. See
    /// `fail_fences`.
    #[cfg(any(test, feature = "testing"))]
    pub fn fail_fences_from_now_on(&self) {
        self.fail_fences
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Bytes currently charged to this node's block cache.
    ///
    /// The number the budget is judged against: the budget is what was *asked
    /// for*, this is what is *held*. It reads one cache, this node's, because a
    /// node has exactly one — the process-wide registry sum it replaces could
    /// not tell six in-process loadgen nodes apart.
    pub fn charged_bytes(&self) -> usize {
        self.cache.get_usage()
    }

    /// The budget as RocksDB sees it — read back off the cache rather than off
    /// the requested `ByteSize`, so it reflects what was actually installed.
    ///
    /// Test-only for now: the number is a constant per node, so the gauge
    /// worth publishing is [`charged_bytes`](Self::charged_bytes) against it,
    /// and nothing in production reads the capacity on its own yet.
    #[cfg(any(test, feature = "testing"))]
    pub fn cache_capacity(&self) -> usize {
        self.cache.get_capacity()
    }

    /// Every column family this storage has open, read back off the list
    /// [`open`](Self::open) populated rather than re-deriving it, so a
    /// metrics loop cannot drift from what was actually opened.
    pub fn column_families(&self) -> impl Iterator<Item = AnyFamily> + '_ {
        self.families.iter().copied()
    }

    /// One RocksDB property for a single column family (e.g.
    /// `rocksdb.num-immutable-mem-table`), parsed as `u64`.
    ///
    /// `None` covers both "the family was not opened" and "RocksDB has no
    /// such property or returned a value that does not parse as `u64`" —
    /// deliberately: a broken reading must not be published as a healthy
    /// zero, so the caller's job is to skip the gauge, never to substitute
    /// one.
    pub fn property_u64(&self, family: AnyFamily, name: &str) -> Option<u64> {
        let cf = self.handle(family.name()).ok()?;
        self.db.property_int_value_cf(&cf, name).ok().flatten()
    }

    /// One DB-wide RocksDB property (e.g. `rocksdb.actual-delayed-write-rate`),
    /// parsed as `u64`. Same "no fabricated zero" contract as
    /// [`property_u64`](Self::property_u64).
    pub fn property_u64_db(&self, name: &str) -> Option<u64> {
        self.db.property_int_value(name).ok().flatten()
    }

    /// Force the retirement-tombstone rewrite of one family now. Blocking.
    ///
    /// Test-only: production reclamation is `periodic_compaction_seconds`
    /// (`cf_options`), which reruns compaction over aged SSTs on its own
    /// cadence — nothing in production drives a compaction by hand. This
    /// exists so a test can push a purge's `DeleteRange` tombstone (and the
    /// keys under it) out of the memtable and into an SST, which is a
    /// different code path from the all-in-memtable case a plain reopen
    /// exercises.
    #[cfg(any(test, feature = "testing"))]
    pub fn compact<A: Access>(&self, family: ColumnFamily<A>) -> Result<(), StorageError> {
        let cf = self.cf(family)?;
        self.db
            .compact_range_cf(&cf.0, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
}

/// Read options for a scan that stays inside one prefix.
///
/// `prefix_same_as_start` is the caller promising not to ask for anything
/// outside the seek key's prefix, and that promise is what lets the seek
/// consult the prefix filter at all.
fn prefix_scan_opts() -> ReadOptions {
    let mut opts = ReadOptions::default();
    opts.set_prefix_same_as_start(true);
    opts
}

/// Read options for walking a whole family. A configured prefix extractor puts
/// iterators in prefix mode, where `seek_to_first` and `seek_to_last` are
/// unsupported; anything walking a family end to end has to opt back out.
fn total_order_opts() -> ReadOptions {
    let mut opts = ReadOptions::default();
    opts.set_total_order_seek(true);
    opts
}

/// A consistent read view, taken by [`Storage::snapshot`].
pub struct Snapshot<'db> {
    snapshot: rust_rocksdb::SnapshotWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>,
}

impl<'db> Snapshot<'db> {
    /// One point read as of the snapshot. The value pins its block while alive.
    pub fn get_pinned<A: Access>(
        &self,
        cf: &Cf<'_, A>,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Pinned<'_>>, StorageError> {
        let mut opts = ReadOptions::default();
        opts.set_snapshot(&self.snapshot);
        self.snapshot
            .get_pinned_cf_opt(&cf.0, key, opts)
            .map(|found| found.map(Pinned))
            .map_err(|e| StorageError::Storage(e.to_string()))
    }

    /// An ordered scan as of the snapshot; see [`Storage::scan`].
    #[must_use]
    pub fn scan<A: Access>(&self, cf: &Cf<'_, A>) -> Scan<'_> {
        let mut opts = match A::KIND {
            AccessKind::Prefixed(_) => total_order_opts(),
            AccessKind::Plain | AccessKind::Lookup => ReadOptions::default(),
        };
        opts.set_snapshot(&self.snapshot);
        Scan(self.snapshot.raw_iterator_cf_opt(&cf.0, opts))
    }

    /// A prefix scan as of the snapshot; see [`Storage::scan_prefix`].
    #[must_use]
    pub fn scan_prefix<const N: usize>(
        &self,
        cf: &Cf<'_, Prefixed<N>>,
        prefix: &[u8; N],
    ) -> Scan<'_> {
        let mut opts = prefix_scan_opts();
        opts.set_snapshot(&self.snapshot);
        let mut scan = Scan(self.snapshot.raw_iterator_cf_opt(&cf.0, opts));
        scan.seek(prefix);
        scan
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests;
