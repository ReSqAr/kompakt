//! The node's one RocksDB — opened explicitly, with every column family named.
//!
//! # Why one open, at one place
//!
//! Inferring DB identity from directory layout lets on-disk layout decide how
//! many database instances a node runs: a store opening `<dir>` takes
//! `dir.parent()` as the DB and `dir`'s leaf name as the column family, one
//! store sits a level deeper than the rest — its parent is a different root,
//! and a node running a learner quietly opens a second RocksDB with a second
//! block cache sized at the *full* budget. The declared memory budget then
//! means nothing: a node is charged two to three times it.
//!
//! Here the caller states the root, the budget and the complete list of column
//! families. The descriptor list **is** the schema — no lazy creation, no
//! reopen path guessing which families exist — which is also what makes the
//! live family count known at open, so memtable sizing is exact rather than
//! divided by a planning constant.
//!
//! # Why the budget is an argument
//!
//! A budget is a *node's* property, and loadgen runs six nodes in one process.
//! It was a process-wide `static` before, which meant two nodes could not hold
//! different budgets, whichever store opened first silently decided it, and a
//! node type that forgot to set it inherited whatever the last one wanted.
//!
//! # Memory
//!
//! One knob. Block cache, memtables (via [`WriteBufferManager`]) and SST index
//! and filter blocks are all charged to the same [`Cache`], so RSS tracks the
//! budget with a known multiplier instead of being the sum of independent
//! pools. Iterator-pinned blocks, compaction buffers, table readers for open
//! SSTs and allocator fragmentation still sit outside it — bounded indirectly
//! by `max_open_files` and the memtable sizing.

use std::path::Path;
use std::sync::Arc;

use rust_rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor,
    DBCompressionType, DBRecoveryMode, DBWithThreadMode, MultiThreaded, Options, WriteBatch,
    WriteBufferManager,
};

use super::Subsystem;
use crate::Error;
use crate::error::ToStorage;
use crate::unit::ByteSize;

/// Background compaction + flush threads. Small machine: 2.
///
/// Derived constant, overridable per node via
/// [`StorageConfig::max_background_jobs`]: it is the documented remediation
/// when RSS overshoots the budget (compaction input/output buffers scale with
/// it and sit outside the cache).
const BACKGROUND_JOBS: i32 = 2;

/// Memtables per column family. RocksDB needs at least two: one flushing
/// while the other takes writes.
///
/// **Derived, not a knob.** Two is RocksDB's structural minimum, and every
/// memtable term below divides the budget share by it, so raising it shrinks
/// each memtable rather than buying anything. No measurement has wanted a
/// third, so it stays a constant.
const WRITE_BUFFERS_PER_CF: u64 = 2;

/// Floor on a single memtable. Below roughly this, RocksDB flushes so often
/// that compaction cannot keep up and the write path stalls on L0 instead of
/// on memory — smaller stops buying memory and starts buying stalls.
const MIN_WRITE_BUFFER_BYTES: u64 = 64 * 1024;

/// Share of the budget memtables may take before flushes are forced.
const MEMTABLE_SHARE_PERCENT: u64 = 40;

/// How often RocksDB rewrites aged SSTs so retirement is actually reclaimed —
/// that is, so the range tombstones purge writes are dropped and the space
/// comes back. RocksDB's own default here is 30 days; this only shortens it.
/// Pure space-versus-write-amplification — not a correctness value, because
/// purge is durable the moment its batch is fenced.
const RETIRE_SWEEP_DEFAULT: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// One column family to create: which store owns it, and the one knob the
/// owning store gets to choose.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CfSpec {
    pub(crate) subsystem: Subsystem,
    /// zstd, or none for the high-volume rebuildable stores.
    pub(crate) compress: bool,
}

fn memtable_budget(budget: ByteSize) -> u64 {
    // saturating: a budget large enough to overflow this is not a budget.
    budget
        .as_u64()
        .saturating_mul(MEMTABLE_SHARE_PERCENT)
        .checked_div(100)
        .unwrap_or(0)
}

/// The node's storage tuning: the memory budget, plus the few RocksDB terms
/// worth overriding on a node that has been measured.
///
/// **Every override is `None` by default and every `None` derives from
/// `memory`.** The derivations are the accessors below and they are the only
/// place a value is written, so an unset knob and the pre-knob behaviour are
/// the same number by construction rather than by two defaults agreeing.
///
/// The exposed set is deliberately short (CLAUDE.md §15's spirit, applied to
/// configuration): everything else RocksDB offers stays a derived constant
/// carrying a comment naming what it came from.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StorageConfig {
    /// Total memory the engine may hold — block cache, index and filter
    /// blocks, and the memtables charged against it. The one number to move.
    pub(crate) memory: ByteSize,
    /// One memtable's size. Overridable because it is the term a measurement
    /// actually caught: RocksDB's 64 MiB default was applied regardless of the
    /// budget and a 64 MiB node was over its whole budget on one memtable.
    pub(crate) write_buffer: Option<ByteSize>,
    /// SST target size. Derived at 2x the memtable — a heuristic shape, not a
    /// measured quantity, which is exactly why it has an override.
    pub(crate) target_file_size: Option<ByteSize>,
    /// Cap on retained WAL. Measured at 92 WAL files / 9.7 GB in five seconds
    /// when it was unset.
    pub(crate) max_total_wal: Option<ByteSize>,
    /// Background compaction + flush threads. Compaction buffers scale with it
    /// and sit outside the budget, so lowering it is the RSS-overshoot lever.
    pub(crate) max_background_jobs: Option<i32>,
    /// Open SST files. Each holds a table reader outside the block cache — the
    /// other term that escapes the budget as the dataset grows.
    pub(crate) max_open_files: Option<i32>,
    /// Override for [`RETIRE_SWEEP_DEFAULT`]. See that constant.
    pub(crate) retire_sweep: Option<std::time::Duration>,
}

impl StorageConfig {
    /// A node's storage tuning with every override unset: the budget alone,
    /// everything else derived from it.
    ///
    /// Test-only: production always comes from a node config, which has a field
    /// per override and so builds the struct literally. Nothing outside a test
    /// wants "the budget and nothing else".
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn new(memory: ByteSize) -> Self {
        Self {
            memory,
            write_buffer: None,
            target_file_size: None,
            max_total_wal: None,
            max_background_jobs: None,
            max_open_files: None,
            retire_sweep: None,
        }
    }

    /// One memtable's size. The [`WriteBufferManager`] caps the total
    /// independently; this is what keeps a SINGLE family from claiming the
    /// whole share, which the manager alone does not prevent.
    ///
    /// `cf_count` is the real number of families, not a planning constant: an
    /// explicit descriptor list makes the count known at open, so each replica
    /// is sized for exactly the families it declares.
    fn write_buffer_bytes(self, cf_count: usize) -> u64 {
        if let Some(explicit) = self.write_buffer {
            return explicit.as_u64().max(MIN_WRITE_BUFFER_BYTES);
        }
        let families = u64::try_from(cf_count).unwrap_or(1).max(1);
        memtable_budget(self.memory)
            .checked_div(families.saturating_mul(WRITE_BUFFERS_PER_CF))
            .unwrap_or(MIN_WRITE_BUFFER_BYTES)
            .max(MIN_WRITE_BUFFER_BYTES)
    }

    /// SST target size. Sized to the memtable that produces them: RocksDB's
    /// 64 MiB default would have compaction assembling files far larger than
    /// the whole budget, and its input/output buffers are outside the cache.
    fn target_file_size_bytes(self, cf_count: usize) -> u64 {
        match self.target_file_size {
            Some(explicit) => explicit.as_u64().max(1),
            // saturating: the factor is heuristic shape, not a quantity to
            // preserve exactly.
            None => self.write_buffer_bytes(cf_count).saturating_mul(2),
        }
    }

    /// Cap on retained WAL. Bounds *disk*, but it is sized from the memory
    /// budget on purpose: exceeding it forces the memtables holding the oldest
    /// WAL to flush, so it is the backstop that keeps unflushed data — and the
    /// WAL replay a restart would have to do — proportional to the budget.
    fn max_total_wal_bytes(self) -> u64 {
        match self.max_total_wal {
            Some(explicit) => explicit.as_u64(),
            // saturating: the doubling is heuristic headroom, not an exact
            // quantity.
            None => memtable_budget(self.memory)
                .saturating_mul(2)
                .max(1024 * 1024),
        }
    }

    /// Background compaction + flush threads.
    fn background_jobs(self) -> i32 {
        self.max_background_jobs.unwrap_or(BACKGROUND_JOBS)
    }

    /// Open SST files, scaled off the budget when unset.
    fn open_files(self) -> i32 {
        if let Some(explicit) = self.max_open_files {
            return explicit;
        }
        let scaled = self
            .memory
            .as_u64()
            .checked_div(2 * 1024 * 1024)
            .unwrap_or(0)
            .clamp(16, 512);
        i32::try_from(scaled).unwrap_or(64)
    }

    /// Reclamation cadence, in seconds. See [`RETIRE_SWEEP_DEFAULT`].
    ///
    /// `.max(1)`: RocksDB reads `periodic_compaction_seconds == 0` as
    /// *disabled*, not "as often as possible". `Duration::as_secs()` truncates
    /// sub-second durations to zero, so an operator configuring a short sweep
    /// (say, 500ms, to reclaim aggressively in a test) would otherwise
    /// silently turn reclamation off instead — the opposite of what they
    /// asked for. Clamping here makes that trap unrepresentable rather than
    /// merely unlikely, for every override however it arrives.
    fn retire_sweep_secs(self) -> u64 {
        self.retire_sweep
            .unwrap_or(RETIRE_SWEEP_DEFAULT)
            .as_secs()
            .max(1)
    }

    /// Reject a tuning that cannot open, naming the knob and the constraint.
    ///
    /// Only the *explicit* values are checked: a derived one is produced by the
    /// accessors above, which are total.
    pub(crate) fn validate(self) -> Result<(), Error> {
        if self.memory.as_u64() == 0 {
            return Err(Error::InvalidConfiguration(
                "storage_memory must be > 0: it is the whole engine's budget".to_owned(),
            ));
        }
        if let Some(wb) = self.write_buffer
            && wb.as_u64() < MIN_WRITE_BUFFER_BYTES
        {
            return Err(Error::InvalidConfiguration(format!(
                "storage_write_buffer ({} bytes) must be >= {MIN_WRITE_BUFFER_BYTES} bytes: \
                 below that RocksDB flushes faster than compaction can keep up and the \
                 write path stalls on L0 instead of on memory",
                wb.as_u64(),
            )));
        }
        if let Some(tf) = self.target_file_size
            && tf.as_u64() == 0
        {
            return Err(Error::InvalidConfiguration(
                "storage_target_file_size must be > 0".to_owned(),
            ));
        }
        if let Some(wal) = self.max_total_wal
            && wal.as_u64() == 0
        {
            return Err(Error::InvalidConfiguration(
                "storage_max_total_wal must be > 0: zero would force a flush on every write"
                    .to_owned(),
            ));
        }
        if let Some(jobs) = self.max_background_jobs
            && jobs < 1
        {
            return Err(Error::InvalidConfiguration(format!(
                "storage_max_background_jobs ({jobs}) must be >= 1: with no background \
                 thread nothing flushes or compacts",
            )));
        }
        if let Some(files) = self.max_open_files
            && files == 0
        {
            return Err(Error::InvalidConfiguration(
                "storage_max_open_files must be non-zero (-1 means unlimited)".to_owned(),
            ));
        }
        if let Some(files) = self.max_open_files
            && files < -1
        {
            return Err(Error::InvalidConfiguration(format!(
                "storage_max_open_files ({files}) must be -1 (unlimited) or positive",
            )));
        }
        Ok(())
    }
}

/// DB-wide options plus the cache everything is charged to.
fn db_options(cfg: StorageConfig) -> Result<(Options, Cache), Error> {
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
    compress: bool,
) -> Result<Options, Error> {
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

    let mut o = Options::default();
    o.set_block_based_table_factory(&bb);
    o.set_compression_type(if compress {
        DBCompressionType::Zstd
    } else {
        DBCompressionType::None
    });

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
pub(crate) struct Storage {
    db: DBWithThreadMode<MultiThreaded>,
    cache: Cache,
    /// Every column family this storage has open, in the order [`open`]
    /// created them — the node's runtime schema, so a metrics loop reads
    /// back what was actually opened instead of re-deriving it.
    ///
    /// [`open`]: Self::open
    subsystems: Vec<Subsystem>,
    /// Fault injection for the fail-stop tests. The claim under test — an
    /// append failure fail-stops the replica's flush loop and never continues —
    /// is worth pinning, but RocksDB writes into an already-open WAL and buffers
    /// in the memtable, so no filesystem trick fails a write deterministically.
    /// Hence an explicit seam. See [`Self::fail_writes_from_now_on`].
    #[cfg(test)]
    fail_writes: std::sync::atomic::AtomicBool,
    /// The same seam for the fence. Separate from
    /// [`fail_writes`](Self::fail_writes) because the claims differ: a failed
    /// *write* must leave the batch unapplied, a failed *fence* must take the
    /// node down. See [`Self::fail_fences_from_now_on`].
    #[cfg(test)]
    fail_fences: std::sync::atomic::AtomicBool,
}

impl Storage {
    /// Open the node's one RocksDB.
    ///
    /// `cfs` is the complete schema: every family that will ever be used is
    /// named here, so there is no lazy creation and no reopen path that has to
    /// guess. That is what makes the live family count exact for sizing.
    pub(crate) fn open(
        root: &Path,
        cfg: StorageConfig,
        cfs: &[CfSpec],
    ) -> Result<Arc<Self>, Error> {
        cfg.validate()?;
        std::fs::create_dir_all(root).to_storage("create rocksdb root")?;
        let (db_opts, cache) = db_options(cfg)?;

        let mut subsystems = Vec::with_capacity(cfs.len());
        for spec in cfs {
            subsystems.push(spec.subsystem);
        }

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
                    let opts = cf_options(cfg, cfs.len(), &cache, spec.compress)?;
                    descriptors.push(ColumnFamilyDescriptor::new(spec.subsystem.cf_name(), opts));
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
                        return Err(Error::Storage(format!(
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
            subsystems,
            #[cfg(test)]
            fail_writes: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_fences: std::sync::atomic::AtomicBool::new(false),
        });
        Ok(storage)
    }

    /// Open a storage root carrying **every** column family, for tests.
    ///
    /// A root's descriptor list must name every family it has ever held, so a
    /// test helper that opened a subset would make the roots two test
    /// constructors produce mutually incompatible. Naming all of them means any
    /// test store can share a root with any other — which is what lets one
    /// `Storage` back a stable log and a local store in one test.
    #[cfg(test)]
    pub(crate) fn open_for_test(root: &Path) -> Result<Arc<Self>, Error> {
        let cfs: Vec<CfSpec> = [
            Subsystem::ReplicaLog,
            Subsystem::StableLog,
            Subsystem::LocalStoreData,
            Subsystem::LocalStoreCursors,
            Subsystem::RaftLog,
            Subsystem::RaftMeta,
            Subsystem::NodeId,
        ]
        .into_iter()
        .map(|subsystem| CfSpec {
            subsystem,
            compress: false,
        })
        .collect();
        Self::open(root, StorageConfig::new(ByteSize::from_mib(64)), &cfs)
    }

    /// The raw handle, for the `get_pinned_cf` / `iterator_cf_opt` calls the
    /// stores make directly. Wrapping those would only be a shorter spelling of
    /// a call the caller can already make.
    pub(crate) fn db(&self) -> &DBWithThreadMode<MultiThreaded> {
        &self.db
    }

    /// The column-family handle. Missing means the store was not named in the
    /// `CfSpec` list at open, which is a boot-wiring bug, not a runtime state.
    pub(crate) fn cf(&self, s: Subsystem) -> Result<Arc<BoundColumnFamily<'_>>, Error> {
        self.db.cf_handle(s.cf_name()).ok_or_else(|| {
            Error::Storage(format!(
                "column family {} was not opened (not in the node's CfSpec list)",
                s.cf_name()
            ))
        })
    }

    /// Atomic across the batch, and across column families. Not durable until
    /// [`fence`](Self::fence).
    pub(crate) fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        #[cfg(test)]
        if self.fail_writes.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Storage("injected write failure".into()));
        }
        self.db
            .write(&batch)
            .map_err(|e| Error::Storage(format!("rocksdb write (batch of {}): {e}", batch.len())))
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
    pub(crate) fn fence(&self) -> Result<(), Error> {
        self.fence_now()
    }

    /// [`fence`](Self::fence) for an async caller — the local store's timer
    /// flush, which has no blocking body of its own to sit in. Same ordering
    /// contract: the writes this covers committed long before it is called.
    /// Off the runtime worker (CLAUDE.md §19): the fsync runs on a blocking
    /// thread.
    pub(crate) async fn fence_async(self: &Arc<Self>) -> Result<(), Error> {
        let storage = Arc::clone(self);
        tokio::task::spawn_blocking(move || storage.fence_now())
            .await
            .map_err(|e| Error::Storage(format!("fence join: {e}")))?
    }

    /// The durability call: one `flush_wal(true)` per [`fence`](Self::fence).
    pub(crate) fn fence_now(&self) -> Result<(), Error> {
        #[cfg(test)]
        if self.fail_fences.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Storage("injected fence failure".into()));
        }
        self.db
            .flush_wal(true)
            .map_err(|e| Error::Storage(format!("rocksdb flush_wal: {e}")))
    }

    /// Make every subsequent [`write`](Self::write) fail. See
    /// [`fail_writes`](Self::fail_writes).
    #[cfg(test)]
    pub(crate) fn fail_writes_from_now_on(&self) {
        self.fail_writes
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Make every subsequent [`fence_now`](Self::fence_now) fail. See
    /// [`fail_fences`](Self::fail_fences).
    #[cfg(test)]
    pub(crate) fn fail_fences_from_now_on(&self) {
        self.fail_fences
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Bytes currently charged to this node's block cache.
    ///
    /// The number the budget is judged against: the budget is what was *asked
    /// for*, this is what is *held*. It reads one cache, this node's, because a
    /// node has exactly one — the process-wide registry sum it replaces could
    /// not tell six in-process loadgen nodes apart.
    pub(crate) fn charged_bytes(&self) -> usize {
        self.cache.get_usage()
    }

    /// The budget as RocksDB sees it — read back off the cache rather than off
    /// the requested `ByteSize`, so it reflects what was actually installed.
    ///
    /// Test-only for now: the number is a constant per node, so the gauge
    /// worth publishing is [`charged_bytes`](Self::charged_bytes) against it,
    /// and nothing in production reads the capacity on its own yet.
    #[cfg(test)]
    pub(crate) fn cache_capacity(&self) -> usize {
        self.cache.get_capacity()
    }

    /// Every column family this storage has open, read back off the list
    /// [`open`](Self::open) populated rather than re-deriving it, so a
    /// metrics loop cannot drift from what was actually opened.
    pub(crate) fn column_families(&self) -> impl Iterator<Item = Subsystem> + '_ {
        self.subsystems.iter().copied()
    }

    /// One RocksDB property for a single column family (e.g.
    /// `rocksdb.num-immutable-mem-table`), parsed as `u64`.
    ///
    /// `None` covers both "the family was not opened" and "RocksDB has no
    /// such property or returned a value that does not parse as `u64`" —
    /// deliberately: a broken reading must not be published as a healthy
    /// zero, so the caller's job is to skip the gauge, never to substitute
    /// one.
    pub(crate) fn property_u64(&self, s: Subsystem, name: &str) -> Option<u64> {
        let cf = self.cf(s).ok()?;
        self.db.property_int_value_cf(&cf, name).ok().flatten()
    }

    /// One DB-wide RocksDB property (e.g. `rocksdb.actual-delayed-write-rate`),
    /// parsed as `u64`. Same "no fabricated zero" contract as
    /// [`property_u64`](Self::property_u64).
    pub(crate) fn property_u64_db(&self, name: &str) -> Option<u64> {
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
    #[cfg(test)]
    pub(crate) fn compact(&self, s: Subsystem) -> Result<(), Error> {
        let cf = self.cf(s)?;
        self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        Ok(())
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

    fn specs() -> Vec<CfSpec> {
        vec![
            CfSpec {
                subsystem: Subsystem::ReplicaLog,
                compress: false,
            },
            CfSpec {
                subsystem: Subsystem::StableLog,
                compress: true,
            },
        ]
    }

    /// Every RocksDB instance puts exactly one LOCK file in its own root, so
    /// counting them under the storage root counts instances. No `walkdir` —
    /// it is not a dependency and CLAUDE.md §15 does not want one for this.
    fn count_lock_files(dir: &Path) -> usize {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|e| {
                let path = e.path();
                if path.is_dir() {
                    count_lock_files(&path)
                } else {
                    usize::from(path.file_name().is_some_and(|n| n == "LOCK"))
                }
            })
            .sum()
    }

    /// The bug this whole task exists to fix. `Engine::open` derived the DB
    /// from `dir.parent()`, so a store nested one level deeper opened a
    /// SECOND DB with a SECOND full-size cache.
    #[tokio::test]
    async fn a_node_opens_exactly_one_rocksdb() {
        let tmp = tempfile::tempdir().unwrap();
        let _s = Storage::open(
            tmp.path(),
            StorageConfig::new(ByteSize::from_mib(64)),
            &specs(),
        )
        .unwrap();
        assert_eq!(
            count_lock_files(tmp.path()),
            1,
            "a node must open exactly one RocksDB instance",
        );
    }

    /// The budget was a process-wide `static`, but loadgen runs six nodes in
    /// one process. Two nodes with different budgets must get different
    /// caches — under the static they could not.
    #[tokio::test]
    async fn the_budget_is_per_node_not_per_process() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let small = Storage::open(
            a.path(),
            StorageConfig::new(ByteSize::from_mib(16)),
            &specs(),
        )
        .unwrap();
        let large = Storage::open(
            b.path(),
            StorageConfig::new(ByteSize::from_mib(256)),
            &specs(),
        )
        .unwrap();
        assert!(
            small.cache_capacity() < large.cache_capacity(),
            "two nodes in one process must not share a budget",
        );
    }

    /// `charged_bytes` summed a registry of every DB the process had open.
    /// With one DB per node it reads that node's cache and nothing else.
    #[tokio::test]
    async fn charged_bytes_reads_this_node_only() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let one = Storage::open(
            a.path(),
            StorageConfig::new(ByteSize::from_mib(64)),
            &specs(),
        )
        .unwrap();
        let two = Storage::open(
            b.path(),
            StorageConfig::new(ByteSize::from_mib(64)),
            &specs(),
        )
        .unwrap();
        let before = one.charged_bytes();
        let mut batch = WriteBatch::default();
        let cf = two.cf(Subsystem::ReplicaLog).unwrap();
        for i in 0u64..2_000 {
            batch.put_cf(&cf, i.to_be_bytes(), vec![0u8; 4096]);
        }
        two.write(batch).unwrap();
        two.fence().unwrap();
        let _ = two.cf(Subsystem::ReplicaLog).unwrap();
        assert_eq!(
            before,
            one.charged_bytes(),
            "writing to another node's DB must not move this node's charge",
        );
    }

    /// The memtable share was divided by a fixed PLANNED_COLUMN_FAMILIES = 8
    /// because the live count was unknown at open. It is known now.
    #[test]
    fn write_buffer_is_sized_from_the_real_cf_count() {
        let cfg = StorageConfig::new(ByteSize::from_mib(64));
        let two = cfg.write_buffer_bytes(2);
        let eight = cfg.write_buffer_bytes(8);
        assert!(
            two > eight,
            "fewer families must each get a larger memtable"
        );
    }

    /// The knobs are overrides, not replacements: an unset one must still be
    /// the number the budget derives, and the 20 MiB low-water config — the one
    /// that finds sizing bugs — must stay runnable.
    #[test]
    fn unset_sizing_knobs_derive_from_the_budget() {
        let low = StorageConfig::new(ByteSize::from_mib(20));
        low.validate()
            .expect("the 20 MiB low-water config is valid");

        assert!(
            low.write_buffer_bytes(6) >= MIN_WRITE_BUFFER_BYTES,
            "a derived memtable must never fall below the stall floor",
        );
        assert!(
            low.max_total_wal_bytes() >= memtable_budget(low.memory),
            "retained WAL must cover at least the memtable share it backs",
        );
        assert!(
            low.target_file_size_bytes(6) >= low.write_buffer_bytes(6),
            "an SST target below the memtable that fills it is a split, not a target",
        );
        assert_eq!(low.background_jobs(), BACKGROUND_JOBS);
        assert!((16..=512).contains(&low.open_files()));

        // And every term scales with the budget rather than pinning at a
        // constant — the property "derived" actually means.
        let high = StorageConfig::new(ByteSize::from_mib(256));
        assert!(high.write_buffer_bytes(6) > low.write_buffer_bytes(6));
        assert!(high.max_total_wal_bytes() > low.max_total_wal_bytes());
        assert!(high.open_files() > low.open_files());
    }

    /// An explicit knob wins over the derivation, in both directions.
    #[test]
    fn an_explicit_knob_overrides_the_derived_value() {
        let mut cfg = StorageConfig::new(ByteSize::from_mib(256));
        cfg.write_buffer = Some(ByteSize::from_mib(1));
        cfg.max_total_wal = Some(ByteSize::from_mib(7));
        cfg.target_file_size = Some(ByteSize::from_mib(3));
        cfg.max_background_jobs = Some(5);
        cfg.max_open_files = Some(-1);
        cfg.validate().unwrap();

        assert_eq!(cfg.write_buffer_bytes(6), 1024 * 1024);
        assert_eq!(cfg.max_total_wal_bytes(), 7 * 1024 * 1024);
        assert_eq!(cfg.target_file_size_bytes(6), 3 * 1024 * 1024);
        assert_eq!(cfg.background_jobs(), 5);
        assert_eq!(cfg.open_files(), -1);
    }

    #[test]
    fn retire_sweep_defaults_to_an_hour_and_honours_an_override() {
        let cfg = StorageConfig::new(ByteSize::from_mib(64));
        assert_eq!(cfg.retire_sweep_secs(), RETIRE_SWEEP_DEFAULT.as_secs());
        assert_eq!(cfg.retire_sweep_secs(), 3600);

        let cfg = StorageConfig {
            retire_sweep: Some(std::time::Duration::from_secs(120)),
            ..StorageConfig::new(ByteSize::from_mib(64))
        };
        assert_eq!(cfg.retire_sweep_secs(), 120);
    }

    /// A sub-second override must never truncate to zero: RocksDB reads
    /// `periodic_compaction_seconds == 0` as *disabled*, so an operator asking
    /// for an aggressive short sweep would otherwise silently turn reclamation
    /// off — the exact inverse of what they configured.
    #[test]
    fn a_sub_second_retire_sweep_never_truncates_to_disabled() {
        let cfg = StorageConfig {
            retire_sweep: Some(std::time::Duration::from_millis(500)),
            ..StorageConfig::new(ByteSize::from_mib(64))
        };
        assert_eq!(cfg.retire_sweep_secs(), 1);
    }

    /// Validation names the knob and the constraint, so an operator does not
    /// have to read this file to know what they set wrong.
    #[test]
    fn validation_names_the_knob_it_rejected() {
        let cases: Vec<(StorageConfig, &str)> = vec![
            (
                StorageConfig::new(ByteSize::from_bytes(0)),
                "storage_memory",
            ),
            (
                StorageConfig {
                    write_buffer: Some(ByteSize::from_bytes(1)),
                    ..StorageConfig::new(ByteSize::from_mib(20))
                },
                "storage_write_buffer",
            ),
            (
                StorageConfig {
                    max_total_wal: Some(ByteSize::from_bytes(0)),
                    ..StorageConfig::new(ByteSize::from_mib(20))
                },
                "storage_max_total_wal",
            ),
            (
                StorageConfig {
                    target_file_size: Some(ByteSize::from_bytes(0)),
                    ..StorageConfig::new(ByteSize::from_mib(20))
                },
                "storage_target_file_size",
            ),
            (
                StorageConfig {
                    max_background_jobs: Some(0),
                    ..StorageConfig::new(ByteSize::from_mib(20))
                },
                "storage_max_background_jobs",
            ),
            (
                StorageConfig {
                    max_open_files: Some(0),
                    ..StorageConfig::new(ByteSize::from_mib(20))
                },
                "storage_max_open_files",
            ),
        ];
        for (cfg, knob) in cases {
            let err = cfg.validate().expect_err("must reject");
            assert!(
                format!("{err}").contains(knob),
                "error must name {knob}, got: {err}"
            );
        }
    }

    /// The one live WAL of a closed storage root. RocksDB names it
    /// `<number>.log` and keeps exactly one while nothing has rolled, so the
    /// highest-numbered `.log` is the file the last batch was appended to.
    fn live_wal(root: &Path) -> std::path::PathBuf {
        let mut logs: Vec<std::path::PathBuf> = std::fs::read_dir(root)
            .expect("read storage root")
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "log"))
            .collect();
        logs.sort();
        logs.pop().expect("a live WAL exists after a fenced write")
    }

    fn wal_len(root: &Path) -> u64 {
        std::fs::metadata(live_wal(root))
            .expect("stat the live WAL")
            .len()
    }

    /// Write one batch of `keys` into the replica-log family and fence it, so
    /// it is committed and durable in the WAL before this returns.
    fn put_batch_and_fence(storage: &Storage, keys: &[&[u8]]) {
        let cf = storage.cf(Subsystem::ReplicaLog).unwrap();
        let mut batch = WriteBatch::default();
        for key in keys {
            batch.put_cf(&cf, key, vec![7u8; 128]);
        }
        storage.write(batch).unwrap();
        storage.fence().unwrap();
    }

    fn has_key(storage: &Storage, key: &[u8]) -> bool {
        let cf = storage.cf(Subsystem::ReplicaLog).unwrap();
        storage
            .db()
            .get_pinned_cf(&cf, key)
            .expect("read back")
            .is_some()
    }

    /// Synchronous: no runtime means no background compaction task, so the
    /// last `Arc` really is the last one and dropping it releases the LOCK —
    /// which is what lets these tests reopen the root.
    fn open_test_storage(root: &Path) -> Result<Arc<Storage>, Error> {
        Storage::open(root, StorageConfig::new(ByteSize::from_mib(64)), &specs())
    }

    /// CLAUDE.md §8's documented exception: a torn, un-fsynced WAL tail is
    /// ordinary crash debris and is TRUNCATED, not refused — otherwise a
    /// healthy node that was killed mid-append refuses to come back.
    ///
    /// The garbage below fails the WAL record CRC, which is exactly what a
    /// half-written record looks like; `AbsoluteConsistency` refuses such a
    /// reopen and this test fails under it (mutation-verified).
    #[test]
    fn a_torn_wal_tail_is_truncated_not_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let keys: [&[u8]; 3] = [b"fenced/1", b"fenced/2", b"fenced/3"];
        {
            let storage = open_test_storage(tmp.path()).unwrap();
            put_batch_and_fence(&storage, &keys);
        }

        // A torn tail: a WAL record header — crc(4) | len(2, LE) | type(1) —
        // announcing a 200-byte body of which only 80 bytes made it to disk.
        // That is exactly the shape a never-fsynced, half-written append
        // leaves behind, and it is the shape this recovery mode exists for.
        let mut debris: Vec<u8> = vec![0xA5, 0xA5, 0xA5, 0xA5, 200, 0, 1];
        debris.extend_from_slice(&[0x5Au8; 80]);
        let mut wal = std::fs::OpenOptions::new()
            .append(true)
            .open(live_wal(tmp.path()))
            .unwrap();
        std::io::Write::write_all(&mut wal, &debris).unwrap();
        std::io::Write::flush(&mut wal).unwrap();
        drop(wal);

        let storage = open_test_storage(tmp.path())
            .expect("a torn un-fsynced tail must be truncated, not refused");
        for key in keys {
            assert!(
                has_key(&storage, key),
                "every fenced key must survive a torn tail: {}",
                String::from_utf8_lossy(key),
            );
        }
    }

    /// CLAUDE.md §8: a crash mid-batch discards the batch WHOLE. The
    /// interrupted batch below carries five keys and the assertion is that
    /// NONE of them survives while every key of the earlier fenced batch
    /// does — a partial batch would show up as some of the five.
    ///
    /// The batch boundary is the WAL length *after* fencing the first batch:
    /// a `WriteBatch` is one WAL record, so the second batch occupies exactly
    /// the bytes past that offset, and cutting inside them is a crash inside
    /// the record. Mutation-verified against `AbsoluteConsistency`, which
    /// refuses the reopen.
    #[test]
    fn an_interrupted_batch_is_discarded_whole_on_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let fenced: [&[u8]; 3] = [b"fenced/1", b"fenced/2", b"fenced/3"];
        let torn: [&[u8]; 5] = [b"torn/1", b"torn/2", b"torn/3", b"torn/4", b"torn/5"];

        let (boundary, end) = {
            let storage = open_test_storage(tmp.path()).unwrap();
            put_batch_and_fence(&storage, &fenced);
            let boundary = wal_len(tmp.path());
            put_batch_and_fence(&storage, &torn);
            // Control: the batch really was written and fenced, so its absence
            // after the reopen can only come from the truncation below.
            for key in torn {
                assert!(has_key(&storage, key), "the batch must be written first");
            }
            (boundary, wal_len(tmp.path()))
        };
        assert!(end > boundary, "the second batch must have grown the WAL");

        // Cut half way through the second batch's record. checked_*: the
        // arithmetic is over two file lengths of the same file with
        // `end > boundary` asserted, so neither step can wrap.
        let span = end.checked_sub(boundary).unwrap();
        let cut = boundary.checked_add(span.checked_div(2).unwrap()).unwrap();
        assert!(cut > boundary && cut < end, "the cut must be mid-record");
        std::fs::OpenOptions::new()
            .write(true)
            .open(live_wal(tmp.path()))
            .unwrap()
            .set_len(cut)
            .unwrap();

        let storage = open_test_storage(tmp.path())
            .expect("an interrupted batch is crash debris, not a refusal");
        for key in fenced {
            assert!(
                has_key(&storage, key),
                "the earlier fenced batch must survive whole: {}",
                String::from_utf8_lossy(key),
            );
        }
        for key in torn {
            assert!(
                !has_key(&storage, key),
                "no key of the interrupted batch may survive: {}",
                String::from_utf8_lossy(key),
            );
        }
    }

    /// The other half of CLAUDE.md §8, and what makes the exception above
    /// *narrow*: a WAL record that is present in full and fails its checksum
    /// is genuine corruption of committed state, and the open REFUSES.
    ///
    /// This is the assertion that pins the recovery mode itself — rust-rocksdb
    /// exposes no getter for `wal_recovery_mode`, so behaviour is the only
    /// readback available. Under the self-heal-by-discard modes §8 rejects
    /// (`PointInTimeRecovery`, `SkipAnyCorruptedRecord`) the open succeeds and
    /// this test fails (mutation-verified).
    #[test]
    fn a_checksum_failing_wal_record_refuses_to_open() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let storage = open_test_storage(tmp.path()).unwrap();
            put_batch_and_fence(&storage, &[b"fenced/1"]);
        }

        // A complete record — crc(4) | len(2, LE) | type(1) | 200-byte body —
        // whose checksum is garbage. Nothing is missing; the bytes are wrong.
        let mut corrupt: Vec<u8> = vec![0xA5, 0xA5, 0xA5, 0xA5, 200, 0, 1];
        corrupt.extend_from_slice(&[0x5Au8; 200]);
        let mut wal = std::fs::OpenOptions::new()
            .append(true)
            .open(live_wal(tmp.path()))
            .unwrap();
        std::io::Write::write_all(&mut wal, &corrupt).unwrap();
        std::io::Write::flush(&mut wal).unwrap();
        drop(wal);

        let err = open_test_storage(tmp.path())
            .err()
            .expect("a checksum-failing WAL record must refuse to open");
        // The refusal is the invariant (CLAUDE.md §8): a *complete* record
        // failing its CRC must fail open, not self-heal by discard. The exact
        // wording comes from rust-rocksdb and is not part of the contract, so
        // it is not asserted. Mutation-verified: under
        // `PointInTimeRecovery` / `SkipAnyCorruptedRecord` the open returns
        // `Ok` and the `expect` above trips.
        let _ = err;
    }
}
