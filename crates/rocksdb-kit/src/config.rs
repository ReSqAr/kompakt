//! The node's storage tuning: the memory budget, plus the few RocksDB terms
//! worth overriding on a node that has been measured. See [`StorageConfig`].

use bytesize::ByteSize;

use crate::error::StorageError;

/// Background compaction + flush threads. Small machine: 2.
///
/// Derived constant, overridable per node via
/// [`StorageConfig::max_background_jobs`]: it is the documented remediation
/// when RSS overshoots the budget (compaction input/output buffers scale with
/// it and sit outside the cache).
pub(crate) const BACKGROUND_JOBS: i32 = 2;

/// Memtables per column family. RocksDB needs at least two: one flushing
/// while the other takes writes.
///
/// **Derived, not a knob.** Two is RocksDB's structural minimum, and every
/// memtable term below divides the budget share by it, so raising it shrinks
/// each memtable rather than buying anything. No measurement has wanted a
/// third, so it stays a constant.
pub(crate) const WRITE_BUFFERS_PER_CF: u64 = 2;

/// Floor on a single memtable. Below roughly this, RocksDB flushes so often
/// that compaction cannot keep up and the write path stalls on L0 instead of
/// on memory — smaller stops buying memory and starts buying stalls.
pub(crate) const MIN_WRITE_BUFFER_BYTES: u64 = 64 * 1024;

/// Share of the budget memtables may take before flushes are forced.
const MEMTABLE_SHARE_PERCENT: u64 = 40;

/// How often RocksDB rewrites aged SSTs so retirement is actually reclaimed —
/// that is, so the range tombstones purge writes are dropped and the space
/// comes back. RocksDB's own default here is 30 days; this only shortens it.
/// Pure space-versus-write-amplification — not a correctness value, because
/// purge is durable the moment its batch is fenced.
pub(crate) const RETIRE_SWEEP_DEFAULT: std::time::Duration =
    std::time::Duration::from_secs(60 * 60);

pub(crate) fn memtable_budget(budget: ByteSize) -> u64 {
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
pub struct StorageConfig {
    /// Total memory the engine may hold — block cache, index and filter
    /// blocks, and the memtables charged against it. The one number to move.
    pub memory: ByteSize,
    /// One memtable's size. Overridable because it is the term a measurement
    /// actually caught: RocksDB's 64 MiB default was applied regardless of the
    /// budget and a 64 MiB node was over its whole budget on one memtable.
    pub write_buffer: Option<ByteSize>,
    /// SST target size. Derived at 2x the memtable — a heuristic shape, not a
    /// measured quantity, which is exactly why it has an override.
    pub target_file_size: Option<ByteSize>,
    /// Cap on retained WAL. Measured at 92 WAL files / 9.7 GB in five seconds
    /// when it was unset.
    pub max_total_wal: Option<ByteSize>,
    /// Background compaction + flush threads. Compaction buffers scale with it
    /// and sit outside the budget, so lowering it is the RSS-overshoot lever.
    pub max_background_jobs: Option<i32>,
    /// Open SST files. Each holds a table reader outside the block cache — the
    /// other term that escapes the budget as the dataset grows.
    pub max_open_files: Option<i32>,
    /// Override for [`RETIRE_SWEEP_DEFAULT`]. See that constant.
    pub retire_sweep: Option<std::time::Duration>,
}

impl StorageConfig {
    /// A node's storage tuning with every override unset: the budget alone,
    /// everything else derived from it.
    ///
    /// Test-only: production always comes from a node config, which has a field
    /// per override and so builds the struct literally. Nothing outside a test
    /// wants "the budget and nothing else".
    #[must_use]
    pub const fn new(memory: ByteSize) -> Self {
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
    pub(crate) fn write_buffer_bytes(self, cf_count: usize) -> u64 {
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
    pub(crate) fn target_file_size_bytes(self, cf_count: usize) -> u64 {
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
    pub(crate) fn max_total_wal_bytes(self) -> u64 {
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
    pub(crate) fn background_jobs(self) -> i32 {
        self.max_background_jobs.unwrap_or(BACKGROUND_JOBS)
    }

    /// Open SST files, scaled off the budget when unset.
    pub(crate) fn open_files(self) -> i32 {
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
    pub(crate) fn retire_sweep_secs(self) -> u64 {
        self.retire_sweep
            .unwrap_or(RETIRE_SWEEP_DEFAULT)
            .as_secs()
            .max(1)
    }

    /// Reject a tuning that cannot open, naming the knob and the constraint.
    ///
    /// Only the *explicit* values are checked: a derived one is produced by the
    /// accessors above, which are total.
    pub fn validate(self) -> Result<(), StorageError> {
        if self.memory.as_u64() == 0 {
            return Err(StorageError::InvalidConfiguration(
                "storage_memory must be > 0: it is the whole engine's budget".to_owned(),
            ));
        }
        if let Some(wb) = self.write_buffer
            && wb.as_u64() < MIN_WRITE_BUFFER_BYTES
        {
            return Err(StorageError::InvalidConfiguration(format!(
                "storage_write_buffer ({} bytes) must be >= {MIN_WRITE_BUFFER_BYTES} bytes: \
                 below that RocksDB flushes faster than compaction can keep up and the \
                 write path stalls on L0 instead of on memory",
                wb.as_u64(),
            )));
        }
        if let Some(tf) = self.target_file_size
            && tf.as_u64() == 0
        {
            return Err(StorageError::InvalidConfiguration(
                "storage_target_file_size must be > 0".to_owned(),
            ));
        }
        if let Some(wal) = self.max_total_wal
            && wal.as_u64() == 0
        {
            return Err(StorageError::InvalidConfiguration(
                "storage_max_total_wal must be > 0: zero would force a flush on every write"
                    .to_owned(),
            ));
        }
        if let Some(jobs) = self.max_background_jobs
            && jobs < 1
        {
            return Err(StorageError::InvalidConfiguration(format!(
                "storage_max_background_jobs ({jobs}) must be >= 1: with no background \
                 thread nothing flushes or compacts",
            )));
        }
        if let Some(files) = self.max_open_files
            && files == 0
        {
            return Err(StorageError::InvalidConfiguration(
                "storage_max_open_files must be non-zero (-1 means unlimited)".to_owned(),
            ));
        }
        if let Some(files) = self.max_open_files
            && files < -1
        {
            return Err(StorageError::InvalidConfiguration(format!(
                "storage_max_open_files ({files}) must be -1 (unlimited) or positive",
            )));
        }
        Ok(())
    }
}
