use crate::Error;
use std::path::PathBuf;
use std::time::Duration;
use zslg::ByteSize;

const BACKGROUND_JOBS: i32 = 2;
const WRITE_BUFFERS_PER_CF: u64 = 2;
const MIN_WRITE_BUFFER_BYTES: u64 = 64 * 1024;
const MEMTABLE_SHARE_PERCENT: u64 = 40;
const RETIRE_SWEEP_DEFAULT: Duration = Duration::from_secs(60 * 60);

const DEFAULT_MEMORY: ByteSize = ByteSize::from_mib(256);
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// What a fold commit stages, at most, unless configured.
pub(crate) const DEFAULT_FOLD_COMMIT: ByteSize = ByteSize::from_mib(4);

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub path: PathBuf,
    /// Total memory the engine may hold: block cache, index and filter blocks,
    /// and the memtables charged against it. The one number to move.
    pub memory: ByteSize,
    /// How often the store is fsynced.
    ///
    /// A commit is atomic but not durable — projections are derived state and
    /// the log is their source of truth, so a lost tail costs replay, not
    /// correctness. This bounds that replay. Nothing else fsyncs.
    pub flush_interval: Duration,
    /// What one fold commit holds in memory, at most: the fold commits once
    /// its transaction has staged this much (rows and index entries,
    /// estimated), at the end of the entry that crosses it. Larger means fewer
    /// commits through a burst and a higher peak while one is open.
    pub fold_commit: ByteSize,
    /// Whether the `store` and `index` families' runs are zstd-compressed.
    /// On by default, like zslg's stable log: projections are read-mostly
    /// state, so compression is a footprint/CPU trade rather than a
    /// durability one. The single-row families are never compressed.
    pub compression: bool,

    pub write_buffer: Option<ByteSize>,
    pub target_file_size: Option<ByteSize>,
    pub max_total_wal: Option<ByteSize>,
    pub max_background_jobs: Option<i32>,
    pub max_open_files: Option<i32>,
    pub retire_sweep: Option<Duration>,
}

impl StoreConfig {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            memory: DEFAULT_MEMORY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            fold_commit: DEFAULT_FOLD_COMMIT,
            compression: true,
            write_buffer: None,
            target_file_size: None,
            max_total_wal: None,
            max_background_jobs: None,
            max_open_files: None,
            retire_sweep: None,
        }
    }

    pub(crate) fn memtable_budget(&self) -> u64 {
        // saturating: a budget large enough to overflow this is not a budget.
        self.memory
            .as_u64()
            .saturating_mul(MEMTABLE_SHARE_PERCENT)
            .checked_div(100)
            .unwrap_or(0)
    }

    pub(crate) fn write_buffer_bytes(&self, cf_count: usize) -> u64 {
        if let Some(explicit) = self.write_buffer {
            return explicit.as_u64().max(MIN_WRITE_BUFFER_BYTES);
        }
        let families = u64::try_from(cf_count).unwrap_or(1).max(1);
        self.memtable_budget()
            .checked_div(families.saturating_mul(WRITE_BUFFERS_PER_CF))
            .unwrap_or(MIN_WRITE_BUFFER_BYTES)
            .max(MIN_WRITE_BUFFER_BYTES)
    }

    pub(crate) fn target_file_size_bytes(&self, cf_count: usize) -> u64 {
        match self.target_file_size {
            Some(explicit) => explicit.as_u64().max(1),
            None => self.write_buffer_bytes(cf_count).saturating_mul(2),
        }
    }

    pub(crate) fn max_total_wal_bytes(&self) -> u64 {
        match self.max_total_wal {
            Some(explicit) => explicit.as_u64(),
            None => self.memtable_budget().saturating_mul(2).max(1024 * 1024),
        }
    }

    pub(crate) fn background_jobs(&self) -> i32 {
        self.max_background_jobs.unwrap_or(BACKGROUND_JOBS)
    }

    pub(crate) fn open_files(&self) -> i32 {
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

    pub(crate) fn retire_sweep_secs(&self) -> u64 {
        self.retire_sweep
            .unwrap_or(RETIRE_SWEEP_DEFAULT)
            .as_secs()
            .max(1)
    }

    pub(crate) fn fold_commit_bytes(&self) -> usize {
        usize::try_from(self.fold_commit.as_u64()).unwrap_or(usize::MAX)
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.memory.as_u64() == 0 {
            return Err(Error::InvalidConfiguration("memory must be > 0".to_owned()));
        }
        if self.fold_commit.as_u64() == 0 {
            return Err(Error::InvalidConfiguration(
                "fold_commit must be > 0: every entry would commit alone".to_owned(),
            ));
        }
        if self.flush_interval.is_zero() {
            return Err(Error::InvalidConfiguration(
                "flush_interval must be > 0: a zero interval is a busy loop, not a fence"
                    .to_owned(),
            ));
        }
        if let Some(b) = self.write_buffer
            && b.as_u64() == 0
        {
            return Err(Error::InvalidConfiguration(
                "write_buffer must be > 0".to_owned(),
            ));
        }
        if let Some(b) = self.target_file_size
            && b.as_u64() == 0
        {
            return Err(Error::InvalidConfiguration(
                "target_file_size must be > 0".to_owned(),
            ));
        }
        if let Some(b) = self.max_total_wal
            && b.as_u64() == 0
        {
            return Err(Error::InvalidConfiguration(
                "max_total_wal must be > 0: zero would force a flush on every write".to_owned(),
            ));
        }
        if let Some(jobs) = self.max_background_jobs
            && jobs < 1
        {
            return Err(Error::InvalidConfiguration(format!(
                "max_background_jobs ({jobs}) must be >= 1: with no background thread \
                 nothing flushes or compacts",
            )));
        }
        if let Some(files) = self.max_open_files
            && (files == 0 || files < -1)
        {
            return Err(Error::InvalidConfiguration(format!(
                "max_open_files ({files}) must be -1 (unlimited) or positive",
            )));
        }
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

    type Break = Box<dyn Fn(&mut StoreConfig)>;

    fn config() -> StoreConfig {
        StoreConfig::new("/tmp/nvzn-config-test")
    }

    #[test]
    fn validate_rejects_unusable_tunings() {
        let cases: Vec<(&str, Break)> = vec![
            (
                "memory",
                Box::new(|c: &mut StoreConfig| c.memory = ByteSize::from_bytes(0)),
            ),
            (
                "flush_interval",
                Box::new(|c: &mut StoreConfig| c.flush_interval = Duration::ZERO),
            ),
            (
                "fold_commit",
                Box::new(|c: &mut StoreConfig| c.fold_commit = ByteSize::from_bytes(0)),
            ),
            (
                "write_buffer",
                Box::new(|c: &mut StoreConfig| c.write_buffer = Some(ByteSize::from_bytes(0))),
            ),
            (
                "target_file_size",
                Box::new(|c: &mut StoreConfig| c.target_file_size = Some(ByteSize::from_bytes(0))),
            ),
            (
                "max_total_wal",
                Box::new(|c: &mut StoreConfig| c.max_total_wal = Some(ByteSize::from_bytes(0))),
            ),
            (
                "max_background_jobs",
                Box::new(|c: &mut StoreConfig| c.max_background_jobs = Some(0)),
            ),
            (
                "max_open_files",
                Box::new(|c: &mut StoreConfig| c.max_open_files = Some(0)),
            ),
            (
                "max_open_files",
                Box::new(|c: &mut StoreConfig| c.max_open_files = Some(-2)),
            ),
        ];

        config().validate().expect("the default tuning must open");
        assert!(config().compression, "compression is on by default");

        for (knob, break_it) in cases {
            let mut c = config();
            break_it(&mut c);
            let err = c.validate().expect_err("must be rejected");
            let Error::InvalidConfiguration(msg) = &err else {
                panic!("unexpected error for {knob}: {err:?}");
            };
            assert!(msg.contains(knob), "{knob} not named in {msg:?}");
        }

        // -1 is "unlimited", not a mistake.
        let mut c = config();
        c.max_open_files = Some(-1);
        c.validate().unwrap();
    }

    #[test]
    fn derivations_scale_off_memory_and_yield_to_overrides() {
        let mut c = config();
        c.memory = ByteSize::from_mib(256);
        let cfs = 4;

        let small = c.write_buffer_bytes(cfs);
        c.memory = ByteSize::from_mib(1024);
        assert!(
            c.write_buffer_bytes(cfs) > small,
            "the memtable share must follow the budget"
        );
        assert_eq!(c.target_file_size_bytes(cfs), c.write_buffer_bytes(cfs) * 2);

        c.write_buffer = Some(ByteSize::from_mib(7));
        assert_eq!(c.write_buffer_bytes(cfs), ByteSize::from_mib(7).as_u64());

        // A sub-second sweep must not truncate to "disabled".
        c.retire_sweep = Some(Duration::from_millis(500));
        assert_eq!(c.retire_sweep_secs(), 1);
    }
}
