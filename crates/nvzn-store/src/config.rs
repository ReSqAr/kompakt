use nvzn_types::Error;
use std::path::PathBuf;
use std::time::Duration;
use zslg::ByteSize;

const DEFAULT_MEMORY: ByteSize = ByteSize::from_mib(256);
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// What a fold commit stages, at most, unless configured.
pub const DEFAULT_FOLD_COMMIT: ByteSize = ByteSize::from_mib(4);

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

    // Engine tuning. Unset means derived from `memory`; the derivation (and its
    // constants) belongs to `rocksdb-kit::StorageConfig`, which these map onto
    // field for field when the store opens.
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

    #[must_use]
    pub fn fold_commit_bytes(&self) -> usize {
        usize::try_from(self.fold_commit.as_u64()).unwrap_or(usize::MAX)
    }

    pub fn validate(&self) -> Result<(), Error> {
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
}
