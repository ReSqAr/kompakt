//! The learner's stable prefix: the [`StableLog`] storage plus the
//! `snapshot_loader` that populates it from object-store snapshots.
//!
//! Storage is a RocksDB column family: primary = the gapless global
//! `LogIndex`, secondary = the entry's creation HLC. With TTL purge disabled
//! (the default) no retire threshold is ever armed, so the log never retires (it
//! is the CQRS replay source); with purge on, an HLC-keyed threshold retires an
//! old index prefix, served just-in-time from object store on read
//! (`crate::learner::stable_archive`). The watermark (`end_hlc`) is purely
//! in-memory: recovered from the log's own tail at open (just below the
//! frontier entry's HLC) and re-raised by the first manifest event after
//! startup, so it can never claim more coverage than the entries this node
//! actually holds.

use error_union::error_union;
use zslg_consensus::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log::LogError;
use zslg_net::NetError;
use zslg_snapshot::SnapshotError;
use zslg_time::ClockError;

use rocksdb_kit::ColumnFamily;

mod appender;
mod config;
mod engine;
mod key;
mod log;
pub mod metrics;
mod purge;
mod snapshot_loader;
#[cfg(test)]
mod testing;
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests;

pub use config::StableLogConfig;
pub use log::{StableLog, spawn_stable_store};

/// The settled prefix of the log, keyed by global `LogIndex`.
pub const STABLE_LOG_CF: ColumnFamily = ColumnFamily::new("stable_log");

// Consumed only by `spawn_learner`.
pub use purge::run_stable_purge_worker;
pub use snapshot_loader::{SnapshotLoader, integrate_manifest};
// Re-exported for leader-side snapshot round-trip tests; production callers live
// inside `snapshot_loader` and call `integrate_manifest` directly through the
// shared `StableArchiveReader`.

error_union! {
    /// What the stable log can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(LifecycleError),
        Config(ConfigError),
        Clock(ClockError),
        Log(LogError),
        Snapshot(SnapshotError),
        Net(NetError),
        Consensus(ConsensusError),
    }
    from zslg_time::Error { Clock }
    bridge zslg_log::Error
    bridge zslg_snapshot::Error
    bridge zslg_consensus::Error
    bridge zslg_raft::Error
    bridge zslg_net::Error
    bridge zslg_quorum_read::Error
    bridge zslg_error::Error
}

impl From<rocksdb_kit::StorageError> for Error {
    fn from(e: rocksdb_kit::StorageError) -> Self {
        match e {
            rocksdb_kit::StorageError::InvalidConfiguration(msg) => {
                Self::Config(ConfigError::InvalidConfiguration(msg))
            }
            rocksdb_kit::StorageError::Storage(msg) => Self::Log(LogError::Storage(msg)),
        }
    }
}
