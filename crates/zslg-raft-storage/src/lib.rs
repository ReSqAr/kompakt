//! The raft log and its meta singletons in RocksDB: openraft's storage traits
//! implemented over two column families of the node's one database.

use error_union::error_union;
use zslg_consensus::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log::LogError;
use zslg_net::NetError;
use zslg_snapshot::SnapshotError;
use zslg_time::ClockError;

pub mod metrics;
mod store;

pub use store::{RAFT_LOG_CF, RAFT_META_CF, RocksRaftStorage, raft_cf_specs};
#[cfg(feature = "testing")]
pub use store::{build_raft_for_test, seed_foreign_membership_for_test};

error_union! {
    /// What opening and reading the store can fail with.
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
