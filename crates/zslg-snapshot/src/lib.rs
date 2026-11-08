//! The snapshot group's ports: the object-store-backed snapshot seam. Re-exports
//! the group's types.

use error_union::error_union;
use zslg_consensus_types::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log_types::LogError;
use zslg_net_types::NetError;
use zslg_time_types::ClockError;

mod snapshot_store;

pub use snapshot_store::{CleanupReport, EntryMeta, SnapshotStore, SnapshotWrite};
pub use zslg_snapshot_types::{
    CompactionTier, CompressionLevel, ObjectStoreReason, SnapshotError, SnapshotId,
    SnapshotMetadata,
};

error_union! {
    /// What the ports of this crate can fail with. Holds all seven leaf errors,
    /// including ones no implementation raises (a clock cannot fail with a log
    /// error); narrowing to the leaves actually raised is open work, see
    /// `docs/logs/2026-11-07_post_crate_split.md`.
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
    bridge zslg_error::Error
}
