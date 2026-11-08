//! `leader:compaction` — merges old snapshot objects into fewer, larger ones.

pub mod metrics;

mod job;
mod merge;

pub use job::{CompactionConfig, CompactionHandles, spawn};
pub use merge::{CompactionOutcome, compact};

error_union::error_union! {
    /// What this crate can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(zslg_lifecycle_types::LifecycleError),
        Config(zslg_lifecycle_types::ConfigError),
        Clock(zslg_time::ClockError),
        Log(zslg_log::LogError),
        Snapshot(zslg_snapshot::SnapshotError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

error_union::error_bridge!(Error => zslg_snapshot::Error, zslg_consensus::Error, zslg_raft::Error;  Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus);
error_union::error_home!(Error);

impl From<shutdown::Stopped> for Error {
    fn from(_: shutdown::Stopped) -> Self {
        Self::Lifecycle(zslg_lifecycle_types::LifecycleError::Shutdown)
    }
}
