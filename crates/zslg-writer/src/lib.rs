//! The writer: client writes fanned out to the replica set and acknowledged at
//! quorum.

pub mod metrics;

mod coordinator;
mod writer;

pub use writer::{WriterConfig, WriterService};

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
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

error_union::error_bridge!(Error => zslg_log::Error, zslg_raft::Error; Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus);
