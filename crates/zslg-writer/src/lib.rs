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
    bridge zslg_log::Error
    bridge zslg_raft::Error
    into zslg_error::Error
}
