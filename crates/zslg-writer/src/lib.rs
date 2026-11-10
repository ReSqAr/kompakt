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
        Log(zslg_log::LogError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    into zslg_log::Error
    into zslg_raft::Error
    into zslg_error::Error
}
