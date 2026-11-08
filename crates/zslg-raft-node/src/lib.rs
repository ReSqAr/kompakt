//! The raft node: a thin owned wrapper around the consensus engine implementing
//! the raft and consensus ports — RPC passthroughs, leadership queries, fenced
//! writes and the cluster-formation verbs startup needs.

use error_union::error_union;
use zslg_consensus::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log::LogError;
use zslg_net::NetError;
use zslg_snapshot::SnapshotError;
use zslg_time::ClockError;

pub mod metrics;
mod node;

pub use node::RaftNode;

error_union! {
    /// What the node's own operations can fail with.
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
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    into zslg_consensus::Error
    into zslg_raft::Error
    into zslg_error::Error
}
