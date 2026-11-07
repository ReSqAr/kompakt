//! The raft group's ports: the server and client sides of the Raft RPCs.
//! Re-exports the group's types.

use error_union::error_union;
use zslg_consensus_types::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log_types::LogError;
use zslg_net_types::NetError;
use zslg_snapshot_types::SnapshotError;
use zslg_time_types::ClockError;

mod cluster_bootstrap;
mod raft_consensus;
mod raft_metrics;
mod raft_transport;

pub use cluster_bootstrap::ClusterBootstrap;
pub use raft_consensus::RaftConsensus;
pub use raft_metrics::RaftMetricsSource;
pub use raft_transport::RaftTransport;

error_union! {
    /// What the ports of this crate can fail with. Holds every leaf error for
    /// now; narrowed to the leaves its implementations actually raise once they
    /// have moved out of `zslg`.
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

pub use zslg_raft_types::{
    FencedStateWrite, NodeInfo, Raft, RaftResponse, TypeConfig, membership_map,
};
