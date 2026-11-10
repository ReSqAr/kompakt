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
    /// What the ports of this crate can fail with. All seven leaves: the
    /// implementations of these ports raise every one of them (a transport
    /// fails with `Net`, the object store with `Log`, a shut-down node with
    /// `Lifecycle`, ...), and a port's error type must hold what any of them
    /// returns. Narrowing needs per-method error types; see
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

pub use zslg_raft_types::{
    FencedStateWrite, NodeInfo, Raft, RaftResponse, TypeConfig, membership_map,
};
