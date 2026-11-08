//! The RPC client: the unary connection pool, the write and subscribe streams,
//! the per-peer `ReplicaClient` over them, and the `Router` that hands those
//! out by `NodeId`.

use error_union::error_union;
use zslg_consensus::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log::LogError;
use zslg_net::NetError;
use zslg_snapshot::SnapshotError;
use zslg_time::ClockError;

mod client;
pub mod metrics;
mod remote;
mod transport;

pub use client::{RpcClient, RpcClientConfig};
pub use remote::RemoteRouter;
pub use transport::TcpPeerClient;

error_union! {
    /// What this crate can fail with.
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
    bridge zslg_time::Error
    bridge zslg_log::Error
    bridge zslg_snapshot::Error
    bridge zslg_consensus::Error
    bridge zslg_raft::Error
    bridge zslg_net::Error
    bridge zslg_net_wire::Error
    into zslg_error::Error
}
