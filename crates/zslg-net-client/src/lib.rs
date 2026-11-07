//! The RPC client: the unary connection pool, the write and subscribe streams,
//! the per-peer `ReplicaClient` over them, and the `Router` that hands those
//! out by `NodeId`.

use error_union::{error_bridge, error_union};
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
    from zslg_time::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net_wire::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

error_bridge!(Error => zslg_time::Error, zslg_log::Error, zslg_snapshot::Error, zslg_consensus::Error, zslg_raft::Error, zslg_net::Error, zslg_net_wire::Error; Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus);
