//! The startup decision procedure: one gate on durable facts, then one phase
//! per outcome — bootstrap the cluster, join it, or rejoin it — talking to the
//! engine only through `ClusterBootstrap`.

use error_union::error_union;
use zslg_consensus::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log::LogError;
use zslg_net::NetError;
use zslg_snapshot::SnapshotError;
use zslg_time::ClockError;

mod bootstrap;
pub mod metrics;
mod rejoin;

pub use bootstrap::{RaftStartupConfig, run_raft_startup};

error_union! {
    /// What startup can fail with.
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
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_quorum_read::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    into zslg_consensus::Error
    into zslg_log::Error
    into zslg_snapshot::Error
    into zslg_raft::Error
    into zslg_net::Error
    into zslg_quorum_read::Error
    into zslg_error::Error
}

impl From<shutdown::Stopped> for Error {
    fn from(_: shutdown::Stopped) -> Self {
        Self::Lifecycle(LifecycleError::Shutdown)
    }
}
