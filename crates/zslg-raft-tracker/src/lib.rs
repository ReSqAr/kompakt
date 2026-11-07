//! Tracks the cluster's `RaftState` as seen from outside the consensus group:
//! a tracker folds observations from peers, a streamer polls them.

use error_union::error_union;
use zslg_consensus::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log::LogError;
use zslg_net::NetError;
use zslg_snapshot::SnapshotError;
use zslg_time::ClockError;

mod noop;
mod raft_tracker;
mod streamer;

pub use noop::NoOpRaftViewer;
pub use raft_tracker::{RaftTracker, RaftTrackerConfig};
pub use streamer::spawn_raft_tracker_streamer;

error_union! {
    /// What waiting on the tracker can fail with.
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

impl From<shutdown::Stopped> for Error {
    fn from(_: shutdown::Stopped) -> Self {
        Self::Lifecycle(LifecycleError::Shutdown)
    }
}
