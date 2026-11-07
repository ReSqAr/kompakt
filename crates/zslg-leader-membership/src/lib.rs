//! The leader's membership jobs: the liveness prober, the replica-set change
//! state machine (`leader:membership`) and Raft voter eviction (`leader:voters`).

pub mod metrics;

mod change;
mod config;
mod job;
mod liveness;
mod voters;

pub use change::{LivenessQuery, MembershipChangeInputs, MembershipChangeManager, needs_retarget};
pub use config::MembershipConfig;
pub use job::{
    MEMBERSHIP_STEP_FLOOR, MembershipDriver, MembershipHandles, MembershipJob, MembershipObserver,
    NetworkMembershipObserver, TickOutcome, VoterHandles, spawn_membership, spawn_voters,
};
pub use liveness::{LivenessConfig, LivenessState};

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
    from zslg_time::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_quorum_read::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

error_union::error_bridge!(Error => zslg_time::Error, zslg_log::Error, zslg_snapshot::Error, zslg_consensus::Error, zslg_raft::Error, zslg_net::Error, zslg_quorum_read::Error; Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus);
error_union::error_home!(Error);

impl From<shutdown::Stopped> for Error {
    fn from(_: shutdown::Stopped) -> Self {
        Self::Lifecycle(zslg_lifecycle_types::LifecycleError::Shutdown)
    }
}
