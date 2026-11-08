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
    bridge zslg_consensus::Error
    bridge zslg_raft::Error
    bridge zslg_net::Error
    bridge zslg_error::Error
}

impl From<shutdown::Stopped> for Error {
    fn from(_: shutdown::Stopped) -> Self {
        Self::Lifecycle(zslg_lifecycle_types::LifecycleError::Shutdown)
    }
}
