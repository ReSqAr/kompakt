//! The time group's ports: the clock every stamp and validation goes through.
//! Re-exports the group's types, so callers import `Hlc` from here.

use error_union::error_union;
use zslg_consensus_types::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log_types::LogError;
use zslg_net_types::NetError;
use zslg_snapshot_types::SnapshotError;

mod clock;
mod write_gate;

pub use clock::Clock;
pub use write_gate::WriteGate;
pub use zslg_time_types::{
    Admission, ClockError, CutoffError, HLC_MAX_PHYSICAL_MS, HasHlc, Hlc, HlcParts,
    duration_as_ms_clamped, hlc_subtract,
};

error_union! {
    /// What the ports of this crate can fail with. Holds all seven leaf errors,
    /// including ones no implementation raises (a clock cannot fail with a log
    /// error); narrowing to the leaves actually raised is open work, see
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
