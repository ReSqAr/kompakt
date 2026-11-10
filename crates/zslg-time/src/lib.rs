//! The time group's ports: the clock every stamp and validation goes through.
//! Re-exports the group's types, so callers import `Hlc` from here.

use error_union::error_union;

mod clock;
mod write_gate;

pub use clock::Clock;
pub use write_gate::WriteGate;
pub use zslg_time_types::{
    Admission, ClockError, CutoffError, HLC_MAX_PHYSICAL_MS, HasHlc, Hlc, HlcParts,
    duration_as_ms_clamped, hlc_subtract,
};

error_union! {
    /// What a clock can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Clock(ClockError),
    }
    into zslg_error::Error
}
