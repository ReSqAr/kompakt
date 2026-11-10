//! Write admission against the HLC cutoff.
//!
//! | [`HlcGate`] | admits | rejects writes at or below the cutoff, drains before advancing it |
//! | [`CutoffApplier`] | applies | drives the gate from committed Raft state |

mod cutoff;
mod gate;
#[cfg(test)]
mod gate_concurrency_tests;
pub mod metrics;

pub use cutoff::CutoffApplier;
pub use gate::{HlcGate, HlcGuard};

/// The one error this crate raises: the time port's.
pub use zslg_time::Error;
