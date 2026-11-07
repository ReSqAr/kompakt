//! The hybrid logical clock: monotonic stamps, and absorption of peer clocks
//! within a skew limit. Implements `Clock`.
//!
//! ```text
//!    63                                  16 15             0
//!   ┌──────────────────────────────────────┬────────────────┐
//!   │         physical_ms (48 bits)        │  logical (16)  │
//!   └──────────────────────────────────────┴────────────────┘
//! ```
//!
//! Packing it this way makes the `u64` compare exactly as `(physical, logical)`
//! compares lexicographically, which is why `Ord` is derived rather than
//! written: every ordering in the system — snapshot windows, cutoffs, log
//! merges — is an integer comparison. 48 bits of milliseconds runs to the year
//! 10889; the 16-bit counter is what separates events inside one millisecond.
//!
//! **Comparing raws is meaningful; subtracting them is not.** A difference of
//! raw `u64`s is dominated by the logical bits — one millisecond reads as
//! 65 536 — so anything measuring a *duration* (clock skew, drift, a tier
//! window) must go through `Hlc::parts` and subtract `physical_ms`.

mod clock;
pub mod metrics;

pub use clock::{HlcClock, run_clock_gauges};

/// The one error this implementation raises: its port's.
pub use zslg_time::Error;
