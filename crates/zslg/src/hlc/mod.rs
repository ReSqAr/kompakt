//! Hybrid logical clocks — the crate's one notion of time and order.
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
//! window) must go through [`Hlc::parts`] and subtract `physical_ms`.
//!
//! Three responsibilities, one per file:
//!
//! | [`HlcClock`] | mints | monotonic stamps, absorbs peer clocks within a skew limit |
//! | [`HlcGate`] | admits | rejects writes at or below the cutoff, drains before advancing it |
//! | [`CutoffApplier`] | applies | drives the gate from committed Raft state |

mod clock;
mod cutoff;
mod gate;
#[cfg(test)]
mod gate_concurrency_tests;

pub use clock::HlcClock;
pub(crate) use clock::run_clock_gauges;
pub(crate) use cutoff::CutoffApplier;
pub(crate) use gate::{HlcGate, HlcGuard};
