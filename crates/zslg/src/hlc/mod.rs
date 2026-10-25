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
pub(crate) use gate::{CutoffError, HlcGate, HlcGuard};

use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hlc(pub(crate) u64);

const HLC_LOGICAL_BITS: u32 = 16;
const HLC_LOGICAL_MASK: u64 = 0xFFFF;
const HLC_PHYSICAL_MASK: u64 = !HLC_LOGICAL_MASK;
const HLC_MAX_PHYSICAL_MS: u64 = 0x0000_FFFF_FFFF_FFFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub(crate) struct HlcParts {
    pub(crate) physical_ms: u64,
    pub(crate) logical: u16,
}

impl Hlc {
    pub(crate) const ZERO: Self = Self(0);

    #[must_use]
    pub(crate) fn raw(self) -> u64 {
        self.0
    }

    /// The physical-millisecond part (the top 48 bits).
    #[must_use]
    pub fn physical_ms(self) -> u64 {
        self.parts().physical_ms
    }

    #[must_use]
    #[allow(
        clippy::expect_used,
        reason = "logical mask is constrained to 16 bits by HLC_LOGICAL_MASK"
    )]
    pub(crate) fn parts(self) -> HlcParts {
        let physical_ms = (self.0 & HLC_PHYSICAL_MASK) >> HLC_LOGICAL_BITS;
        let logical_u64 = self.0 & HLC_LOGICAL_MASK;
        let logical = u16::try_from(logical_u64).expect("logical mask <= u16::MAX");
        HlcParts {
            physical_ms,
            logical,
        }
    }

    #[must_use]
    pub(crate) fn from_parts(parts: HlcParts) -> Self {
        debug_assert!(
            parts.physical_ms <= HLC_MAX_PHYSICAL_MS,
            "physical_ms {} exceeds 48-bit HLC range",
            parts.physical_ms
        );
        let physical_part = (parts.physical_ms & HLC_MAX_PHYSICAL_MS) << HLC_LOGICAL_BITS;
        let logical_part = u64::from(parts.logical);
        Self(physical_part | logical_part)
    }

    #[must_use]
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    #[must_use]
    pub fn into_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

/// `None` if `d` reaches below zero. The logical counter is dropped, not
/// carried: the result is an instant, and comparing it against write HLCs must
/// not depend on some node's event count within a millisecond.
pub(crate) fn hlc_subtract(t: Hlc, d: Duration) -> Option<Hlc> {
    let HlcParts { physical_ms, .. } = t.parts();
    let physical_ms = physical_ms.checked_sub(duration_as_ms_clamped(d))?;
    Some(Hlc::from_parts(HlcParts {
        physical_ms,
        logical: 0,
    }))
}

pub(crate) fn duration_as_ms_clamped(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use super::*;

    // ── Hlc type ────────────────────────────────────────────────────────

    #[test]
    fn hlc_zero_is_zero() {
        assert_eq!(Hlc::ZERO, Hlc(0));
        assert_eq!(Hlc::ZERO.raw(), 0);
    }

    #[test]
    fn hlc_raw_roundtrips() {
        assert_eq!(Hlc(42).raw(), 42);
        assert_eq!(Hlc(u64::MAX).raw(), u64::MAX);
    }

    #[test]
    fn hlc_parts_zero() {
        let parts = Hlc::ZERO.parts();
        assert_eq!(parts.physical_ms, 0);
        assert_eq!(parts.logical, 0);
        assert_eq!(Hlc::from_parts(parts), Hlc::ZERO);
    }

    #[test]
    fn hlc_parts_nontrivial() {
        let parts = HlcParts {
            physical_ms: 1_700_000_000_000,
            logical: 42,
        };
        let hlc = Hlc::from_parts(parts);
        let rt = hlc.parts();
        assert_eq!(rt.physical_ms, parts.physical_ms);
        assert_eq!(rt.logical, parts.logical);
    }

    #[test]
    fn hlc_parts_at_max() {
        let parts = HlcParts {
            physical_ms: HLC_MAX_PHYSICAL_MS,
            logical: 0xFFFF,
        };
        let hlc = Hlc::from_parts(parts);
        assert_eq!(hlc.0, u64::MAX);
        assert_eq!(hlc.parts(), parts);
    }

    // ── Ordering ────────────────────────────────────────────────────────

    #[test]
    fn ordering_physical_then_logical() {
        let a = Hlc::from_parts(HlcParts {
            physical_ms: 100,
            logical: 65535,
        });
        let b = Hlc::from_parts(HlcParts {
            physical_ms: 101,
            logical: 0,
        });
        assert!(a < b);

        let c = Hlc::from_parts(HlcParts {
            physical_ms: 100,
            logical: 1,
        });
        let d = Hlc::from_parts(HlcParts {
            physical_ms: 100,
            logical: 2,
        });
        assert!(c < d);
    }

    #[test]
    fn ordering_same_value_is_eq() {
        let a = Hlc::from_parts(HlcParts {
            physical_ms: 500,
            logical: 3,
        });
        let b = Hlc::from_parts(HlcParts {
            physical_ms: 500,
            logical: 3,
        });
        assert_eq!(a, b);
        assert_eq!(a.cmp(&b), std::cmp::Ordering::Equal);
    }

    #[test]
    fn ordering_is_total() {
        let values = [
            Hlc::from_parts(HlcParts {
                physical_ms: 0,
                logical: 0,
            }),
            Hlc::from_parts(HlcParts {
                physical_ms: 0,
                logical: 1,
            }),
            Hlc::from_parts(HlcParts {
                physical_ms: 1,
                logical: 0,
            }),
            Hlc::from_parts(HlcParts {
                physical_ms: 100,
                logical: 0xFFFF,
            }),
            Hlc::from_parts(HlcParts {
                physical_ms: HLC_MAX_PHYSICAL_MS,
                logical: 0xFFFF,
            }),
        ];
        for w in values.windows(2) {
            assert!(w[0] < w[1], "{:?} should be < {:?}", w[0], w[1]);
        }
    }

    // ── hlc_subtract ────────────────────────────────────────────────────

    #[test]
    fn hlc_subtract_normal() {
        let t = Hlc::from_parts(HlcParts {
            physical_ms: 5000,
            logical: 0,
        });
        let result = hlc_subtract(t, Duration::from_millis(1000));
        assert_eq!(
            result,
            Some(Hlc::from_parts(HlcParts {
                physical_ms: 4000,
                logical: 0
            }))
        );
    }

    #[test]
    fn hlc_subtract_underflow_returns_none() {
        let t = Hlc::from_parts(HlcParts {
            physical_ms: 500,
            logical: 0,
        });
        assert_eq!(hlc_subtract(t, Duration::from_millis(1000)), None);
    }

    #[test]
    fn hlc_subtract_zero_duration_is_identity() {
        let t = Hlc::from_parts(HlcParts {
            physical_ms: 1234,
            logical: 7,
        });
        assert_eq!(
            hlc_subtract(t, Duration::ZERO),
            Some(Hlc::from_parts(HlcParts {
                physical_ms: 1234,
                logical: 0
            }))
        );
    }

    #[test]
    fn hlc_subtract_exact_zero() {
        let t = Hlc::from_parts(HlcParts {
            physical_ms: 1000,
            logical: 0,
        });
        assert_eq!(
            hlc_subtract(t, Duration::from_millis(1000)),
            Some(Hlc::ZERO)
        );
    }

    // ── duration_as_ms_clamped ──────────────────────────────────────────

    #[test]
    fn duration_as_ms_normal() {
        assert_eq!(duration_as_ms_clamped(Duration::from_millis(500)), 500);
        assert_eq!(duration_as_ms_clamped(Duration::from_secs(3)), 3000);
    }

    #[test]
    fn duration_as_ms_zero() {
        assert_eq!(duration_as_ms_clamped(Duration::ZERO), 0);
    }

    #[test]
    fn duration_as_ms_overflow_clamps() {
        assert_eq!(
            duration_as_ms_clamped(Duration::from_secs(u64::MAX)),
            u64::MAX
        );
    }

    // ── Serde roundtrip ─────────────────────────────────────────────────

    #[test]
    fn serde_roundtrip() {
        let values = [
            Hlc::ZERO,
            Hlc(42),
            Hlc(u64::MAX),
            Hlc::from_parts(HlcParts {
                physical_ms: 1_000_000,
                logical: 0xABCD,
            }),
        ];
        for v in values {
            let encoded = postcard::to_allocvec(&v).expect("encode");
            let decoded: Hlc = postcard::from_bytes(&encoded).expect("decode");
            assert_eq!(decoded, v);
        }
    }

    // ── Constants ───────────────────────────────────────────────────────

    #[test]
    fn constants_are_consistent() {
        assert_eq!(HLC_LOGICAL_BITS, 16);
        assert_eq!(HLC_LOGICAL_MASK, 0xFFFF);
        assert_eq!(HLC_PHYSICAL_MASK, !HLC_LOGICAL_MASK);
        assert_eq!(HLC_MAX_PHYSICAL_MS, 0x0000_FFFF_FFFF_FFFF);
        // A valid HLC at max physical + max logical must be u64::MAX.
        let max = Hlc::from_parts(HlcParts {
            physical_ms: HLC_MAX_PHYSICAL_MS,
            logical: 0xFFFF,
        });
        assert_eq!(max.0, u64::MAX);
        assert_eq!(max.raw(), u64::MAX);
    }

    // ── generated properties ────────────────────────────────────────────

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// `parts()` masks to 48 bits, so the roundtrip is well-defined for any
        /// raw `u64` — no bound needed on the generator.
        #[test]
        fn parts_roundtrip_for_any_raw(raw in any::<u64>()) {
            let hlc = Hlc(raw);
            prop_assert_eq!(Hlc::from_parts(hlc.parts()), hlc);
        }

        /// The packing exists so that comparing raws *is* comparing
        /// `(physical_ms, logical)` lexicographically — which is why `Ord` is
        /// derived rather than written. This pins that over the whole `u64`
        /// range rather than at the handful of points the examples pick.
        #[test]
        fn raw_ordering_matches_parts_lexicographic(a in any::<u64>(), b in any::<u64>()) {
            let (ha, hb) = (Hlc(a), Hlc(b));
            let (pa, pb) = (ha.parts(), hb.parts());
            prop_assert_eq!(
                ha.cmp(&hb),
                (pa.physical_ms, pa.logical).cmp(&(pb.physical_ms, pb.logical))
            );
        }
    }
}
