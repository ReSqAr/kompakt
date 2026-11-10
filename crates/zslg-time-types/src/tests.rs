//! Tests for the crate root.

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
