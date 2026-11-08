//! Tests for `clock`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

fn clock_at(skew_ms: u64, now_ms: Arc<AtomicU64>) -> HlcClock {
    HlcClock::with_time_source(
        Duration::from_millis(skew_ms),
        Box::new(move || now_ms.load(Ordering::SeqCst)),
    )
}

fn fixed_clock(skew_ms: u64) -> (HlcClock, Arc<AtomicU64>) {
    let now_ms = Arc::new(AtomicU64::new(0));
    let clock = clock_at(skew_ms, now_ms.clone());
    (clock, now_ms)
}

// ── now() ───────────────────────────────────────────────────────────

#[test]
fn now_is_strictly_monotonic() {
    let (clock, now_ms) = fixed_clock(1000);
    now_ms.store(1_000, Ordering::SeqCst);
    let mut prev = clock.now();
    for _ in 0..1000 {
        let cur = clock.now();
        assert!(cur > prev, "{cur:?} should be > {prev:?}");
        prev = cur;
    }
}

#[test]
fn now_bumps_logical_within_same_ms() {
    let (clock, now_ms) = fixed_clock(1000);
    now_ms.store(500, Ordering::SeqCst);
    let a = clock.now().parts();
    let b = clock.now().parts();
    assert_eq!(a.physical_ms, 500);
    assert_eq!(b.physical_ms, 500);
    assert_eq!(a.logical, 0);
    assert_eq!(b.logical, 1);
}

#[test]
fn now_resets_logical_on_wall_advance() {
    let (clock, now_ms) = fixed_clock(1000);
    now_ms.store(500, Ordering::SeqCst);
    let _ = clock.now();
    let _ = clock.now(); // logical >= 1
    now_ms.store(600, Ordering::SeqCst);
    let next = clock.now().parts();
    assert_eq!(next.physical_ms, 600);
    assert_eq!(next.logical, 0);
}

#[test]
fn now_logical_overflow_bumps_physical() {
    // Simulate logical counter at u16::MAX — the next now() must bump
    // physical_ms even though the wall clock hasn't advanced.
    let (clock, now_ms) = fixed_clock(1000);
    now_ms.store(500, Ordering::SeqCst);
    // Force the internal state to logical = u16::MAX.
    {
        let mut inner = clock.inner.lock();
        inner.last_hlc = Hlc::from_parts(HlcParts {
            physical_ms: 500,
            logical: u16::MAX,
        });
    }
    let next = clock.now().parts();
    assert_eq!(next.physical_ms, 501);
    assert_eq!(next.logical, 0);
}

#[test]
fn now_clamps_physical_to_48bit() {
    let now_ms = Arc::new(AtomicU64::new(HLC_MAX_PHYSICAL_MS + 10_000));
    let clock = clock_at(1000, now_ms);
    let hlc = clock.now();
    assert!(hlc.parts().physical_ms <= HLC_MAX_PHYSICAL_MS);
}

#[test]
fn now_stores_last_hlc() {
    let (clock, now_ms) = fixed_clock(1000);
    now_ms.store(100, Ordering::SeqCst);
    let hlc = clock.now();
    assert_eq!(clock.last_hlc(), hlc);
}

// ── integrate() ─────────────────────────────────────────────────────

#[test]
fn integrate_advances_when_peer_ahead() {
    let (clock, now_ms) = fixed_clock(2000);
    now_ms.store(1_000, Ordering::SeqCst);
    let peer = Hlc::from_parts(HlcParts {
        physical_ms: 1_500,
        logical: 7,
    });
    let merged = clock.integrate(peer).expect("ok");
    assert_eq!(merged.parts().physical_ms, 1_500);
    assert_eq!(merged.parts().logical, 8);
    assert!(merged > peer);
}

#[test]
fn integrate_peer_behind_local() {
    let (clock, now_ms) = fixed_clock(2000);
    now_ms.store(2_000, Ordering::SeqCst);
    let local = clock.now();
    let peer = Hlc::from_parts(HlcParts {
        physical_ms: 1_500,
        logical: 0,
    });
    let merged = clock.integrate(peer).expect("ok");
    assert!(merged > local);
    assert!(merged > peer);
}

#[test]
fn integrate_peer_same_ms_higher_logical() {
    let (clock, now_ms) = fixed_clock(2000);
    now_ms.store(1_000, Ordering::SeqCst);
    // 5 now() calls at the same ms: logical goes 0,1,2,3,4 → last is 4.
    for _ in 0..5 {
        let _ = clock.now();
    }
    let peer = Hlc::from_parts(HlcParts {
        physical_ms: 1_000,
        logical: 3,
    });
    let merged = clock.integrate(peer).expect("ok");
    assert!(merged > peer);
    // Same ms on all three: bump(max(local=4, peer=3)) = 5.
    assert_eq!(merged.parts().physical_ms, 1_000);
    assert_eq!(merged.parts().logical, 5);
}

#[test]
fn integrate_rejects_excessive_drift() {
    let (clock, now_ms) = fixed_clock(100);
    now_ms.store(1_000, Ordering::SeqCst);
    let peer = Hlc::from_parts(HlcParts {
        physical_ms: 5_000,
        logical: 0,
    });
    let err = clock.integrate(peer).expect_err("drift must reject");
    assert!(matches!(
        err,
        Error::Clock(ClockError::HlcDriftTooLarge { .. })
    ));
    // last_hlc must not be polluted by the rejected peer.
    assert!(clock.last_hlc().parts().physical_ms < 5_000);
}

#[test]
fn integrate_accepts_at_skew_limit() {
    let (clock, now_ms) = fixed_clock(100);
    now_ms.store(1_000, Ordering::SeqCst);
    let peer = Hlc::from_parts(HlcParts {
        physical_ms: 1_100,
        logical: 0,
    });
    clock.integrate(peer).expect("at-limit should pass");
}

#[test]
fn integrate_advances_beyond_wall_and_peer() {
    // When physical clock is ahead of both wall and peer, local dominates.
    let (clock, now_ms) = fixed_clock(2000);
    now_ms.store(3_000, Ordering::SeqCst);
    let _ = clock.now(); // set internal last to ~3000
    let peer = Hlc::from_parts(HlcParts {
        physical_ms: 2_000,
        logical: 0,
    });
    let merged = clock.integrate(peer).expect("ok");
    assert!(merged.parts().physical_ms >= 3_000);
}

// ── skew_against ────────────────────────────────────────────────────

#[test]
fn skew_against_is_absolute_difference_of_physical_ms() {
    let clock = HlcClock::with_time_source(Duration::from_millis(200), Box::new(|| 5_000));
    // peer 4_000 ms behind local wall clock (5_000) → 1000 ms skew.
    let peer_behind = Hlc::from_parts(HlcParts {
        physical_ms: 4_000,
        logical: 0,
    });
    assert_eq!(
        clock.skew_against(peer_behind),
        Duration::from_millis(1_000)
    );
    // peer ahead is the same absolute difference.
    let peer_ahead = Hlc::from_parts(HlcParts {
        physical_ms: 6_000,
        logical: 0,
    });
    assert_eq!(clock.skew_against(peer_ahead), Duration::from_millis(1_000));
}

// ── bump_logical ────────────────────────────────────────────────────

#[test]
fn bump_logical_normal() {
    let parts = bump_logical(100, 5);
    assert_eq!(parts.physical_ms, 100);
    assert_eq!(parts.logical, 6);
}

#[test]
fn bump_logical_overflow_bumps_physical() {
    let parts = bump_logical(100, u16::MAX);
    assert_eq!(parts.physical_ms, 101);
    assert_eq!(parts.logical, 0);
}

// ── wall_clock_ms_now ───────────────────────────────────────────────

#[test]
fn wall_clock_returns_reasonable_value() {
    let ms = wall_clock_ms_now();
    // 2020–2099 in milliseconds from epoch is well over 1.5 trillion.
    assert!(
        ms > 1_500_000_000_000,
        "wall clock ms looks too small: {ms}"
    );
    assert!(ms <= HLC_MAX_PHYSICAL_MS, "wall clock exceeds 48-bit range");
}

// ── Clock trait impl ────────────────────────────────────────────────

#[test]
fn clock_trait_now_delegates() {
    let (clock, now_ms) = fixed_clock(1000);
    now_ms.store(777, Ordering::SeqCst);
    let hlc = <HlcClock as zslg_time::Clock>::now(&clock);
    assert_eq!(hlc.parts().physical_ms, 777);
}

#[test]
fn clock_trait_integrate_delegates() {
    let (clock, now_ms) = fixed_clock(2000);
    now_ms.store(1_000, Ordering::SeqCst);
    let peer = Hlc::from_parts(HlcParts {
        physical_ms: 2_000,
        logical: 0,
    });
    let merged = <HlcClock as zslg_time::Clock>::integrate(&clock, peer).expect("ok");
    assert!(merged > peer);
}

// ── generated properties ────────────────────────────────────────────

use proptest::prelude::*;

/// Wall readings stay well below `HLC_MAX_PHYSICAL_MS` on purpose. At the
/// very top of the 48-bit range, 65 536 stamps inside one millisecond make
/// `bump_logical` carry past the range and trip `from_parts`'s
/// `debug_assert` — the documented year-10889 limit, not a monotonicity
/// bug. Generating up there would rediscover it and say nothing about the
/// property under test.
const WALL_MAX: u64 = 1_000_000;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// `now()` is strictly increasing no matter what the wall clock does —
    /// including standing still and running backwards. The examples above
    /// cover the logical bump, the reset on advance and logical overflow
    /// separately; generating sequences covers their interaction.
    #[test]
    fn now_is_strictly_monotonic_across_arbitrary_wall_readings(
        walls in proptest::collection::vec(0u64..WALL_MAX, 1..64),
    ) {
        let (clock, now_ms) = fixed_clock(1_000);
        let mut prev: Option<Hlc> = None;
        for wall in walls {
            now_ms.store(wall, Ordering::SeqCst);
            let got = clock.now();
            if let Some(p) = prev {
                prop_assert!(
                    got > p,
                    "now() went backwards: {p:?} then {got:?} at wall {wall}"
                );
            }
            prev = Some(got);
        }
    }

    /// `integrate` accepts exactly within the skew limit, never lets the
    /// local clock regress, and leaves it untouched on rejection.
    #[test]
    fn integrate_accepts_within_skew_and_never_regresses(
        steps in proptest::collection::vec(
            (0u64..WALL_MAX, -1_500i64..1_500i64, 0u16..8),
            1..32,
        ),
    ) {
        const SKEW_MS: u64 = 500;
        let (clock, now_ms) = fixed_clock(SKEW_MS);

        for (wall, offset, peer_logical) in steps {
            now_ms.store(wall, Ordering::SeqCst);
            let before = clock.last_hlc();

            // Compute peer_ms from wall and offset, clamping to valid range.
            // wall is u64, offset is i64; saturate the result into 0..WALL_MAX.
            let wall_i64 = i64::try_from(wall).expect("wall fits in i64");
            let peer_ms_i64 = wall_i64.saturating_add(offset);
            let peer_ms = u64::try_from(peer_ms_i64.max(0)).expect("clamped to non-negative")
                .min(WALL_MAX - 1);

            let peer = Hlc::from_parts(HlcParts {
                physical_ms: peer_ms,
                logical: peer_logical,
            });
            let drift = peer_ms.abs_diff(wall);

            match clock.integrate(peer) {
                Ok(got) => {
                    prop_assert!(
                        drift <= SKEW_MS,
                        "accepted a peer {drift} ms away, limit {SKEW_MS}"
                    );
                    prop_assert!(got >= peer, "result {got:?} below peer {peer:?}");
                    prop_assert!(
                        got > before,
                        "result {got:?} did not advance past {before:?}"
                    );
                }
                Err(_) => {
                    prop_assert!(
                        drift > SKEW_MS,
                        "rejected a peer only {drift} ms away, limit {SKEW_MS}"
                    );
                    prop_assert_eq!(
                        clock.last_hlc(),
                        before,
                        "a rejected integrate moved the local clock"
                    );
                }
            }
        }
    }
}
