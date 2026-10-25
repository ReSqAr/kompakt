//! The clock that mints HLCs.
//!
//! ```text
//!   now()        phys > last.phys ─> (phys, 0)
//!                otherwise        ─> bump(last)
//!
//!   integrate(p) |p.phys - phys| > skew_limit ─> Err(HlcDriftTooLarge)
//!                phys' = max(phys, last.phys, p.phys)
//!                logical' = 1 + max(logical of every source tying phys')
//!                         = 0   if none ties it
//! ```
//!
//! Strictly monotonic: no stamp is ever issued twice, and the logical counter is
//! what covers a millisecond that produces more than one event (or a wall clock
//! that stands still or steps back).
//!
//! `integrate` is where another node's time enters this one, so it is also where
//! a wrong clock would spread. The skew limit bounds that blast radius: absorbing
//! a peer moves this node's HLC *permanently* forward, and nothing pulls it back,
//! so a peer far enough out is refused rather than believed.

// `expect` calls here are for HLC arithmetic invariants (checked_add on
// physical_ms far from u64::MAX, i64::try_from on values ≤ 2^48).
#![allow(
    clippy::expect_used,
    reason = "HLC arithmetic invariants on bounded values (physical_ms ≤ 2^48)"
)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex as SyncMutex;

use crate::error::Error;
use crate::hlc;
use crate::hlc::{HLC_MAX_PHYSICAL_MS, Hlc, HlcParts};

pub struct HlcClock {
    inner: SyncMutex<Inner>,
    skew_limit: Duration,
    time_source: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl Default for HlcClock {
    fn default() -> Self {
        Self::new(Duration::default())
    }
}

struct Inner {
    last_hlc: Hlc,
}

impl HlcClock {
    #[must_use]
    pub(crate) fn new(skew_limit: Duration) -> Self {
        Self::with_time_source(skew_limit, Box::new(wall_clock_ms_now))
    }

    #[must_use]
    pub fn with_time_source(
        skew_limit: Duration,
        time_source: Box<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            inner: SyncMutex::new(Inner {
                last_hlc: Hlc::ZERO,
            }),
            skew_limit,
            time_source,
        }
    }

    #[must_use]
    #[cfg(test)]
    fn last_hlc(&self) -> Hlc {
        self.inner.lock().last_hlc
    }

    #[must_use]
    pub fn now(&self) -> Hlc {
        let pt_ms = self.read_physical_ms();
        let mut inner = self.inner.lock();
        let last = inner.last_hlc.parts();

        let next = if pt_ms > last.physical_ms {
            HlcParts {
                physical_ms: pt_ms,
                logical: 0,
            }
        } else {
            bump_logical(last.physical_ms, last.logical)
        };

        let hlc = Hlc::from_parts(next);
        inner.last_hlc = hlc;
        hlc
    }

    fn integrate(&self, peer: Hlc) -> Result<Hlc, Error> {
        let pt_ms = self.read_physical_ms();
        let peer_parts = peer.parts();

        let drift_ms = peer_parts.physical_ms.abs_diff(pt_ms);
        crate::metrics::hlc_drift(Duration::from_millis(drift_ms));
        let limit_ms = hlc::duration_as_ms_clamped(self.skew_limit);
        if drift_ms > limit_ms {
            // Direction disambiguates the common latency case (peer stamp older
            // than local → `behind`) from a genuinely fast peer clock (`ahead`).
            let direction = if peer_parts.physical_ms >= pt_ms {
                crate::metrics::DriftDirection::Ahead
            } else {
                crate::metrics::DriftDirection::Behind
            };
            crate::metrics::hlc_drift_rejection(direction);
            let local_now = Hlc::from_parts(HlcParts {
                physical_ms: pt_ms.min(HLC_MAX_PHYSICAL_MS),
                logical: 0,
            });
            return Err(Error::HlcDriftTooLarge {
                peer,
                local: local_now,
            });
        }

        let mut inner = self.inner.lock();
        let last = inner.last_hlc.parts();

        let new_phys = pt_ms.max(last.physical_ms).max(peer_parts.physical_ms);
        // Only a source sitting *at* the new physical ms has a logical counter
        // worth preserving; if the wall clock alone set it, the millisecond is
        // fresh and the counter restarts.
        let tied = [
            (new_phys == last.physical_ms).then_some(last.logical),
            (new_phys == peer_parts.physical_ms).then_some(peer_parts.logical),
        ];
        let new_parts = match tied.into_iter().flatten().max() {
            Some(logical) => bump_logical(new_phys, logical),
            None => HlcParts {
                physical_ms: new_phys,
                logical: 0,
            },
        };

        let hlc = Hlc::from_parts(new_parts);
        inner.last_hlc = hlc;
        Ok(hlc)
    }

    /// Absolute physical-ms difference between the local clock and a peer HLC,
    /// without advancing or mutating state. Used to watch a peer's HLC drift
    /// toward the rejection threshold before it triggers `integrate`'s error.
    pub(crate) fn skew_against(&self, peer: Hlc) -> Duration {
        let pt_ms = self.read_physical_ms();
        Duration::from_millis(peer.parts().physical_ms.abs_diff(pt_ms))
    }

    fn read_physical_ms(&self) -> u64 {
        let raw = (self.time_source)();
        raw.min(HLC_MAX_PHYSICAL_MS)
    }
}

impl crate::api::Clock for HlcClock {
    fn now(&self) -> Hlc {
        self.now()
    }

    fn integrate(&self, peer: Hlc) -> Result<Hlc, Error> {
        self.integrate(peer)
    }
}

/// 1 s ticker emitting `zslg_hlc_now_physical_ms` / `zslg_hlc_logical` /
/// `zslg_hlc_wall_clock_drift_ms`. Spawned once per node from `Node::new`.
pub(crate) async fn run_clock_gauges(
    clock: std::sync::Arc<HlcClock>,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), Error> {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = ticker.tick() => {}
        }
        let now_hlc = clock.now();
        crate::metrics::hlc_now_physical_ms(now_hlc);
        crate::metrics::hlc_logical(u64::from(now_hlc.parts().logical));

        // Wall-clock drift: HLC physical ms minus the raw wall clock — the
        // HLC can ride ahead after `integrate` absorbed a fast peer.
        let physical_ms = now_hlc.parts().physical_ms;
        let wall_ms = clock.read_physical_ms();
        let drift_i64 = i64::try_from(physical_ms)
            .expect("physical_ms ≤ 2^48, fits i64")
            .checked_sub(i64::try_from(wall_ms).expect("wall ms fits i64"))
            .expect("drift fits i64");
        crate::metrics::hlc_wall_clock_drift_ms(drift_i64);
    }
}

fn bump_logical(physical_ms: u64, logical: u16) -> HlcParts {
    match logical.checked_add(1) {
        Some(next) => HlcParts {
            physical_ms,
            logical: next,
        },
        None => HlcParts {
            physical_ms: physical_ms
                .checked_add(1)
                .expect("HLC ms counter overflowed"),
            logical: 0,
        },
    }
}

fn wall_clock_ms_now() -> u64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    hlc::duration_as_ms_clamped(dur)
}

#[cfg(test)]
mod tests {
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
        assert!(matches!(err, Error::HlcDriftTooLarge { .. }));
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
        let hlc = <HlcClock as crate::api::Clock>::now(&clock);
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
        let merged = <HlcClock as crate::api::Clock>::integrate(&clock, peer).expect("ok");
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
}
