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
use zslg_time::ClockError;

use parking_lot::Mutex as SyncMutex;

use crate::Error;
use zslg_time::{HLC_MAX_PHYSICAL_MS, Hlc, HlcParts, duration_as_ms_clamped};

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
    pub fn new(skew_limit: Duration) -> Self {
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
        let limit_ms = duration_as_ms_clamped(self.skew_limit);
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
            return Err(Error::Clock(ClockError::HlcDriftTooLarge {
                peer,
                local: local_now,
            }));
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
    #[must_use]
    pub fn skew_against(&self, peer: Hlc) -> Duration {
        let pt_ms = self.read_physical_ms();
        Duration::from_millis(peer.parts().physical_ms.abs_diff(pt_ms))
    }

    fn read_physical_ms(&self) -> u64 {
        let raw = (self.time_source)();
        raw.min(HLC_MAX_PHYSICAL_MS)
    }
}

impl zslg_time::Clock for HlcClock {
    fn now(&self) -> Hlc {
        self.now()
    }

    fn integrate(&self, peer: Hlc) -> Result<Hlc, zslg_time::Error> {
        self.integrate(peer)
    }

    fn skew_against(&self, peer: Hlc) -> Duration {
        self.skew_against(peer)
    }
}

/// 1 s ticker emitting `zslg_hlc_now_physical_ms` / `zslg_hlc_logical` /
/// `zslg_hlc_wall_clock_drift_ms`. Spawned once per node from `Node::new`.
pub async fn run_clock_gauges<E>(
    clock: std::sync::Arc<HlcClock>,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), E> {
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
    duration_as_ms_clamped(dur)
}

#[cfg(test)]
mod tests;
