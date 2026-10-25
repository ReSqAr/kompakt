//! The store's health: the circuit breaker, the first-byte tail that sets the
//! hedge delay, and the hedge budget. All of it is per scheduler.

use super::{Scheduler, Urgency};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Recent first-byte latencies (for the hedge delay) and the hedge budget.
#[derive(Debug, Default)]
pub(super) struct Tail {
    /// The last [`SAMPLES`] first-byte latencies of successful demand fetches.
    pub(super) samples: std::collections::VecDeque<Duration>,
    /// Hedge budget in tenths: +1 per demand fetch, −10 per hedge, at most
    /// 100 — so at most one hedge per ten fetches, with a little burst.
    pub(super) tokens: u32,
}

pub(super) const SAMPLES: usize = 200;

/// The circuit breaker's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Breaker {
    /// Failed fetches in a row so far.
    Closed(u32),
    Open {
        until: Instant,
    },
    /// The probe is out (`true`) or not yet (`false`).
    HalfOpen {
        pappg: bool,
    },
}

impl Default for Breaker {
    fn default() -> Self {
        Breaker::Closed(0)
    }
}

/// What the breaker says to a fetch about to start.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Pass {
    Go,
    /// Half-open: this fetch is the probe.
    Probe,
    /// Open: a browser's fetch fails now.
    Refused,
    /// Open: a background fetch waits this long, then asks again.
    Wait(Duration),
}

/// What a scheduler knows of its store's health. One value, shared by the
/// scheduler's clones.
#[derive(Default)]
pub(super) struct Health {
    pub(super) breaker: parking_lot::Mutex<Breaker>,
    /// Bumped whenever the breaker closes again: a failure of an attempt
    /// started before (a request that hung through the outage and times out
    /// after it) is old news and must not reopen it (found by the e2e outage
    /// run: such stragglers reopened the breaker seconds after recovery).
    pub(super) epoch: std::sync::atomic::AtomicU64,
    /// What the failures since the last success were, for the log line when
    /// they open the breaker.
    pub(super) recent: parking_lot::Mutex<Vec<String>>,
    /// First-byte latencies and the hedge budget.
    pub(super) tail: parking_lot::Mutex<Tail>,
}

/// Gives the probe back if its fetch ends without reporting: a report moves
/// the breaker to closed or open, so after one this does nothing.
pub(super) struct ProbeGuard(pub(super) Arc<Health>);

impl Drop for ProbeGuard {
    fn drop(&mut self) {
        let mut b = self.0.breaker.lock();
        if *b == (Breaker::HalfOpen { pappg: true }) {
            *b = Breaker::HalfOpen { pappg: false };
        }
    }
}

impl Scheduler {
    /// The store's recent p95 first-byte latency, clamped: when to hedge.
    pub(super) fn hedge_delay(&self) -> Duration {
        let tail = self.health.tail.lock();
        // Too few samples for a p95: a modest delay, not the ceiling (which
        // left the first fetches of a fresh server unhedged).
        if tail.samples.len() < 20 {
            return Duration::from_millis(100).clamp(self.limits.hedge_min, self.limits.hedge_max);
        }
        let mut v: Vec<Duration> = tail.samples.iter().copied().collect();
        v.sort_unstable();
        let at = v.len().saturating_mul(95).checked_div(100).unwrap_or(0);
        v.get(at)
            .copied()
            .unwrap_or(self.limits.hedge_max)
            .clamp(self.limits.hedge_min, self.limits.hedge_max)
    }

    /// A demand fetch reached its first byte after `took`.
    pub(super) fn sample(&self, took: Duration) {
        let mut tail = self.health.tail.lock();
        if tail.samples.len() >= SAMPLES {
            tail.samples.pop_front();
        }
        tail.samples.push_back(took);
    }

    /// Spend a hedge from the budget, if there is one.
    pub(super) fn hedge_allowed(&self) -> bool {
        let mut tail = self.health.tail.lock();
        if tail.tokens >= 10 {
            tail.tokens = tail.tokens.saturating_sub(10);
            true
        } else {
            false
        }
    }

    /// May a fetch of `urgency` start now?
    pub(super) fn pass(&self, urgency: Urgency) -> Pass {
        let mut b = self.health.breaker.lock();
        let now = Instant::now();
        if let Breaker::Open { until } = *b
            && now >= until
        {
            *b = Breaker::HalfOpen { pappg: false };
        }
        match *b {
            Breaker::Closed(_) => Pass::Go,
            Breaker::HalfOpen { pappg: false } => {
                *b = Breaker::HalfOpen { pappg: true };
                Pass::Probe
            }
            Breaker::HalfOpen { pappg: true } => match urgency {
                Urgency::Demand | Urgency::Prefetch => Pass::Refused,
                Urgency::Background => Pass::Wait(Duration::from_millis(500)),
            },
            Breaker::Open { until } => match urgency {
                Urgency::Demand | Urgency::Prefetch => Pass::Refused,
                Urgency::Background => Pass::Wait(until.saturating_duration_since(now)),
            },
        }
    }

    pub(super) fn epoch(&self) -> u64 {
        self.health.epoch.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// A fetch reached its first byte, or the store answered (even "no such
    /// object"): the store works.
    pub(super) fn succeeded(&self, since: u64) {
        self.record(None, since);
    }

    /// The store failed a fetch (`cause`: "error" or "timeout", what the
    /// failure counter says), as `detail` tells.
    pub(super) fn failed(&self, since: u64, cause: &'static str, detail: String) {
        self.record(Some((cause, detail)), since);
    }

    /// `since`: the epoch the attempt started in — a failure from before the
    /// breaker last closed is ignored.
    pub(super) fn record(&self, failure: Option<(&'static str, String)>, since: u64) {
        let ok = failure.is_none();
        let mut b = self.health.breaker.lock();
        if !ok && since != self.epoch() {
            return;
        }
        let recent = {
            let mut recent = self.health.recent.lock();
            match failure {
                None => recent.clear(),
                Some((cause, detail)) => {
                    metrics::counter!("objcache_store_failures_total", "app" => self.app, "store" => self.store, "cause" => cause)
                        .increment(1);
                    if recent.len() < 8 {
                        recent.push(detail);
                    }
                }
            }
            recent.join("; ")
        };
        let next = match (*b, ok) {
            (_, true) => Breaker::Closed(0),
            (Breaker::Closed(n), false) if n.saturating_add(1) < self.limits.breaker_failures => {
                Breaker::Closed(n.saturating_add(1))
            }
            (_, false) => Breaker::Open {
                until: Instant::now()
                    .checked_add(self.limits.breaker_open)
                    .unwrap_or_else(Instant::now),
            },
        };
        let level = |b: Breaker| match b {
            Breaker::Closed(_) => 0u32,
            Breaker::HalfOpen { .. } => 1,
            Breaker::Open { .. } => 2,
        };
        if matches!(next, Breaker::Closed(_)) && !matches!(*b, Breaker::Closed(_)) {
            self.health
                .epoch
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if level(next) != level(*b) {
            match level(next) {
                2 => tracing::warn!(
                    store = self.store,
                    failures = %recent,
                    "circuit breaker open: the store is failing"
                ),
                0 => tracing::info!(
                    store = self.store,
                    "circuit breaker closed: the store answers again"
                ),
                _ => {}
            }
            metrics::gauge!("objcache_store_breaker_state", "app" => self.app, "store" => self.store)
                .set(f64::from(level(next)));
        }
        *b = next;
    }
}
