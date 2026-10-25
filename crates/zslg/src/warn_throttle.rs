//! Rate limiter for hot-path `warn!` sites that can fire once per failed
//! attempt under churn (replica write rejections, replication subscriber
//! retries). `should_warn()` returns `true` at most once per `interval` (always
//! on the first call); callers keep the exact per-event counts in metrics and
//! use the gate only to bound log volume.

use parking_lot::Mutex;
use std::time::Duration;

/// Throttles a repeated `warn!` to at most once per `interval` (always firing
/// on the first call), stamping the current time when it does.
///
/// Safe on the hot paths that use it (replica write rejections, replication
/// subscriber retries): the `parking_lot::Mutex` is held only across the
/// synchronous timestamp compare inside [`WarnThrottle::should_warn`] — never
/// across an `.await` (CLAUDE.md §4).
pub(crate) struct WarnThrottle {
    last: Mutex<Option<tokio::time::Instant>>,
    interval: Duration,
}

impl WarnThrottle {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            last: Mutex::new(None),
            interval,
        }
    }

    /// Returns `true` at most once per `interval` (and always on the first
    /// call), stamping the current time when it does.
    pub(crate) fn should_warn(&self) -> bool {
        let mut last = self.last.lock();
        match *last {
            Some(t) if t.elapsed() < self.interval => false,
            _ => {
                *last = Some(tokio::time::Instant::now());
                true
            }
        }
    }
}

/// The "first + periodic summary" variant of [`WarnThrottle`]: the first call
/// in an interval reports, the rest are counted, and the next time the gate
/// reopens the suppressed count is returned so the caller can log it in a
/// one-line summary. Exact per-event counts stay in metrics; this bounds the
/// log volume for churn loops (replication subscriber retries, snapshot build
/// retries).
pub(crate) struct ThrottledWarn {
    gate: WarnThrottle,
    suppressed: u64,
}

impl ThrottledWarn {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            gate: WarnThrottle::new(interval),
            suppressed: 0,
        }
    }

    /// Returns `Some(n)` when a `warn!` should be emitted now — `n` is the
    /// number of events suppressed since the previous emit (0 on the first
    /// call, and whenever nothing was suppressed). Returns `None` when the call
    /// is throttled; the caller counts it and it will surface in a later
    /// summary.
    pub(crate) fn gate(&mut self) -> Option<u64> {
        if self.gate.should_warn() {
            let n = self.suppressed;
            self.suppressed = 0;
            Some(n)
        } else {
            self.suppressed = self.suppressed.saturating_add(1);
            None
        }
    }
}

#[cfg(test)]
mod warn_throttle_tests {
    use super::{ThrottledWarn, WarnThrottle};

    #[tokio::test(start_paused = true)]
    async fn throttle_fires_once_per_interval() {
        let t = WarnThrottle::new(std::time::Duration::from_secs(5));
        assert!(t.should_warn(), "first call always warns");
        assert!(!t.should_warn(), "immediate second call is throttled");
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        assert!(t.should_warn(), "warns again once the interval has elapsed");
        assert!(!t.should_warn(), "throttled again immediately after");
    }

    #[tokio::test(start_paused = true)]
    async fn throttled_warn_returns_suppressed_count_on_reopen() {
        let mut t = ThrottledWarn::new(std::time::Duration::from_secs(5));
        assert_eq!(
            t.gate(),
            Some(0),
            "first call emits with nothing suppressed"
        );
        assert_eq!(t.gate(), None, "second call is throttled");
        assert_eq!(t.gate(), None, "third call is throttled");
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        assert_eq!(
            t.gate(),
            Some(2),
            "reopen reports the two suppressed events",
        );
        assert_eq!(t.gate(), None, "throttled again immediately after");
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        assert_eq!(
            t.gate(),
            Some(1),
            "the previous throttled call is reported on the next reopen",
        );
    }
}
