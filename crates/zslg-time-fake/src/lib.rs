//! A `Clock` that does what the port promises and nothing else: strictly
//! monotonic stamps, absorbing any peer without a skew check. For tests that
//! need *a* clock rather than the production one.

mod skew;
pub use skew::SharedClockController;

use parking_lot::Mutex;
use zslg_time::{Clock, Hlc};

/// Counts up from zero; `integrate` jumps ahead of any peer stamp.
#[derive(Debug, Default)]
pub struct CountingClock {
    last: Mutex<u64>,
}

impl CountingClock {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Clock for CountingClock {
    fn now(&self) -> Hlc {
        let mut last = self.last.lock();
        *last += 1;
        Hlc(*last)
    }

    fn integrate(&self, peer: Hlc) -> Result<Hlc, zslg_time::Error> {
        let mut last = self.last.lock();
        *last = (*last).max(peer.0) + 1;
        Ok(Hlc(*last))
    }

    fn skew_against(&self, peer: Hlc) -> std::time::Duration {
        let now = *self.last.lock();
        std::time::Duration::from_millis(peer.0.abs_diff(now))
    }
}

/// Pinned to a fixed HLC; `integrate` echoes the peer and no skew is ever
/// reported.
#[derive(Debug, Clone, Copy)]
pub struct FixedClock {
    pub now: Hlc,
}

impl Clock for FixedClock {
    fn now(&self) -> Hlc {
        self.now
    }

    fn integrate(&self, peer: Hlc) -> Result<Hlc, zslg_time::Error> {
        Ok(peer)
    }

    fn skew_against(&self, _peer: Hlc) -> std::time::Duration {
        std::time::Duration::ZERO
    }
}
