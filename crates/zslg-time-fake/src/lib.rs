//! A `Clock` that does what the port promises and nothing else: strictly
//! monotonic stamps, absorbing any peer without a skew check. For tests that
//! need *a* clock rather than the production one.

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
}
