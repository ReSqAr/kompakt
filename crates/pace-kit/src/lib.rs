//! A floor on a loop's iteration *rate*, measured start-to-start.

use std::time::Duration;

use tokio::time::Instant;

/// A floor on a loop's iteration *rate*, measured start-to-start.
///
/// Never an added delay: an iteration that already outran the floor waits for
/// nothing. Load-bearing for loops whose immediate loop-back is how a backlog
/// drains.
pub struct Pace {
    floor: Duration,
    last: Option<Instant>,
}

impl Pace {
    #[must_use]
    pub fn new(floor: Duration) -> Self {
        Self { floor, last: None }
    }

    /// Wait out the remainder of the floor, then mark this iteration started.
    pub async fn wait(&mut self) {
        if let Some(last) = self.last {
            // Overflow falls back to "now", i.e. no wait: it can never stall a
            // loop, only fail to slow one on a clock about to wrap.
            let earliest = last.checked_add(self.floor).unwrap_or_else(Instant::now);
            tokio::time::sleep_until(earliest).await;
        }
        self.last = Some(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn first_wait_is_immediate_and_later_ones_honour_the_floor() {
        let mut pace = Pace::new(Duration::from_secs(5));
        let start = Instant::now();
        pace.wait().await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        tokio::time::advance(Duration::from_secs(2)).await;
        pace.wait().await;
        assert_eq!(start.elapsed(), Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn back_to_back_iterations_are_spaced_by_the_floor() {
        let floor = Duration::from_millis(100);
        let mut pace = Pace::new(floor);
        let start = Instant::now();
        for _ in 0..5 {
            pace.wait().await;
        }
        // 5 iterations => 4 gaps. Exact, because virtual time only moves for timers.
        assert_eq!(start.elapsed(), floor * 4);
    }

    #[tokio::test(start_paused = true)]
    async fn an_iteration_that_outran_the_floor_waits_for_nothing() {
        let mut pace = Pace::new(Duration::from_secs(1));
        pace.wait().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let before = Instant::now();
        pace.wait().await;
        assert_eq!(before.elapsed(), Duration::ZERO);
    }
}
