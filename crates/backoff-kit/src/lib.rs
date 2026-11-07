//! Jitter for retry delays: so peers that failed together do not retry together.

use std::time::Duration;

/// `d` spread uniformly over ±25 %.
#[must_use]
pub fn jitter(d: Duration) -> Duration {
    let ms = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let quarter = ms.checked_div(4).unwrap_or(1).max(1);
    let spread = quarter.saturating_mul(2);
    let range = spread.saturating_add(1);
    let delta = rand::random::<u64>().checked_rem(range).unwrap_or(0);
    let ms = ms.saturating_sub(quarter).saturating_add(delta);
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stays_within_a_quarter_either_way() {
        for _ in 0..1000 {
            let j = jitter(Duration::from_millis(1000));
            assert!((750..=1250).contains(&u64::try_from(j.as_millis()).unwrap_or(0)));
        }
    }
}
