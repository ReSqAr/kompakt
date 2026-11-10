//! Failed attempts, rate-limited per key: a token bucket drained by failures
//! only.
//!
//! ```text
//!   key: a username (logins) · a session or API-key id (a wrong secret for a known id)
//!   capacity 10, one back every 30 s   ⇒ a burst of 10, then ~2 a minute
//!   empty ⇒ the attempt is refused as an auth failure, before any hashing
//! ```
//!
//! A success costs nothing, so a person who types their password right is
//! never slowed. Per key rather than per address: behind a tunnel or a
//! tailnet an address says little. In memory, per process (a restart refills
//! every bucket — accepted), and bounded: past [`MAX_KEYS`] the buckets that
//! have refilled are dropped (a full bucket says nothing), then the fullest.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Failures a key may have in a burst.
pub const CAPACITY: u32 = 10;
/// One failure is forgiven this often.
pub const REFILL: Duration = Duration::from_secs(30);
/// Keys tracked at once.
pub const MAX_KEYS: usize = 10_000;

#[derive(Clone, Copy)]
struct Bucket {
    tokens: u32,
    /// When `tokens` was last brought up to date.
    at: Instant,
}

impl Bucket {
    /// The tokens at `now`, counting the refills since `at`.
    fn refilled(self, now: Instant) -> Self {
        let elapsed = now.saturating_duration_since(self.at);
        let steps = elapsed
            .as_nanos()
            .checked_div(REFILL.as_nanos())
            .unwrap_or(0);
        let tokens = u128::from(self.tokens)
            .saturating_add(steps)
            .min(u128::from(CAPACITY));
        let used = Duration::from_nanos(
            u64::try_from(steps.saturating_mul(REFILL.as_nanos())).unwrap_or(u64::MAX),
        );
        Self {
            tokens: u32::try_from(tokens).unwrap_or(CAPACITY),
            at: if tokens >= u128::from(CAPACITY) {
                now
            } else {
                self.at.checked_add(used).unwrap_or(now)
            },
        }
    }
}

/// The buckets of one kind of attempt (logins, sessions, API keys). Cheap to
/// clone: clones share the buckets.
#[derive(Clone, Default)]
pub struct Buckets {
    inner: Arc<Mutex<HashMap<String, Bucket>>>,
}

impl Buckets {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether an attempt for `key` may go ahead (its bucket is not empty).
    #[must_use]
    pub fn allows(&self, key: &str) -> bool {
        self.allows_at(key, Instant::now())
    }

    /// Record a failed attempt for `key`.
    pub fn failed(&self, key: &str) {
        self.failed_at(key, Instant::now());
    }

    fn allows_at(&self, key: &str, now: Instant) -> bool {
        let map = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.get(key).is_none_or(|b| b.refilled(now).tokens > 0)
    }

    fn failed_at(&self, key: &str, now: Instant) {
        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !map.contains_key(key) && map.len() >= MAX_KEYS {
            make_room(&mut map, now);
        }
        let bucket = map.get(key).map_or(
            Bucket {
                tokens: CAPACITY,
                at: now,
            },
            |b| b.refilled(now),
        );
        map.insert(
            key.to_owned(),
            Bucket {
                tokens: bucket.tokens.saturating_sub(1),
                at: bucket.at,
            },
        );
    }
}

/// Drop the buckets that have refilled; if none has, the fullest one.
fn make_room(map: &mut HashMap<String, Bucket>, now: Instant) {
    map.retain(|_, b| b.refilled(now).tokens < CAPACITY);
    if map.len() >= MAX_KEYS
        && let Some(fullest) = map
            .iter()
            .max_by_key(|(_, b)| b.refilled(now).tokens)
            .map(|(key, _)| key.clone())
    {
        map.remove(&fullest);
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]
mod tests {
    use super::*;

    #[test]
    fn a_burst_empties_the_bucket_and_time_refills_it() {
        let buckets = Buckets::new();
        let t0 = Instant::now();
        for _ in 0..CAPACITY {
            assert!(buckets.allows_at("ada", t0));
            buckets.failed_at("ada", t0);
        }
        assert!(!buckets.allows_at("ada", t0), "the eleventh is refused");
        assert!(buckets.allows_at("bob", t0), "other keys are not");
        assert!(!buckets.allows_at("ada", t0 + REFILL / 2));
        assert!(
            buckets.allows_at("ada", t0 + REFILL),
            "one back after a refill"
        );
        buckets.failed_at("ada", t0 + REFILL);
        assert!(!buckets.allows_at("ada", t0 + REFILL), "and spent again");
    }

    #[test]
    fn a_success_costs_nothing_and_a_bucket_never_overfills() {
        let buckets = Buckets::new();
        let t0 = Instant::now();
        buckets.failed_at("ada", t0);
        // An hour later the bucket is full again — not 120 tokens.
        let later = t0 + Duration::from_secs(3_600);
        for _ in 0..CAPACITY {
            buckets.failed_at("ada", later);
        }
        assert!(!buckets.allows_at("ada", later));
    }

    #[test]
    fn the_map_is_bounded() {
        let buckets = Buckets::new();
        let t0 = Instant::now();
        for i in 0..MAX_KEYS + 5 {
            buckets.failed_at(&i.to_string(), t0);
        }
        assert!(buckets.inner.lock().unwrap().len() <= MAX_KEYS);
    }
}
