//! Who is waiting, what is being fetched, and how patient each case is.

use std::time::Duration;

/// Who waits for a fetch, and so how patient it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Urgency {
    /// A browser is waiting.
    Demand,
    /// A prediction (a hint from the web layer): spare capacity only.
    Prefetch,
    /// A pipeline stage of the worker.
    Background,
}

/// What a fetch is for: its name labels the metrics; `hedge` says that a
/// demand fetch of it may be duplicated when slow (small files a person
/// waits for — never a 30 MB video).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Kind {
    pub name: &'static str,
    pub hedge: bool,
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)
    }
}

impl Urgency {
    pub(super) fn label(self) -> &'static str {
        match self {
            Urgency::Demand => "demand",
            Urgency::Prefetch => "prefetch",
            Urgency::Background => "background",
        }
    }

    /// The longest until the first byte (queueing included); `None`: ours is
    /// none (object_store's own timeouts and retries).
    pub(super) fn first_byte(self, l: &Limits) -> Option<Duration> {
        match self {
            Urgency::Demand => Some(l.demand_first_byte),
            Urgency::Prefetch => Some(l.prefetch_first_byte),
            Urgency::Background => None,
        }
    }

    /// The longest a body may go without a chunk.
    pub(super) fn stall(self, l: &Limits) -> Option<Duration> {
        match self {
            Urgency::Demand => Some(l.demand_stall),
            Urgency::Prefetch => Some(l.prefetch_first_byte),
            Urgency::Background => None,
        }
    }
}

/// The deadlines and the breaker's thresholds (design §5.2, §5.5).
/// Constants in production; tests shrink them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    /// Failed fetches in a row that open the breaker …
    pub(super) breaker_failures: u32,
    /// A timeout counts as the store's failure only after this long with
    /// the store (from its slot to the deadline).
    pub(super) store_time_counts: Duration,
    /// … for this long.
    pub(super) breaker_open: Duration,
    pub(super) demand_first_byte: Duration,
    pub(super) demand_stall: Duration,
    /// The one quick retry of a demand fetch: after this pause …
    pub(super) retry_after: Duration,
    /// … if at least this much of its deadline is left.
    pub(super) retry_needs: Duration,
    /// The hedge delay is the recent p95 first-byte latency, clamped here.
    pub(super) hedge_min: Duration,
    pub(super) hedge_max: Duration,
    /// A prediction: at most this long queued, and again to its first byte.
    pub(super) prefetch_first_byte: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            breaker_failures: 5,
            store_time_counts: Duration::from_secs(1),
            breaker_open: Duration::from_secs(5),
            demand_first_byte: Duration::from_secs(5),
            demand_stall: Duration::from_secs(5),
            retry_after: Duration::from_millis(50),
            retry_needs: Duration::from_millis(500),
            hedge_min: Duration::from_millis(50),
            hedge_max: Duration::from_millis(500),
            prefetch_first_byte: Duration::from_secs(2),
        }
    }
}
