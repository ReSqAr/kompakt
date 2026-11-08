//! The stable log's public configuration.

use zslg_lifecycle_types::ConfigError;

use crate::Error;
use crate::appender::STABLE_LOAD_BATCH;

/// Public configuration of the stable log: where it lives, how its runs are
/// stored, and how deep the append broadcast is. Arrivals are index-ordered
/// (time-like), so the log grows without retirement by default — it is the CQRS
/// replay source.
#[derive(Debug, Clone)]
pub struct StableLogConfig {
    /// Depth of each stable-log subscriber's own event queue — how far behind
    /// a consumer may fall before the fan-out hangs up on it and it has to
    /// replay from its cursor (the fan-out).
    ///
    /// Per subscriber, not shared: an `Appended` carries the entry `Data`, so
    /// this bounds ~capacity x payload x live subscribers. A latency/memory
    /// trade, never a correctness one — a hung-up consumer re-reads what it
    /// missed from the durable log, so a shallow queue costs re-scans
    /// (`zslg_stable_read_resyncs_total`), not data.
    pub subscriber_queue_capacity: std::num::NonZeroUsize,
}

impl StableLogConfig {
    /// Default depth of each subscriber's queue — see the field.
    pub const DEFAULT_SUBSCRIBER_QUEUE_CAPACITY: std::num::NonZeroUsize = bytesize::nonzero(256);

    /// Default sizing. Test-only — production builds the config from the
    /// owning node config's flat fields. The store's location and compression
    /// live in the node type's `COLUMN_FAMILIES`: it is a column family of the
    /// node's one DB.
    #[cfg(test)]
    #[must_use]
    pub fn for_test() -> Self {
        Self {
            subscriber_queue_capacity: Self::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        // A single load batch fans out one `Appended` per entry. A queue
        // shallower than the batch is overrun by one batch on its own, so every
        // subscriber resyncs for reasons unrelated to how fast it drains.
        // Stated in prose next to STABLE_LOAD_BATCH since it was introduced;
        // executable here.
        if self.subscriber_queue_capacity.get() < STABLE_LOAD_BATCH {
            return Err(Error::Config(ConfigError::InvalidConfiguration(format!(
                "stable_log_subscriber_queue_capacity ({}) must be >= the load \
                 batch ({STABLE_LOAD_BATCH}); a shallower queue is overrun by one batch",
                self.subscriber_queue_capacity,
            ))));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod config_tests {
    use super::{STABLE_LOAD_BATCH, StableLogConfig};

    #[test]
    fn subscriber_queue_shallower_than_a_load_batch_is_rejected() {
        let mut cfg = StableLogConfig::for_test();
        cfg.subscriber_queue_capacity = bytesize::nonzero(STABLE_LOAD_BATCH - 1);
        let err = cfg
            .validate()
            .expect_err("one batch would overrun the queue");
        assert!(
            format!("{err}").contains("subscriber_queue_capacity"),
            "the error must name the knob to change: {err}",
        );
    }

    #[test]
    fn the_default_broadcast_depth_clears_the_rule() {
        StableLogConfig::for_test()
            .validate()
            .expect("defaults validate");
    }
}
