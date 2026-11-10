//! The node-level snapshot / compaction / cleanup tunings. The leader assembly
//! hands each job crate its own slice of these.

use std::time::Duration;

use zslg_lifecycle_types::ConfigError;
use zslg_snapshot::{CompactionTier, CompressionLevel};

/// Snapshot creation, compaction, and cleanup configuration.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    pub interval: Duration,
    pub cutoff_timeout: Duration,
    pub compaction_tiers: Vec<CompactionTier>,
    pub compaction_interval: Duration,
    pub cleanup_retention: Duration,
    pub cleanup_interval: Duration,
    pub compression: CompressionLevel,
    /// Soft cap on entries per snapshot object. Bounds build time, object size,
    /// and a catching-up node's load granularity: without it, a stalled cluster
    /// produces one unbounded object that must be loaded monolithically.
    ///
    /// Soft because a chunk always runs to the end of the HLC it lands in
    /// (`stream_window`), so the real count can exceed this slightly.
    pub max_snapshot_entries: std::num::NonZeroUsize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(1),
            cutoff_timeout: std::time::Duration::from_millis(200),
            compaction_tiers: vec![
                Duration::from_secs(60).into(),
                Duration::from_secs(60 * 15).into(),
                Duration::from_secs(60 * 60).into(),
                Duration::from_secs(60 * 60 * 24).into(),
                Duration::from_secs(60 * 60 * 24 * 7).into(),
                Duration::from_secs(60 * 60 * 24 * 30).into(),
                Duration::from_secs(60 * 60 * 24 * 365).into(),
            ],
            compaction_interval: Duration::from_secs(60),
            cleanup_retention: Duration::from_secs(60 * 60),
            cleanup_interval: Duration::from_secs(5 * 60),
            compression: CompressionLevel::standard(),
            max_snapshot_entries: std::num::NonZeroUsize::new(100_000)
                .unwrap_or(std::num::NonZeroUsize::MIN),
        }
    }
}

impl SnapshotConfig {
    pub fn validate(&self) -> Result<(), crate::Error> {
        match self.compaction_tiers.first() {
            None => {
                return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                    "compaction_tiers must not be empty".to_owned(),
                )));
            }
            Some(first) if first.as_duration() < self.interval => {
                return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                    format!(
                        "compaction_tiers[0] ({:?}) must be >= snapshot_interval ({:?})",
                        first, self.interval
                    ),
                )));
            }
            Some(_) => {}
        }
        for window in self.compaction_tiers.windows(2) {
            let (a, b) = (window[0], window[1]);
            if b < a {
                return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                    format!("compaction_tiers must be non-decreasing; saw {a:?} before {b:?}"),
                )));
            }
        }
        if self.compaction_interval.is_zero() {
            return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                "compaction_interval must be > 0".to_owned(),
            )));
        }
        // `compression` needs no check here: `CompressionLevel` cannot be
        // constructed out of zstd's range.
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
    use super::*;
    use crate::Error;
    use std::time::Duration;

    // ── config validation ────────────────────────────────────────────────

    fn ok_snapshot_config() -> SnapshotConfig {
        SnapshotConfig::default()
    }

    // Note: the `max_clock_skew >= cutoff_timeout` cross-field constraint lives on
    // `ReplicaNodeConfig::validate` (both knobs meet there) and is tested in
    // `replica_node`.

    #[test]
    fn rejects_first_tier_below_snapshot_interval() {
        let mut c = ok_snapshot_config();
        c.interval = Duration::from_secs(2);
        c.compaction_tiers[0] = Duration::from_secs(1).into();
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn rejects_empty_compaction_tiers() {
        let mut c = ok_snapshot_config();
        c.compaction_tiers.clear();
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn rejects_non_monotonic_compaction_tiers() {
        let mut c = ok_snapshot_config();
        c.compaction_tiers = vec![
            Duration::from_secs(60).into(),
            Duration::from_secs(10).into(),
            Duration::from_secs(3600).into(),
        ];
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn accepts_first_tier_equal_to_snapshot_interval() {
        let mut c = ok_snapshot_config();
        c.interval = Duration::from_secs(60);
        c.compaction_tiers[0] = Duration::from_secs(60).into();
        c.validate().expect("equality boundary admitted");
    }

    #[test]
    fn accepts_the_default_snapshot_entry_cap() {
        let c = ok_snapshot_config();
        assert_eq!(c.max_snapshot_entries.get(), 100_000);
        c.validate().expect("the default config is valid");
    }
}
