//! The membership policy: which spare nodes get promoted to fill a replica-set
//! deficit ([`SpareSelectionStrategy`]) and the cluster-shape knobs
//! ([`MembershipConfig`]) — how many replica nodes to maintain and how long a
//! Raft voter may be absent from `config.peers` before eviction.
use std::time::Duration;
use zslg_lifecycle_types::ConfigError;

use crate::api::{CandidateInfo, SpareSelector};
use zslg_consensus_types::SpareSelectionStrategy;
use zslg_node_types::NodeId;

/// Membership and replica node management configuration.
#[derive(Debug, Clone)]
pub(crate) struct MembershipConfig {
    pub(crate) replica_node_count: u32,
    pub(crate) replica_node_max_failures: u32,
    /// Uninterrupted absence from `config.peers` before a Raft voter is
    /// evicted. Zero turns eviction off (`jobs::voters::Eviction`).
    pub(crate) forced_node_removal_timeout: std::time::Duration,
    pub(crate) poll_interval: std::time::Duration,
    pub(crate) spare_selection: SpareSelectionStrategy,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            replica_node_count: 3,
            replica_node_max_failures: 5,
            forced_node_removal_timeout: std::time::Duration::from_secs(10),
            poll_interval: std::time::Duration::from_secs(1),
            spare_selection: SpareSelectionStrategy::Random,
        }
    }
}

impl MembershipConfig {
    pub(crate) fn validate(&self) -> Result<(), crate::error::Error> {
        if self.replica_node_count < 1 {
            return Err(crate::error::Error::Config(
                ConfigError::InvalidConfiguration(format!(
                    "replica_node_count must be >= 1 (got {})",
                    self.replica_node_count
                )),
            ));
        }
        Ok(())
    }
}

impl SpareSelector for SpareSelectionStrategy {
    fn rank(&self, candidates: &[CandidateInfo]) -> Vec<NodeId> {
        match self {
            Self::Random => {
                let mut v: Vec<NodeId> = candidates.iter().map(|c| c.node_id).collect();
                rand::seq::SliceRandom::shuffle(v.as_mut_slice(), &mut rand::rng());
                v
            }
            Self::LowestLatency => {
                let mut v = candidates.to_vec();
                // None RTT sorts last (Duration::MAX); NodeId breaks ties for a
                // total order.
                v.sort_by_key(|c| (c.rtt.unwrap_or(Duration::MAX), c.node_id));
                v.into_iter().map(|c| c.node_id).collect()
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn rejects_replica_count_below_one() {
        let c = MembershipConfig {
            replica_node_count: 0,
            ..MembershipConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn accepts_replica_node_count_3_5_7() {
        for &n in &[3u32, 5, 7] {
            let c = MembershipConfig {
                replica_node_count: n,
                ..MembershipConfig::default()
            };
            c.validate().expect("valid");
        }
    }
}
