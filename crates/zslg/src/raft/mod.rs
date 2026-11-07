//! The raft module: composition root for one node's openraft instance.
//!
//! `setup_raft` adopts the node's [`Storage`] column families (`raft_storage`),
//! wires the
//! RPC router (`network`), and spawns the two background tasks that keep the
//! node's view of the cluster fresh — `PeerRegistrationWatcher` (a route for
//! every peer) and `RaftStatePublisher` (gauges + leadership events) — then
//! hands back `(RaftNode, has_durable_state)` so the caller can run the
//! startup decision procedure in `bootstrap`.
//!
//! `RaftConfig` carries the timing/election tunables together with the
//! cross-module contracts that justify their values — startup is unbounded
//! (`startup_phase_timeout`) and the client-write backstop must outlast an
//! election (`client_write_timeout`) — see the field docs.
use std::sync::Arc;
use zslg_consensus::MembershipGate;
use zslg_lifecycle_types::ConfigError;
use zslg_net::Router;

use raft_kit::storage::Adaptor;

use crate::error::Error;
use rocksdb_kit::Storage;
use shutdown::Shutdown;
use zslg_consensus::ToRaft;
use zslg_consensus_types::RaftStateHandle;
use zslg_node_types::NodeId;
use zslg_raft_rpc::{NetworkFactory, PeerRegistrationWatcher};
use zslg_raft_state::RaftStatePublisher;
use zslg_raft_storage::RocksRaftStorage;

mod membership;

#[cfg(test)]
mod tests;

pub(crate) use membership::MembershipConfig;
pub(crate) use zslg_raft_node::RaftNode;
pub(crate) use zslg_raft_startup::{RaftStartupConfig, run_raft_startup};

pub(crate) async fn setup_raft(
    router: Arc<dyn Router>,
    raft_state: RaftStateHandle,
    storage: Arc<Storage>,
    config: &RaftConfig,
    gate: Arc<dyn MembershipGate>,
    shutdown: &mut Shutdown<Error>,
) -> Result<(Arc<RaftNode>, bool), Error> {
    let raft_storage = RocksRaftStorage::open(raft_state.clone(), storage).await?;
    let has_local_state = raft_storage.has_durable_state().await?;
    let (log_store, state_handle): (Adaptor<_, _>, Adaptor<_, _>) = raft_storage.into_adaptor();
    let network_factory = NetworkFactory::new(router.clone());
    let raft_config = config.to_openraft()?;
    let raw_raft = raft_kit::Raft::new(
        config.node_id,
        Arc::new(raft_config),
        network_factory,
        log_store,
        state_handle,
    )
    .await
    .to_raft("Raft::new")?;
    let node = Arc::new(RaftNode::new(raw_raft, gate, config.client_write_timeout));

    let registration_watcher = PeerRegistrationWatcher::new(node.clone(), router);
    shutdown.spawn(
        "raft:registration_watcher",
        registration_watcher.run(shutdown.token()),
    );

    let state_publisher = RaftStatePublisher::new(raft_state.clone(), node.clone(), config.node_id);
    shutdown.spawn(
        "raft:state_publisher",
        state_publisher.run(shutdown.token()),
    );

    Ok((node, has_local_state))
}

/// Raft timing + election configuration.
#[derive(Debug, Clone)]
pub(crate) struct RaftConfig {
    pub(crate) heartbeat_interval: std::time::Duration,
    pub(crate) election_timeout_min: std::time::Duration,
    pub(crate) election_timeout_max: std::time::Duration,
    /// Log-escalation interval for a still-blocked startup/resumption phase in
    /// `run_raft_startup` (fence check, local-membership load, join, replica-set
    /// wait, quorum catch-up). Startup is **intentionally unbounded** — a
    /// full-cluster cold start must wait for a quorum of the original nodes to
    /// return, however long that takes (bounding it made nodes exit and
    /// crash-loop, which *prevented* the quorum from assembling; see
    /// `docs/LIVENESS_REVIEW.md` §2). A phase never aborts on time; instead,
    /// once it has been blocked for this interval (and every interval after) it
    /// logs at `error!` on top of its 5s `warn!` ticker, so a genuinely stuck
    /// resume is loud without killing the node. Bound *total* startup in the
    /// caller if you want a hard cap. Default: 10s. Must be > 0.
    pub(crate) startup_phase_timeout: std::time::Duration,
    /// Bound on a single `client_write` — the fenced replicated-state write the
    /// leader's integrator makes. openraft's own call has no deadline: it
    /// resolves when the entry commits, which needs a quorum, so a partitioned
    /// leader that has not yet stepped down waits indefinitely. The integrator
    /// is a single serial task, so one such wait stalls *every* leader job
    /// behind it.
    ///
    /// Generous on purpose: this is a backstop against a wedge, not a latency
    /// target. It must stay well above `election_timeout_max`, or a write
    /// issued during a normal election would time out before the new leader
    /// could commit it. Default: 30s. Must be > 0.
    pub(crate) client_write_timeout: std::time::Duration,
    /// Raft-log snapshot cadence: openraft takes a snapshot (and then purges the
    /// log) once this many entries have been appended since the last snapshot
    /// (→ `SnapshotPolicy::LogsSinceLast`). The raft state machine holds only
    /// cluster metadata, so a snapshot is cheap; a low value keeps the
    /// `raft_log` store small. Default: 100. Must be > 0.
    pub(crate) snapshot_logs_since_last: u64,
    /// Number of already-snapshotted entries retained below the snapshot
    /// frontier before `purge_logs_upto` removes them (→
    /// `max_in_snapshot_log_to_keep`). `0` truncates right up to the frontier.
    /// Default: 10.
    pub(crate) max_in_snapshot_log_to_keep: u64,
    /// The durable node id of the local node — the one `raft_config()` was called
    /// with. Identity, not a tunable: filled by the producer, not `new()`.
    pub(crate) node_id: NodeId,
}

impl RaftConfig {
    /// Default timing/election tunables. There is no `Default` impl: the node's
    /// durable identity has no sensible default, so it must be supplied here.
    #[must_use]
    #[cfg(test)]
    pub(in crate::raft) fn for_node(node_id: NodeId) -> Self {
        Self {
            heartbeat_interval: std::time::Duration::from_millis(500),
            election_timeout_min: std::time::Duration::from_millis(2000),
            election_timeout_max: std::time::Duration::from_millis(4000),
            startup_phase_timeout: std::time::Duration::from_secs(10),
            client_write_timeout: std::time::Duration::from_secs(30),
            snapshot_logs_since_last: 100,
            max_in_snapshot_log_to_keep: 10,
            node_id,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), crate::error::Error> {
        if self.heartbeat_interval.is_zero() {
            return Err(crate::error::Error::Config(
                ConfigError::InvalidConfiguration("raft_heartbeat_interval must be > 0".to_owned()),
            ));
        }
        if self.election_timeout_min.is_zero() {
            return Err(crate::error::Error::Config(
                ConfigError::InvalidConfiguration(
                    "raft_election_timeout_min must be > 0".to_owned(),
                ),
            ));
        }
        if self.election_timeout_max.is_zero() {
            return Err(crate::error::Error::Config(
                ConfigError::InvalidConfiguration(
                    "raft_election_timeout_max must be > 0".to_owned(),
                ),
            ));
        }
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(crate::error::Error::Config(
                ConfigError::InvalidConfiguration(format!(
                    "raft_election_timeout_min ({:?}) must be < raft_election_timeout_max ({:?})",
                    self.election_timeout_min, self.election_timeout_max
                )),
            ));
        }
        if self.startup_phase_timeout.is_zero() {
            return Err(crate::error::Error::Config(
                ConfigError::InvalidConfiguration(
                    "raft_startup_phase_timeout must be > 0".to_owned(),
                ),
            ));
        }
        // Above the election window, not merely non-zero: a write issued while
        // the cluster is electing must survive the election, or the backstop
        // fires on the one case it is not meant to catch.
        if self.client_write_timeout <= self.election_timeout_max {
            return Err(crate::error::Error::Config(
                ConfigError::InvalidConfiguration(format!(
                    "raft_client_write_timeout ({:?}) must be > raft_election_timeout_max ({:?})",
                    self.client_write_timeout, self.election_timeout_max
                )),
            ));
        }
        if self.snapshot_logs_since_last == 0 {
            return Err(crate::error::Error::Config(
                ConfigError::InvalidConfiguration(
                    "raft_snapshot_logs_since_last must be > 0".to_owned(),
                ),
            ));
        }
        Ok(())
    }

    /// Build an openraft [`raft_kit::Config`] from this config.
    pub(in crate::raft) fn to_openraft(&self) -> Result<raft_kit::Config, crate::error::Error> {
        let cfg = raft_kit::Config {
            heartbeat_interval: zslg_time_types::duration_as_ms_clamped(self.heartbeat_interval),
            election_timeout_min: zslg_time_types::duration_as_ms_clamped(
                self.election_timeout_min,
            ),
            election_timeout_max: zslg_time_types::duration_as_ms_clamped(
                self.election_timeout_max,
            ),
            // Truncate the raft log aggressively: snapshot every
            // `snapshot_logs_since_last` entries, then purge all but
            // `max_in_snapshot_log_to_keep` below the frontier. openraft's own
            // `cfg.validate()` (below) enforces `replication_lag_threshold`
            // (default 5000) > `LogsSinceLast`, so an out-of-range cadence fails
            // loud rather than being silently clamped.
            snapshot_policy: raft_kit::SnapshotPolicy::LogsSinceLast(self.snapshot_logs_since_last),
            max_in_snapshot_log_to_keep: self.max_in_snapshot_log_to_keep,
            ..Default::default()
        };
        cfg.validate().map_err(|e| {
            crate::error::Error::Config(ConfigError::InvalidConfiguration(format!(
                "raft config: {e}"
            )))
        })
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
mod config_tests {
    use super::*;
    use std::time::Duration;

    fn ok_raft_config() -> RaftConfig {
        RaftConfig::for_node(zslg_node_types::NodeId::new_v4())
    }

    #[test]
    fn rejects_raft_heartbeat_interval_zero() {
        let mut c = ok_raft_config();
        c.heartbeat_interval = Duration::ZERO;
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn rejects_raft_election_timeout_min_zero() {
        let mut c = ok_raft_config();
        c.election_timeout_min = Duration::ZERO;
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn rejects_raft_election_timeout_max_zero() {
        let mut c = ok_raft_config();
        c.election_timeout_max = Duration::ZERO;
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn rejects_raft_election_timeout_min_ge_max() {
        let mut c = ok_raft_config();
        c.election_timeout_min = Duration::from_millis(4000);
        c.election_timeout_max = Duration::from_millis(4000);
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn raft_config_is_valid() {
        let c = ok_raft_config();
        let raft_cfg = c.to_openraft().expect("raft config");
        assert!(raft_cfg.heartbeat_interval > 0);
        assert!(raft_cfg.election_timeout_min > 0);
        assert!(raft_cfg.election_timeout_max > raft_cfg.election_timeout_min);
    }

    #[test]
    fn rejects_zero_startup_phase_timeout() {
        let mut c = ok_raft_config();
        c.startup_phase_timeout = Duration::ZERO;
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn default_startup_phase_timeout_is_ten_seconds() {
        assert_eq!(
            RaftConfig::for_node(zslg_node_types::NodeId::new_v4()).startup_phase_timeout,
            Duration::from_secs(10)
        );
    }

    /// The backstop must outlast a normal election, or it fires on the one
    /// case it is not meant to catch: a write issued while the cluster is
    /// electing, which the new leader would have committed.
    #[test]
    fn rejects_client_write_timeout_at_or_below_the_election_window() {
        let mut c = ok_raft_config();
        c.client_write_timeout = c.election_timeout_max;
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));

        c.client_write_timeout = c.election_timeout_max + Duration::from_millis(1);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_zero_snapshot_logs_since_last() {
        let mut c = ok_raft_config();
        c.snapshot_logs_since_last = 0;
        assert!(matches!(
            c.validate(),
            Err(Error::Config(ConfigError::InvalidConfiguration(_)))
        ));
    }

    #[test]
    fn default_truncation_cadence() {
        let c = RaftConfig::for_node(zslg_node_types::NodeId::new_v4());
        assert_eq!(c.snapshot_logs_since_last, 100);
        assert_eq!(c.max_in_snapshot_log_to_keep, 10);
    }

    #[test]
    fn to_openraft_sets_snapshot_policy_and_keep() {
        let raft_cfg = RaftConfig::for_node(zslg_node_types::NodeId::new_v4())
            .to_openraft()
            .expect("raft config");
        assert!(matches!(
            raft_cfg.snapshot_policy,
            raft_kit::SnapshotPolicy::LogsSinceLast(100)
        ));
        assert_eq!(raft_cfg.max_in_snapshot_log_to_keep, 10);
    }
}
