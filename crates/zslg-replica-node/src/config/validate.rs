//! Cross-field validation: the relations between knobs that no single field states.

use zslg_error::Error;
use zslg_lifecycle_types::ConfigError;

use super::ReplicaNodeConfig;
use zslg_consensus::NodeId;

impl ReplicaNodeConfig {
    pub fn validate(&self) -> Result<(), Error> {
        // Dummy identity — validation doesn't need a real node_id, just tunings.
        let dummy = NodeId::new_v4();
        self.raft_config(dummy).validate()?;
        self.snapshot_config().validate()?;
        self.replica_log_config(dummy).validate()?;
        self.membership_config().validate()?;
        self.liveness_config().validate()?;
        self.learner_config().validate()?;
        self.router_config().validate()?;
        if self.peers.is_empty() {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "peers must not be empty".to_owned(),
            )));
        }
        // Distinct from the group removing this node from the voter set, which
        // is NOT fatal (see `raft::startup::rejoin`). This is the other
        // direction: our *own* configuration says we are not a member of this
        // cluster, which no amount of running can fix.
        if !self
            .peers
            .iter()
            .any(|p| p.server_name == self.network_server_name)
        {
            return Err(Error::Config(ConfigError::InvalidConfiguration(format!(
                "peers must include this node's own server_name {:?}; \
                 configured peers: {:?}",
                self.network_server_name,
                self.peers
                    .iter()
                    .map(|p| &p.server_name)
                    .collect::<Vec<_>>()
            ))));
        }
        self.writer_config(dummy).validate()?;
        if self.db_path.as_os_str().is_empty() {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "db_path must not be empty".to_owned(),
            )));
        }
        self.stable_log_config().validate()?;
        self.storage_config().validate()?;

        // Cross-field (snapshot vs HLC): the HLC skew bound must cover the
        // snapshot cutoff timeout. Lives here — the node config is where both
        // subsystems' knobs meet.
        if self.hlc_max_clock_skew < self.snapshot_cutoff_timeout {
            return Err(Error::Config(ConfigError::InvalidConfiguration(format!(
                "hlc_max_clock_skew ({:?}) must be >= snapshot_cutoff_timeout ({:?})",
                self.hlc_max_clock_skew, self.snapshot_cutoff_timeout
            ))));
        }

        if self.drain_timeout.is_zero() {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "drain_timeout must be non-zero".to_owned(),
            )));
        }

        // Cross-field: cleanup_retention must be >= purge_grace_period
        if self.snapshot_cleanup_retention < self.replica_log_purge_grace_period {
            return Err(Error::Config(ConfigError::InvalidConfiguration(format!(
                "cleanup_retention ({:?}) must be >= purge_grace_period ({:?})",
                self.snapshot_cleanup_retention, self.replica_log_purge_grace_period
            ))));
        }

        // Cross-field (replica purge vs quorum read): a quorum read waits for
        // the stable log to reach the manifest watermark it saw when it started.
        // Entries a replica purges while the read is still catching up to it
        // are recovered only via a later snapshot, so they must sit below that
        // watermark — which holds while the manifest cannot outrun the grace
        // within one read. The read is bounded by its timeout, hence the floor.
        // It does not cover a manifest catching up in one jump after a stall.
        if self.replica_log_purge_grace_period < self.learner_quorum_read_timeout {
            return Err(Error::Config(ConfigError::InvalidConfiguration(format!(
                "replica_log_purge_grace_period ({:?}) must be >= \
                 learner_quorum_read_timeout ({:?})",
                self.replica_log_purge_grace_period, self.learner_quorum_read_timeout
            ))));
        }

        Ok(())
    }
}
