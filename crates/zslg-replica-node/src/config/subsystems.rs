//! The per-subsystem configs a replica node derives from its one config.

use std::net::SocketAddr;
use std::sync::Arc;
use zslg_learner::{LOCAL_STORE_CURSORS_CF, LOCAL_STORE_DATA_CF, STABLE_LOG_CF};
use zslg_net::PeersWatcher;
use zslg_node_id::NODE_ID_CF;
use zslg_replica_log::REPLICA_LOG_CF;

use super::ReplicaNodeConfig;
use rocksdb_kit::{CfSpec, StorageConfig};
use zslg_consensus::NodeId;
use zslg_leader::{LivenessConfig, SnapshotConfig};
use zslg_leader_membership::MembershipConfig;
use zslg_learner::LearnerConfig;
use zslg_net::{HostPort, PeerAddr};
use zslg_net_setup::{RpcClientConfig, RpcServerConfig, SocketOpts};
use zslg_object_store::SnapshotStoreConfig;
use zslg_raft_setup::RaftConfig;
use zslg_replica_log::ReplicaLogConfig;

impl ReplicaNodeConfig {
    /// Produce the raft subsystem config: the raft tunables plus the node's
    /// durable identity. The raft store's two column families come from the
    /// [`Storage`](rocksdb_kit::Storage) opened in `start`, not from a path in here.
    #[must_use]
    pub fn raft_config(&self, node_id: NodeId) -> RaftConfig {
        RaftConfig {
            heartbeat_interval: self.raft_heartbeat_interval,
            election_timeout_min: self.raft_election_timeout_min,
            election_timeout_max: self.raft_election_timeout_max,
            startup_phase_timeout: self.raft_startup_phase_timeout,
            client_write_timeout: self.raft_client_write_timeout,
            snapshot_logs_since_last: self.raft_snapshot_logs_since_last,
            max_in_snapshot_log_to_keep: self.raft_max_in_snapshot_log_to_keep,
            node_id,
        }
    }

    /// Produce the snapshot subsystem config.
    #[must_use]
    pub fn snapshot_config(&self) -> SnapshotConfig {
        SnapshotConfig {
            interval: self.snapshot_interval,
            cutoff_timeout: self.snapshot_cutoff_timeout,
            compaction_tiers: self.snapshot_compaction_tiers.clone(),
            compaction_interval: self.snapshot_compaction_interval,
            cleanup_retention: self.snapshot_cleanup_retention,
            cleanup_interval: self.snapshot_cleanup_interval,
            compression: self.snapshot_compression,
            max_snapshot_entries: self.snapshot_max_entries,
        }
    }

    /// Produce the membership subsystem config.
    #[must_use]
    pub fn membership_config(&self) -> MembershipConfig {
        MembershipConfig {
            replica_node_count: self.membership_replica_node_count,
            replica_node_max_failures: self.membership_replica_node_max_failures,
            forced_node_removal_timeout: self.membership_forced_node_removal_timeout,
            poll_interval: self.membership_poll_interval,
            spare_selection: self.membership_spare_selection,
        }
    }

    /// Produce the liveness subsystem config.
    #[must_use]
    pub fn liveness_config(&self) -> LivenessConfig {
        LivenessConfig {
            probe_interval: self.liveness_probe_interval,
            probe_timeout: self.liveness_probe_timeout,
        }
    }

    /// Produce the outbound router's client config.
    #[must_use]
    pub fn router_config(&self) -> RpcClientConfig {
        RpcClientConfig {
            tcp_keepalive_interval: self.keepalive_interval,
            #[cfg(target_os = "linux")]
            tcp_user_timeout: self.user_timeout,
            tcp_keepalive_retries: self.keepalive_retries,
            socket_buffer_size: self.socket_buffer,
            unary_timeout: self.unary_timeout,
            connect_timeout: self.connect_timeout,
            max_unary_conns_per_peer: self.max_unary_conns_per_peer,
            max_idle_unary_conns_per_peer: self.max_idle_unary_conns_per_peer,
            pool_channel_capacity: self.pool_channel_capacity,
            subscribe_channel_capacity: self.subscribe_channel_capacity,
        }
    }

    /// Produce the persistent peers watcher from the flat peer list.
    #[must_use]
    pub fn peers_watcher(&self) -> Arc<dyn PeersWatcher> {
        zslg_peers::static_peers(self.peers.clone())
    }

    /// Produce the RPC server bind config, including the TCP socket tuning and
    /// the node's durable identity.
    #[must_use]
    pub fn rpc_server_config(&self, node_id: NodeId) -> RpcServerConfig {
        RpcServerConfig {
            listen_addr: self.network_listen_addr,
            socket_opts: SocketOpts {
                keepalive_interval: self.keepalive_interval,
                #[cfg(target_os = "linux")]
                user_timeout: self.user_timeout,
                keepalive_retries: self.keepalive_retries,
                buffer_size: self.socket_buffer,
            },
            node_id,
        }
    }

    /// Whether this node is the bootstrap node.
    #[must_use]
    pub fn is_bootstrap_node(&self) -> bool {
        self.network_server_name == self.network_bootstrap_server_name
    }

    /// Every column family a replica node runs — the complete schema of its one
    /// RocksDB. RocksDB refuses to open a root whose on-disk family set is not
    /// fully named here, so adding a family to this list is a wipe; and the
    /// length is what sizes each memtable, so a family that is listed but never
    /// used still costs the ones that are.
    #[must_use]
    pub fn column_families(&self) -> Vec<CfSpec> {
        let mut cfs = Vec::from(zslg_raft_storage::raft_cf_specs());
        cfs.extend([
            // Compression is a per-store footprint/CPU trade, never a
            // durability one — the block CRC is what protects a record.
            CfSpec {
                cf: REPLICA_LOG_CF,
                compress: self.replica_log_compression,
            },
            CfSpec {
                cf: STABLE_LOG_CF,
                compress: self.stable_log_compression,
            },
            CfSpec {
                cf: LOCAL_STORE_DATA_CF,
                compress: self.learner_local_store_compression,
            },
            // Cursors follow the data family rather than carrying a second
            // knob: one overwritten slot per peer has no workload behind it.
            CfSpec {
                cf: LOCAL_STORE_CURSORS_CF,
                compress: self.learner_local_store_compression,
            },
            // One tiny, never-compressed row: the write-once node identity.
            CfSpec {
                cf: NODE_ID_CF,
                compress: false,
            },
        ]);
        cfs
    }

    /// Produce the storage tuning: the budget plus the overrides, with every
    /// unset one left to `rocksdb_kit`'s derivation from the budget.
    #[must_use]
    pub fn storage_config(&self) -> StorageConfig {
        StorageConfig {
            memory: self.storage_memory,
            write_buffer: self.storage_write_buffer,
            target_file_size: self.storage_target_file_size,
            max_total_wal: self.storage_max_total_wal,
            max_background_jobs: self.storage_max_background_jobs,
            max_open_files: self.storage_max_open_files,
            retire_sweep: self.storage_retire_sweep,
        }
    }

    /// Produce the learner subsystem config: the learner tunables plus the
    /// store directory derived from the storage root.
    #[must_use]
    pub fn learner_config(&self) -> LearnerConfig {
        LearnerConfig {
            fetch_sweep_interval: self.learner_fetch_sweep_interval,
            purge_grace_period: self.learner_purge_grace_period,
            local_store_flush_interval: self.learner_local_store_flush_interval,
            local_store_dedup_ring: self.learner_local_store_dedup_ring,
            local_store_integrate_channel_capacity: self
                .learner_local_store_integrate_channel_capacity,
            quorum_read_deadtime: self.learner_quorum_read_deadtime,
            quorum_read_timeout: self.learner_quorum_read_timeout,
            stable_purge_ttl: self.learner_stable_purge_ttl,
        }
    }

    /// Produce the replica-log subsystem config: the replica-log tunables plus
    /// the log directory derived from the storage root and the node identity.
    #[must_use]
    pub fn replica_log_config(&self, effective_node_id: NodeId) -> ReplicaLogConfig {
        ReplicaLogConfig {
            purge_grace_period: self.replica_log_purge_grace_period,
            fetch_sweep_interval: self.replica_log_fetch_sweep_interval,
            subscribe_channel_capacity: self.replica_log_subscribe_channel_capacity,
            write_channel_capacity: self.replica_log_write_channel_capacity,
            write_out_capacity: self.replica_log_write_out_capacity,
            max_flush_batch: self.replica_log_max_flush_batch,
            effective_node_id,
        }
    }

    /// Produce the stable-log subsystem config: the engine sizing plus the log
    /// directory derived from the storage root.
    #[must_use]
    pub fn stable_log_config(&self) -> zslg_learner::StableLogConfig {
        zslg_learner::StableLogConfig {
            subscriber_queue_capacity: self.stable_log_subscriber_queue_capacity,
        }
    }

    /// Produce the leader-loop config bundle from the snapshot / membership /
    /// liveness / quorum-timing / network fields.
    #[must_use]
    pub fn leader_loop_config(&self, self_node_id: NodeId) -> zslg_leader::LeaderLoopConfig {
        zslg_leader::LeaderLoopConfig {
            snapshot: self.snapshot_config(),
            membership: self.membership_config(),
            liveness: self.liveness_config(),
            poll_interval: self.learner_quorum_poll_interval,
            quorum_wait_timeout: self.learner_quorum_wait_timeout,
            peers_watcher: self.peers_watcher(),
            self_server_name: self.network_server_name.clone(),
            self_node_id,
        }
    }

    /// Produce the snapshot-store constructor config.
    #[must_use]
    pub fn snapshot_store_config(&self) -> SnapshotStoreConfig {
        SnapshotStoreConfig {
            object_store: self.object_store.clone(),
        }
    }

    /// Validate all sub-configs and cross-field constraints.
    /// Produce the writer constructor config.
    #[must_use]
    pub fn writer_config(&self, node_id: NodeId) -> zslg_writer::WriterConfig {
        zslg_writer::WriterConfig {
            node_id,
            write_timeout: self.write_timeout,
        }
    }

    /// Compute the `PeerAddr` this node advertises to the cluster, given the
    /// socket `bind_server` returned.
    #[must_use]
    pub fn self_peer_addr(&self, server_addr: SocketAddr) -> PeerAddr {
        PeerAddr {
            addr: if server_addr.ip().is_unspecified() {
                HostPort::new(&self.network_server_name, server_addr.port())
            } else {
                HostPort::from(server_addr)
            },
            server_name: self.network_server_name.clone(),
        }
    }

    /// Produce the raft-startup config bundle: identity, bootstrap status, and
    /// timing. Active handles (`raft`, `raft_state`, `snapshot_store`, `router`)
    /// are passed directly to `run_raft_startup`.
    #[must_use]
    pub fn raft_startup_config(
        &self,
        node_id: NodeId,
        has_local_state: bool,
        peers_watcher: Arc<dyn PeersWatcher>,
        server_addr: SocketAddr,
    ) -> zslg_raft_setup::RaftStartupConfig {
        zslg_raft_setup::RaftStartupConfig {
            node_id,
            peers_watcher,
            is_bootstrap_node: self.is_bootstrap_node(),
            has_local_state,
            self_peer_addr: self.self_peer_addr(server_addr),
            startup_phase_timeout: self.raft_startup_phase_timeout,
        }
    }
}
