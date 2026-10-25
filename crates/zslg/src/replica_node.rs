//! The full node: consensus, replication, reads and writes.
//!
//! `ReplicaNode` is the only node type that carries every role, and the other
//! three are proper subsets of it:
//!
//! | [`ReplicaNode`] | raft voter + replica log + writer + reader |
//! | [`FollowerNode`](crate::follower_node) | writer + reader, no raft, no replica log |
//! | [`LearnerNode`](crate::learner_node) | reader only |
//! | [`WriterNode`](crate::writer_node) | writer only, no durable state |
//!
//! This file is composition, not logic: it validates config, opens the stores in
//! dependency order, and hands every long-lived task to [`Shutdown`] so that any
//! one of them failing stops the node. Behaviour lives in the modules it wires
//! together.
//!
//! Leader-only work is not started here. Every leader job is spawned
//! unconditionally and gates on leadership itself (`crate::leader`), so
//! promotion and demotion move no tasks.

use futures::FutureExt as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use std::num::NonZeroUsize;

use crate::ByteSize;
use crate::api::PeersWatcher;
use crate::api::{RaftStateView, SnapshotStore};
use crate::database::Subsystem;
use crate::database::storage::{CfSpec, Storage, StorageConfig};
use crate::error::Error;
use crate::hlc::{HlcClock, HlcGate, run_clock_gauges};
use crate::ids::ServerName;
use crate::ids::{HostPort, NodeId, PeerAddr};
use crate::leader::spawn_leader_loop;
use crate::leader::{CompactionTier, LivenessConfig, SnapshotConfig};
use crate::learner::LearnerConfig;
use crate::learner::{LearnerService, spawn_learner, spawn_stable_store};
use crate::network::PeersMembershipGate;
use crate::network::{
    LocalReplicaClient, PeerRouter, RpcClientConfig, RpcServerConfig, SocketOpts, setup_router,
};
use crate::object_store::CompressionLevel;
use crate::object_store::IntoArcObjectStore;
use crate::object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use crate::raft::{
    EvictionStatus, NoOpRaftViewer, RaftNode, RaftStateHandle, run_raft_startup, setup_raft,
};
use crate::raft::{MembershipConfig, RaftConfig, SpareSelectionStrategy};
use crate::replica::ReplicaLogConfig;
use crate::replica::{ReplicaService, spawn_replica};
use crate::shutdown::Shutdown;
use crate::writer::WriterService;
use crate::{api, database, network};

#[derive(Debug, Clone)]
pub struct ReplicaNodeConfig {
    pub raft_heartbeat_interval: Duration,
    pub raft_election_timeout_min: Duration,
    pub raft_election_timeout_max: Duration,
    pub raft_startup_phase_timeout: Duration,
    /// Bound on one fenced replicated-state write. Backstop against a
    /// partitioned leader stalling the integrator; must exceed
    /// `raft_election_timeout_max`.
    pub raft_client_write_timeout: Duration,
    pub raft_snapshot_logs_since_last: u64,
    pub raft_max_in_snapshot_log_to_keep: u64,
    pub snapshot_interval: Duration,
    pub snapshot_cutoff_timeout: Duration,
    pub snapshot_compaction_tiers: Vec<CompactionTier>,
    pub snapshot_compaction_interval: Duration,
    pub snapshot_cleanup_retention: Duration,
    pub snapshot_cleanup_interval: Duration,
    pub snapshot_compression: CompressionLevel,
    /// Soft cap on entries per snapshot object; see
    /// `SnapshotConfig::max_snapshot_entries`.
    pub snapshot_max_entries: std::num::NonZeroUsize,
    pub replica_log_purge_grace_period: Duration,
    pub replica_log_fetch_sweep_interval: Duration,
    /// Total memory the storage engine may hold on this node: block cache,
    /// index and filter blocks, and the memtables charged against it. Sized
    /// from here rather than per store, because a node runs one DB and its
    /// budget is fixed when that DB opens.
    ///
    /// Everything else scales off it — memtable size, SST target size, retained
    /// WAL, open file count — so this is the one number to move. Expect process
    /// RSS above it: table readers, compaction buffers, iterator-pinned blocks
    /// and allocator slack sit outside the budget.
    pub storage_memory: ByteSize,
    /// RocksDB overrides. `None` — the default — derives the term from
    /// `storage_memory`; the derivations live in `database::storage` and are
    /// the only place a value is written, so unsetting a knob is exactly the
    /// pre-knob behaviour. Set one only with a measurement behind it.
    ///
    /// `storage_write_buffer` is one memtable, per column family;
    /// `storage_target_file_size` the SST size compaction aims at;
    /// `storage_max_total_wal` the retained-WAL cap that forces a flush;
    /// `storage_max_background_jobs` the compaction/flush thread count (the
    /// lever when RSS overshoots the budget, since compaction buffers sit
    /// outside it); `storage_max_open_files` the open-SST cap (-1 = unlimited),
    /// the other term that escapes the budget as the dataset grows;
    /// `storage_retire_sweep` how often aged SSTs are rewritten so purged
    /// space is reclaimed (default 1 h).
    pub storage_write_buffer: Option<ByteSize>,
    pub storage_target_file_size: Option<ByteSize>,
    pub storage_max_total_wal: Option<ByteSize>,
    pub storage_max_background_jobs: Option<i32>,
    pub storage_max_open_files: Option<i32>,
    pub storage_retire_sweep: Option<Duration>,
    /// How the replica log's runs are stored. Uncompressed by default: it is
    /// the second-highest-volume store, and the per-block CRC32 rather than
    /// zstd is what protects a committed record.
    pub replica_log_compression: bool,
    /// Live-tail channel depth per replica-log subscriber. Must be >=
    /// `replica_log_max_flush_batch`, or one flush batch sheds every
    /// subscriber on the node however fast it is draining — the evicted
    /// subscriber then reconnects and re-scans from its cursor, burning the
    /// cores that caused the shed. Validated, not merely documented.
    pub replica_log_subscribe_channel_capacity: NonZeroUsize,
    /// See `ReplicaLogConfig::{write_channel_capacity, write_out_capacity}` —
    /// the two payload-carrying queues on the replica write path.
    pub replica_log_write_channel_capacity: NonZeroUsize,
    pub replica_log_write_out_capacity: NonZeroUsize,
    /// Max records drained into one replica-log append + fsync.
    pub replica_log_max_flush_batch: NonZeroUsize,
    pub learner_fetch_sweep_interval: Duration,
    pub learner_purge_grace_period: Duration,
    pub learner_local_store_flush_interval: Duration,
    /// Entries in the local store's recent-`DataId` filter, which drops the
    /// duplicate copies of a write arriving from each replica this node
    /// subscribes to. `None` disables it.
    ///
    /// Sized in records, so it spans less arrival skew as throughput rises.
    /// Measured plateau 10k-50k; 8,192 is worth only +28.5% and 1M regresses.
    pub learner_local_store_dedup_ring: Option<NonZeroUsize>,
    /// How the local store's runs are stored. Uncompressed by default: it is a
    /// rebuildable cache and the highest-volume column family on the node,
    /// and compression is a footprint/CPU trade rather than a durability one.
    pub learner_local_store_compression: bool,
    /// Depth of the node's shared local-store integration channel, which every
    /// subscription forwards into. Sized for a few batches across all
    /// upstreams so readers keep producing while a commit is in flight.
    pub learner_local_store_integrate_channel_capacity: NonZeroUsize,
    pub learner_quorum_read_deadtime: Duration,
    pub learner_quorum_read_timeout: Duration,
    pub learner_quorum_wait_timeout: Duration,
    pub learner_quorum_poll_interval: Duration,
    pub learner_stable_purge_ttl: Option<Duration>,
    pub hlc_max_clock_skew: Duration,
    pub membership_replica_node_count: u32,
    pub membership_replica_node_max_failures: u32,
    pub membership_forced_node_removal_timeout: Duration,
    pub membership_poll_interval: Duration,
    pub membership_spare_selection: SpareSelectionStrategy,
    pub liveness_probe_interval: Duration,
    pub liveness_probe_timeout: Duration,
    pub network_server_name: ServerName,
    pub peers: Vec<PeerAddr>,
    pub network_bootstrap_server_name: ServerName,
    pub network_listen_addr: SocketAddr,
    pub keepalive_interval: Duration,
    pub user_timeout: Duration,
    pub keepalive_retries: u32,
    pub max_unary_conns_per_peer: usize,
    pub max_idle_unary_conns_per_peer: usize,
    pub socket_buffer: ByteSize,
    pub unary_timeout: Duration,
    pub connect_timeout: Duration,
    pub pool_channel_capacity: usize,
    /// Receive-buffer depth for a replica-log subscription. Matches the
    /// replica's live-channel depth by default; a smaller value would just
    /// move the bottleneck one hop downstream.
    pub subscribe_channel_capacity: NonZeroUsize,
    pub write_timeout: Duration,
    pub db_path: PathBuf,
    pub object_store: Arc<dyn object_store::ObjectStore>,
    /// See [`StableLogConfig::subscriber_queue_capacity`] — each stable-log
    /// subscriber gets a queue this deep, and each queued event carries an
    /// entry payload, so this bounds memory per live subscriber. A consumer
    /// that overruns it is hung up on and replays from its cursor.
    pub stable_log_subscriber_queue_capacity: std::num::NonZeroUsize,
    /// How the stable log's runs are stored. Keeps zstd: it is the CQRS replay
    /// source and is retained indefinitely, so footprint dominates CPU here.
    pub stable_log_compression: bool,
    pub drain_timeout: Duration,
}

impl ReplicaNodeConfig {
    pub fn new(
        server_name: ServerName,
        peers: Vec<PeerAddr>,
        bootstrap_server_name: ServerName,
        listen_addr: SocketAddr,
        db_path: PathBuf,
        object_store: impl IntoArcObjectStore,
    ) -> Result<Self, Error> {
        Ok(Self {
            raft_heartbeat_interval: Duration::from_millis(500),
            raft_election_timeout_min: Duration::from_millis(2000),
            raft_election_timeout_max: Duration::from_millis(4000),
            raft_startup_phase_timeout: Duration::from_secs(10),
            raft_client_write_timeout: Duration::from_secs(30),
            raft_snapshot_logs_since_last: 100,
            raft_max_in_snapshot_log_to_keep: 10,
            snapshot_interval: Duration::from_secs(5),
            snapshot_cutoff_timeout: Duration::from_millis(200),
            snapshot_compaction_tiers: vec![
                Duration::from_secs(60).into(),
                Duration::from_secs(60 * 15).into(),
                Duration::from_secs(60 * 60).into(),
                Duration::from_secs(60 * 60 * 24).into(),
                Duration::from_secs(60 * 60 * 24 * 7).into(),
                Duration::from_secs(60 * 60 * 24 * 30).into(),
                Duration::from_secs(60 * 60 * 24 * 365).into(),
            ],
            snapshot_compaction_interval: Duration::from_secs(60),
            snapshot_cleanup_retention: Duration::from_secs(60 * 60),
            snapshot_cleanup_interval: Duration::from_secs(5 * 60),
            snapshot_compression: CompressionLevel::standard(),
            snapshot_max_entries: std::num::NonZeroUsize::new(100_000)
                .unwrap_or(std::num::NonZeroUsize::MIN),
            replica_log_purge_grace_period: Duration::from_secs(5 * 60),
            replica_log_fetch_sweep_interval: Duration::from_secs(2),
            storage_memory: ByteSize::from_mib(256),
            storage_write_buffer: None,
            storage_target_file_size: None,
            storage_max_total_wal: None,
            storage_max_background_jobs: None,
            storage_max_open_files: None,
            storage_retire_sweep: None,
            replica_log_compression: false,
            replica_log_subscribe_channel_capacity: crate::unit::nonzero(1024),
            replica_log_write_channel_capacity: crate::unit::nonzero(4096),
            replica_log_write_out_capacity: crate::unit::nonzero(2048),
            replica_log_max_flush_batch: crate::unit::nonzero(1024),
            learner_fetch_sweep_interval: Duration::from_secs(2),
            learner_purge_grace_period: Duration::from_secs(5 * 60),
            learner_local_store_flush_interval: Duration::from_secs(1),
            learner_local_store_dedup_ring: NonZeroUsize::new(16_384),
            learner_local_store_compression: false,
            learner_local_store_integrate_channel_capacity: crate::unit::nonzero(2048),
            learner_quorum_read_deadtime: Duration::from_millis(5),
            learner_quorum_read_timeout: Duration::from_secs(10),
            learner_quorum_wait_timeout: Duration::from_secs(10),
            learner_quorum_poll_interval: Duration::from_millis(20),
            learner_stable_purge_ttl: None,
            hlc_max_clock_skew: Duration::from_millis(200),
            membership_replica_node_count: 3,
            membership_replica_node_max_failures: 5,
            membership_forced_node_removal_timeout: Duration::from_secs(10),
            membership_poll_interval: Duration::from_secs(1),
            membership_spare_selection: SpareSelectionStrategy::Random,
            liveness_probe_interval: Duration::from_millis(500),
            liveness_probe_timeout: Duration::from_secs(1),
            network_server_name: server_name,
            peers,
            network_bootstrap_server_name: bootstrap_server_name,
            network_listen_addr: listen_addr,
            keepalive_interval: Duration::from_secs(5),
            user_timeout: Duration::from_secs(15),
            keepalive_retries: 3,
            max_unary_conns_per_peer: 32,
            max_idle_unary_conns_per_peer: 8,
            socket_buffer: ByteSize::from_mib(2),
            unary_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            pool_channel_capacity: 4096,
            subscribe_channel_capacity: crate::unit::nonzero(1024),
            write_timeout: Duration::from_secs(10),
            db_path,
            object_store: object_store.into_arc_object_store()?,
            stable_log_subscriber_queue_capacity:
                crate::learner::StableLogConfig::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
            stable_log_compression: true,
            drain_timeout: Duration::from_secs(30),
        })
    }

    /// Produce the raft subsystem config: the raft tunables plus the node's
    /// durable identity. The raft store's two column families come from the
    /// [`Storage`] opened in `start`, not from a path in here.
    #[must_use]
    pub(crate) fn raft_config(&self, node_id: NodeId) -> RaftConfig {
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
    pub(crate) fn snapshot_config(&self) -> SnapshotConfig {
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
    pub(crate) fn membership_config(&self) -> MembershipConfig {
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
    pub(crate) fn liveness_config(&self) -> LivenessConfig {
        LivenessConfig {
            probe_interval: self.liveness_probe_interval,
            probe_timeout: self.liveness_probe_timeout,
        }
    }

    /// Produce the outbound router's client config.
    #[must_use]
    pub(crate) fn router_config(&self) -> RpcClientConfig {
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
    pub(crate) fn peers_watcher(&self) -> Arc<dyn PeersWatcher> {
        crate::network::static_peers(self.peers.clone())
    }

    /// Produce the RPC server bind config, including the TCP socket tuning and
    /// the node's durable identity.
    #[must_use]
    pub(crate) fn rpc_server_config(&self, node_id: NodeId) -> RpcServerConfig {
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
    pub(crate) fn is_bootstrap_node(&self) -> bool {
        self.network_server_name == self.network_bootstrap_server_name
    }

    /// Every column family a replica node runs — the complete schema of its one
    /// RocksDB. RocksDB refuses to open a root whose on-disk family set is not
    /// fully named here, so adding a family to this list is a wipe; and the
    /// length is what sizes each memtable, so a family that is listed but never
    /// used still costs the ones that are.
    #[must_use]
    pub(crate) fn column_families(&self) -> Vec<CfSpec> {
        let mut cfs = Vec::from(crate::raft::raft_cf_specs());
        cfs.extend([
            // Compression is a per-store footprint/CPU trade, never a
            // durability one — the block CRC is what protects a record.
            CfSpec {
                subsystem: Subsystem::ReplicaLog,
                compress: self.replica_log_compression,
            },
            CfSpec {
                subsystem: Subsystem::StableLog,
                compress: self.stable_log_compression,
            },
            CfSpec {
                subsystem: Subsystem::LocalStoreData,
                compress: self.learner_local_store_compression,
            },
            // Cursors follow the data family rather than carrying a second
            // knob: one overwritten slot per peer has no workload behind it.
            CfSpec {
                subsystem: Subsystem::LocalStoreCursors,
                compress: self.learner_local_store_compression,
            },
            // One tiny, never-compressed row: the write-once node identity.
            CfSpec {
                subsystem: Subsystem::NodeId,
                compress: false,
            },
        ]);
        cfs
    }

    /// Produce the storage tuning: the budget plus the overrides, with every
    /// unset one left to `database::storage`'s derivation from the budget.
    #[must_use]
    pub(crate) fn storage_config(&self) -> StorageConfig {
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
    pub(crate) fn learner_config(&self) -> LearnerConfig {
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
    pub(crate) fn replica_log_config(&self, effective_node_id: NodeId) -> ReplicaLogConfig {
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
    pub(crate) fn stable_log_config(&self) -> crate::learner::StableLogConfig {
        crate::learner::StableLogConfig {
            subscriber_queue_capacity: self.stable_log_subscriber_queue_capacity,
        }
    }

    /// Produce the leader-loop config bundle from the snapshot / membership /
    /// liveness / quorum-timing / network fields.
    #[must_use]
    pub(crate) fn leader_loop_config(
        &self,
        self_node_id: NodeId,
    ) -> crate::leader::LeaderLoopConfig {
        crate::leader::LeaderLoopConfig {
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
    pub(crate) fn snapshot_store_config(&self) -> SnapshotStoreConfig {
        SnapshotStoreConfig {
            object_store: self.object_store.clone(),
        }
    }

    /// Validate all sub-configs and cross-field constraints.
    /// Produce the writer constructor config.
    #[must_use]
    pub(crate) fn writer_config(&self, node_id: NodeId) -> crate::writer::WriterConfig {
        crate::writer::WriterConfig {
            node_id,
            write_timeout: self.write_timeout,
        }
    }

    /// Compute the `PeerAddr` this node advertises to the cluster, given the
    /// socket `bind_server` returned.
    #[must_use]
    pub(crate) fn self_peer_addr(&self, server_addr: SocketAddr) -> PeerAddr {
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
    pub(crate) fn raft_startup_config(
        &self,
        node_id: NodeId,
        has_local_state: bool,
        peers_watcher: Arc<dyn PeersWatcher>,
        server_addr: SocketAddr,
    ) -> crate::raft::RaftStartupConfig {
        crate::raft::RaftStartupConfig {
            node_id,
            peers_watcher,
            is_bootstrap_node: self.is_bootstrap_node(),
            has_local_state,
            self_peer_addr: self.self_peer_addr(server_addr),
            startup_phase_timeout: self.raft_startup_phase_timeout,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
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
            return Err(Error::InvalidConfiguration(
                "peers must not be empty".to_owned(),
            ));
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
            return Err(Error::InvalidConfiguration(format!(
                "peers must include this node's own server_name {:?}; \
                 configured peers: {:?}",
                self.network_server_name,
                self.peers
                    .iter()
                    .map(|p| &p.server_name)
                    .collect::<Vec<_>>()
            )));
        }
        self.writer_config(dummy).validate()?;
        if self.db_path.as_os_str().is_empty() {
            return Err(Error::InvalidConfiguration(
                "db_path must not be empty".to_owned(),
            ));
        }
        self.stable_log_config().validate()?;
        self.storage_config().validate()?;

        // Cross-field (snapshot vs HLC): the HLC skew bound must cover the
        // snapshot cutoff timeout. Lives here — the node config is where both
        // subsystems' knobs meet.
        if self.hlc_max_clock_skew < self.snapshot_cutoff_timeout {
            return Err(Error::InvalidConfiguration(format!(
                "hlc_max_clock_skew ({:?}) must be >= snapshot_cutoff_timeout ({:?})",
                self.hlc_max_clock_skew, self.snapshot_cutoff_timeout
            )));
        }

        if self.drain_timeout.is_zero() {
            return Err(Error::InvalidConfiguration(
                "drain_timeout must be non-zero".to_owned(),
            ));
        }

        // Cross-field: cleanup_retention must be >= purge_grace_period
        if self.snapshot_cleanup_retention < self.replica_log_purge_grace_period {
            return Err(Error::InvalidConfiguration(format!(
                "cleanup_retention ({:?}) must be >= purge_grace_period ({:?})",
                self.snapshot_cleanup_retention, self.replica_log_purge_grace_period
            )));
        }

        // Cross-field (replica purge vs quorum read): a quorum read waits for
        // the stable log to reach the manifest watermark it saw when it started.
        // Entries a replica purges while the read is still catching up to it
        // are recovered only via a later snapshot, so they must sit below that
        // watermark — which holds while the manifest cannot outrun the grace
        // within one read. The read is bounded by its timeout, hence the floor.
        // It does not cover a manifest catching up in one jump after a stall.
        if self.replica_log_purge_grace_period < self.learner_quorum_read_timeout {
            return Err(Error::InvalidConfiguration(format!(
                "replica_log_purge_grace_period ({:?}) must be >= \
                 learner_quorum_read_timeout ({:?})",
                self.replica_log_purge_grace_period, self.learner_quorum_read_timeout
            )));
        }

        Ok(())
    }
}

pub struct ReplicaNode {
    config: ReplicaNodeConfig,
    node_id: NodeId,
    hlc_clock: Arc<HlcClock>,
    raft_state: RaftStateHandle,
    server_addr: std::net::SocketAddr,
    writer: Arc<WriterService>,
    replica: Arc<ReplicaService>,
    learner: Arc<LearnerService>,
    raft: Arc<RaftNode>,
    /// Published by `raft::startup::rejoin::RejoinMonitor`; consulted by
    /// [`Self::can_serve_clients`] and [`Self::reader`]'s [`GatedReader`](crate::learner::GatedReader).
    /// Cheap to clone (one `Arc<AtomicBool>`) — held here rather than
    /// reconstructed per call.
    eviction: EvictionStatus,
    _self_client: Arc<dyn api::ReplicaClient>,
    /// Held for shutdown only: its background retire compactions must be joined
    /// before the node's `Storage` handle drops.
    stable_log: Arc<crate::learner::StableLog>,
    /// The node's one RocksDB. Held so [`run`](Self::run) can join the
    /// background retire compactions before the handle drops — a detached one
    /// keeps the DB, and its LOCK, alive past this node.
    storage: Arc<Storage>,
    shutdown: Shutdown,
}

impl std::fmt::Debug for ReplicaNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaNode")
            .field("node_id", &self.node_id)
            .field("server_addr", &self.server_addr)
            .finish_non_exhaustive()
    }
}

/// A [`ReplicaNode`] that is running and serving its peers but has not yet
/// caught up to a quorum.
///
/// This is a real state, not a wrapper: during it the node has no learner, no
/// writer and no leader loop, because those are constructed only after the
/// freshness gate. There is therefore nothing to read from and nothing to write
/// to, which is why this type exposes neither `reader()` nor `writer()` — the
/// services do not exist.
///
/// Serving peers while not yet ready is what lets a whole cluster cold-start:
/// every node answers `fetch_status` for the others while none of them is
/// caught up, so a quorum can form.
pub struct InitialisingReplicaNode {
    config: ReplicaNodeConfig,
    node_id: NodeId,
    hlc_clock: Arc<HlcClock>,
    raft_state: RaftStateHandle,
    raft_state_view: Arc<dyn RaftStateView>,
    server_addr: std::net::SocketAddr,
    replica: Arc<ReplicaService>,
    raft: Arc<RaftNode>,
    snapshot_store: Arc<dyn SnapshotStore>,
    stable_log: Arc<crate::learner::StableLog>,
    storage: Arc<Storage>,
    router: Arc<PeerRouter>,
    has_local_state: bool,
    _self_client: Arc<dyn api::ReplicaClient>,
    shutdown: Shutdown,
}

impl std::fmt::Debug for InitialisingReplicaNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitialisingReplicaNode")
            .field("node_id", &self.node_id)
            .field("server_addr", &self.server_addr)
            .finish_non_exhaustive()
    }
}

impl InitialisingReplicaNode {
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The node's raft state view. Watching it during catch-up is exactly what
    /// an operator wants while a cluster is forming.
    #[must_use]
    pub fn state_handle(&self) -> &RaftStateHandle {
        &self.raft_state
    }

    /// Cancel this to stop a node that will never become ready.
    ///
    /// Not optional: the freshness gate retries indefinitely by design, so
    /// without it a node waiting for a quorum that never returns could not be
    /// stopped. Capture it **before** calling [`Self::wait_until_ready`], which
    /// consumes `self`.
    ///
    /// Cancelling makes a pending [`Self::wait_until_ready`] return
    /// [`Error::StartupCancelled`] promptly — a distinct variant precisely so an
    /// operator-requested stop is not mistaken for a startup fault — after
    /// shutting the node down gracefully. Cancelling it before or after that
    /// call is equally fine; a node that is only ever [`Self::run`] sees the same
    /// token.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.token()
    }

    /// True from construction: the server is bound and peers are being served.
    ///
    /// This is the signal an orchestrator's readiness probe should use. It goes
    /// green without needing a quorum, which is what lets a cold start converge
    /// under any pod-management policy.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        true
    }

    /// Run to completion (i.e. until the shutdown token is cancelled).
    ///
    /// The counterpart to [`ReplicaNode::run`] for a node that never became
    /// ready: same drain, same Raft shutdown, so a cold-start abort leaves the
    /// same state behind as a graceful stop of a ready node.
    pub async fn run(self) -> Result<(), Error> {
        let drain_timeout = self.config.drain_timeout;
        let raft = self.raft.clone();
        let result = self.shutdown.run(drain_timeout).await;
        // All background tasks have drained — shut down Raft, as `ReplicaNode::run`
        // does.
        let _ = raft.shutdown().await;
        result
    }

    /// Wait until this node's state is at least as fresh as a responding
    /// quorum, then finish construction.
    ///
    /// Consumes `self`. On failure the node is shut down gracefully here rather
    /// than handed back — there is nothing a caller could do with a node whose
    /// startup failed except stop it.
    ///
    /// Returns [`Error::StartupCancelled`] if [`Self::shutdown_token`] fires
    /// while the gate is still waiting. That is a clean stop, not a fault: it is
    /// the documented way to abandon a node whose quorum never returns, and it
    /// is a separate variant so a caller can log it as such instead of alerting.
    ///
    /// **Cancel via [`Self::shutdown_token`], not by dropping this future.**
    /// Dropping it drops the node, which aborts its `JoinSet` mid-flight — the
    /// hard-kill path, not a graceful stop.
    pub async fn wait_until_ready(self: Box<Self>) -> Result<ReplicaNode, Error> {
        match self.into_ready().boxed().await {
            Ok(node) => Ok(node),
            Err(boxed) => {
                let (shutdown, drain_timeout, raft, e) = *boxed;
                shutdown.token().cancel();
                // Best-effort: report the startup failure, not a shutdown error
                // that would mask it.
                let _ = shutdown.run(drain_timeout).await;
                let _ = raft.shutdown().await;
                Err(e)
            }
        }
    }

    /// The second half of construction. The error arm carries the pieces
    /// [`Self::wait_until_ready`] needs to stop the node it can no longer
    /// return; it is internal only and deliberately not exposed.
    #[allow(
        clippy::type_complexity,
        reason = "the tuple is boxed for clippy::result_large_err, not exposed"
    )]
    async fn into_ready(
        self,
    ) -> Result<ReplicaNode, Box<(Shutdown, Duration, Arc<RaftNode>, Error)>> {
        let Self {
            config,
            node_id,
            hlc_clock,
            raft_state,
            raft_state_view,
            server_addr,
            replica,
            raft,
            snapshot_store,
            stable_log,
            storage,
            router,
            has_local_state,
            _self_client,
            mut shutdown,
        } = self;
        let drain_timeout = config.drain_timeout;

        // `?` would drop `shutdown` on the error path, aborting every task where
        // it stands; each step therefore hands the shutdown handle back via an
        // explicit match instead of `?`.
        let startup_config = config.raft_startup_config(
            node_id,
            has_local_state,
            config.peers_watcher(),
            server_addr,
        );
        // The freshness gate retries indefinitely by design: every phase inside
        // `run_raft_startup` (fence read, join, quorum generation, catch-up) loops
        // on transient failure and only escalates its logging. That is right for
        // an unattended cold start and wrong for an operator who wants the node
        // to stop, so cancellation is imposed *here*, at the one place that owns
        // the token, rather than threaded through every retry loop in
        // `raft::startup::bootstrap`.
        //
        // Dropping the startup future at an await point is the intended effect —
        // the node is being stopped, and `wait_until_ready` shuts it down (drain
        // + `raft.shutdown()`) on this error exactly as on any other.
        let cancelled = shutdown.token();
        let startup = tokio::select! {
            res = run_raft_startup(
                raft.clone(),
                raft_state_view.clone(),
                snapshot_store.clone(),
                router.clone(),
                startup_config,
                &mut shutdown,
            ).boxed() => res,
            () = cancelled.cancelled() => {
                info!("raft startup cancelled — node stopped before it became ready");
                Err(Error::StartupCancelled)
            }
        };
        let eviction = match startup {
            Ok(v) => v,
            Err(e) => return Err(Box::new((shutdown, drain_timeout, raft, e))),
        };
        info!("raft startup complete");

        let learner = match spawn_learner(
            hlc_clock.clone(),
            stable_log.clone(),
            storage.clone(),
            snapshot_store.clone(),
            raft_state_view.clone(),
            router.clone(),
            &config.learner_config(),
            &mut shutdown,
        )
        .boxed()
        .await
        {
            Ok(v) => v,
            Err(e) => return Err(Box::new((shutdown, drain_timeout, raft, e))),
        };
        info!("learner spawned");

        spawn_leader_loop(
            &mut shutdown,
            hlc_clock.clone(),
            learner.local_store(),
            stable_log.clone(),
            snapshot_store.clone(),
            raft.clone(),
            raft.clone(),
            raft_state_view.clone(),
            router.clone(),
            config.leader_loop_config(node_id),
        );
        info!("leader loop spawned");

        let writer = Arc::new(WriterService::new(
            router.clone(),
            Arc::new(NoOpRaftViewer::new(raft_state_view.clone())),
            hlc_clock.clone(),
            config.writer_config(node_id),
        ));

        Ok(ReplicaNode {
            config,
            node_id,
            hlc_clock,
            raft_state,
            server_addr,
            writer,
            replica,
            learner,
            raft,
            eviction,
            _self_client,
            stable_log,
            storage,
            shutdown,
        })
    }
}

impl ReplicaNode {
    /// Start the node's local services and bind its RPC server, returning as
    /// soon as it is serving its peers.
    ///
    /// It deliberately does **not** wait for the node to catch up to a quorum:
    /// that is [`InitialisingReplicaNode::wait_until_ready`]. Splitting the two
    /// is what makes a whole-cluster cold start expressible — every node can be
    /// started (and so answer its peers) before any of them is awaited.
    pub async fn start(config: ReplicaNodeConfig) -> Result<Box<InitialisingReplicaNode>, Error> {
        config.validate()?;

        let mut shutdown = Shutdown::new();
        database::create_storage_root(&config.db_path)
            .boxed()
            .await?;

        // The node's ONE RocksDB: one root, one block cache, one column family
        // per store. The budget is an argument rather than a process-wide
        // static because it is a *node's* property — the loadgen runs six nodes
        // in one process.
        let storage = Storage::open(
            &config.db_path,
            config.storage_config(),
            &config.column_families(),
        )?;

        let node_id = database::load_or_create_node_id(&storage)?;
        info!(node_id = ?node_id, "node_id loaded or created");

        let snapshot_store: Arc<dyn SnapshotStore> = Arc::new(RemoteSnapshotStore::new(
            config.snapshot_store_config(),
            shutdown.token(),
        ));

        let hlc_clock = Arc::new(HlcClock::new(config.hlc_max_clock_skew));
        shutdown.spawn(
            "clock_gauges",
            run_clock_gauges(hlc_clock.clone(), shutdown.token()),
        );
        let hlc_gate = Arc::new(HlcGate::new());

        let router = setup_router(hlc_clock.clone(), config.router_config(), shutdown.token())?;

        let raft_state = RaftStateHandle::new();
        let raft_state_view: Arc<dyn RaftStateView> = Arc::new(raft_state.clone());
        let membership_gate = Arc::new(PeersMembershipGate::new(config.peers_watcher()));
        // Spawned here rather than at the top of `start`: the storage-memory
        // gauge reads this node's `Storage`, so the tick has to know it.
        shutdown.spawn(
            "process_gauges",
            crate::observability::run_process_gauges(
                Some(storage.clone()),
                // A replica node runs both pipelines: the replica-side write
                // queue and live-tail rings, and the learner-side integrate
                // channel and dedup ring. saturating: two independently-set
                // operator knobs, and an overflowed sum must read as huge.
                Some(
                    config
                        .replica_log_config(node_id)
                        .queue_capacity_bytes()
                        .saturating_add(config.learner_config().queue_capacity_bytes()),
                ),
                shutdown.token(),
            ),
        );
        let (raft, has_local_state) = setup_raft(
            router.clone(),
            raft_state.clone(),
            storage.clone(),
            &config.raft_config(node_id),
            membership_gate,
            &mut shutdown,
        )
        .boxed()
        .await?;
        info!(?has_local_state, "raft setup complete");

        let stable_log = spawn_stable_store(&config.stable_log_config(), storage.clone())
            .boxed()
            .await?;
        info!("stable log initialized");

        let replica = spawn_replica(
            raft_state_view.clone(),
            &hlc_clock,
            &hlc_gate,
            storage.clone(),
            &config.replica_log_config(node_id),
            &mut shutdown,
            config.peers_watcher(),
        )
        .boxed()
        .await?;
        info!("replica node spawned");

        // Self/loopback client: created first so it can be shared with bind_server
        // and PeerRouter. The manager holds a `Weak` to it, so it must be
        // kept alive for the node's lifetime (stored in `ReplicaNode`).
        let self_client: Arc<dyn api::ReplicaClient> = Arc::new(LocalReplicaClient::new(
            replica.clone(),
            raft.clone(),
            raft_state_view.clone(),
        ));

        let server_addr = network::bind_server(
            self_client.clone(),
            hlc_clock.clone(),
            config.rpc_server_config(node_id),
            &mut shutdown,
        )?;
        info!(addr = %server_addr, "server bound");

        let router = Arc::new(PeerRouter::new(
            node_id,
            Arc::downgrade(&self_client),
            router,
        ));

        Ok(Box::new(InitialisingReplicaNode {
            config,
            node_id,
            hlc_clock,
            raft_state,
            raft_state_view,
            server_addr,
            replica,
            raft,
            snapshot_store,
            stable_log,
            storage,
            router,
            has_local_state,
            _self_client: self_client,
            shutdown,
        }))
    }

    /// True unless a reachable peer has affirmatively denied this node's
    /// rejoin attempt (removed it from `config.peers`) — i.e. whether client
    /// reads will succeed. Distinct from `is_serving`.
    ///
    /// NOT the openraft voter set: under a `retain: false` eviction (what
    /// production eviction actually performs, `leader::jobs::voters`) the
    /// leader stops replicating to this node entirely, so its own view of
    /// `voters` would freeze and never reflect the removal — the same reason
    /// the gate is "has a peer told us we're out", not "do we still see
    /// ourselves as in". See `raft::startup::rejoin::EvictionStatus`.
    #[must_use]
    pub fn can_serve_clients(&self) -> bool {
        crate::learner::can_serve_clients(&self.eviction)
    }

    /// Remove `victim` from this node's openraft voter set. Test-only:
    /// exercises group removal (CLAUDE.md: membership is the group's
    /// decision) without standing up a full `leader:voters` reconciliation
    /// cycle.
    ///
    /// Demotes to learner (`retain: true`) rather than dropping the node
    /// outright (production's `RaftLeadership::remove_member` uses
    /// `retain: false`, matching `leader::jobs::voters`'s permanent eviction).
    /// The distinction matters for *this test's observability*, not for
    /// production correctness: `retain: false` removes the node from
    /// openraft's membership map entirely, so the leader stops replicating to
    /// it and the victim's own view of `voters` can legitimately lag forever
    /// (`tests::reconciliation` hits exactly this and asserts on the
    /// *remaining* nodes instead). `retain: true` keeps it as a learner, which
    /// still receives `AppendEntries` — so its own metrics promptly reflect
    /// the demotion, which is what this test needs to assert on.
    #[cfg(test)]
    pub(crate) async fn remove_member_for_test(&self, victim: NodeId) -> Result<(), Error> {
        self.raft.demote_to_learner_for_test(victim).await
    }

    /// True from construction: a `ReplicaNode` only exists after its server is
    /// bound. The mirror of [`InitialisingReplicaNode::is_serving`], so a
    /// readiness probe can be written once against either state.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        true
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn writer(&self) -> Arc<dyn api::Writer> {
        self.writer.clone()
    }

    /// Reads are gated on voter-set membership (see [`Self::can_serve_clients`]):
    /// a node removed from the group stays up but must not serve a snapshot of
    /// replicated state that has stopped advancing. Writes are not gated here —
    /// `Writer::write` only coordinates a fan-out over the router and writes
    /// nothing locally, so this node's own membership does not bear on it (see
    /// `writer.rs`).
    #[must_use]
    pub fn reader(&self) -> Arc<dyn api::Reader> {
        Arc::new(crate::learner::GatedReader::new(
            self.learner.reader(),
            self.eviction.clone(),
            self.node_id,
        ))
    }

    /// Returns a clone of the global [`CancellationToken`] that all background
    /// tasks check. Cancelling it triggers a graceful shutdown of every
    /// subsystem. Useful for wiring the node into an external signal handler
    /// (e.g. SIGTERM) when using [`ReplicaNode::run`].
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.token()
    }

    /// Run the node until shutdown is requested or a background task fails.
    ///
    /// On the first unexpected task exit (error, panic, or clean completion
    /// without prior cancellation), cancels all remaining tasks, drains them
    /// with [`ReplicaNodeConfig::drain_timeout`], shuts down Raft, and returns
    /// the error.
    ///
    /// Returns `Ok(())` when the [`shutdown_token`](Self::shutdown_token) was
    /// cancelled externally **before** the first task exited — i.e. the
    /// embedder requested a graceful shutdown.
    ///
    /// Consumes `self`. Clone any needed handles ([`writer`](Self::writer),
    /// [`reader`](Self::reader), etc.) **before** calling this method.
    pub async fn run(self) -> Result<(), Error> {
        let drain_timeout = self.config.drain_timeout;
        let Self {
            config,
            node_id,
            hlc_clock,
            raft_state,
            server_addr,
            writer,
            replica,
            learner,
            raft,
            eviction,
            _self_client,
            stable_log,
            storage,
            shutdown,
        } = self;

        // Wait for first unexpected task exit, cancel + drain the rest.
        let result = shutdown.run(drain_timeout).await;

        // All background tasks have drained — shut down Raft.
        let _ = raft.shutdown().await;

        drop((
            config,
            node_id,
            hlc_clock,
            raft_state,
            server_addr,
            writer,
            replica,
            learner,
            eviction,
            _self_client,
            stable_log,
            storage,
        ));

        result
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn replica(&self) -> Arc<dyn api::Replica> {
        self.replica.clone()
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    /// This node's committed-and-derived Raft state view.
    ///
    /// Read the current state with [`RaftStateHandle::snapshot`] and follow
    /// changes with [`RaftStateHandle::subscribe`]; both are also available
    /// through the [`RaftStateView`](crate::RaftStateView) trait.
    ///
    /// Note which fields are replicated and which are local: `generation`,
    /// `replica` and `manifest_state` are the state machine's applied output
    /// and agree across nodes at the same generation, while `leader`,
    /// `raft_term` and `voters` are blended from *this* node's openraft
    /// metrics. In particular `voters` (Raft's membership) and `replica` (the
    /// set that serves writes) are different sets, and a membership job's
    /// whole task is moving one toward the other.
    ///
    /// This doc example is also the crate's check that the surface really is
    /// reachable from outside: a doc test compiles as a separate crate, so it
    /// fails if any of these items is `pub(crate)`.
    ///
    /// ```no_run
    /// use zslg::{RaftStateView, ReplicaMembership, ReplicaNode};
    ///
    /// fn inspect(node: &ReplicaNode) {
    ///     // Inherent methods, no trait import needed…
    ///     let state = node.state_handle().snapshot();
    ///     println!(
    ///         "leader={:?} term={} generation={} voters={:?}",
    ///         state.leader,
    ///         state.raft_term,
    ///         state.generation.raw(),
    ///         state.voters,
    ///     );
    ///     if let ReplicaMembership::Joint { old, next } = &state.replica {
    ///         println!("membership change in flight: {old:?} -> {next:?}");
    ///     }
    ///     for snapshot in &state.manifest_state.manifest {
    ///         println!("{:?} covers {:?}", snapshot.snapshot_id, snapshot.end_hlc);
    ///     }
    ///
    ///     // …and through the trait, which is what a generic consumer takes.
    ///     let _rx = RaftStateView::subscribe(node.state_handle());
    /// }
    /// ```
    #[must_use]
    pub fn state_handle(&self) -> &RaftStateHandle {
        &self.raft_state
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
    use crate::object_store::ObjectStoreConfig;

    fn ok_config() -> ReplicaNodeConfig {
        let peers = vec![PeerAddr {
            addr: "127.0.0.1:1".into(),
            server_name: "localhost".into(),
        }];
        ReplicaNodeConfig::new(
            "localhost".into(),
            peers,
            "localhost".into(),
            "127.0.0.1:0".parse().expect("addr"),
            PathBuf::from("/tmp/unused"),
            ObjectStoreConfig::InMemory,
        )
        .expect("ok_config")
    }

    // ── validate: baseline ──────────────────────────────────────────────

    #[test]
    fn defaults_validate_with_one_peer() {
        ok_config().validate().expect("valid");
    }

    /// The 20 MiB low-water config is the one that finds sizing bugs, so it
    /// stays runnable: it must validate, and — because every storage knob is
    /// unset by default — every derived term must come from that budget rather
    /// than from a constant that ignores it.
    #[test]
    fn the_low_water_budget_validates_and_derives_its_sizing() {
        let mut c = ok_config();
        c.storage_memory = ByteSize::from_mib(20);
        c.validate()
            .expect("the 20 MiB low-water config must stay valid");

        let low = c.storage_config();
        assert_eq!(low.memory, ByteSize::from_mib(20));
        assert!(
            low.write_buffer.is_none(),
            "sizing knobs default to derived"
        );
        assert!(low.max_total_wal.is_none());
        low.validate().expect("derived storage tuning is valid");
    }

    /// A knob a node *does* set is rejected at node-config validation, not at
    /// `Storage::open` in the middle of startup, and the message names it.
    #[test]
    fn a_bad_storage_knob_is_rejected_by_node_validation() {
        let mut c = ok_config();
        c.storage_write_buffer = Some(ByteSize::from_bytes(1));
        let err = c.validate().expect_err("must reject");
        assert!(
            format!("{err}").contains("storage_write_buffer"),
            "error must name the offending knob, got: {err}"
        );
    }

    // ── validate: constraint rejections ─────────────────────────────────

    #[test]
    fn rejects_cleanup_retention_below_purge_grace() {
        let mut c = ok_config();
        c.replica_log_purge_grace_period = Duration::from_secs(120);
        c.snapshot_cleanup_retention = Duration::from_secs(60);
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }

    #[test]
    fn rejects_purge_grace_below_quorum_read_timeout() {
        let mut c = ok_config();
        c.learner_quorum_read_timeout = Duration::from_secs(30);
        c.replica_log_purge_grace_period = Duration::from_secs(29);
        let err = c.validate().expect_err("must reject");
        assert!(
            format!("{err}").contains("learner_quorum_read_timeout"),
            "error must name both knobs, got: {err}"
        );
    }

    #[test]
    fn accepts_purge_grace_equal_to_quorum_read_timeout() {
        let mut c = ok_config();
        c.learner_quorum_read_timeout = Duration::from_secs(30);
        c.replica_log_purge_grace_period = Duration::from_secs(30);
        c.validate().expect("equality boundary admitted");
    }

    #[test]
    fn accepts_cleanup_retention_equal_to_purge_grace() {
        let mut c = ok_config();
        c.snapshot_interval = Duration::from_secs(60);
        c.snapshot_compaction_tiers[0] = Duration::from_secs(60).into();
        c.replica_log_purge_grace_period = Duration::from_secs(60);
        c.snapshot_cleanup_retention = Duration::from_secs(60);
        c.validate().expect("equality boundary admitted");
    }

    // ── ObjectStoreConfig ───────────────────────────────────────────────

    #[test]
    fn object_store_inmemory_builds() {
        let cfg = ObjectStoreConfig::InMemory;
        let _store = cfg.build().expect("InMemory store builds");
    }

    #[test]
    fn rejects_empty_db_path() {
        let mut c = ok_config();
        c.db_path = PathBuf::new();
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }

    // Cross-field (moved here from SnapshotConfig::validate): the HLC skew bound
    // must cover the snapshot cutoff timeout.
    #[test]
    fn rejects_max_clock_skew_below_snapshot_cutoff() {
        let mut c = ok_config();
        c.snapshot_cutoff_timeout = Duration::from_secs(1);
        c.hlc_max_clock_skew = Duration::from_millis(500);
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }

    #[test]
    fn accepts_max_clock_skew_equal_to_snapshot_cutoff() {
        let mut c = ok_config();
        c.snapshot_cutoff_timeout = Duration::from_secs(1);
        c.hlc_max_clock_skew = Duration::from_secs(1);
        c.validate().expect("equality boundary admitted");
    }

    // ── validate: own config must include self ──────────────────────────

    /// A node whose own `peers` omits its own server name is misconfigured: it
    /// has been told it is not part of this cluster. That is fatal at startup,
    /// and is deliberately different from the *group* removing it from the
    /// voter set — which is not fatal (see the rejoin monitor).
    #[test]
    fn validate_rejects_config_whose_peers_omit_self() {
        let cfg = ReplicaNodeConfig::new(
            "node-a".into(),
            vec![PeerAddr {
                addr: "127.0.0.1:9001".into(),
                server_name: "node-b".into(),
            }],
            "node-a".into(),
            "127.0.0.1:9000".parse().expect("addr"),
            PathBuf::from("/tmp/zslg-validate-test"),
            ObjectStoreConfig::InMemory,
        )
        .expect("config builds");

        let err = cfg.validate().expect_err("must reject");
        assert!(
            matches!(err, Error::InvalidConfiguration(ref m) if m.contains("node-a")),
            "the error must name the missing server name: {err:?}"
        );
    }

    /// The ordinary case still validates: self present among the peers.
    #[test]
    fn validate_accepts_config_whose_peers_include_self() {
        let cfg = ReplicaNodeConfig::new(
            "node-a".into(),
            vec![
                PeerAddr {
                    addr: "127.0.0.1:9000".into(),
                    server_name: "node-a".into(),
                },
                PeerAddr {
                    addr: "127.0.0.1:9001".into(),
                    server_name: "node-b".into(),
                },
            ],
            "node-a".into(),
            "127.0.0.1:9000".parse().expect("addr"),
            PathBuf::from("/tmp/zslg-validate-test"),
            ObjectStoreConfig::InMemory,
        )
        .expect("config builds");

        cfg.validate().expect("self is present, must accept");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, reason = "test code")]
mod compression_default_tests {
    use super::*;

    fn cfg() -> ReplicaNodeConfig {
        ReplicaNodeConfig::new(
            "localhost".into(),
            Vec::new(),
            "localhost".into(),
            "127.0.0.1:0".parse().expect("addr"),
            PathBuf::from("/tmp/unused"),
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        )
        .expect("config")
    }

    /// These defaults *are* the measured configuration (+22% for the local
    /// store alone, up to +36% with the replica log too). A silent drift would
    /// invalidate docs/OPTIMISATION.md, so pin them.
    #[test]
    fn compression_defaults_match_the_measured_configuration() {
        let c = cfg();
        assert!(!c.replica_log_compression);
        assert!(!c.learner_local_store_compression);
        assert!(c.stable_log_compression);
    }

    /// The flat fields must actually reach the column-family list — a default
    /// that is never carried down is worse than no default at all. This is now
    /// the *only* hop: compression is a property of the family, chosen once
    /// when the node's one DB opens.
    #[test]
    fn compression_reaches_the_column_families() {
        let c = cfg();
        let compress = |s: Subsystem| {
            c.column_families()
                .into_iter()
                .find(|spec| spec.subsystem == s)
                .map(|spec| spec.compress)
        };
        assert_eq!(compress(Subsystem::ReplicaLog), Some(false));
        assert_eq!(compress(Subsystem::StableLog), Some(true));
        assert_eq!(compress(Subsystem::LocalStoreData), Some(false));
        assert_eq!(compress(Subsystem::LocalStoreCursors), Some(false));
    }

    /// A replica runs seven families and no more. The list IS the schema — it
    /// is what RocksDB creates, and its length is what sizes each memtable —
    /// so a silent addition costs every other family memory and makes the
    /// root un-openable by a build that does not name it.
    #[test]
    fn a_replica_runs_exactly_seven_column_families() {
        let names: Vec<&str> = cfg()
            .column_families()
            .iter()
            .map(|spec| spec.subsystem.cf_name())
            .collect();
        assert_eq!(
            names,
            vec![
                "raft_log",
                "raft_meta",
                "replica_log",
                "stable_log",
                "local_store_data",
                "local_store_cursors",
                "node_id",
            ],
        );
    }
}
