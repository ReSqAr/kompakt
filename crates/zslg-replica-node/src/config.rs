//! `ReplicaNodeConfig`: every knob of a replica node, the per-subsystem configs
//! derived from it, and the cross-field validation that keeps them consistent.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use zslg_error::Error;

use std::num::NonZeroUsize;

use bytesize::ByteSize;

#[cfg(test)]
use rocksdb_kit::ColumnFamily;
use zslg_consensus::SpareSelectionStrategy;
use zslg_net::PeerAddr;
use zslg_net::ServerName;
use zslg_object_store_config::IntoArcObjectStore;
use zslg_snapshot::CompactionTier;
use zslg_snapshot::CompressionLevel;

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
    /// `storage_memory`; the derivations live in `rocksdb_kit` and are
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
    /// See `StableLogConfig::subscriber_queue_capacity` — each stable-log
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
            replica_log_subscribe_channel_capacity: bytesize::nonzero(1024),
            replica_log_write_channel_capacity: bytesize::nonzero(4096),
            replica_log_write_out_capacity: bytesize::nonzero(2048),
            replica_log_max_flush_batch: bytesize::nonzero(1024),
            learner_fetch_sweep_interval: Duration::from_secs(2),
            learner_purge_grace_period: Duration::from_secs(5 * 60),
            learner_local_store_flush_interval: Duration::from_secs(1),
            learner_local_store_dedup_ring: NonZeroUsize::new(16_384),
            learner_local_store_compression: false,
            learner_local_store_integrate_channel_capacity: bytesize::nonzero(2048),
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
            subscribe_channel_capacity: bytesize::nonzero(1024),
            write_timeout: Duration::from_secs(10),
            db_path,
            object_store: object_store.into_arc_object_store()?,
            stable_log_subscriber_queue_capacity:
                zslg_learner::StableLogConfig::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
            stable_log_compression: true,
            drain_timeout: Duration::from_secs(30),
        })
    }
}

mod subsystems;
mod validate;

#[cfg(test)]
#[allow(clippy::expect_used, reason = "test code")]
mod compression_default_tests;
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests;
