//! A free-standing follower node: learner + writer merged.
//!
//! `FollowerNode` is the read-write counterpart of
//! `LearnerNode`: it has persistent identity,
//! durable local storage, subscribes to the replica cluster's log streams to
//! build a local data cache, serves the [`Reader`](zslg_log::Reader) API, *and* can
//! write data to a quorum of replicas. It joins no Raft and never binds a server.
//!
//! Its identity is persistent (a [`NodeId`], one write-once row of the node's
//! RocksDB) — the follower has durable state in its local store and stable
//! log, so the id must survive restarts.
//!
//! For write-stability tracking (metrics that verify successfully-written data
//! eventually lands in the stable log), see `examples/write_tracker.rs` — that
//! logic lives in the examples because it can be built entirely from the public
//! [`Writer`] + [`Reader`] traits.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;
use zslg_consensus::RaftStateView;
use zslg_learner::{LOCAL_STORE_CURSORS_CF, LOCAL_STORE_DATA_CF, STABLE_LOG_CF};
use zslg_lifecycle_types::ConfigError;
use zslg_net::NetError;
use zslg_node_id::NODE_ID_CF;
use zslg_snapshot::SnapshotStore;

use std::num::NonZeroUsize;

use bytesize::ByteSize;

use rocksdb_kit::{CfSpec, Storage, StorageConfig};
use shutdown::Shutdown;
use zslg_consensus::NodeId;
use zslg_hlc_clock::{HlcClock, run_clock_gauges};
use zslg_learner::{LearnerConfig, LearnerService, StableLog, spawn_learner, spawn_stable_store};
use zslg_net::PeerAddr;
use zslg_net_setup::setup_router;
use zslg_object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use zslg_object_store_config::IntoArcObjectStore;
use zslg_raft_tracker::{RaftTrackerConfig, spawn_raft_tracker_streamer};
use zslg_writer::WriterService;

/// Configuration for a follower node.
///
/// A follower is a learner + writer merged: it has durable local storage,
/// subscribes to replica streams, serves reads, and can write to quorum.
/// The config is essentially `LearnerNodeConfig`
/// — write-pool settings are flat on the node config itself.
#[derive(Debug, Clone)]
pub struct FollowerNodeConfig {
    pub hlc_max_clock_skew: Duration,
    pub learner_fetch_sweep_interval: Duration,
    pub learner_purge_grace_period: Duration,
    pub learner_local_store_flush_interval: Duration,
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
    /// the only place a value is written, so leaving a knob unset is exactly
    /// the pre-knob behaviour. Set one only with a measurement behind it.
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
    pub learner_stable_purge_ttl: Option<Duration>,
    pub raft_tracker_min_backoff: Duration,
    pub raft_tracker_max_backoff: Duration,
    pub peers: Vec<PeerAddr>,
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
    /// Maximum time to wait for a quorum of replicas to be discovered during
    /// [`FollowerNode::start`].
    pub startup_timeout: Duration,
    /// Maximum time to wait for background tasks to drain during shutdown
    /// before forcefully aborting them.
    pub drain_timeout: Duration,
}

impl FollowerNodeConfig {
    /// Create a follower config with defaults, customising only the required
    /// fields: the replica `peers` to subscribe to / write to and storage.
    pub fn new(
        peers: Vec<PeerAddr>,
        db_path: PathBuf,
        object_store: impl IntoArcObjectStore,
    ) -> Result<Self, Error> {
        Ok(Self {
            hlc_max_clock_skew: Duration::from_millis(200),
            learner_fetch_sweep_interval: Duration::from_secs(2),
            learner_purge_grace_period: Duration::from_secs(5 * 60),
            learner_local_store_flush_interval: Duration::from_secs(1),
            storage_memory: ByteSize::from_mib(256),
            storage_write_buffer: None,
            storage_target_file_size: None,
            storage_max_total_wal: None,
            storage_max_background_jobs: None,
            storage_max_open_files: None,
            storage_retire_sweep: None,
            learner_local_store_dedup_ring: NonZeroUsize::new(16_384),
            learner_local_store_compression: false,
            learner_local_store_integrate_channel_capacity: bytesize::nonzero(2048),
            learner_quorum_read_deadtime: Duration::from_millis(5),
            learner_quorum_read_timeout: Duration::from_secs(10),
            learner_stable_purge_ttl: None,
            raft_tracker_min_backoff: Duration::from_millis(200),
            raft_tracker_max_backoff: Duration::from_secs(5),
            peers,
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
            startup_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(30),
        })
    }

    /// Produce the raft-tracker subsystem config.
    #[must_use]
    pub fn raft_tracker_config(&self) -> RaftTrackerConfig {
        RaftTrackerConfig {
            peers: self.peers_watcher(),
            min_backoff: self.raft_tracker_min_backoff,
            max_backoff: self.raft_tracker_max_backoff,
            startup_timeout: self.startup_timeout,
        }
    }

    /// Produce the outbound router's client config.
    #[must_use]
    pub fn router_config(&self) -> zslg_net_setup::RpcClientConfig {
        zslg_net_setup::RpcClientConfig {
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
    pub fn peers_watcher(&self) -> Arc<dyn zslg_net::PeersWatcher> {
        zslg_peers::static_peers(self.peers.clone())
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

    /// Every column family a follower node runs — the complete schema of its
    /// one RocksDB. A follower is a learner that also writes, so it runs the
    /// same four families and no raft or replica log. RocksDB refuses to open
    /// a root whose on-disk family set is not fully named here, so adding a
    /// family to this list is a wipe.
    #[must_use]
    pub fn column_families(&self) -> [CfSpec; 4] {
        [
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
        ]
    }

    /// Produce the stable-log subsystem config: the engine sizing plus the log
    /// directory derived from the storage root.
    #[must_use]
    pub fn stable_log_config(&self) -> zslg_learner::StableLogConfig {
        zslg_learner::StableLogConfig {
            subscriber_queue_capacity: self.stable_log_subscriber_queue_capacity,
        }
    }

    /// Produce the snapshot-store constructor config.
    #[must_use]
    pub fn snapshot_store_config(&self) -> SnapshotStoreConfig {
        SnapshotStoreConfig {
            object_store: self.object_store.clone(),
        }
    }

    /// Validate all sub-configs and required timeouts.
    /// Produce the writer constructor config.
    #[must_use]
    pub fn writer_config(&self, node_id: NodeId) -> zslg_writer::WriterConfig {
        zslg_writer::WriterConfig {
            node_id,
            write_timeout: self.write_timeout,
        }
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

    pub fn validate(&self) -> Result<(), Error> {
        self.storage_config().validate()?;
        self.router_config().validate()?;
        if self.peers.is_empty() {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "peers must not be empty".to_owned(),
            )));
        }
        // Dummy identity — validation doesn't need a real node_id, just tunings.
        self.writer_config(NodeId::new_v4()).validate()?;
        self.raft_tracker_config().validate()?;
        self.learner_config().validate()?;
        if self.db_path.as_os_str().is_empty() {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "db_path must not be empty".to_owned(),
            )));
        }
        self.stable_log_config().validate()?;
        if self.drain_timeout.is_zero() {
            return Err(Error::Config(ConfigError::InvalidConfiguration(
                "drain_timeout must be non-zero".to_owned(),
            )));
        }
        Ok(())
    }
}

pub struct FollowerNode {
    config: FollowerNodeConfig,
    node_id: NodeId,
    hlc_clock: Arc<HlcClock>,
    raft_tracker: Arc<zslg_raft_tracker::RaftTracker>,
    router: Arc<dyn zslg_net::Router>,
    writer: Arc<WriterService>,
    learner: Arc<LearnerService>,
    stable_log: Arc<StableLog>,
    /// The node's one RocksDB. Held so [`run`](Self::run) can join the
    /// background retire compactions before the handle drops.
    storage: Arc<Storage>,
    shutdown: Shutdown<Error>,
}

impl FollowerNode {
    /// Start a follower node: open the node's RocksDB, connect to the
    /// configured replica peers via streaming subscriptions, build the local
    /// data cache, block until the latest manifest has been loaded, and set up
    /// the write path. Returns an error if the
    /// [`FollowerNodeConfig::startup_timeout`] elapses first. Once this
    /// returns, the follower is ready for reads and writes.
    #[allow(
        clippy::too_many_lines,
        reason = "start() wires every subsystem; kept linear for auditability"
    )]
    pub async fn start(config: FollowerNodeConfig) -> Result<Box<Self>, Error> {
        config.validate()?;

        // ── Shutdown + durable identity ────────────────────────────────────
        let mut shutdown = Shutdown::new();
        rocksdb_kit::create_storage_root(&config.db_path).await?;

        // The node's ONE RocksDB: one root, one block cache, one column family
        // per store, sized from this node's own budget.
        let storage = Storage::open(
            &config.db_path,
            config.storage_config(),
            &config.column_families(),
        )?;

        let node_id = zslg_node_id::load_or_create_node_id(&storage)?;
        info!(node_id = ?node_id, "follower node starting");

        // ── HLC ────────────────────────────────────────────────────────────
        let hlc_clock = Arc::new(HlcClock::new(config.hlc_max_clock_skew));
        shutdown.spawn(
            "clock_gauges",
            run_clock_gauges(hlc_clock.clone(), shutdown.token()),
        );
        shutdown.spawn(
            "process_gauges",
            zslg_observability::run_process_gauges(
                Some(storage.clone()),
                Some(config.learner_config().queue_capacity_bytes()),
                shutdown.token(),
            ),
        );

        // ── Outbound-only network (discard server config) ─────────────────
        let router = setup_router(hlc_clock.clone(), config.router_config(), shutdown.token())?;

        // ── Stable log (segment files; WAL fsync is its own durability fence) ──
        let stable_log = spawn_stable_store(&config.stable_log_config(), storage.clone()).await?;

        // ── Snapshot store (follower only loads, never writes) ────────────
        let snapshot_store: Arc<dyn SnapshotStore> = Arc::new(RemoteSnapshotStore::new(
            config.snapshot_store_config(),
            shutdown.token(),
        ));

        // ── Streaming RaftTracker ─────────────────────────────────────────
        let raft_tracker = spawn_raft_tracker_streamer(
            router.clone(),
            config.raft_tracker_config(),
            &mut shutdown,
        )
        .await?;

        let raft_state_view: Arc<dyn RaftStateView> = raft_tracker.clone();

        // ── Learner subsystem (reuses spawn_learner) ──────────────────────
        let learner = match tokio::time::timeout(
            config.startup_timeout,
            spawn_learner(
                hlc_clock.clone(),
                stable_log.clone(),
                storage.clone(),
                snapshot_store,
                raft_state_view,
                router.clone(),
                &config.learner_config(),
                &mut shutdown,
            ),
        )
        .await
        {
            Ok(res) => res?,
            Err(_) => {
                return Err(Error::Net(NetError::Rpc(format!(
                    "learner subsystem did not become ready within {:?}",
                    config.startup_timeout,
                ))));
            }
        };

        // ── WriterService ──────────────────────────────────────────────────
        let writer = Arc::new(WriterService::new(
            router.clone(),
            raft_tracker.clone(),
            hlc_clock.clone(),
            config.writer_config(node_id),
        ));

        info!(node_id = ?node_id, "follower node ready");

        Ok(Box::new(Self {
            config,
            node_id,
            hlc_clock,
            raft_tracker,
            router,
            writer,
            learner,
            stable_log,
            storage,
            shutdown,
        }))
    }

    /// The persistent node identity, loaded from or minted into the write-once
    /// `node_id` file under the storage root.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns a handle to the quorum-gated reader API.
    ///
    /// Callers should clone this [`Arc`] before calling [`run`](Self::run) if
    /// reads should continue after the node is consumed.
    #[must_use]
    pub fn reader(&self) -> Arc<dyn zslg_log::Reader> {
        self.learner.reader()
    }

    /// Returns a handle to the write API.
    ///
    /// Callers should clone this [`Arc`] before calling [`run`](Self::run) if
    /// writes should continue after the node is consumed.
    #[must_use]
    pub fn writer(&self) -> Arc<dyn zslg_log::Writer> {
        self.writer.clone()
    }

    #[must_use]
    pub fn config(&self) -> &FollowerNodeConfig {
        &self.config
    }

    /// Clone of the global [`CancellationToken`]; cancelling it triggers a
    /// graceful shutdown of every background task.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.token()
    }

    /// Run the node until shutdown is requested or a background task fails.
    /// Consumes `self`; clone any needed handles ([`reader`](Self::reader),
    /// [`writer`](Self::writer)) before calling.
    pub async fn run(self) -> Result<(), Error> {
        let drain_timeout = self.config.drain_timeout;
        let Self {
            config,
            node_id,
            hlc_clock,
            raft_tracker,
            router,
            writer,
            learner,
            stable_log,
            storage,
            shutdown,
        } = self;

        let result = shutdown.run(drain_timeout).await;

        drop((
            config,
            node_id,
            hlc_clock,
            raft_tracker,
            router,
            writer,
            learner,
            stable_log,
            storage,
        ));
        result
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests {
    use super::*;

    /// The list IS the schema: RocksDB refuses to open a root whose on-disk
    /// family set is not fully named, and its length sizes every memtable. The
    /// names are the on-disk layout, so a change here is a wipe.
    #[test]
    fn the_node_runs_exactly_these_column_families() {
        let config = FollowerNodeConfig::new(
            Vec::new(),
            PathBuf::from("/tmp/unused"),
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        )
        .expect("config");
        let names: Vec<&str> = config
            .column_families()
            .iter()
            .map(|spec| spec.cf.name())
            .collect();
        assert_eq!(
            names,
            vec![
                "stable_log",
                "local_store_data",
                "local_store_cursors",
                "node_id"
            ],
        );
    }
}

error_union::error_union! {
    /// What this node assembly can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(zslg_lifecycle_types::LifecycleError),
        Config(zslg_lifecycle_types::ConfigError),
        Clock(zslg_time::ClockError),
        Log(zslg_log::LogError),
        Snapshot(zslg_snapshot::SnapshotError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    from zslg_time::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_learner::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net_client::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net_setup::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_object_store_config::Error { Snapshot }
    from zslg_raft_tracker::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_local_store::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_stable_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_reader::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_subscriber::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_node_id::Error { Config, Log }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_quorum_read::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_observability::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_writer::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

impl From<shutdown::Stopped> for Error {
    fn from(_: shutdown::Stopped) -> Self {
        Self::Lifecycle(zslg_lifecycle_types::LifecycleError::Shutdown)
    }
}

impl From<rocksdb_kit::StorageError> for Error {
    fn from(e: rocksdb_kit::StorageError) -> Self {
        match e {
            rocksdb_kit::StorageError::InvalidConfiguration(msg) => {
                Self::Config(zslg_lifecycle_types::ConfigError::InvalidConfiguration(msg))
            }
            rocksdb_kit::StorageError::Storage(msg) => Self::Log(zslg_log::LogError::Storage(msg)),
        }
    }
}
