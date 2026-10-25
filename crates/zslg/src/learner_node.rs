//! A free-standing learner node.
//!
//! `LearnerNode` is the read half of [`ReplicaNode`](crate::replica_node::ReplicaNode)
//! with none of the consensus responsibilities: it joins no Raft, owns no replica
//! log, and never binds a server. It subscribes to the replica cluster's log
//! streams to build a local data cache, tracks the cluster's `RaftState` via
//! streaming subscriptions (see [`crate::raft::spawn_raft_tracker_streamer`]),
//! and serves the [`Reader`](crate::Reader) API.
//!
//! Its identity is persistent — a [`NodeId`], one write-once row of the
//! node's RocksDB. The learner has durable state in its local store and
//! stable log, so the id must survive restarts.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use std::num::NonZeroUsize;

use crate::ByteSize;
use crate::api::{self, RaftStateView, SnapshotStore};
use crate::database;
use crate::database::storage::{CfSpec, Storage, StorageConfig};
use crate::error::Error;
use crate::hlc::{HlcClock, run_clock_gauges};
use crate::ids::NodeId;
use crate::ids::PeerAddr;
use crate::learner::LearnerConfig;
use crate::learner::{LearnerService, StableLog, spawn_learner, spawn_stable_store};
use crate::network::setup_router;
use crate::object_store::IntoArcObjectStore;
use crate::object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use crate::raft::{RaftTrackerConfig, spawn_raft_tracker_streamer};
use crate::shutdown::Shutdown;

/// Configuration for a free-standing learner node.
///
/// A learner subscribes to the replica cluster's log streams to build a local
/// data cache, serves the [`Reader`](crate::Reader) API, and does **not**
/// participate in Raft consensus. It has durable state (its own RocksDB)
/// so it carries storage config, but it never binds a server — outbound-only.
#[derive(Debug, Clone)]
pub struct LearnerNodeConfig {
    pub hlc_max_clock_skew: Duration,
    pub fetch_sweep_interval: Duration,
    pub purge_grace_period: Duration,
    pub local_store_flush_interval: Duration,
    /// Entries in the local store's recent-`DataId` filter, which drops the
    /// duplicate copies of a write arriving from each replica this node
    /// subscribes to. `None` disables it.
    ///
    /// Sized in records, so it spans less arrival skew as throughput rises.
    /// Measured plateau 10k-50k; 8,192 is worth only +28.5% and 1M regresses.
    pub local_store_dedup_ring: Option<NonZeroUsize>,
    /// How the local store's runs are stored. Uncompressed by default: it is a
    /// rebuildable cache and the highest-volume column family on the node,
    /// and compression is a footprint/CPU trade rather than a durability one.
    pub local_store_compression: bool,
    /// Depth of the node's shared local-store integration channel, which every
    /// subscription forwards into. Sized for a few batches across all
    /// upstreams so readers keep producing while a commit is in flight.
    pub local_store_integrate_channel_capacity: NonZeroUsize,
    pub quorum_read_deadtime: Duration,
    pub quorum_read_timeout: Duration,
    pub stable_purge_ttl: Option<Duration>,
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
    /// Total memory the storage engine may hold on this node: block cache,
    /// index and filter blocks, and the memtables charged against it. Sized
    /// from here rather than per store, because a node runs one DB and its
    /// budget is fixed when that DB opens.
    ///
    /// Everything else scales off it — memtable size, SST target size, retained
    /// WAL, open file count — so this is the one number to move. Expect process
    /// RSS above it: table readers, compaction buffers, iterator-pinned blocks
    /// and allocator slack sit outside the budget.
    ///
    /// A learner had no such field until the budget became a required argument
    /// of the one open: it never set the process-wide default, so it ran at
    /// 256 MiB unconfigurably.
    pub storage_memory: ByteSize,
    /// RocksDB overrides. `None` — the default — derives the term from
    /// `storage_memory`; the derivations live in `database::storage` and are
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
    /// [`LearnerNode::start`](LearnerNode::start).
    pub startup_timeout: Duration,
    /// Maximum time to wait for background tasks to drain during shutdown
    /// before forcefully aborting them.
    pub drain_timeout: Duration,
}

impl LearnerNodeConfig {
    /// Create a learner config with defaults, customising only the required
    /// fields: the replica `peers` to subscribe to and storage.
    pub fn new(
        peers: Vec<PeerAddr>,
        db_path: PathBuf,
        object_store: impl IntoArcObjectStore,
    ) -> Result<Self, Error> {
        Ok(Self {
            hlc_max_clock_skew: Duration::from_millis(200),
            fetch_sweep_interval: Duration::from_secs(2),
            purge_grace_period: Duration::from_secs(5 * 60),
            local_store_flush_interval: Duration::from_secs(1),
            local_store_dedup_ring: NonZeroUsize::new(16_384),
            local_store_compression: false,
            local_store_integrate_channel_capacity: crate::unit::nonzero(2048),
            quorum_read_deadtime: Duration::from_millis(5),
            quorum_read_timeout: Duration::from_secs(10),
            stable_purge_ttl: None,
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
            subscribe_channel_capacity: crate::unit::nonzero(1024),
            write_timeout: Duration::from_secs(10),
            db_path,
            storage_memory: ByteSize::from_mib(256),
            storage_write_buffer: None,
            storage_target_file_size: None,
            storage_max_total_wal: None,
            storage_max_background_jobs: None,
            storage_max_open_files: None,
            storage_retire_sweep: None,
            object_store: object_store.into_arc_object_store()?,
            stable_log_subscriber_queue_capacity:
                crate::learner::StableLogConfig::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
            stable_log_compression: true,
            startup_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(30),
        })
    }

    /// Produce the raft-tracker subsystem config.
    #[must_use]
    pub(crate) fn raft_tracker_config(&self) -> RaftTrackerConfig {
        RaftTrackerConfig {
            peers: self.peers_watcher(),
            min_backoff: self.raft_tracker_min_backoff,
            max_backoff: self.raft_tracker_max_backoff,
            startup_timeout: self.startup_timeout,
        }
    }

    /// Produce the outbound router's client config.
    #[must_use]
    pub(crate) fn router_config(&self) -> crate::network::RpcClientConfig {
        crate::network::RpcClientConfig {
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
    pub(crate) fn peers_watcher(&self) -> Arc<dyn crate::api::PeersWatcher> {
        crate::network::static_peers(self.peers.clone())
    }

    /// Produce the learner subsystem config: the learner tunables plus the
    /// store directory derived from the storage root.
    #[must_use]
    pub(crate) fn learner_config(&self) -> LearnerConfig {
        LearnerConfig {
            fetch_sweep_interval: self.fetch_sweep_interval,
            purge_grace_period: self.purge_grace_period,
            local_store_flush_interval: self.local_store_flush_interval,
            local_store_dedup_ring: self.local_store_dedup_ring,
            local_store_integrate_channel_capacity: self.local_store_integrate_channel_capacity,
            quorum_read_deadtime: self.quorum_read_deadtime,
            quorum_read_timeout: self.quorum_read_timeout,
            stable_purge_ttl: self.stable_purge_ttl,
        }
    }

    /// Every column family a learner node runs — the complete schema of its one
    /// RocksDB. A learner joins no Raft and owns no replica log, so it names
    /// four families where a replica names seven; that is what makes its
    /// memtable sizing exact rather than divided by a planning constant.
    /// RocksDB refuses to open a root whose on-disk family set is not fully
    /// named here, so adding a family to this list is a wipe.
    #[must_use]
    pub(crate) fn column_families(&self) -> [CfSpec; 4] {
        [
            CfSpec {
                subsystem: database::Subsystem::StableLog,
                compress: self.stable_log_compression,
            },
            CfSpec {
                subsystem: database::Subsystem::LocalStoreData,
                compress: self.local_store_compression,
            },
            // Cursors follow the data family rather than carrying a second
            // knob: one overwritten slot per peer has no workload behind it.
            CfSpec {
                subsystem: database::Subsystem::LocalStoreCursors,
                compress: self.local_store_compression,
            },
            // One tiny, never-compressed row: the write-once node identity.
            CfSpec {
                subsystem: database::Subsystem::NodeId,
                compress: false,
            },
        ]
    }

    /// Produce the stable-log subsystem config: the engine sizing plus the log
    /// directory derived from the storage root.
    #[must_use]
    pub(crate) fn stable_log_config(&self) -> crate::learner::StableLogConfig {
        crate::learner::StableLogConfig {
            subscriber_queue_capacity: self.stable_log_subscriber_queue_capacity,
        }
    }

    /// Produce the snapshot-store constructor config.
    #[must_use]
    pub(crate) fn snapshot_store_config(&self) -> SnapshotStoreConfig {
        SnapshotStoreConfig {
            object_store: self.object_store.clone(),
        }
    }

    /// Validate all sub-configs and required timeouts.
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

    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.storage_config().validate()?;
        self.router_config().validate()?;
        if self.peers.is_empty() {
            return Err(Error::InvalidConfiguration(
                "peers must not be empty".to_owned(),
            ));
        }
        self.raft_tracker_config().validate()?;
        self.learner_config().validate()?;
        if self.db_path.as_os_str().is_empty() {
            return Err(Error::InvalidConfiguration(
                "db_path must not be empty".to_owned(),
            ));
        }
        self.stable_log_config().validate()?;
        if self.drain_timeout.is_zero() {
            return Err(Error::InvalidConfiguration(
                "drain_timeout must be non-zero".to_owned(),
            ));
        }
        Ok(())
    }
}

pub struct LearnerNode {
    config: LearnerNodeConfig,
    node_id: NodeId,
    hlc_clock: Arc<HlcClock>,
    raft_tracker: Arc<crate::raft::RaftTracker>,
    router: Arc<dyn api::Router>,
    learner: Arc<LearnerService>,
    stable_log: Arc<StableLog>,
    /// The node's one RocksDB. Held so [`run`](Self::run) can join the
    /// background retire compactions before the handle drops.
    storage: Arc<Storage>,
    shutdown: Shutdown,
}

impl LearnerNode {
    /// Start a learner node: open the node's RocksDB, connect to the
    /// configured replica peers via streaming subscriptions, build the local
    /// data cache, and block until the latest manifest has been loaded and a
    /// quorum of replicas have been discovered. Returns an error if the
    /// [`LearnerNodeConfig::startup_timeout`] elapses first. Once this returns,
    /// the learner is ready for reads.
    #[allow(
        clippy::too_many_lines,
        reason = "start() wires every subsystem; kept linear for auditability"
    )]
    pub async fn start(config: LearnerNodeConfig) -> Result<Self, Error> {
        config.validate()?;

        // ── Shutdown + durable identity ────────────────────────────────────
        let mut shutdown = Shutdown::new();
        database::create_storage_root(&config.db_path).await?;

        // The node's ONE RocksDB. `storage_memory` is a required argument, and
        // that is how this node type turned out never to have had the field: it
        // never called the old process-wide setter, so a standalone learner ran
        // at the 256 MiB default with no way to configure it.
        let storage = Storage::open(
            &config.db_path,
            config.storage_config(),
            &config.column_families(),
        )?;

        let node_id = database::load_or_create_node_id(&storage)?;
        info!(node_id = ?node_id, "learner node starting");
        shutdown.spawn(
            "process_gauges",
            crate::observability::run_process_gauges(
                Some(storage.clone()),
                Some(config.learner_config().queue_capacity_bytes()),
                shutdown.token(),
            ),
        );

        // ── HLC ────────────────────────────────────────────────────────────

        let hlc_clock = Arc::new(HlcClock::new(config.hlc_max_clock_skew));
        shutdown.spawn(
            "clock_gauges",
            run_clock_gauges(hlc_clock.clone(), shutdown.token()),
        );

        // ── Outbound-only network (discard server config) ─────────────────
        let router = setup_router(hlc_clock.clone(), config.router_config(), shutdown.token())?;

        // ── Stable log (segment files; WAL fsync is its own durability fence) ──
        let stable_log = spawn_stable_store(&config.stable_log_config(), storage.clone()).await?;

        // ── Snapshot store (learner only loads, never writes) ─────────────
        // `write_snapshot` / `cleanup_snapshots` take their compression +
        // retention per call (leader-supplied), so a load-only learner needs
        // nothing but the object store here.
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
        // `spawn_learner` blocks on loading the latest manifest, which is
        // otherwise unbounded; cap the whole wire-up by `startup_timeout`. On
        // timeout the early return drops `shutdown`, whose `JoinSet` aborts the
        // background tasks already registered above.
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
                return Err(Error::Rpc(format!(
                    "learner subsystem did not become ready within {:?}",
                    config.startup_timeout,
                )));
            }
        };

        info!(node_id = ?node_id, "learner node ready");

        Ok(Self {
            config,
            node_id,
            hlc_clock,
            raft_tracker,
            router,
            learner,
            stable_log,
            storage,
            shutdown,
        })
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
    pub fn reader(&self) -> Arc<dyn api::Reader> {
        self.learner.reader()
    }

    /// Clone of the global [`CancellationToken`]; cancelling it triggers a
    /// graceful shutdown of every background task.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.token()
    }

    /// Run the node until shutdown is requested or a background task fails.
    /// Consumes `self`; clone any needed handles ([`reader`](Self::reader))
    /// before calling.
    pub async fn run(self) -> Result<(), Error> {
        let drain_timeout = self.config.drain_timeout;
        let Self {
            config,
            node_id,
            hlc_clock,
            raft_tracker,
            router,
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
            learner,
            stable_log,
            storage,
        ));
        result
    }
}
