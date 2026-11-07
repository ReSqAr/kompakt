//! The read side: everything a node needs to answer a read, assembled from two
//! stores with different durability stories and different owners.
//!
//! ```text
//!   replica peers ──subscribe──> subscriber/ ──> local_store   unstable, weak
//!                                                    │          (HLC-keyed)
//!                                                    │
//!   object store ──snapshots──> stable_log ──────────┴──> read.rs ──> Reader
//!                  (never GC'd)  stable, index-keyed          │
//!                       └────────> stable_archive ────────────┘
//!                                  (purged prefix, just-in-time)
//! ```
//!
//! The split is the design doc's: `stable_log` holds the settled prefix the
//! leader has snapshotted, keyed by the gapless global `LogIndex`;
//! `local_store` holds everything above that watermark, replicated directly
//! from the replica logs and keyed by `DataId`/HLC. One HLC — the stable
//! watermark (`StableLog::end_hlc`) — is the seam: `read.rs` serves at or below
//! it from the stable side and above it from the local side, so a write that
//! never reached quorum simply disappears from the unstable view once the
//! watermark passes it. That disappearance is what makes nodes converge.
//!
//! Neither store is a durability source of truth for the cluster: the stable
//! log is re-derivable from object-store snapshots and the local store from the
//! replica logs. That is why both accept bounded loss — the local store fsyncs
//! on a timer rather than per batch, and a purged stable prefix is fetched back
//! from object store on demand.
//!
//! `quorum.rs` is the freshness opt-in: `sync_quorum` blocks until this node has
//! caught up to a quorum of replicas, which is what makes a subsequent read
//! read-your-writes rather than merely eventually consistent.

use zslg_consensus::RaftStateView;
use zslg_lifecycle_types::ConfigError;
pub use zslg_local_store::LocalStore;
pub use zslg_local_store::{LOCAL_STORE_CURSORS_CF, LOCAL_STORE_DATA_CF};
use zslg_net::Router;
pub use zslg_reader::{GatedReader, can_serve_clients};
use zslg_reader::{MultiReplicaQuorumCheck, QuorumReadDebouncer, ReaderImpl};
use zslg_snapshot::SnapshotStore;
pub use zslg_stable_log::STABLE_LOG_CF;
pub use zslg_stable_log::StableLogConfig;
pub use zslg_stable_log::{StableLog, spawn_stable_store};

#[cfg(any(test, feature = "testing"))]
pub mod testing;

#[cfg(test)]
mod reader_tests;
#[cfg(test)]
mod snapshot_loader_tests;

#[derive(Debug, Clone)]
pub struct LearnerConfig {
    pub fetch_sweep_interval: Duration,
    pub purge_grace_period: Duration,
    pub local_store_flush_interval: Duration,
    pub quorum_read_deadtime: Duration,
    pub quorum_read_timeout: Duration,
    pub stable_purge_ttl: Option<Duration>,
    /// Local-store engine sizing — see [`zslg_local_store::LocalStoreConfig`].
    pub local_store_dedup_ring: Option<std::num::NonZeroUsize>,
    pub local_store_integrate_channel_capacity: std::num::NonZeroUsize,
}

impl LearnerConfig {
    pub fn validate(&self) -> Result<(), crate::Error> {
        if self.quorum_read_timeout.is_zero() {
            return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                "quorum_read_timeout must be > 0".to_owned(),
            )));
        }
        if self.fetch_sweep_interval.is_zero() {
            return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                "fetch_sweep_interval must be > 0".to_owned(),
            )));
        }
        if self.local_store_flush_interval.is_zero() {
            return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                "local_store_flush_interval must be > 0".to_owned(),
            )));
        }
        // Kept as a call rather than inlined `Ok(())`: the local store's own
        // knobs are operator-reachable, so if one ever grows a rule again this
        // is where it is checked. RocksDB validates its own sizing at open.
        self.local_store_config().validate().map_err(Into::into)
    }

    /// Bytes this node's learner-side queues can hold at full depth — the
    /// pipeline half of the memory picture, reported beside (never folded
    /// into) the engine's `storage_memory` budget.
    #[must_use]
    pub fn queue_capacity_bytes(&self) -> u64 {
        self.local_store_config().queue_capacity_bytes()
    }

    /// Produce the local-store subsystem config (directory + its tunables).
    fn local_store_config(&self) -> zslg_local_store::LocalStoreConfig {
        zslg_local_store::LocalStoreConfig {
            flush_interval: self.local_store_flush_interval,
            purge_grace_period: self.purge_grace_period,
            dedup_ring: self.local_store_dedup_ring,
            integrate_channel_capacity: self.local_store_integrate_channel_capacity,
            unstable_subscriber_queue_capacity:
                zslg_local_store::DEFAULT_UNSTABLE_SUBSCRIBER_QUEUE_CAPACITY,
        }
    }

    /// Produce the quorum-read debouncer config.
    fn quorum_read_debouncer_config(&self) -> zslg_reader::QuorumReadDebouncerConfig {
        zslg_reader::QuorumReadDebouncerConfig {
            deadtime: self.quorum_read_deadtime,
            check_timeout: self.quorum_read_timeout,
        }
    }
}

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use shutdown::Shutdown;
use zslg_hlc_clock::HlcClock;
use zslg_log::LocalStoreMut;
use zslg_log::Reader;
use zslg_stable_log::SnapshotLoader;

#[derive(Clone)]
pub struct LearnerService {
    reader: Arc<ReaderImpl>,
    local_store: Arc<LocalStore>,
}

impl Drop for LearnerService {
    fn drop(&mut self) {
        tracing::info!("LearnerService dropped");
    }
}

impl LearnerService {
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "new() gathers all subsystem handles needed by the learner node"
    )]
    fn new(local_store: Arc<LocalStore>, reader: Arc<ReaderImpl>) -> Self {
        Self {
            reader,
            local_store,
        }
    }

    #[must_use]
    pub fn reader(&self) -> Arc<dyn Reader> {
        self.reader.clone()
    }

    #[must_use]
    pub fn local_store(&self) -> Arc<dyn LocalStoreMut> {
        self.local_store.clone()
    }
}

/// Spawn every learner subsystem over the node's one RocksDB: the local store
/// (a sibling column family of `storage`), the reader, the snapshot loader and
/// the replication manager.
///
/// `stable_log` and `storage` are the same object at two levels of abstraction
/// — the log is a column family of the DB — so they travel as two parameters
/// rather than a bundle.
#[allow(
    clippy::too_many_arguments,
    reason = "one wiring point for every learner subsystem; each argument is a distinct handle, and grouping them would only hide that"
)]
pub async fn spawn_learner<E>(
    clock: Arc<HlcClock>,
    stable_log: Arc<dyn zslg_log::StableLogSnapshot>,
    storage: Arc<rocksdb_kit::Storage>,
    snapshot_store: Arc<dyn SnapshotStore>,
    raft_state: Arc<dyn RaftStateView>,
    router: Arc<dyn Router>,
    config: &LearnerConfig,
    shutdown: &mut Shutdown<E>,
) -> Result<Arc<LearnerService>, Error>
where
    E: From<Error>
        + From<zslg_local_store::Error>
        + From<shutdown::Stopped>
        + std::fmt::Display
        + Send
        + 'static,
{
    info!("spawning learner subsystems");
    let local_store = zslg_local_store::spawn_local_store(
        stable_log.clone(),
        storage,
        &config.local_store_config(),
        shutdown,
    )
    .await?;

    let quorum_check = MultiReplicaQuorumCheck::new(
        router.clone(),
        local_store.clone(),
        raft_state.clone(),
        stable_log.clone(),
    );

    let debouncer = Arc::new(QuorumReadDebouncer::new(
        Arc::new(quorum_check),
        &config.quorum_read_debouncer_config(),
    ));
    // The shared object-store access path for the stable prefix — used by both
    // the reader (purged-index fallback) and the snapshot loader. Threaded as a
    // trait object so neither sees the object-store/manifest detail.
    let archive_reader: Arc<dyn zslg_log::StableArchiveReader> =
        Arc::new(zslg_stable_archive::ObjectStoreArchiveReader::new(
            snapshot_store.clone(),
            raft_state.clone(),
        ));

    let reader = Arc::new(ReaderImpl::new(
        local_store.clone(),
        stable_log.clone(),
        archive_reader.clone(),
        debouncer,
        crossbeam_combinators::Cancellation::from_token(&shutdown.token()),
        clock.clone(),
    ));
    let learner = LearnerService::new(local_store, reader);

    let snapshot_loader = SnapshotLoader::new(
        archive_reader,
        stable_log.clone(),
        raft_state.clone(),
        shutdown.token(),
    );
    snapshot_loader.load_initial().await;
    shutdown.spawn("learner:snapshot_loader", async move {
        snapshot_loader
            .run()
            .await
            .map_err(Error::from)
            .map_err(E::from)
    });

    // Optional stable-log TTL purge (off unless configured). A purged read is
    // transparently served from object store by the reader above.
    if let Some(ttl) = config.stable_purge_ttl {
        let (state, log, cancel) = (raft_state.clone(), stable_log.clone(), shutdown.token());
        shutdown.spawn("learner:stable_purge", async move {
            zslg_stable_log::run_stable_purge_worker(state, log, ttl, cancel)
                .await
                .map_err(Error::from)
                .map_err(E::from)
        });
    }

    let manager = zslg_subscriber::ReplicationManager::new(
        clock,
        raft_state.clone(),
        learner.local_store.clone(),
        router.clone(),
    );
    let cancel = shutdown.token();
    shutdown.spawn("learner:replication", async move {
        manager
            .run(cancel)
            .await
            .map_err(Error::from)
            .map_err(E::from)
    });

    learner.reader.sync().await?;

    Ok(Arc::new(learner))
}

error_union::error_union! {
    /// What the learner assembly can fail with.
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
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_quorum_read::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_local_store::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_stable_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_subscriber::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_reader::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

error_union::error_bridge!(Error => zslg_time::Error, zslg_log::Error, zslg_snapshot::Error, zslg_consensus::Error, zslg_raft::Error, zslg_net::Error, zslg_quorum_read::Error; Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus);
error_union::error_home!(Error);

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
