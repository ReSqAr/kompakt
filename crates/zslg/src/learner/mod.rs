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

mod fanout;
mod local_store;
mod quorum;
mod read;
mod stable_archive;
mod stable_log;
mod subscriber;

pub(crate) use local_store::LocalStore;
#[cfg(test)]
pub(crate) use local_store::open_test_local_store;
pub(crate) use quorum::ReplicaPoller;
use quorum::{MultiReplicaQuorumCheck, QuorumReadDebouncer};
use read::ReaderImpl;
pub(crate) use read::{GatedReader, can_serve_clients};
pub(crate) use stable_log::StableLogConfig;
pub(crate) use stable_log::{StableLog, spawn_stable_store};

#[cfg(test)]
pub(crate) use stable_log::integrate_manifest_for_test;

#[derive(Debug, Clone)]
pub(crate) struct LearnerConfig {
    pub(crate) fetch_sweep_interval: Duration,
    pub(crate) purge_grace_period: Duration,
    pub(crate) local_store_flush_interval: Duration,
    pub(crate) quorum_read_deadtime: Duration,
    pub(crate) quorum_read_timeout: Duration,
    pub(crate) stable_purge_ttl: Option<Duration>,
    /// Local-store engine sizing — see [`local_store::LocalStoreConfig`].
    pub(crate) local_store_dedup_ring: Option<std::num::NonZeroUsize>,
    pub(crate) local_store_integrate_channel_capacity: std::num::NonZeroUsize,
}

impl LearnerConfig {
    pub(crate) fn validate(&self) -> Result<(), crate::error::Error> {
        if self.quorum_read_timeout.is_zero() {
            return Err(crate::error::Error::InvalidConfiguration(
                "quorum_read_timeout must be > 0".to_owned(),
            ));
        }
        if self.fetch_sweep_interval.is_zero() {
            return Err(crate::error::Error::InvalidConfiguration(
                "fetch_sweep_interval must be > 0".to_owned(),
            ));
        }
        if self.local_store_flush_interval.is_zero() {
            return Err(crate::error::Error::InvalidConfiguration(
                "local_store_flush_interval must be > 0".to_owned(),
            ));
        }
        // Kept as a call rather than inlined `Ok(())`: the local store's own
        // knobs are operator-reachable, so if one ever grows a rule again this
        // is where it is checked. RocksDB validates its own sizing at open.
        self.local_store_config().validate()
    }

    /// Bytes this node's learner-side queues can hold at full depth — the
    /// pipeline half of the memory picture, reported beside (never folded
    /// into) the engine's `storage_memory` budget.
    #[must_use]
    pub(crate) fn queue_capacity_bytes(&self) -> u64 {
        self.local_store_config().queue_capacity_bytes()
    }

    /// Produce the local-store subsystem config (directory + its tunables).
    pub(in crate::learner) fn local_store_config(&self) -> local_store::LocalStoreConfig {
        local_store::LocalStoreConfig {
            flush_interval: self.local_store_flush_interval,
            purge_grace_period: self.purge_grace_period,
            dedup_ring: self.local_store_dedup_ring,
            integrate_channel_capacity: self.local_store_integrate_channel_capacity,
            unstable_subscriber_queue_capacity:
                local_store::DEFAULT_UNSTABLE_SUBSCRIBER_QUEUE_CAPACITY,
        }
    }

    /// Produce the quorum-read debouncer config.
    pub(in crate::learner) fn quorum_read_debouncer_config(
        &self,
    ) -> quorum::QuorumReadDebouncerConfig {
        quorum::QuorumReadDebouncerConfig {
            deadtime: self.quorum_read_deadtime,
            check_timeout: self.quorum_read_timeout,
        }
    }
}

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::api::{LocalStoreMut, RaftStateView, Router, SnapshotStore};
use crate::hlc::HlcClock;
use crate::shutdown::Shutdown;
use crate::{Error, Reader, api};
use stable_log::SnapshotLoader;

#[derive(Clone)]
pub(crate) struct LearnerService {
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
    pub(in crate::learner) fn new(local_store: Arc<LocalStore>, reader: Arc<ReaderImpl>) -> Self {
        Self {
            reader,
            local_store,
        }
    }

    pub(crate) fn reader(&self) -> Arc<dyn Reader> {
        self.reader.clone()
    }

    pub(crate) fn local_store(&self) -> Arc<dyn LocalStoreMut> {
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
pub(crate) async fn spawn_learner(
    clock: Arc<HlcClock>,
    stable_log: Arc<dyn api::StableLogSnapshot>,
    storage: Arc<crate::database::storage::Storage>,
    snapshot_store: Arc<dyn SnapshotStore>,
    raft_state: Arc<dyn RaftStateView>,
    router: Arc<dyn Router>,
    config: &LearnerConfig,
    shutdown: &mut Shutdown,
) -> Result<Arc<LearnerService>, Error> {
    info!("spawning learner subsystems");
    let local_store = local_store::spawn_local_store(
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
    let archive_reader: Arc<dyn api::StableArchiveReader> = Arc::new(
        stable_archive::ObjectStoreArchiveReader::new(snapshot_store.clone(), raft_state.clone()),
    );

    let reader = Arc::new(ReaderImpl::new(
        local_store.clone(),
        stable_log.clone(),
        archive_reader.clone(),
        debouncer,
        crate::channel::Cancellation::from_token(&shutdown.token()),
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
    shutdown.spawn("learner:snapshot_loader", snapshot_loader.run());

    // Optional stable-log TTL purge (off unless configured). A purged read is
    // transparently served from object store by the reader above.
    if let Some(ttl) = config.stable_purge_ttl {
        shutdown.spawn(
            "learner:stable_purge",
            stable_log::run_stable_purge_worker(
                raft_state.clone(),
                stable_log.clone(),
                ttl,
                shutdown.token(),
            ),
        );
    }

    shutdown.spawn(
        "learner:replication",
        subscriber::ReplicationManager::new(
            clock,
            raft_state.clone(),
            learner.local_store.clone(),
            router.clone(),
        )
        .run(shutdown.token()),
    );

    learner.reader.sync().await?;

    Ok(Arc::new(learner))
}
