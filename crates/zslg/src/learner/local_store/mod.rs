//! The unstable half of the read side: everything replicated from the replica
//! logs that the leader has not yet snapshotted.
//!
//! Two column families of the node's one RocksDB:
//!
//! ```text
//!   data     DataId (HLC-major key)     the payloads
//!   cursors  NodeId                      per-peer subscription progress
//!
//!   write:  one WriteBatch across both       flush: one fence
//! ```
//!
//! **A record and the cursor covering it commit in one batch.** They were two
//! separate DBs before, where no batch could span them, so consistency rested
//! on an ordering rule (data first, cursors last) that a crash in between could
//! still break — leaving a durable cursor claiming coverage of a record that was
//! never written. One DB with `atomic_flush` makes the pair genuinely atomic
//! (`store::tests::data_and_cursor_commit_atomically`).
//!
//! Atomicity is all this store needs, which is why it fsyncs on a timer rather
//! than per batch ([`store::run_local_store_flush_worker`]): every byte is a
//! copy of quorum-durable replica data, so a crash loses at most a suffix, the
//! lagging cursor re-delivers it, and the re-delivery dedups by `data_id`.
//!
//! Entries leave by retirement, not deletion: [`purge`] arms an HLC threshold
//! that trails the stable watermark by a grace period, and the engine clamps
//! reads at it.

mod key;
mod purge;
mod recent_ids;
mod store;

#[cfg(test)]
mod local_store_proptest;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crate::Error;
use crate::api::StableLogSnapshot;
use crate::database::storage::Storage;
use crate::learner::local_store::purge::spawn_local_store_purge_worker;
use crate::shutdown::Shutdown;

pub(in crate::learner::local_store) use recent_ids::Dedup;
pub(crate) use store::LocalStore;

/// Everything the local-store subsystem needs, produced by
/// [`LearnerConfig::local_store_config`](crate::learner::LearnerConfig::local_store_config).
/// Bundling the directory with the tunables means `spawn_local_store` takes one
/// argument instead of a config plus a slice of it.
pub(in crate::learner) struct LocalStoreConfig {
    pub(in crate::learner) flush_interval: Duration,
    pub(in crate::learner) purge_grace_period: Duration,
    /// Entries in the recent-`DataId` filter, which drops the duplicate copies
    /// of a write that arrive from each replica this node subscribes to.
    /// `None` disables it.
    ///
    /// Sized in *records*, not time, so it spans proportionally less arrival
    /// skew as throughput rises — hence the headroom in the default. Measured
    /// plateau is 10k-50k (+36..39% throughput); 8,192 gives only +28.5% and
    /// 1,024 nothing, while 1,000,000 regresses to +30% on cache locality.
    /// Default 16,384 is roughly 0.5 MB per store.
    pub(in crate::learner) dedup_ring: Option<NonZeroUsize>,
    /// Depth of the shared integration channel every subscription forwards
    /// into. Sized for a few batches across all upstreams, so the readers keep
    /// producing while a commit is in flight.
    pub(in crate::learner) integrate_channel_capacity: NonZeroUsize,
    /// Depth of each unstable-tail subscriber's own queue — how far behind a
    /// consumer may fall before the fan-out hangs up on it and it rescans the
    /// unstable window. At least one integrate batch, or a single batch would
    /// overrun it.
    pub(in crate::learner) unstable_subscriber_queue_capacity: NonZeroUsize,
}

pub(crate) const QUEUE_REFERENCE_PAYLOAD_BYTES: u64 = 2 * 1024;

/// Default for [`LocalStoreConfig::unstable_subscriber_queue_capacity`]: two
/// full integrate batches.
pub(in crate::learner) const DEFAULT_UNSTABLE_SUBSCRIBER_QUEUE_CAPACITY: NonZeroUsize =
    crate::unit::nonzero(2 * store::MAX_BATCH);

impl LocalStoreConfig {
    /// Test-only constructor carrying the production defaults. Production
    /// builds this from the owning node config's flat fields via
    /// [`LearnerConfig::local_store_config`](crate::learner::LearnerConfig::local_store_config).
    #[cfg(test)]
    #[must_use]
    pub(in crate::learner) fn for_test() -> Self {
        Self {
            flush_interval: Duration::from_secs(1),
            purge_grace_period: Duration::from_secs(5 * 60),
            dedup_ring: NonZeroUsize::new(16_384),
            integrate_channel_capacity: crate::unit::nonzero(2048),
            unstable_subscriber_queue_capacity: DEFAULT_UNSTABLE_SUBSCRIBER_QUEUE_CAPACITY,
        }
    }

    /// Engine sizing is RocksDB's business now; nothing left to validate.
    pub(in crate::learner) fn validate(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Worst-case bytes this store's queues can hold — reported beside the
    /// engine's charged bytes, never folded into that budget (see
    /// [`ReplicaLogConfig::queue_capacity_bytes`](crate::replica::ReplicaLogConfig::queue_capacity_bytes)
    /// for why the two are kept apart).
    ///
    /// Two terms, and only one of them is payload: the integrate channel holds
    /// records on their way to a batch, while the dedup ring holds `DataId`s
    /// only. The ring's ids are resident **twice** — the `HashSet` answers
    /// membership and the `VecDeque` remembers eviction order, both holding
    /// every id (`recent_ids::RecentIds`) — so its residency is `capacity * 2 *
    /// size_of::<DataId>()`, not a payload figure.
    #[must_use]
    pub(in crate::learner) fn queue_capacity_bytes(&self) -> u64 {
        // saturating: operator-set usize knobs, so nonsense must read as huge
        // rather than wrap small — the gauge exists to make oversizing visible.
        let integrate = u64::try_from(self.integrate_channel_capacity.get())
            .unwrap_or(u64::MAX)
            .saturating_mul(QUEUE_REFERENCE_PAYLOAD_BYTES);
        let id_bytes = u64::try_from(std::mem::size_of::<crate::ids::DataId>())
            .unwrap_or(u64::MAX)
            .saturating_mul(2);
        let ring = self
            .dedup_ring
            .map_or(0, |cap| u64::try_from(cap.get()).unwrap_or(u64::MAX))
            .saturating_mul(id_bytes);
        integrate.saturating_add(ring)
    }
}

/// A local store, its stable log and its integration worker on one `Storage`
/// rooted at `root` — plus the handles needed to stop the worker.
///
/// Wired here rather than inside the store because production registers the
/// worker with [`Shutdown`], which cancels and **drains** it before the node's
/// handles drop. A test that reopens the same root in the same process needs
/// the same drain: a worker mid-batch holds an upgraded `Arc<LocalStore>`, and
/// through it the DB's LOCK. A sleep-and-retry reopen ladder is what that looks
/// like when the drain is missing.
#[cfg(test)]
pub(crate) async fn open_test_local_store(
    root: &std::path::Path,
) -> Result<(Arc<LocalStore>, TestWorker), Error> {
    let storage = Storage::open_for_test(root)?;
    let stable_log: Arc<dyn StableLogSnapshot> =
        Arc::new(crate::learner::stable_log::StableLog::open_on(Arc::clone(&storage)).await?);
    let cancel = tokio_util::sync::CancellationToken::new();
    let (store, rx) =
        store::create_local_store(&LocalStoreConfig::for_test(), storage, stable_log).await?;
    let handle = tokio::spawn(store::run_integrate_worker(
        Arc::downgrade(&store),
        rx,
        cancel.clone(),
    ));
    Ok((store, TestWorker { cancel, handle }))
}

/// The integration worker of an [`open_test_local_store`] store.
#[cfg(test)]
pub(crate) struct TestWorker {
    cancel: tokio_util::sync::CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), Error>>,
}

#[cfg(test)]
impl TestWorker {
    /// Cancel and drain — what `Shutdown` does for the production worker. Call
    /// it after dropping the store, before reopening its root.
    pub(crate) async fn stop(self) {
        self.cancel.cancel();
        let _ = self.handle.await;
    }
}

pub(in crate::learner) async fn spawn_local_store(
    stable_log: Arc<dyn StableLogSnapshot>,
    storage: Arc<Storage>,
    config: &LocalStoreConfig,
    shutdown: &mut Shutdown,
) -> Result<Arc<LocalStore>, Error> {
    let (store, integrate_rx) =
        store::create_local_store(config, storage, stable_log.clone()).await?;

    // One integration worker per node, fed by every subscription. Registered
    // with `Shutdown` like `replica:log_flush`: a storage failure here means a
    // dying disk, so the node goes down loudly rather than degrading per-peer.
    shutdown.spawn(
        "local_store:integrate",
        store::run_integrate_worker(Arc::downgrade(&store), integrate_rx, shutdown.token()),
    );

    // Periodic fsync of the local store (it needs atomicity, not per-batch
    // durability — see run_local_store_flush_worker / design doc §Local Store).
    shutdown.spawn(
        "local_store:flush",
        store::run_local_store_flush_worker(
            Arc::downgrade(&store),
            config.flush_interval,
            shutdown.token(),
        ),
    );

    spawn_local_store_purge_worker(
        shutdown,
        store.clone(),
        stable_log.clone(),
        config.purge_grace_period,
    );

    Ok(store)
}
