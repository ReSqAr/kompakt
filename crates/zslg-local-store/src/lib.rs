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
//! than per batch (`store::run_local_store_flush_worker`): every byte is a
//! copy of quorum-durable replica data, so a crash loses at most a suffix, the
//! lagging cursor re-delivers it, and the re-delivery dedups by `data_id`.
//!
//! Entries leave by retirement, not deletion: `purge` arms an HLC threshold
//! that trails the stable watermark by a grace period, and the engine clamps
//! reads at it.

use error_union::error_union;
use zslg_consensus::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log::LogError;
use zslg_net::NetError;
use zslg_snapshot::SnapshotError;
use zslg_time::ClockError;

mod key;
pub mod metrics;
mod purge;
mod recent_ids;
mod store;
#[cfg(test)]
mod testing;

#[cfg(test)]
mod local_store_proptest;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use rocksdb_kit::Storage;
use shutdown::Shutdown;
use zslg_log::StableLogSnapshot;

pub use recent_ids::Dedup;
/// Wiring for harnesses that run a store without the node's `Shutdown`.
pub use store::{IntegrateItem, create_local_store, run_integrate_worker};
pub use store::{LOCAL_STORE_CURSORS_CF, LOCAL_STORE_DATA_CF, LocalStore};

/// Everything the local-store subsystem needs, produced by
/// `LearnerConfig::local_store_config`.
/// Bundling the directory with the tunables means `spawn_local_store` takes one
/// argument instead of a config plus a slice of it.
pub struct LocalStoreConfig {
    pub flush_interval: Duration,
    pub purge_grace_period: Duration,
    /// Entries in the recent-`DataId` filter, which drops the duplicate copies
    /// of a write that arrive from each replica this node subscribes to.
    /// `None` disables it.
    ///
    /// Sized in *records*, not time, so it spans proportionally less arrival
    /// skew as throughput rises — hence the headroom in the default. Measured
    /// plateau is 10k-50k (+36..39% throughput); 8,192 gives only +28.5% and
    /// 1,024 nothing, while 1,000,000 regresses to +30% on cache locality.
    /// Default 16,384 is roughly 0.5 MB per store.
    pub dedup_ring: Option<NonZeroUsize>,
    /// Depth of the shared integration channel every subscription forwards
    /// into. Sized for a few batches across all upstreams, so the readers keep
    /// producing while a commit is in flight.
    pub integrate_channel_capacity: NonZeroUsize,
    /// Depth of each unstable-tail subscriber's own queue — how far behind a
    /// consumer may fall before the fan-out hangs up on it and it rescans the
    /// unstable window. At least one integrate batch, or a single batch would
    /// overrun it.
    pub unstable_subscriber_queue_capacity: NonZeroUsize,
}

pub const QUEUE_REFERENCE_PAYLOAD_BYTES: u64 = 2 * 1024;

/// Default for [`LocalStoreConfig::unstable_subscriber_queue_capacity`]: two
/// full integrate batches.
pub const DEFAULT_UNSTABLE_SUBSCRIBER_QUEUE_CAPACITY: NonZeroUsize =
    bytesize::nonzero(2 * store::MAX_BATCH);

impl LocalStoreConfig {
    /// Test-only constructor carrying the production defaults. Production
    /// builds this from the owning node config's flat fields via
    /// `LearnerConfig::local_store_config`.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn for_test() -> Self {
        Self {
            flush_interval: Duration::from_secs(1),
            purge_grace_period: Duration::from_secs(5 * 60),
            dedup_ring: NonZeroUsize::new(16_384),
            integrate_channel_capacity: bytesize::nonzero(2048),
            unstable_subscriber_queue_capacity: DEFAULT_UNSTABLE_SUBSCRIBER_QUEUE_CAPACITY,
        }
    }

    /// Engine sizing is RocksDB's business now; nothing left to validate.
    pub fn validate(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Worst-case bytes this store's queues can hold — reported beside the
    /// engine's charged bytes, never folded into that budget (see
    /// `ReplicaLogConfig::queue_capacity_bytes`
    /// for why the two are kept apart).
    ///
    /// Two terms, and only one of them is payload: the integrate channel holds
    /// records on their way to a batch, while the dedup ring holds `DataId`s
    /// only. The ring's ids are resident **twice** — the `HashSet` answers
    /// membership and the `VecDeque` remembers eviction order, both holding
    /// every id (`recent_ids::RecentIds`) — so its residency is `capacity * 2 *
    /// size_of::<DataId>()`, not a payload figure.
    #[must_use]
    pub fn queue_capacity_bytes(&self) -> u64 {
        // saturating: operator-set usize knobs, so nonsense must read as huge
        // rather than wrap small — the gauge exists to make oversizing visible.
        let integrate = u64::try_from(self.integrate_channel_capacity.get())
            .unwrap_or(u64::MAX)
            .saturating_mul(QUEUE_REFERENCE_PAYLOAD_BYTES);
        let id_bytes = u64::try_from(std::mem::size_of::<zslg_log::DataId>())
            .unwrap_or(u64::MAX)
            .saturating_mul(2);
        let ring = self
            .dedup_ring
            .map_or(0, |cap| u64::try_from(cap.get()).unwrap_or(u64::MAX))
            .saturating_mul(id_bytes);
        integrate.saturating_add(ring)
    }
}

pub async fn spawn_local_store<E>(
    stable_log: Arc<dyn StableLogSnapshot>,
    storage: Arc<Storage>,
    config: &LocalStoreConfig,
    shutdown: &mut Shutdown<E>,
) -> Result<Arc<LocalStore>, Error>
where
    E: From<Error> + From<shutdown::Stopped> + std::fmt::Display + Send + 'static,
{
    let (store, integrate_rx) =
        store::create_local_store(config, storage, stable_log.clone()).await?;

    // One integration worker per node, fed by every subscription. Registered
    // with `Shutdown` like `replica:log_flush`: a storage failure here means a
    // dying disk, so the node goes down loudly rather than degrading per-peer.
    let (weak, cancel) = (Arc::downgrade(&store), shutdown.token());
    shutdown.spawn("local_store:integrate", async move {
        store::run_integrate_worker(weak, integrate_rx, cancel)
            .await
            .map_err(E::from)
    });

    // Periodic fsync of the local store (it needs atomicity, not per-batch
    // durability — see run_local_store_flush_worker / design doc §Local Store).
    let (weak, interval, cancel) = (
        Arc::downgrade(&store),
        config.flush_interval,
        shutdown.token(),
    );
    shutdown.spawn("local_store:flush", async move {
        store::run_local_store_flush_worker(weak, interval, cancel)
            .await
            .map_err(E::from)
    });

    purge::spawn_local_store_purge_worker(
        shutdown,
        store.clone(),
        stable_log.clone(),
        config.purge_grace_period,
    );

    Ok(store)
}

error_union! {
    /// What the local store can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(LifecycleError),
        Config(ConfigError),
        Clock(ClockError),
        Log(LogError),
        Snapshot(SnapshotError),
        Net(NetError),
        Consensus(ConsensusError),
    }
    from zslg_time::Error { Clock }
    bridge zslg_log::Error
    bridge zslg_snapshot::Error
    bridge zslg_consensus::Error
    bridge zslg_net::Error
    bridge zslg_error::Error
}

impl From<rocksdb_kit::StorageError> for Error {
    fn from(e: rocksdb_kit::StorageError) -> Self {
        match e {
            rocksdb_kit::StorageError::InvalidConfiguration(msg) => {
                Self::Config(ConfigError::InvalidConfiguration(msg))
            }
            rocksdb_kit::StorageError::Storage(msg) => Self::Log(LogError::Storage(msg)),
        }
    }
}

impl From<shutdown::Stopped> for Error {
    fn from(_: shutdown::Stopped) -> Self {
        Self::Lifecycle(LifecycleError::Shutdown)
    }
}
