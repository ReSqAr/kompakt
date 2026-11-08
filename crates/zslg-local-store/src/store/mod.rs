//! The [`LocalStore`] itself: the two column families, the paths that read
//! them, and the blocking loop that writes them.
//!
//! Every mutating path opens its own batch on its own blocking thread with no
//! write lock, which is safe for a reason specific to this store: data rows are
//! content-addressed (concurrent writers can only overwrite a `DataId` with
//! identical bytes) and cursors are partitioned by upstream. Do not generalise
//! it to a keyspace where two writers could disagree about a key's value.
//!
//! Reads are blocking scans pushed into a channel, so a caller draining slowly
//! back-pressures the scan rather than buffering the store into memory.
//!
//! The one live read is the unstable fan-out (`subscribe_unstable`). The
//! integrate worker publishes a batch's rows right after `Storage::write` —
//! only rows the duplicate filter admitted, only those still above the stable
//! watermark — and never waits on a subscriber: a full queue is a hang-up
//! (the fan-out). No fsync is awaited, so an announced entry can be
//! lost to a crash; the subscription is at-least-once and the rescan that
//! follows a hang-up, not the live path, is its recovery.

use rocksdb_kit::ColumnFamily;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::Dedup;
use bytes::Bytes;
use crossbeam_combinators::{Fanout, FanoutConfig, OverflowPolicy, Registry, broadcast};
use rocksdb_kit::Storage;
use tokio::sync::watch;

use crate::Error;
use zslg_log::Data;
use zslg_log::DataId;
use zslg_log::StableLogSnapshot;
use zslg_log::metrics::LogType;

const STREAM_CHANNEL_CAPACITY: usize = 32;

mod engine;
mod flush;
mod integrator;
mod read;
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests;
mod write;

pub(crate) use flush::run_local_store_flush_worker;
use integrator::BatchOutcome;
pub(crate) use integrator::MAX_BATCH;
pub use integrator::{IntegrateItem, run_integrate_worker};

/// Shared local-store state, held behind an `Arc<LocalStore>` by every handle.
pub struct LocalStore {
    /// Self-reference so the persistent `integrate_loop`
    /// blocking threads hold only a `Weak` and upgrade per batch (mirrors the
    /// replica flush loop + `RpcClient`): idle threads pin nothing, so a dropped
    /// store frees its handles. Set via `Arc::new_cyclic` in `create_local_store`.
    ///
    /// The node's one RocksDB. This store owns two of its column families:
    /// [`LOCAL_STORE_DATA_CF`] (the `DataId` key itself — it is
    /// HLC-major — retired at the purge threshold, value = the bare blob) and
    /// [`LOCAL_STORE_CURSORS_CF`] (primary `NodeId`, overwritten in
    /// place, never retired). One DB is what makes a batch spanning them
    /// atomic — see the module header.
    storage: Arc<Storage>,
    stable_log: Arc<dyn StableLogSnapshot>,
    integrated: watch::Sender<()>,
    /// Set on every committed batch; cleared by the periodic flush worker
    /// ([`run_local_store_flush_worker`]) when it fsyncs. Lets an idle store
    /// skip the flush entirely. See `LearnerConfig::local_store_flush_interval`.
    dirty: AtomicBool,
    /// `DataId`s known to be durably written, shared by **all** of this node's
    /// integrate workers — there is one per replica peer
    /// (`subscriber/peer.rs`), so a per-worker filter would never see another
    /// stream's copies.
    ///
    /// `parking_lot` is correct here per CLAUDE.md §4: every critical section
    /// is inside a `spawn_blocking` body and never `.await`s.
    recent: parking_lot::Mutex<Dedup>,
    /// Feeds the node's single integration worker. Every subscription's
    /// `integrate_loop` forwards into this, so one worker builds batches that
    /// span upstreams and per-upstream ordering is preserved by each
    /// forwarder delivering its own log in order.
    integrate_tx: crossbeam_channel::Sender<IntegrateItem>,
    /// Live subscribers to first-time integrations (`subscribe_unstable`).
    unstable: Fanout<Data>,
    unstable_registry: Registry<Data>,
    /// Test-only fault injection for the commit fence.
    #[cfg(test)]
    fail_next_append: AtomicBool,
    /// Test-only observation of the filter: counting rows proves nothing,
    /// because the LSM dedups by `DataId` regardless.
    #[cfg(test)]
    skipped_duplicates: std::sync::atomic::AtomicU64,
}

/// Open the store and hand back the receiving half of its integration
/// channel. The caller starts the single worker over it — see
/// [`super::spawn_local_store`], which registers it with `Shutdown` so a
/// failure takes the node down the way `replica:log_flush` does.
pub async fn create_local_store(
    config: &super::LocalStoreConfig,
    storage: Arc<Storage>,
    stable_log: Arc<dyn StableLogSnapshot>,
) -> Result<(Arc<LocalStore>, crossbeam_channel::Receiver<IntegrateItem>), Error> {
    // Both families are already open: they were named in the node's
    // `COLUMN_FAMILIES` list when the one DB opened. Probe them here anyway, so
    // a boot-wiring omission fails at store creation rather than at the first
    // write.
    drop(storage.cf(LOCAL_STORE_DATA_CF)?);
    drop(storage.cf(LOCAL_STORE_CURSORS_CF)?);

    let (integrate_tx, integrate_rx) =
        crossbeam_channel::bounded(config.integrate_channel_capacity.get());
    let store = Arc::new(LocalStore::new(
        storage,
        stable_log,
        config.dedup_ring,
        integrate_tx,
        broadcast(
            "unstable tail subscription",
            FanoutConfig::with_sub_cap(config.unstable_subscriber_queue_capacity),
            OverflowPolicy::Shed,
        ),
    ));
    Ok((store, integrate_rx))
}

impl LocalStore {
    fn new(
        storage: Arc<Storage>,
        stable_log: Arc<dyn StableLogSnapshot>,
        dedup_ring: Option<std::num::NonZeroUsize>,
        integrate_tx: crossbeam_channel::Sender<IntegrateItem>,
        (unstable, unstable_registry): (Fanout<Data>, Registry<Data>),
    ) -> Self {
        let (integrated, _) = watch::channel(());
        Self {
            storage,
            stable_log,
            integrated,
            dirty: AtomicBool::new(false),
            integrate_tx,
            unstable,
            unstable_registry,
            recent: parking_lot::Mutex::new(Dedup::new(dedup_ring)),
            #[cfg(test)]
            fail_next_append: AtomicBool::new(false),
            #[cfg(test)]
            skipped_duplicates: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Test-only: make the next `append_batch` fail, to exercise the commit
    /// fence in [`BatchIntegrator::drain_one`].
    #[cfg(test)]
    fn fail_next_append(&self) {
        self.fail_next_append.store(true, Ordering::SeqCst);
    }

    /// Test-only: how many duplicate copies the filter has dropped.
    #[cfg(test)]
    fn skipped_duplicates(&self) -> u64 {
        self.skipped_duplicates.load(Ordering::SeqCst)
    }

    /// Mark the store as having uncommitted-to-disk writes, so the periodic
    /// flush worker fsyncs on its next tick. Cheap (relaxed store) — called on
    /// every committed batch.
    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Apply the post-fence side effects of one committed batch: log-write
    /// metric and the `integrated` signal when the batch had integrate activity
    /// (the load-bearing P4 invariant — `sync_quorum`'s "last job
    /// cleared" edge relies on this).
    fn apply_post_fence(&self, outcome: BatchOutcome) {
        if outcome.total_records > 0 {
            zslg_log::metrics::log_writes(LogType::Local, outcome.total_records);
        }

        if outcome.had_integrate_or_fetch {
            self.integrated.send_replace(());
        }
    }

    /// Announce a written batch's first-time rows to the unstable subscribers.
    /// After the write, not the fsync: a subscriber may see an entry a crash
    /// then loses, which at-least-once delivery permits. The watermark is
    /// re-read here, so a row it passed while the batch was being built is not
    /// announced.
    fn publish_unstable(&self, rows: Vec<(DataId, Bytes)>) {
        let watermark = self.stable_log.end_hlc();
        let fresh: Vec<Data> = rows
            .into_iter()
            .filter(|(data_id, _)| watermark.is_none_or(|w| data_id.hlc() > w))
            .map(|(data_id, blob)| Data { data_id, blob })
            .collect();
        for data in fresh {
            // `Stopped` only while the store is being dropped: no subscriber
            // is left to miss it.
            let _ = self.unstable.publish(data);
        }
    }
}

/// Local-store data rows, keyed by `DataId`.
pub const LOCAL_STORE_DATA_CF: ColumnFamily = ColumnFamily::new("local_store_data");

/// Local-store subscription cursors, keyed by upstream `NodeId`.
pub const LOCAL_STORE_CURSORS_CF: ColumnFamily = ColumnFamily::new("local_store_cursors");
