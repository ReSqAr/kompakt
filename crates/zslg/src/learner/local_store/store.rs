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

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rust_rocksdb::WriteBatch;

use crate::database::Subsystem;
use crate::database::storage::Storage;
use crate::learner::local_store::Dedup;
use crate::learner::local_store::key::{
    DATA_PRIMARY_WIDTH, decode_data_key, encode_cursor_key, encode_data_key,
};
use bytes::Bytes;
use crossbeam_combinators::Fanout;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::api::{StableLogSnapshot, WindowItem};
use crate::error::{Error, ToStorage};
use crate::hlc::Hlc;
use crate::ids::{DataId, LogIndex, NodeId};
use crate::meta::Data;
use crate::metrics::{self, LocalWriteKind, LogType, OpState};
use crossbeam_combinators::Channels;

const STREAM_CHANNEL_CAPACITY: usize = 32;

/// Shared local-store state, held behind an `Arc<LocalStore>` by every handle.
pub(crate) struct LocalStore {
    /// Self-reference so the persistent `integrate_loop`
    /// blocking threads hold only a `Weak` and upgrade per batch (mirrors the
    /// replica flush loop + `RpcClient`): idle threads pin nothing, so a dropped
    /// store frees its handles. Set via `Arc::new_cyclic` in `create_local_store`.
    ///
    /// The node's one RocksDB. This store owns two of its column families:
    /// [`Subsystem::LocalStoreData`] (the `DataId` key itself — it is
    /// HLC-major — retired at the purge threshold, value = the bare blob) and
    /// [`Subsystem::LocalStoreCursors`] (primary `NodeId`, overwritten in
    /// place, never retired). One DB is what makes a batch spanning them
    /// atomic — see the module header.
    storage: Arc<Storage>,
    stable_log: Arc<dyn StableLogSnapshot>,
    integrated: watch::Sender<()>,
    /// Set on every committed batch; cleared by the periodic flush worker
    /// ([`run_local_store_flush_worker`]) when it fsyncs. Lets an idle store
    /// skip the flush entirely. See [`crate::learner::LearnerConfig::local_store_flush_interval`].
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
pub(in crate::learner::local_store) async fn create_local_store(
    config: &super::LocalStoreConfig,
    storage: Arc<Storage>,
    stable_log: Arc<dyn StableLogSnapshot>,
) -> Result<(Arc<LocalStore>, crossbeam_channel::Receiver<IntegrateItem>), Error> {
    // Both families are already open: they were named in the node's
    // `COLUMN_FAMILIES` list when the one DB opened. Probe them here anyway, so
    // a boot-wiring omission fails at store creation rather than at the first
    // write.
    drop(storage.cf(Subsystem::LocalStoreData)?);
    drop(storage.cf(Subsystem::LocalStoreCursors)?);

    let (integrate_tx, integrate_rx) =
        crossbeam_channel::bounded(config.integrate_channel_capacity.get());
    let store = Arc::new(LocalStore::new(
        storage,
        stable_log,
        config.dedup_ring,
        integrate_tx,
        Fanout::new(
            config.unstable_subscriber_queue_capacity,
            "unstable tail subscription",
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
        unstable: Fanout<Data>,
    ) -> Self {
        let (integrated, _) = watch::channel(());
        Self {
            storage,
            stable_log,
            integrated,
            dirty: AtomicBool::new(false),
            integrate_tx,
            unstable,
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
            metrics::log_writes(LogType::Local, outcome.total_records);
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
        if !fresh.is_empty() {
            let hung_up = self.unstable.send_all(&fresh);
            if hung_up > 0 {
                metrics::unstable_fanout_hangups(hung_up);
            }
        }
    }

    /// Open a store, and the stable log it reads the watermark from, on **one**
    /// `Storage` rooted at `root` — the production shape, where both are column
    /// families of the node's single DB.
    ///
    /// The integration worker is detached, so this is for tests that use a
    /// fresh root and never reopen it. A test that reopens must drain the
    /// worker first and wants
    /// [`open_test_local_store`](crate::learner::open_test_local_store).
    #[cfg(test)]
    pub(crate) async fn simple_at(root: std::path::PathBuf) -> Result<Arc<Self>, Error> {
        let (store, _) = Self::simple_with(root, &super::LocalStoreConfig::for_test()).await?;
        Ok(store)
    }

    /// [`simple_at`](Self::simple_at) with a config of the test's choosing,
    /// handing back the stable log the store reads its watermark from.
    #[cfg(test)]
    pub(in crate::learner) async fn simple_with(
        root: std::path::PathBuf,
        config: &super::LocalStoreConfig,
    ) -> Result<(Arc<Self>, Arc<crate::learner::stable_log::StableLog>), Error> {
        let storage = Storage::open_for_test(&root)?;
        let stable_log =
            Arc::new(crate::learner::stable_log::StableLog::open_on(Arc::clone(&storage)).await?);
        let watermark_source: Arc<dyn StableLogSnapshot> = stable_log.clone();
        let (store, integrate_rx) = create_local_store(config, storage, watermark_source).await?;
        // Production starts this in `spawn_local_store` under `Shutdown`;
        // tests get a detached one so `integrate_loop` has a worker to feed.
        tokio::spawn(run_integrate_worker(
            Arc::downgrade(&store),
            integrate_rx,
            CancellationToken::new(),
        ));
        Ok((store, stable_log))
    }
}

// ── Read path ──────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl crate::api::LocalStoreView for LocalStore {
    fn stream_above_hlc(&self, threshold: Option<Hlc>) -> Channels<Result<Data, Error>> {
        let storage = self.storage.clone();
        let (tx, rx) =
            crossbeam_channel::bounded::<Result<WindowItem, Error>>(STREAM_CHANNEL_CAPACITY);
        let (ch, closer) = Channels::from(rx, "stream_above_hlc");
        tokio::task::spawn_blocking(move || {
            let start = match threshold {
                None => Bound::Unbounded,
                Some(wm) => Bound::Excluded(DataId::from_parts(wm, NodeId::MAX, u64::MAX)),
            };
            stream_scan(&storage, (start, Bound::Unbounded), &tx, None);
            // Only on the normal path: a panic here drops `closer` instead, and
            // the consumer sees `End::Aborted` rather than a clean end.
            closer.close();
        });
        // Uncapped, so the terminal is always `Complete` and carries nothing
        // this caller needs.
        ch.filter_map(|r| match r {
            Ok(WindowItem::Entry(d)) => Some(Ok(d)),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
    }

    fn stream_window_ordered(
        &self,
        above: Option<Hlc>,
        upto: Hlc,
        max_entries: Option<std::num::NonZeroUsize>,
    ) -> Channels<Result<WindowItem, Error>> {
        let storage = self.storage.clone();
        let (tx, rx) = crossbeam_channel::bounded(STREAM_CHANNEL_CAPACITY);
        let (ch, closer) = Channels::from(rx, "stream_window_ordered");
        tokio::task::spawn_blocking(move || {
            let start = match above {
                None => Bound::Unbounded,
                Some(a) => Bound::Excluded(DataId::from_parts(a, NodeId::MAX, u64::MAX)),
            };
            let end = Bound::Included(DataId::from_parts(upto, NodeId::MAX, u64::MAX));
            stream_scan(&storage, (start, end), &tx, max_entries);
            closer.close();
        });
        ch
    }

    fn subscribe_integrated(&self) -> watch::Receiver<()> {
        self.integrated.subscribe()
    }

    fn subscribe_unstable(&self) -> Channels<Data> {
        self.unstable.subscribe()
    }

    async fn subscription_cursor(&self, upstream: NodeId) -> Result<Option<LogIndex>, Error> {
        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || read_cursor(&storage, upstream))
            .await
            .to_storage("subscription_cursor spawn_blocking panicked")?
    }
}

// ── Write path ─────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl crate::api::LocalStoreMut for LocalStore {
    async fn integrate_loop(
        &self,
        mut records: Channels<Result<(NodeId, LogIndex, Data), Error>>,
    ) -> Result<(), Error> {
        let tx = self.integrate_tx.clone();
        // Forwarding blocks on both halves (Channels::recv and a bounded
        // send), so it belongs on the blocking pool. It does no IO — the
        // single worker owns every commit.
        let forwarded = tokio::task::spawn_blocking(move || -> (Result<(), Error>, bool) {
            let mut sent_any = false;
            loop {
                let Some(batch) = records.ready_batch(MAX_BATCH) else {
                    // A producer that *declared* completion finished its work,
                    // so this is a clean end. Whether a completed stream is
                    // acceptable is the caller's call — a subscription cannot
                    // complete, and `subscriber/peer.rs` treats even a clean
                    // end as a fault for that reason. An abort is a fault
                    // here, though: items may be missing (CLAUDE.md §20).
                    return match records.end_reason() {
                        crossbeam_combinators::End::Complete => (Ok(()), sent_any),
                        crossbeam_combinators::End::Aborted { reason } => (
                            Err(Error::SubscriptionStreamEnded {
                                reason: reason.to_string(),
                            }),
                            sent_any,
                        ),
                    };
                };
                for item in batch {
                    let (node, idx, data) = match item {
                        Ok(rec) => rec,
                        // In-band: the reason reaches the caller directly.
                        Err(e) => return (Err(e), sent_any),
                    };
                    if tx.send(IntegrateItem::Record(node, idx, data)).is_err() {
                        return (
                            Err(Error::Storage("local store integrator gone".into())),
                            sent_any,
                        );
                    }
                    sent_any = true;
                }
            }
        })
        .await
        .to_storage("integrate forward task panicked")?;

        let (outcome, sent_any) = forwarded;
        // Wait for what we forwarded to commit before returning: the caller
        // measures progress by re-reading the durable cursor, so returning
        // early would under-report it.
        if sent_any {
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            if self
                .integrate_tx
                .send(IntegrateItem::Barrier(done_tx))
                .is_ok()
            {
                // A dropped sender resolves this too: the worker is gone, so
                // waiting longer cannot make the records appear.
                let _ = done_rx.await;
            }
        }
        outcome
    }

    async fn purge_below_hlc(&self, threshold: Hlc) -> Result<(), Error> {
        // The data key is the `DataId` itself, which is HLC-major (ids.rs:170,
        // bytes 0..8 are the HLC big-endian), so key order IS HLC order and
        // `hlc <= threshold` is a key-order prefix. A durable range delete says
        // exactly that — no compaction filter, no in-memory clamp, and nothing
        // to re-arm after a reopen.
        let cf = self.storage.cf(Subsystem::LocalStoreData)?;
        // An 8-byte key sorts before every 32-byte key sharing its prefix, so
        // `threshold + 1` big-endian is the exclusive upper bound. checked_add:
        // a threshold of u64::MAX means "purge everything", and an all-0xFF key
        // longer than any real one is above the whole family.
        let end: Vec<u8> = match threshold.raw().checked_add(1) {
            Some(next) => next.to_be_bytes().to_vec(),
            None => vec![0xFFu8; DATA_PRIMARY_WIDTH.saturating_add(1)],
        };
        let mut batch = WriteBatch::default();
        // The start bound is an 8-byte HLC-width prefix of zeros, matching the
        // 8-byte end bound above — neither is a full (32-byte) key.
        batch.delete_range_cf(&cf, &[0u8; 8][..], &end[..]);
        self.storage.write(batch)?;
        // `fence_async`, not `fence`: this is an async fn called directly from
        // the local-store purge worker's task, with no blocking body of its
        // own to sit in (CLAUDE.md §19) — `fence` is documented "call it from
        // spawn_blocking", which this isn't.
        self.storage.fence_async().await?;
        Ok(())
    }
}

// ===========================================================================
// Batch-integrator: drains the integration channel on a dedicated blocking
// thread, batches writes across the two keyspaces, and holds only a
// `Weak<LocalStore>` — idle threads pin nothing.
// ===========================================================================

/// One item on the shared integration channel.
///
/// `Barrier` is an **ordering** point, not a durability one, and the
/// distinction matters. It resolves once every record queued ahead of it has
/// been through `append_batch` — so a subsequent read sees them — but there is
/// no `flush()` on this path, so nothing is fsynced (CLAUDE.md §8: durable
/// means `append_batch` **and** `flush` returned; here durability is bounded by
/// the periodic flush worker, and a crash re-delivers from the durable cursor).
///
/// Readability is exactly what the sender needs. `integrate_loop` measures an
/// attempt's progress by re-reading the subscription cursor, and that read is
/// served from the engine's in-memory state; without the barrier it could run
/// while records were still queued and under-report (`subscriber/peer.rs`).
///
/// It was called `Flush`, which invited the stronger reading.
pub(in crate::learner::local_store) enum IntegrateItem {
    Record(NodeId, LogIndex, Data),
    Barrier(tokio::sync::oneshot::Sender<()>),
}

/// Aggregated side effects of one committed batch, applied after the fence by
/// [`LocalStore::apply_post_fence`].
struct BatchOutcome {
    total_records: usize,
    had_integrate_or_fetch: bool,
}

/// Max records / outcomes drained into one `WriteBatch`.
pub(in crate::learner::local_store) const MAX_BATCH: usize = 1024;

/// The node's single integration worker: drains the shared channel, batches
/// across upstreams, commits. Registered with `Shutdown` by
/// [`super::spawn_local_store`], so an error ends the task and takes the node
/// down — the same contract as `replica:log_flush`.
pub(in crate::learner) async fn run_integrate_worker(
    weak: Weak<LocalStore>,
    records: crossbeam_channel::Receiver<IntegrateItem>,
    cancel: CancellationToken,
) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || BatchIntegrator { weak }.run(&records, &cancel))
        .await
        .to_storage("local store integrate worker panicked")?
}

/// How long an idle worker parks before re-checking cancellation. The store
/// holds the only long-lived sender, so a dropped store also wakes it — this
/// is the belt for the case where the node outlives its subscriptions.
/// Nothing that unblocks a parked blocking loop may depend on async cleanup
/// running, so it cannot be a token wait.
const INTEGRATE_IDLE_POLL: Duration = Duration::from_millis(250);

/// Drains integration records on one blocking thread for its lifetime — mirrors
/// `replica/log.rs::flush_loop_blocking`. Idles in `blocking_recv`, upgrades the
/// `Weak` only while processing a batch, drains consecutive batches while items
/// are queued. Exits when the channel closes (subscriber gone) or the store is
/// dropped.
struct BatchIntegrator {
    weak: Weak<LocalStore>,
}

impl BatchIntegrator {
    fn run(
        self,
        records: &crossbeam_channel::Receiver<IntegrateItem>,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        loop {
            let first = match records.recv_timeout(INTEGRATE_IDLE_POLL) {
                Ok(item) => item,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if cancel.is_cancelled() {
                        return Ok(());
                    }
                    continue;
                }
                // The store holds the only long-lived sender, so this doubles
                // as "the store is gone".
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(()),
            };
            // A barrier that arrives as the *first* item has nothing to wait
            // for: the channel is FIFO and the previous iteration committed
            // before looping back to `recv`, so everything ahead of it is
            // already in the engine. Answer and keep waiting.
            let first = match first {
                IntegrateItem::Barrier(done) => {
                    let _ = done.send(());
                    continue;
                }
                IntegrateItem::Record(n, i, d) => (n, i, d),
            };
            let Some(store) = self.weak.upgrade() else {
                return Ok(()); // store dropped
            };
            let probe = metrics::local_store_integrate_probe();
            // Barriers pulled off the channel while building this batch. They
            // are answered *after* the commit below, never as they are seen: a
            // barrier answered early would let its sender re-read the cursor
            // before the records ahead of it were in the engine, which is the
            // exact under-report it exists to prevent.
            //
            // Answering late is harmless — a barrier may end up covering
            // records queued after it, which is a superset of what it promises.
            let mut barriers = Vec::new();
            let outcome = Self::drain_one(&store, first, records, MAX_BATCH, &mut barriers)?;
            metrics::local_store_integrate_queue_depth(records.len());
            store.apply_post_fence(outcome);
            for done in barriers {
                let _ = done.send(());
            }
            probe.finish(OpState::Success);
        }
    }

    /// Drain one batch of integration records, commit across data → cursors,
    /// and return the `BatchOutcome`.
    fn drain_one(
        store: &LocalStore,
        first: (NodeId, LogIndex, Data),
        records: &crossbeam_channel::Receiver<IntegrateItem>,
        max_batch: usize,
        barriers: &mut Vec<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<BatchOutcome, Error> {
        let end_hlc = store.stable_log.end_hlc();
        let mut sub_cursors: HashMap<NodeId, LogIndex> = HashMap::new();
        // Staged data rows: the id (which is the primary, and whose HLC is the
        // secondary) and the bare blob. `Data` is `{ data_id, blob }`, so
        // encoding it into the value would re-store the key and copy the whole
        // payload to do it, once per record, on the busiest store in the
        // process. `Bytes::clone` is a refcount bump; the reader rebuilds `Data`
        // from the key it already has.
        let mut data_rows: Vec<(DataId, Bytes)> = Vec::new();
        // Level 1 of the duplicate filter: ids already staged in THIS batch.
        let mut batch_seen: HashSet<DataId> = HashSet::new();
        // Ids to remember *if and only if* this batch commits.
        let mut admitted: Vec<DataId> = Vec::new();
        let mut total_records: usize = 0;
        let mut skipped_stale: u64 = 0;
        let mut skipped_duplicate: u64 = 0;
        let mut flush_bytes: usize = 0;

        // Level 2 is the store-wide ring. One short lock for the read half,
        // released before the commit so the per-peer integrate workers never
        // serialise through the engine.
        let recent = store.recent.lock();

        let mut process = |src: NodeId, idx: LogIndex, rec: Data| -> Result<(), Error> {
            // The ack. It advances for EVERY copy, deduped or not — but the
            // cursors are only written after the data batch commits below, so
            // nothing is acked before it is durable.
            sub_cursors
                .entry(src)
                .and_modify(|m| *m = (*m).max(idx))
                .or_insert(idx);

            let rec_hlc = rec.data_id.hlc();
            if end_hlc.is_some_and(|wm| rec_hlc <= wm) {
                skipped_stale = skipped_stale.saturating_add(1);
                return Ok(());
            }
            // Level 2: an earlier batch already committed this id.
            if recent.contains(rec.data_id) {
                skipped_duplicate = skipped_duplicate.saturating_add(1);
                return Ok(());
            }
            // Level 1: already staged in this batch — one write, all cursors.
            if !batch_seen.insert(rec.data_id) {
                skipped_duplicate = skipped_duplicate.saturating_add(1);
                return Ok(());
            }

            admitted.push(rec.data_id);
            total_records = total_records.saturating_add(1);
            let len = rec.blob.len();
            metrics::local_store_entry_bytes(len);
            flush_bytes = flush_bytes.saturating_add(len);
            data_rows.push((rec.data_id, rec.blob));
            Ok(())
        };

        let (src, idx, rec) = first;
        process(src, idx, rec)?;
        let mut count: usize = 1;
        while count < max_batch {
            match records.try_recv() {
                // A barrier rides along with the batch it follows and is
                // answered by the caller once that batch commits.
                Ok(IntegrateItem::Barrier(done)) => barriers.push(done),
                Ok(IntegrateItem::Record(s, i, r)) => {
                    process(s, i, r)?;
                    count = count.saturating_add(1);
                }
                Err(_) => break,
            }
        }

        drop(recent); // release the read lock before the commit fence

        if skipped_stale > 0 {
            metrics::local_store_integrate_skipped_stale(skipped_stale);
        }
        if skipped_duplicate > 0 {
            metrics::local_store_integrate_skipped_duplicate(skipped_duplicate);
            #[cfg(test)]
            store
                .skipped_duplicates
                .fetch_add(skipped_duplicate, Ordering::SeqCst);
        }

        // ONE batch across both column families, so a record and the cursor
        // that covers it commit together or not at all
        // (`data_and_cursor_commit_atomically`). They were separate DBs before,
        // which is why this used to be an *ordering* rule — data first, cursors
        // last — that a crash in between could still break.
        //
        // No fsync here (durability is bounded by the periodic flush worker); a
        // crash re-delivers from the durable cursor and the whole downstream
        // dedups by `data_id` (design doc §Local Store).
        //
        // A batch whose records were all deduped writes no data but must still
        // advance the cursors, or the peers resubscribe from a stale position
        // and re-deliver forever.
        if !data_rows.is_empty() || !sub_cursors.is_empty() {
            #[cfg(test)]
            if store.fail_next_append.swap(false, Ordering::SeqCst) {
                return Err(Error::Storage("induced append failure (test)".into()));
            }
            commit_batch(store, &data_rows, &sub_cursors)?;
            // THE FENCE: only ids whose data is now in the DB are remembered. A
            // failed write returned above, leaving the filter untouched, so the
            // retry writes them.
            store.recent.lock().record_committed(admitted);
            store.publish_unstable(data_rows);
        }
        store.mark_dirty();

        // Flush-level metrics (per-batch, before the ACK).
        metrics::local_store_ops(
            LocalWriteKind::Integrate,
            u64::try_from(total_records).unwrap_or(u64::MAX),
        );
        metrics::local_store_flush_bytes(flush_bytes);
        metrics::local_store_flush_entries(total_records);
        metrics::local_store_flushed();

        let outcome = BatchOutcome {
            total_records,
            had_integrate_or_fetch: true,
        };
        Ok(outcome)
    }
}

// ===========================================================================
// Free helpers — record builders, engine helpers, periodic fsync.
// ===========================================================================

/// A point read of one upstream's cursor.
fn read_cursor(storage: &Storage, upstream: NodeId) -> Result<Option<LogIndex>, Error> {
    let cf = storage.cf(Subsystem::LocalStoreCursors)?;
    let Some(pinned) = storage
        .db()
        .get_pinned_cf(&cf, encode_cursor_key(upstream))
        .map_err(|e| Error::Storage(format!("read local-store cursor for {upstream:?}: {e}")))?
    else {
        return Ok(None);
    };
    // PINNING RULE: `pinned` holds its block in the block cache for as long as
    // it lives. It must not cross an `.await` or a batch commit — decode here
    // and let it drop at the end of this expression.
    let bytes: [u8; 8] = pinned.as_ref().try_into().unwrap_or([0u8; 8]);
    Ok(Some(LogIndex(u64::from_be_bytes(bytes))))
}

/// What a data scan feeds. The payload is borrowed from the iterator's block
/// for the duration of the call only — a sink that keeps it must copy.
type DataSink<'a> = dyn FnMut(DataId, &[u8]) -> bool + 'a;

/// `DataId`-ordered scan of the data family. `sink` returning `false` stops it.
///
/// Purge is a durable `DeleteRange` (`purge_below_hlc`), so a purged prefix is
/// simply absent from the keyspace here — no read-side clamp to apply.
fn for_each_data(
    storage: &Storage,
    range: (Bound<DataId>, Bound<DataId>),
    sink: &mut DataSink<'_>,
) -> Result<(), Error> {
    let cf = storage.cf(Subsystem::LocalStoreData)?;
    let mut it = storage.db().raw_iterator_cf(&cf);
    let (start, end) = range;
    match start {
        Bound::Unbounded => it.seek_to_first(),
        Bound::Included(p) => it.seek(encode_data_key(p)),
        Bound::Excluded(p) => {
            // A data key IS its primary and primaries are unique, so seeking
            // `p` itself lands on it (to skip) or past it.
            let primary = encode_data_key(p);
            it.seek(primary);
            if it.key().is_some_and(|key| key == primary.as_slice()) {
                it.next();
            }
        }
    }
    while let Some(key) = it.key() {
        // PINNING RULE: `key` and `value` below both borrow the iterator's
        // current block and pin it in the block cache. Neither may cross an
        // `.await` or a batch commit; the sink copies what it wants to keep.
        let Some(data_id) = decode_data_key(key) else {
            return Err(Error::Storage(format!(
                "undecodable local-store data key ({} bytes)",
                key.len()
            )));
        };
        let past_end = match end {
            Bound::Unbounded => false,
            Bound::Included(e) => data_id > e,
            Bound::Excluded(e) => data_id >= e,
        };
        if past_end {
            break;
        }
        // `value()` is `None` only when `key()` is, so this is unreachable —
        // but substituting an empty payload for a missing one would hand a
        // caller a silently truncated record (§20).
        let Some(value) = it.value() else {
            return Err(Error::Storage(format!(
                "local-store data key with no value ({} bytes): {key:02x?}",
                key.len()
            )));
        };
        if !sink(data_id, value) {
            return Ok(());
        }
        it.next();
    }
    it.status()
        .map_err(|e| Error::Storage(format!("rocksdb scan (local store data): {e}")))
}

/// Stage one batch's data rows and cursor advances into a **single**
/// `WriteBatch` and commit it. Atomic across both column families — the win one
/// DB unlocks; see [`BatchIntegrator::drain_one`].
///
/// The local store never deletes, and identical payloads per id make
/// last-write-wins a no-op, so there is no get-before-insert on the data side.
/// Cursors do read first, to keep the stored value a running max.
fn commit_batch(
    store: &LocalStore,
    data_rows: &[(DataId, Bytes)],
    cursors: &HashMap<NodeId, LogIndex>,
) -> Result<(), Error> {
    let data_cf = store.storage.cf(Subsystem::LocalStoreData)?;
    let cursor_cf = store.storage.cf(Subsystem::LocalStoreCursors)?;
    // No global write lock: batches commit atomically and are ordered at the
    // WAL, so opening one never blocks on another writer.
    let mut batch = WriteBatch::default();
    for (data_id, blob) in data_rows {
        batch.put_cf(&data_cf, encode_data_key(*data_id), blob);
    }
    for (source, source_index) in cursors {
        // The cursor only ever moves forward (`max` against the stored value),
        // so out-of-order application within a source is harmless. The
        // integrate worker is the sole cursor writer, so the read-then-write is
        // race-free.
        let new = match read_cursor(&store.storage, *source)? {
            Some(prev) => LogIndex(prev.raw().max(source_index.raw())),
            None => *source_index,
        };
        batch.put_cf(&cursor_cf, encode_cursor_key(*source), new.0.to_be_bytes());
    }
    store.storage.write(batch)
}

/// Drive one blocking range scan into the stream channel `tx`, ending with the
/// terminal [`WindowItem`] that says whether the whole range was covered. A scan
/// or decode error is the stream's final item; nothing follows it.
/// [`for_each_data`] clamps at the retire threshold, so no purge-cutoff filter
/// is needed here.
///
/// `max_entries` is a **floor, not a ceiling**: once the count reaches it the
/// scan keeps emitting while the HLC is unchanged, then stops. Splitting an HLC
/// would be a correctness bug — the consumer resumes with
/// `Excluded(DataId::from_parts(hlc, NodeId::MAX, u64::MAX))`, which skips every
/// entry at that HLC, so a mid-HLC stop would drop the remainder silently.
fn stream_scan(
    storage: &Storage,
    range: (Bound<DataId>, Bound<DataId>),
    tx: &crossbeam_channel::Sender<Result<WindowItem, Error>>,
    max_entries: Option<std::num::NonZeroUsize>,
) {
    let mut sent: usize = 0;
    // Set once the cap is reached: the HLC we are finishing before stopping.
    let mut finishing: Option<Hlc> = None;
    let mut capped = false;
    let mut consumer_gone = false;
    let scan = for_each_data(
        storage,
        range,
        // The record *is* the key plus the blob — nothing to decode. The copy
        // is paid here because this `Data` outlives the scan on a channel (and
        // the payload is iterator-pinned, so it must not).
        &mut |data_id, payload| {
            let d = Data {
                data_id,
                blob: Bytes::copy_from_slice(payload),
            };
            let hlc = d.data_id.hlc();
            if finishing.is_some_and(|b| hlc != b) {
                capped = true;
                return false;
            }
            if tx.send(Ok(WindowItem::Entry(d))).is_err() {
                consumer_gone = true;
                return false;
            }
            // Saturating: the count is only compared against the cap, so
            // pinning at usize::MAX keeps the comparison true forever, which
            // is the correct behaviour past any conceivable cap.
            sent = sent.saturating_add(1);
            if finishing.is_none() && max_entries.is_some_and(|c| sent >= c.get()) {
                finishing = Some(hlc);
            }
            true
        },
    );
    if let Err(e) = scan {
        let _ = tx.send(Err(e));
        return;
    }
    if consumer_gone {
        return;
    }
    // The cap alone does not truncate: it only matters if it actually stopped
    // the scan short of the range's end.
    let terminal = match finishing.filter(|_| capped) {
        Some(hlc) => WindowItem::TruncatedAt(hlc),
        None => WindowItem::Complete,
    };
    let _ = tx.send(Ok(terminal));
}

/// Periodically fsync the local store when it has writes that aren't on disk
/// yet, bounding the post-crash re-fetch window to `interval` (design doc
/// §"Implementation Details: Local Store"; the module header says why per-batch
/// durability is not needed). Holds a `Weak` so an idle worker pins nothing,
/// and flushes once more on cancellation so a graceful stop persists the last
/// batch.
pub(in crate::learner::local_store) async fn run_local_store_flush_worker(
    weak: Weak<LocalStore>,
    interval: Duration,
    cancel: CancellationToken,
) -> Result<(), Error> {
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            () = tokio::time::sleep(interval) => {}
        }
        let Some(store) = weak.upgrade() else {
            return Ok(());
        };
        flush_if_dirty(&store).await?;
    }
    if let Some(store) = weak.upgrade() {
        flush_if_dirty(&store).await?;
    }
    Ok(())
}

/// Fsync. One DB, so one fence covers both families — the "data before cursors"
/// ordering the split used to need is gone with the split.
///
/// `dirty` is cleared *before* the flush so a commit landing mid-flush re-sets
/// it and is caught next tick — at worst one redundant flush, never a missed
/// one. A failure re-arms it and then ends the worker.
///
/// **A failed fence is fatal.** It is the same judgement `run_integrate_worker`
/// already makes one line above it in [`super::spawn_local_store`]: a storage
/// failure means a dying disk, and the node goes down loudly rather than
/// degrading. Retrying was worse than it looked — the store kept accepting
/// integrations and kept acking peers against a durability claim it could no
/// longer honour, and the only record was a `warn` per tick. §8's procedure
/// for a node that cannot keep its own state is wipe and rejoin under a new
/// `NodeId`, which an operator cannot start until the node stops.
async fn flush_if_dirty(store: &LocalStore) -> Result<(), Error> {
    if !store.dirty.swap(false, Ordering::AcqRel) {
        return Ok(());
    }
    // `fence_async` runs the call on its own blocking thread. Still a *timer*
    // flush and deliberately not a per-batch one — every byte here is a copy
    // of quorum-durable replica data and the durable cursor re-delivers after
    // a crash (see the module header).
    store.storage.fence_async().await.inspect_err(|e| {
        error!(error = %e, "local-store flush failed; shutting the node down");
        store.dirty.store(true, Ordering::Release);
    })
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
    use std::path::Path;
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::TempDir;

    use super::*;
    use crate::api::{LocalStoreMut, LocalStoreView};

    use crate::hlc::Hlc;
    use crate::ids::{DataId, NodeId};

    use crate::tests::make_data;
    use crate::tests::upstream;

    async fn integrate_batch(
        store: &LocalStore,
        source: NodeId,
        records: Vec<(LogIndex, Data)>,
    ) -> Result<(), crate::Error> {
        if records.is_empty() {
            return Ok(());
        }
        let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
        for (idx, rec) in records {
            let _ = tx.send(Ok((source, idx, rec)));
        }
        let (ch, closer) = crossbeam_combinators::Channels::from(rx, "test integrate");
        // Close before the last sender drops, or the consumer forms its
        // verdict as "disconnected, not yet closed" — an abort.
        closer.close();
        drop(tx);
        store.integrate_loop(ch).await
    }

    /// One `Storage` backing both this store and the stable log it reads the
    /// watermark from — the production shape.
    async fn shared_storage(root: &Path) -> (Arc<Storage>, Arc<dyn StableLogSnapshot>) {
        let storage = Storage::open_for_test(root).expect("open storage");
        let stable_log: Arc<dyn StableLogSnapshot> = Arc::new(
            crate::learner::stable_log::StableLog::open_on(Arc::clone(&storage))
                .await
                .expect("stable_log"),
        );
        (storage, stable_log)
    }

    async fn fresh_store() -> (TempDir, Arc<LocalStore>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("local.db");
        let store = LocalStore::simple_at(path.with_extension("stable"))
            .await
            .expect("from_db");
        (dir, store)
    }

    /// A store whose integration worker can be drained — required before
    /// reopening the same root (see [`super::open_test_local_store`]).
    async fn store_at(path: &Path) -> (Arc<LocalStore>, crate::learner::local_store::TestWorker) {
        crate::learner::open_test_local_store(path)
            .await
            .expect("open")
    }

    /// Integrate a single full body via the batch API.
    async fn put_full(store: &LocalStore, source: NodeId, index: LogIndex, data: Data) {
        integrate_batch(store, source, vec![(index, data)])
            .await
            .expect("integrate_batch");
    }

    /// The integration signal is **level-triggered**: it says "state moved, go
    /// look", so a waiter that was not draining while a burst landed must still
    /// be woken, and woken without an error it has to interpret. An
    /// edge-triggered channel reports a lag here instead, and the guarantee then
    /// rests on every consumer remembering to treat that lag as a wakeup —
    /// which is exactly the invariant `apply_post_fence` calls load-bearing
    /// (`sync_quorum`'s last-job-cleared edge).
    #[tokio::test]
    async fn the_integration_signal_survives_a_burst_nobody_drained() {
        let (_dir, store) = fresh_store().await;
        let mut integrated = store.subscribe_integrated();

        // Far more integrations than a buffered channel would hold, with no
        // consumer draining any of them.
        for i in 0..64u64 {
            let seed = u8::try_from(i % 251).expect("fits");
            put_full(
                &store,
                upstream(1),
                LogIndex(i),
                make_data(seed, 100 + i, b"x"),
            )
            .await;
        }

        tokio::time::timeout(Duration::from_secs(5), integrated.changed())
            .await
            .expect("a waiter must be woken by the burst")
            .expect("waking must not be an error the caller has to decode");
        assert_eq!(
            store
                .subscription_cursor(upstream(1))
                .await
                .expect("cursor"),
            Some(LogIndex(63)),
            "and the state it points at is the state after the burst"
        );
    }

    #[tokio::test]
    async fn integrate_full_writes_one_row() {
        let (_dir, store) = fresh_store().await;
        let d = make_data(1, 100, b"hello");
        put_full(&store, upstream(1), LogIndex(7), d.clone()).await;
        // Verify the data is present via stream.
        use futures::TryStreamExt as _;
        let all: Vec<Data> = store
            .stream_above_hlc(None)
            .into_stream()
            .try_collect()
            .await
            .expect("ok");
        assert!(all.iter().any(|item| item.data_id == d.data_id));
        assert_eq!(
            store.subscription_cursor(upstream(1)).await.expect("ok"),
            Some(LogIndex(7))
        );
    }

    /// A store over a stable log already at `watermark`, with its own config.
    async fn store_with(
        dir: &Path,
        config: &crate::learner::local_store::LocalStoreConfig,
        watermark: Option<Hlc>,
    ) -> Arc<LocalStore> {
        let (storage, stable_log) = shared_storage(dir).await;
        if let Some(hlc) = watermark {
            stable_log
                .record_end_hlc(hlc)
                .await
                .expect("record_end_hlc");
        }
        let (store, integrate_rx) = create_local_store(config, storage, stable_log)
            .await
            .expect("create");
        tokio::spawn(run_integrate_worker(
            Arc::downgrade(&store),
            integrate_rx,
            CancellationToken::new(),
        ));
        store
    }

    /// Only a first-time integration is news: the copy of the same write that
    /// arrives from a second replica is deduped and not announced again.
    #[tokio::test]
    async fn unstable_subscribers_see_first_time_integrations_only() {
        let (_dir, store) = fresh_store().await;
        let mut live = store.subscribe_unstable();

        let d = make_data(1, 100, b"d");
        let e = make_data(2, 200, b"e");
        put_full(&store, upstream(1), LogIndex(0), d.clone()).await;
        put_full(&store, upstream(2), LogIndex(0), d.clone()).await;
        integrate_batch(
            &store,
            upstream(1),
            vec![(LogIndex(1), e.clone()), (LogIndex(2), e.clone())],
        )
        .await
        .expect("integrate");

        assert_eq!(live.recv().expect("d"), d);
        assert_eq!(live.recv().expect("e"), e);
        assert_eq!(live.try_recv(), None, "deduped copies are not published");
        assert_eq!(store.skipped_duplicates(), 2);
    }

    /// An entry at or below the stable watermark belongs to the stable log;
    /// announcing it would duplicate the stable half (CLAUDE.md §12).
    #[tokio::test]
    async fn entries_at_or_below_the_watermark_are_not_published() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = crate::learner::local_store::LocalStoreConfig::for_test();
        let store = store_with(dir.path(), &config, Some(Hlc(100))).await;
        let mut live = store.subscribe_unstable();

        let fresh = make_data(2, 101, b"fresh");
        integrate_batch(
            &store,
            upstream(1),
            vec![
                (LogIndex(0), make_data(1, 100, b"settled")),
                (LogIndex(1), fresh.clone()),
            ],
        )
        .await
        .expect("integrate");

        assert_eq!(live.recv().expect("fresh"), fresh);
        assert_eq!(live.try_recv(), None);
    }

    /// The integrate worker never waits on a subscriber: one whose queue is
    /// full is hung up on, after everything already queued.
    #[tokio::test]
    async fn an_overrun_unstable_subscriber_is_hung_up_on() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crate::learner::local_store::LocalStoreConfig::for_test();
        config.unstable_subscriber_queue_capacity = crate::unit::nonzero(2);
        let store = store_with(dir.path(), &config, None).await;
        let mut live = store.subscribe_unstable();

        for i in 0..3u8 {
            put_full(
                &store,
                upstream(1),
                LogIndex(u64::from(i)),
                make_data(i, 100 + u64::from(i), b"x"),
            )
            .await;
        }

        assert_eq!(live.recv().expect("first").data_id.hlc(), Hlc(100));
        assert_eq!(live.recv().expect("second").data_id.hlc(), Hlc(101));
        assert_eq!(
            live.recv().err(),
            Some(crossbeam_combinators::End::Aborted {
                reason: crossbeam_combinators::AbortReason::ProducerGone {
                    what: "unstable tail subscription",
                },
            })
        );
    }

    #[tokio::test]
    async fn integrate_skips_full_record_at_or_below_end_hlc() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (storage, stable_log) = shared_storage(dir.path()).await;
        stable_log
            .record_end_hlc(Hlc(100))
            .await
            .expect("record_end_hlc");
        let (store, integrate_rx) = create_local_store(
            &crate::learner::local_store::LocalStoreConfig::for_test(),
            storage,
            stable_log,
        )
        .await
        .expect("create");
        tokio::spawn(run_integrate_worker(
            Arc::downgrade(&store),
            integrate_rx,
            CancellationToken::new(),
        ));

        // At/below end_hlc → already stable → skipped (not written), but the
        // cursor still advances so it is never re-pulled from upstream.
        let stale = make_data(1, 100, b"stale");
        put_full(&store, upstream(1), LogIndex(5), stale.clone()).await;
        assert_eq!(
            store.subscription_cursor(upstream(1)).await.expect("ok"),
            Some(LogIndex(5)),
            "cursor must advance over skipped records"
        );

        // Strictly above end_hlc → integrated as normal.
        let fresh = make_data(1, 101, b"fresh");
        put_full(&store, upstream(1), LogIndex(6), fresh.clone()).await;
        use futures::TryStreamExt as _;
        let all: Vec<Data> = store
            .stream_above_hlc(None)
            .into_stream()
            .try_collect()
            .await
            .expect("ok");
        assert!(
            all.iter().any(|item| item.data_id == fresh.data_id),
            "record above end_hlc must be integrated"
        );
        assert_eq!(
            store.subscription_cursor(upstream(1)).await.expect("ok"),
            Some(LogIndex(6))
        );
    }

    #[tokio::test]
    async fn integrate_skips_meta_only_at_or_below_end_hlc() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (storage, stable_log) = shared_storage(dir.path()).await;
        stable_log
            .record_end_hlc(Hlc(200))
            .await
            .expect("record_end_hlc");
        let (store, integrate_rx) = create_local_store(
            &crate::learner::local_store::LocalStoreConfig::for_test(),
            storage,
            stable_log,
        )
        .await
        .expect("create");
        tokio::spawn(run_integrate_worker(
            Arc::downgrade(&store),
            integrate_rx,
            CancellationToken::new(),
        ));

        // Meta-only at/below end_hlc: data is stale, cursor still advances.
        let data_id = DataId::from_parts(Hlc(150), NodeId::from_uuid(uuid::Uuid::nil()), 2);
        integrate_batch(
            &store,
            upstream(1),
            vec![(
                LogIndex(3),
                Data {
                    data_id,
                    blob: Bytes::from_static(b""),
                },
            )],
        )
        .await
        .expect("integrate_batch");
        assert_eq!(
            store.subscription_cursor(upstream(1)).await.expect("ok"),
            Some(LogIndex(3)),
            "cursor must advance over skipped meta-only records"
        );
    }

    #[tokio::test]
    async fn integrate_dedups_on_data_id() {
        let (_dir, store) = fresh_store().await;
        let d = make_data(3, 300, b"first");
        put_full(&store, upstream(1), LogIndex(5), d.clone()).await;
        put_full(&store, upstream(2), LogIndex(11), d.clone()).await;
        // Same DataId from two upstreams: stored once, both cursors advance.
        assert_eq!(
            store.subscription_cursor(upstream(1)).await.expect("ok"),
            Some(LogIndex(5))
        );
        assert_eq!(
            store.subscription_cursor(upstream(2)).await.expect("ok"),
            Some(LogIndex(11))
        );
    }

    /// Multi-source variant of `integrate_batch`: every record lands in ONE
    /// batch, which is what exercises the batch-level half of the filter.
    async fn integrate_mixed_batch(
        store: &LocalStore,
        records: Vec<(NodeId, LogIndex, Data)>,
    ) -> Result<(), crate::Error> {
        let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
        for (src, idx, rec) in records {
            let _ = tx.send(Ok((src, idx, rec)));
        }
        let (ch, closer) = crossbeam_combinators::Channels::from(rx, "test integrate");
        // Close before the last sender drops, or the consumer forms its
        // verdict as "disconnected, not yet closed" — an abort.
        closer.close();
        drop(tx);
        store.integrate_loop(ch).await
    }

    /// Cross-batch (the global ring): the three subscriber streams each deliver
    /// every write, so the same `data_id` arrives once per replica in
    /// *separate* batches. Only the first may reach the engine.
    ///
    /// Counting rows would prove nothing — the LSM dedups by `DataId` anyway —
    /// so this asserts the filter actually skipped the later copies.
    #[tokio::test]
    async fn later_batches_skip_an_already_committed_data_id() {
        let (_dir, store) = fresh_store().await;
        let d = make_data(4, 400, b"payload");
        for (i, peer) in [upstream(1), upstream(2), upstream(3)]
            .into_iter()
            .enumerate()
        {
            let idx = LogIndex(u64::try_from(i).expect("small") + 1);
            put_full(&store, peer, idx, d.clone()).await;
        }
        assert_eq!(
            store.skipped_duplicates(),
            2,
            "copies 2 and 3 must be dropped before the engine write"
        );
        // Every peer's cursor advanced: dedup must never stall replication, or
        // the skipped peers would resubscribe from a stale position.
        for (i, peer) in [upstream(1), upstream(2), upstream(3)]
            .into_iter()
            .enumerate()
        {
            let want = LogIndex(u64::try_from(i).expect("small") + 1);
            assert_eq!(
                store.subscription_cursor(peer).await.expect("ok"),
                Some(want),
                "peer cursor must advance even though its copy was skipped"
            );
        }
    }

    /// Batch level: the same `data_id` twice inside ONE batch is written once,
    /// and both sources' cursors still advance after that write commits.
    #[tokio::test]
    async fn one_batch_writes_a_repeated_data_id_once_and_acks_both_sources() {
        let (_dir, store) = fresh_store().await;
        let d = make_data(5, 500, b"payload");
        integrate_mixed_batch(
            &store,
            vec![
                (upstream(1), LogIndex(1), d.clone()),
                (upstream(2), LogIndex(7), d.clone()),
            ],
        )
        .await
        .expect("integrate");

        assert_eq!(store.skipped_duplicates(), 1, "one copy collapsed in-batch");
        assert_eq!(
            store.subscription_cursor(upstream(1)).await.expect("ok"),
            Some(LogIndex(1))
        );
        assert_eq!(
            store.subscription_cursor(upstream(2)).await.expect("ok"),
            Some(LogIndex(7))
        );
    }

    /// A failed fence ends the worker, so `Shutdown` takes the node down.
    ///
    /// The same claim `a_failed_batch_ends_the_worker_and_leaves_the_filter_clean`
    /// makes for the integrate worker. Retrying instead would keep the store
    /// accepting integrations and acking peers against a durability claim it
    /// could no longer honour.
    #[tokio::test]
    async fn a_failed_fence_ends_the_flush_worker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (storage, stable_log) = shared_storage(dir.path()).await;
        let cfg = crate::learner::local_store::LocalStoreConfig::for_test();
        let (store, _rx) = create_local_store(&cfg, Arc::clone(&storage), stable_log)
            .await
            .expect("create");

        // Something for the tick to fence.
        store.dirty.store(true, Ordering::Release);
        storage.fail_fences_from_now_on();

        let worker = tokio::spawn(run_local_store_flush_worker(
            Arc::downgrade(&store),
            Duration::from_millis(10),
            CancellationToken::new(),
        ));

        let outcome = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("worker ends promptly")
            .expect("worker joins");

        assert!(
            outcome.is_err(),
            "a failed fence must end the worker so the node shuts down"
        );
        assert!(
            store.dirty.load(Ordering::Acquire),
            "a failed fence must leave the store still owing an fsync"
        );
    }

    /// **The safety test.** A batch that fails to commit must leave the filter
    /// untouched, or the records it did not write would be skipped forever
    /// when the peer re-delivers them — silent data loss.
    ///
    /// With one shared worker the failure no longer returns to the caller: it
    /// ends the worker, and `Shutdown` turns that into a node shutdown, the
    /// same contract as `replica:log_flush`. Driven straight through the
    /// shared channel so nothing waits on a fence a dead worker cannot answer.
    #[tokio::test]
    async fn a_failed_batch_ends_the_worker_and_leaves_the_filter_clean() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (storage, stable_log) = shared_storage(dir.path()).await;
        let cfg = crate::learner::local_store::LocalStoreConfig::for_test();
        let (store, rx) = create_local_store(&cfg, storage, stable_log)
            .await
            .expect("create");
        let worker = tokio::spawn(run_integrate_worker(
            Arc::downgrade(&store),
            rx,
            CancellationToken::new(),
        ));

        let d = make_data(6, 600, b"payload");
        store.fail_next_append();
        store
            .integrate_tx
            .send(IntegrateItem::Record(upstream(1), LogIndex(1), d.clone()))
            .expect("worker alive");

        let outcome = worker.await.expect("worker joins");
        assert!(
            outcome.is_err(),
            "an append failure must end the worker so the node shuts down"
        );
        assert!(
            store
                .subscription_cursor(upstream(1))
                .await
                .expect("ok")
                .is_none(),
            "nothing may be acked when the data was never written"
        );
        assert!(
            !store.recent.lock().contains(d.data_id),
            "a failed batch must leave the filter untouched, or the retry is \
             skipped forever"
        );
    }

    #[tokio::test]
    async fn subscription_cursor_uses_max() {
        let (_dir, store) = fresh_store().await;
        put_full(&store, upstream(1), LogIndex(10), make_data(1, 10, b"a")).await;
        put_full(&store, upstream(1), LogIndex(5), make_data(2, 11, b"b")).await;
        assert_eq!(
            store.subscription_cursor(upstream(1)).await.expect("ok"),
            Some(LogIndex(10))
        );
    }

    #[tokio::test]
    async fn stream_above_hlc_returns_all_matching_entries() {
        let (_dir, store) = fresh_store().await;
        for (seed, hlc) in [(2u8, 200u64), (1, 100), (3, 300)] {
            put_full(
                &store,
                upstream(1),
                LogIndex(u64::from(seed)),
                make_data(seed, hlc, &[seed]),
            )
            .await;
        }
        use futures::TryStreamExt as _;
        use std::collections::BTreeSet;
        let all: Vec<Data> = store
            .stream_above_hlc(None)
            .into_stream()
            .try_collect()
            .await
            .expect("ok");
        let all_hlcs: BTreeSet<u64> = all.iter().map(|d| d.data_id.hlc().raw()).collect();
        assert_eq!(all_hlcs, BTreeSet::from([100, 200, 300]));

        let above: Vec<Data> = store
            .stream_above_hlc(Some(Hlc(150)))
            .into_stream()
            .try_collect()
            .await
            .expect("ok");
        let above_hlcs: BTreeSet<u64> = above.iter().map(|d| d.data_id.hlc().raw()).collect();
        assert_eq!(above_hlcs, BTreeSet::from([200, 300]));
    }

    #[tokio::test]
    async fn stream_window_ordered_sorts_by_hlc_then_data_id() {
        let (_dir, store) = fresh_store().await;
        // Build entries with known (HLC, random) values, where the random part
        // is in decreasing order — the HLC should dominate the sort.
        let mut expected: Vec<(u64, [u8; 32])> = Vec::new();
        for i in 0u8..8 {
            let hlc = 100u64.checked_add(u64::from(i)).expect("hlc");
            let r = u64::from(0xF0u8.checked_sub(i).expect("random"));
            let data_id = DataId::from_parts(Hlc(hlc), NodeId::from_uuid(uuid::Uuid::nil()), r);
            expected.push((hlc, data_id.0));
            put_full(
                &store,
                upstream(1),
                LogIndex(u64::from(i)),
                Data {
                    data_id,
                    blob: Bytes::copy_from_slice(&[i]),
                },
            )
            .await;
        }
        // Two entries at the same HLC: random part breaks the tie.
        for (slot, r) in [(8u8, 0x30u64), (9u8, 0x20u64)] {
            let data_id = DataId::from_parts(Hlc(200), NodeId::from_uuid(uuid::Uuid::nil()), r);
            expected.push((200, data_id.0));
            put_full(
                &store,
                upstream(1),
                LogIndex(u64::from(slot)),
                Data {
                    data_id,
                    blob: Bytes::copy_from_slice(&[slot]),
                },
            )
            .await;
        }
        expected.sort();

        let got: Vec<Data> = store
            .stream_window_ordered(None, Hlc(1_000), None)
            .into_iter()
            .filter_map(|r| match r {
                Ok(WindowItem::Entry(d)) => Some(d),
                _ => None,
            })
            .collect();
        let got_keys: Vec<(u64, [u8; 32])> = got
            .iter()
            .map(|d| (d.data_id.hlc().raw(), d.data_id.0))
            .collect();
        assert_eq!(got_keys, expected, "must be (HLC, data_id) ordered");
    }

    #[tokio::test]
    async fn stream_window_ordered_respects_bounds() {
        let (_dir, store) = fresh_store().await;
        for (seed, hlc) in [(1u8, 100u64), (2, 200), (3, 300)] {
            put_full(
                &store,
                upstream(1),
                LogIndex(u64::from(seed)),
                make_data(seed, hlc, &[seed]),
            )
            .await;
        }
        // Lower bound exclusive, upper bound inclusive: (100, 300] → 200, 300.
        let got: Vec<Data> = store
            .stream_window_ordered(Some(Hlc(100)), Hlc(300), None)
            .into_iter()
            .filter_map(|r| match r {
                Ok(WindowItem::Entry(d)) => Some(d),
                _ => None,
            })
            .collect();
        let hlcs: Vec<u64> = got.iter().map(|d| d.data_id.hlc().raw()).collect();
        assert_eq!(hlcs, vec![200, 300]);
    }

    /// The cap must never split an HLC. The next chunk's lower bound is
    /// `Excluded(DataId::from_parts(hlc, NodeId::MAX, u64::MAX))`, which skips
    /// every entry at that HLC — so a chunk that stopped mid-HLC would silently
    /// drop the rest of it. The cap is therefore a floor, not a ceiling.
    #[tokio::test]
    async fn a_capped_window_never_splits_an_hlc() {
        let (_dir, store) = fresh_store().await;
        // Five entries at HLC 100, then five at HLC 101.
        for (hlc, r) in (0u64..5)
            .map(|r| (100u64, r))
            .chain((0u64..5).map(|r| (101u64, r)))
        {
            let data_id = DataId::from_parts(Hlc(hlc), NodeId::from_uuid(uuid::Uuid::nil()), r);
            put_full(
                &store,
                upstream(1),
                LogIndex(r),
                Data {
                    data_id,
                    blob: Bytes::copy_from_slice(&[0u8]),
                },
            )
            .await;
        }

        // A cap of 2 lands inside the HLC-100 run; all five must still come out.
        let cap = std::num::NonZeroUsize::new(2).expect("nonzero");
        let items: Vec<WindowItem> = store
            .stream_window_ordered(None, Hlc(200), Some(cap))
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        let (terminal, entries) = items.split_last().expect("a terminal item");
        let hlcs: Vec<Hlc> = entries
            .iter()
            .map(|i| match i {
                WindowItem::Entry(d) => d.data_id.hlc(),
                other => panic!("expected an entry, got {other:?}"),
            })
            .collect();
        assert_eq!(
            hlcs,
            vec![Hlc(100); 5],
            "the cap must extend to the end of the HLC it lands in, and stop there",
        );
        assert_eq!(
            *terminal,
            WindowItem::TruncatedAt(Hlc(100)),
            "the HLC-101 run is uncovered, so the scan must say where it stopped",
        );
    }

    /// A cap larger than the window is not a truncation.
    #[tokio::test]
    async fn a_cap_above_the_window_size_streams_everything() {
        let (_dir, store) = fresh_store().await;
        for r in 0u64..4 {
            let hlc = 100u64.checked_add(r).expect("hlc");
            let data_id = DataId::from_parts(Hlc(hlc), NodeId::from_uuid(uuid::Uuid::nil()), r);
            put_full(
                &store,
                upstream(1),
                LogIndex(r),
                Data {
                    data_id,
                    blob: Bytes::copy_from_slice(&[0u8]),
                },
            )
            .await;
        }
        let cap = std::num::NonZeroUsize::new(100).expect("nonzero");
        let items: Vec<WindowItem> = store
            .stream_window_ordered(None, Hlc(200), Some(cap))
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let (terminal, entries) = items.split_last().expect("a terminal item");
        assert_eq!(entries.len(), 4);
        assert_eq!(
            *terminal,
            WindowItem::Complete,
            "a cap the scan never reached is not a truncation",
        );
    }

    /// `None` is exactly today's behaviour.
    #[tokio::test]
    async fn no_cap_streams_the_whole_window() {
        let (_dir, store) = fresh_store().await;
        for r in 0u64..6 {
            let hlc = 100u64.checked_add(r).expect("hlc");
            let data_id = DataId::from_parts(Hlc(hlc), NodeId::from_uuid(uuid::Uuid::nil()), r);
            put_full(
                &store,
                upstream(1),
                LogIndex(r),
                Data {
                    data_id,
                    blob: Bytes::copy_from_slice(&[0u8]),
                },
            )
            .await;
        }
        let items: Vec<WindowItem> = store
            .stream_window_ordered(None, Hlc(200), None)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let (terminal, entries) = items.split_last().expect("a terminal item");
        assert_eq!(entries.len(), 6);
        assert_eq!(*terminal, WindowItem::Complete);
    }

    #[tokio::test]
    async fn purge_below_hlc_drops_matching_entries() {
        let (_dir, store) = fresh_store().await;
        for (seed, hlc) in [(1u8, 100u64), (2, 200), (3, 300)] {
            put_full(
                &store,
                upstream(1),
                LogIndex(u64::from(seed)),
                make_data(seed, hlc, &[seed]),
            )
            .await;
        }
        store.purge_below_hlc(Hlc(200)).await.expect("ok");
        use futures::TryStreamExt as _;
        // `purge_below_hlc` is a durable `DeleteRange`: rows with HLC <= 200
        // are gone the moment it returns, not merely hidden pending a later
        // compaction (CLAUDE.md §8).
        let remaining: Vec<Data> = store
            .stream_above_hlc(None)
            .into_stream()
            .try_collect()
            .await
            .expect("ok");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].data_id.hlc(), Hlc(300));
    }

    /// **The win one DB unlocks.** A record and the cursor that covers it are
    /// one `WriteBatch` across two column families, so after a fence they are
    /// both durable or neither is. They were separate DBs before, where no
    /// batch could span them: a crash between the two writes could leave a
    /// durable cursor claiming coverage of a record that was never written, and
    /// the peer would never re-deliver it.
    ///
    /// The failure half of the claim is
    /// `a_failed_batch_ends_the_worker_and_leaves_the_filter_clean`, which
    /// asserts the cursor did *not* advance when the write failed.
    #[tokio::test]
    async fn data_and_cursor_commit_atomically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("db");
        let d = make_data(9, 900, b"payload");
        {
            let (store, worker) = store_at(&root).await;
            put_full(&store, upstream(1), LogIndex(4), d.clone()).await;
            // The §8 fence: everything written before this returns is durable.
            flush_if_dirty(&store).await.expect("fence");
            drop(store);
            worker.stop().await;
        }

        let (store, worker) = store_at(&root).await;
        use futures::TryStreamExt as _;
        let has_data = store
            .stream_above_hlc(None)
            .into_stream()
            .try_collect::<Vec<Data>>()
            .await
            .expect("stream")
            .iter()
            .any(|item| item.data_id == d.data_id);
        let has_cursor = store
            .subscription_cursor(upstream(1))
            .await
            .expect("cursor")
            .is_some();
        assert_eq!(
            has_data, has_cursor,
            "a record and the cursor covering it must survive together or not at all",
        );
        assert!(has_data, "the fenced batch must be durable");
        drop(store);
        worker.stop().await;
    }

    /// Purge is durable. The in-memory clamp it replaced started empty at
    /// every open, so purged entries reappeared until a compaction happened
    /// to rewrite their SST.
    #[tokio::test]
    async fn purged_prefix_stays_gone_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("db");
        let old = make_data(1, 100, b"old");
        let new = make_data(2, 300, b"new");
        {
            let (store, worker) = store_at(&root).await;
            put_full(&store, upstream(1), LogIndex(1), old.clone()).await;
            put_full(&store, upstream(1), LogIndex(2), new.clone()).await;
            store.purge_below_hlc(Hlc(200)).await.expect("purge ok");
            drop(store);
            worker.stop().await;
        }

        let (store, worker) = store_at(&root).await;
        use futures::TryStreamExt as _;
        let remaining: Vec<Data> = store
            .stream_above_hlc(None)
            .into_stream()
            .try_collect()
            .await
            .expect("stream");
        assert!(
            !remaining.iter().any(|item| item.data_id == old.data_id),
            "an entry purged below the threshold must not come back after a reopen"
        );
        assert!(
            remaining.iter().any(|item| item.data_id == new.data_id),
            "an entry above the threshold survives"
        );
        drop(store);
        worker.stop().await;
    }

    #[tokio::test]
    async fn subscription_persists_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("db");
        {
            let (store, worker) = store_at(&path).await;
            for i in 1u8..=3 {
                put_full(
                    &store,
                    upstream(1),
                    LogIndex(u64::from(i)),
                    make_data(i, u64::from(i) * 100, &[i]),
                )
                .await;
            }
            // Drain before reopening the root: a worker mid-batch holds the
            // DB's LOCK.
            drop(store);
            worker.stop().await;
        }
        let (store, worker) = store_at(&path).await;
        assert_eq!(
            store.subscription_cursor(upstream(1)).await.expect("ok"),
            Some(LogIndex(3))
        );
        drop(store);
        worker.stop().await;
    }
}
