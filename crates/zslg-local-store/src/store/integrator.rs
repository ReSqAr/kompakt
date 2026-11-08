//! The batch integrator: drains the integration channel on a dedicated blocking
//! thread, batches writes across the two keyspaces, and holds only a
//! `Weak<LocalStore>` — idle threads pin nothing.

use std::collections::{HashMap, HashSet};
use std::sync::Weak;
use std::time::Duration;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::Error;
use crate::metrics::{self, LocalWriteKind};
use metrics_kit::OpState;
#[cfg(test)]
use std::sync::atomic::Ordering;
use zslg_consensus::NodeId;
use zslg_log::Data;
use zslg_log::ToStorage;
use zslg_log::{DataId, LogIndex};

use super::LocalStore;
use super::engine::commit_batch;
#[cfg(test)]
use zslg_log::LogError;

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
pub enum IntegrateItem {
    Record(NodeId, LogIndex, Data),
    Barrier(tokio::sync::oneshot::Sender<()>),
}

/// Aggregated side effects of one committed batch, applied after the fence by
/// [`LocalStore::apply_post_fence`].
pub(super) struct BatchOutcome {
    pub(super) total_records: usize,
    pub(super) had_integrate_or_fetch: bool,
}

/// Max records / outcomes drained into one `WriteBatch`.
pub(crate) const MAX_BATCH: usize = 1024;

/// The node's single integration worker: drains the shared channel, batches
/// across upstreams, commits. Registered with `Shutdown` by
/// [`crate::spawn_local_store`], so an error ends the task and takes the node
/// down — the same contract as `replica:log_flush`.
pub async fn run_integrate_worker(
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
pub(super) const INTEGRATE_IDLE_POLL: Duration = Duration::from_millis(250);

/// Drains integration records on one blocking thread for its lifetime — mirrors
/// `replica/log.rs::flush_loop_blocking`. Idles in `blocking_recv`, upgrades the
/// `Weak` only while processing a batch, drains consecutive batches while items
/// are queued. Exits when the channel closes (subscriber gone) or the store is
/// dropped.
pub(super) struct BatchIntegrator {
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
                return Err(Error::Log(LogError::Storage(
                    "induced append failure (test)".into(),
                )));
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
