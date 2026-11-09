//! The flush loop: what drains the write queue into batches, and the seam it commits through.

#[cfg(test)]
use super::subscribe_handoff_pause;
use super::{Committer, PendingWrite, ReplicaLogStore};
use crate::Error;
use crate::metrics::{self, FlushTrigger};
use crossbeam_combinators::{Channel, Seam};
use metrics_kit::OpState;
use std::sync::{Arc, Weak};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use zslg_log::Data;
use zslg_log::LogError;
use zslg_log::LogIndex;
use zslg_log::metrics::LogType;

/// The live subscription and the frontier it is anchored to.
///
/// [`Seam::subscribe`] registers the fan-out before [`Seam::cut`] reads the
/// frontier. The flush loop advances the frontier before it broadcasts, so
/// an entry broadcast before registration is at or below the cut and the
/// historic scan serves it; one broadcast after registration is on the live
/// tail. The overlap is filtered by [`Seam::above`]; [`Segment::open`] fixes the order. Reversing the order
/// would lose entries committed between the snapshot and the registration.
pub(crate) struct ReplicaSeam {
    pub(crate) log: Arc<ReplicaLogStore>,
    pub(crate) from: Option<LogIndex>,
}

impl Seam for ReplicaSeam {
    type Item = Result<(LogIndex, Data), Error>;
    type Cut = Option<LogIndex>;

    fn subscribe(&self) -> Channel<Self::Item, crossbeam_combinators::Infinite> {
        let live = self.log.live_registry.subscribe_infinite();
        #[cfg(test)]
        subscribe_handoff_pause();
        live.map(Ok)
    }

    fn cut(&self) -> Self::Cut {
        self.log.frontier.index()
    }

    fn above(
        &self,
        live: Channel<Self::Item, crossbeam_combinators::Infinite>,
        cut: Self::Cut,
    ) -> Channel<Self::Item, crossbeam_combinators::Infinite> {
        live.filter_above::<LogIndex>(cut)
    }

    fn range(&self, cut: Option<LogIndex>) -> Channel<Self::Item, crossbeam_combinators::Finite> {
        let start = LogIndex::next_after(self.from);
        // How far behind this subscriber is resuming. Rising across
        // successive connects for the same peer = the deficit is diverging,
        // i.e. shedding has stopped being protection and started preventing
        // catch-up.
        metrics::subscription_catchup_backlog(match cut {
            Some(end) => end.raw().saturating_sub(start.raw()),
            None => 0,
        });
        match cut {
            // A subscriber already at the frontier gives `start == end + 1`:
            // an empty window, not an error — the routine state of a peer
            // that reconnects with nothing to catch up on.
            Some(end) if start <= end => self.log.pump_range(start, end),
            // Nothing to replay (no committed data, or already caught up):
            // live-only tail.
            _ => ReplicaLogStore::empty_historic(),
        }
    }
}

/// Flush loop — the single appender of the dense log.
///
/// Thin async wrapper: runs `flush_loop_blocking` on the blocking pool for its
/// lifetime and **joins it** on either exit — a detached body would outlive the
/// node's teardown, still holding the DB. The blocking body uses a plain
/// (timer-free) crossbeam `recv` for the idle wait — no per-batch async↔blocking
/// hop. The signature is kept (`async` + `CancellationToken` + `Result`) so all
/// spawn sites and tests are unchanged. A plain crossbeam `Receiver` (not
/// `crossbeam_combinators::Channel`): the loop only recv/try_recv-batches — no transforms
/// — so the `Channel` combinators would add nothing.
pub async fn flush_loop(
    weak: Weak<ReplicaLogStore>,
    rx: crossbeam_channel::Receiver<PendingWrite>,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let cancel_cl = cancel.clone();
    // The wake channel exists so cancellation can *interrupt* the idle `recv`,
    // not merely be noticed between batches: the cancel arm below joins the
    // thread, and a thread parked in the timerless `recv` would never reach the
    // join — `rx` closes only when the store drops its `Sender`, which happens
    // after shutdown has drained its workers. Dropping `wake_tx` disconnects
    // `wake_rx`, which is the signal; no value is ever sent.
    let (wake_tx, wake_rx) = crossbeam_channel::bounded::<()>(1);
    let mut handle =
        tokio::task::spawn_blocking(move || flush_loop_blocking(weak, &rx, &wake_rx, &cancel_cl));
    let res = tokio::select! {
        // Cancellation is only checked *between* batches, so at this point the
        // blocking body may still be inside `flush_one_batch_blocking` holding an
        // `Arc<ReplicaLogStore>` → `Arc<Storage>`. Returning here would let the
        // node finish teardown and drop the DB while that thread still uses
        // it — the reopen-LOCK race this migration already fixed for the retire
        // compactions and the local-store integrator. Join, don't detach.
        () = cancel.cancelled() => {
            drop(wake_tx);
            (&mut handle).await
        }
        r = &mut handle => r,
    };
    // A fail-stop error that lands after cancellation is still a real storage
    // failure; the shutdown machinery logs a worker error at `debug` during a
    // graceful drain, so log it here at `error` before returning it.
    let out = res.unwrap_or(Ok(()));
    if let Err(e) = &out {
        error!(error = %e, "replica flush loop failed");
    }
    out
}

/// The sole appender, on its own thread for the store's lifetime.
///
/// Idles in `recv` with no timer, so an empty log costs nothing. Three exits,
/// and between them nothing can wedge shutdown: the channel closing (the store's
/// only `Sender` lives inside it, so this doubles as "the store is gone"), the
/// `wake` channel disconnecting (cancellation, which must interrupt the idle
/// wait because the wrapper joins this thread), and the cancellation check
/// between batches for the loaded case.
///
/// Holds a `Weak` and upgrades only while a batch is in flight — parked in
/// `recv` it pins nothing, so the last `Arc<ReplicaLogStore>` really does free
/// the store.
pub(crate) fn flush_loop_blocking(
    weak: Weak<ReplicaLogStore>,
    rx: &crossbeam_channel::Receiver<PendingWrite>,
    wake: &crossbeam_channel::Receiver<()>,
    cancel: &CancellationToken,
) -> Result<(), Error> {
    info!("replica flush loop: starting");
    loop {
        let first = crossbeam_channel::select! {
            recv(rx) -> msg => match msg {
                Ok(w) => w,
                Err(_) => {
                    info!("replica flush loop: write channel closed");
                    return Ok(());
                }
            },
            // `wake` never carries a value: its disconnect is the cancellation
            // signal, sent by the async wrapper so it can then join this thread.
            recv(wake) -> _ => {
                info!("replica flush loop: cancelled while idle");
                return Ok(());
            }
        };
        let Some(store) = weak.upgrade() else {
            return Ok(());
        };

        // Keep draining inline while items are queued, rather than bouncing
        // back through `recv` between consecutive batches under load.
        let mut item = Some(first);
        while let Some(w) = item.take() {
            flush_one_batch_blocking(&store, w, rx)?;
            item = rx.try_recv().ok();
        }

        if cancel.is_cancelled() {
            info!("replica flush loop: cancelled");
            return Ok(());
        }
        // `store` drops here, so the idle wait above holds only the `Weak`.
    }
}

/// Drains `first` plus up to `max_flush_batch` more into one append + fsync, then
/// broadcasts and answers. All in one blocking body, which is what keeps the
/// header's fence intact.
///
/// **A commit error fails the whole loop, and with it the node.** Nothing was
/// ACKed, so per-batch recovery looks tempting — but a failed fsync may be
/// reported only once (fsyncgate), and continuing would let a later append
/// paper over a durable gap. Crashing loudly and recovering from known-good
/// state is the only option that cannot silently lose an entry.
pub(crate) fn flush_one_batch_blocking(
    store: &ReplicaLogStore,
    first: PendingWrite,
    rx: &crossbeam_channel::Receiver<PendingWrite>,
) -> Result<(), Error> {
    let flush_cycle_probe = metrics::replica_log_flush_cycle();
    metrics::replica_append_queue_wait(first.enqueued_at.elapsed());
    let next = store.frontier.next_index();
    let batch_build_probe = metrics::replica_log_batch_build();
    let batch = store.batch_builder.build(first, rx, next)?;
    batch_build_probe.finish(OpState::Success);

    let flush_entries = batch.staged.len();
    if flush_entries == 0 {
        metrics::replica_log_queue_depth(rx.len());
        return Ok(());
    }

    let mut flush_bytes: usize = 0;
    for (_, data, _) in &batch.staged {
        flush_bytes = flush_bytes.saturating_add(data.blob.len());
    }

    let drained = flush_entries;
    metrics::replica_log_flush_trigger(if drained >= store.max_flush_batch {
        FlushTrigger::BatchFull
    } else {
        FlushTrigger::Idle
    });

    let commit_t0 = std::time::Instant::now();
    match Committer::append_and_flush(&store.storage, &batch.staged, store.frontier.max_hlc()) {
        Ok(committed_max) => {
            metrics::replica_log_commit_elapsed(commit_t0.elapsed());
            let postcommit_probe = metrics::replica_append_postcommit_probe();

            let Some((last_index, _, _)) = batch.staged.last() else {
                return Err(Error::Log(LogError::Storage(
                    "internal: empty staged batch".into(),
                )));
            };
            // Every staged entry is folded in `append_and_flush` (it is what the
            // key carries), so a non-empty batch always yields a running max.
            let Some(running_max) = committed_max else {
                return Err(Error::Log(LogError::Storage(
                    "internal: non-empty batch produced no running max".into(),
                )));
            };
            store.frontier.advance(*last_index, running_max);

            zslg_log::metrics::log_writes(LogType::Replica, flush_entries);
            zslg_log::metrics::log_index_frontier(LogType::Replica, last_index.raw());
            metrics::replica_log_flush_bytes(flush_bytes);
            metrics::replica_log_flush_entries(flush_entries);
            metrics::replica_log_flushed();
            for (i, (index, data, guard)) in batch.staged.into_iter().enumerate() {
                let data_id = data.data_id;
                // `Stopped` only while the store is being dropped: the live
                // tail has no reader left to miss it.
                let _ = store.live.publish((index, data));
                metrics::replica_log_append_probe().finish(OpState::Success);
                // `try_send`, never `send`: this runs on the replica's *single
                // shared* flush thread, so blocking here for one slow connection
                // would stop appends for every connection on the node. Mirrors
                // the fan-out `try_send` above.
                //
                // Dropping an outcome is safe by construction — the write IS
                // durable. The writer's reconnect path resends every un-acked
                // write and the whole downstream dedups by `data_id` (CLAUDE.md
                // §8), so a drop costs a resend, never correctness.
                if batch.completions[i].try_send((data_id, Ok(index))).is_err() {
                    metrics::replica_outcome_dropped();
                }
                drop(guard);
            }
            drop(postcommit_probe);
            metrics::replica_log_queue_depth(rx.len());
            flush_cycle_probe.finish(OpState::Success);
            Ok(())
        }
        Err(e) => {
            metrics::replica_log_commit_elapsed(commit_t0.elapsed());
            let msg = format!("replica log append failed: {e}");
            for (i, (_, data, _)) in batch.staged.iter().enumerate() {
                if batch.completions[i]
                    .try_send((
                        data.data_id,
                        Err(zslg_log::Error::Log(LogError::Storage(msg.clone()))),
                    ))
                    .is_err()
                {
                    metrics::replica_outcome_dropped();
                }
            }
            Err(e)
        }
    }
}
