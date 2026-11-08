//! The periodic fsync worker.

use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::error;

use super::LocalStore;
use crate::Error;

/// Periodically fsync the local store when it has writes that aren't on disk
/// yet, bounding the post-crash re-fetch window to `interval` (design doc
/// §"Implementation Details: Local Store"; the module header says why per-batch
/// durability is not needed). Holds a `Weak` so an idle worker pins nothing,
/// and flushes once more on cancellation so a graceful stop persists the last
/// batch.
pub(crate) async fn run_local_store_flush_worker(
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
/// already makes one line above it in [`crate::spawn_local_store`]: a storage
/// failure means a dying disk, and the node goes down loudly rather than
/// degrading. Retrying was worse than it looked — the store kept accepting
/// integrations and kept acking peers against a durability claim it could no
/// longer honour, and the only record was a `warn` per tick. §8's procedure
/// for a node that cannot keep its own state is wipe and rejoin under a new
/// `NodeId`, which an operator cannot start until the node stops.
pub(super) async fn flush_if_dirty(store: &LocalStore) -> Result<(), Error> {
    if !store.dirty.swap(false, Ordering::AcqRel) {
        return Ok(());
    }
    // `fence_async` runs the call on its own blocking thread. Still a *timer*
    // flush and deliberately not a per-batch one — every byte here is a copy
    // of quorum-durable replica data and the durable cursor re-delivers after
    // a crash (see the module header).
    store
        .storage
        .fence_async()
        .await
        .inspect_err(|e| {
            error!(error = %e, "local-store flush failed; shutting the node down");
            store.dirty.store(true, Ordering::Release);
        })
        .map_err(Into::into)
}
