//! `Dispatcher::finalise`: choose where the fold starts, run it beside the
//! fence task, and turn whichever ends first into the dispatcher's result.
//!
//! ```text
//!   stored config hash == registered ─> resume at next_after(stored log index)
//!                      != registered ─> reset the store, refold from ZERO
//!                      absent        ─> fold from ZERO
//!
//!   select { fold ends   ─> cancel, final fence, fold result then fence result
//!            fence fails ─> stop(StoreError), cancel the fold }
//! ```
//!
//! The fold commits; only the fence makes a commit durable, every
//! `flush_interval` and once more on the way out. A failed fence strands the
//! fold — nothing it folds from here can be made durable — so readers are owed
//! the same answer as if the fold itself had stopped.

use super::fold::{self, FoldInputs};
use super::{Dispatcher, compute_config_hash};
use crate::metrics::{self, FenceOutcome, StopReason};
use crate::{ArcStore, Error};
use crossbeam_combinators::Cancellation;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info};
use zslg::LogIndex;

impl Dispatcher {
    /// Start the fold, and return once it has replayed the stable log this
    /// node had loaded when `finalise` was called.
    ///
    /// Every [`Reader`](crate::Reader) handed out before this call reads the
    /// same store, so without the wait a refold (a changed projection config
    /// resets the store) would be served to them as an almost empty store with
    /// nothing to say it is still filling. The target is the local
    /// [`stable_watermark`](zslg::Reader::stable_watermark), not a quorum read:
    /// this gate is about replaying history already here, and freshness stays
    /// [`Reader::sync`](crate::Reader::sync)'s job. Fails with
    /// [`Error::FoldStopped`] if the fold stops before getting there.
    pub async fn finalise(self) -> Result<JoinHandle<Result<(), Error>>, Error> {
        let actual_config_hash =
            compute_config_hash(&self.registered_projection_types, &self.registered_indices)?;

        let from = if let Some(stored_config_hash) = self.store.config_hash()? {
            if stored_config_hash != actual_config_hash {
                info!(
                    projection_types = self.registered_projection_types.len(),
                    indices = self.registered_indices.len(),
                    "projection config changed; resetting the store and refolding the log"
                );
                metrics::store_reset();
                self.store.reset()?;
                self.store.set_config_hash(&actual_config_hash)?;
                LogIndex::ZERO
            } else {
                let from = LogIndex::next_after(self.log_index_watermark()?);
                info!(log_index = ?from, "resuming from the stored watermark");
                from
            }
        } else {
            info!("no stored projection config; folding the log from the start");
            self.store.set_config_hash(&actual_config_hash)?;
            LogIndex::ZERO
        };

        let flush_interval = self.flush_interval;
        let fold_state = self.fold_state.clone();

        let reader = self.reader.clone();
        let mut folded = self.rx.clone();
        let gate_state = self.fold_state.clone();
        let replay_target = reader.stable_watermark();
        info!(log_index = ?from, "subscribing to the stable log");
        let store = self.store.clone();
        let cancel = fold_state.cancellation();
        // Bound to `cancel` so asking the fold to stop wakes it even while it
        // is parked on a quiet log; zslg binds its own shutdown the same way,
        // and either reaches the fold as `AbortReason::Cancelled`.
        let tail = reader
            .subscribe_stable_log(from)
            .into_channels()
            .until(&Cancellation::from_token(&cancel));
        let inputs = self.fold_inputs();
        let fold = tokio::task::spawn_blocking({
            let cancel = cancel.clone();
            move || fold::run(inputs, tail, &cancel)
        });

        let cancel_cl = cancel.clone();
        let fence = tokio::spawn(async move {
            let fence = |store: ArcStore| async {
                let started = std::time::Instant::now();
                let outcome = tokio::task::spawn_blocking(move || store.flush())
                    .await
                    .map_err(|e| Error::TaskJoin(e.to_string()))?;
                metrics::store_fence(
                    if outcome.is_ok() {
                        FenceOutcome::Ok
                    } else {
                        FenceOutcome::Failed
                    },
                    started.elapsed(),
                );
                outcome
                    .inspect_err(|e| {
                        error!(
                            error = %e,
                            "store fence failed; nothing further can be made durable, stopping the dispatcher"
                        );
                    })
            };

            loop {
                tokio::select! {
                    () = cancel_cl.cancelled() => {
                        fence(store.clone()).await?;
                        return Ok(());
                    },
                    () = tokio::time::sleep(flush_interval) => {}
                }
                fence(store.clone()).await?;
            }
        });

        let dispatcher_handle = tokio::spawn(async move {
            tokio::pin!(fold);
            tokio::pin!(fence);

            tokio::select! {
                folded = &mut fold => {
                    cancel.cancel();
                    // Every exit of the loop itself records why; a task that
                    // died never reached one. A reader blocked on the fold's
                    // progress is waiting for exactly this to wake it.
                    if folded.is_err() {
                        metrics::fold_stopped(StopReason::Panicked);
                        fold_state.stop(StopReason::Panicked);
                    }
                    let fenced = fence.await.map_err(|e| Error::TaskJoin(e.to_string()))?;
                    let folded = folded.map_err(|e| Error::TaskJoin(e.to_string()))?;
                    // `cancel` fires in these two arms and in `FoldState::halt`,
                    // which the fold returns as a failure: reaching here means
                    // folding has ended for good and the projections are
                    // frozen. `Ok` is the log simply ending or the store going
                    // away — a shutdown, not a fault — so only a failure is an
                    // error.
                    match &folded {
                        Err(e) => error!(
                            error = %e,
                            "nvzn fold stopped: projections are frozen and will not \
                             resume without a new dispatcher"
                        ),
                        Ok(()) => info!("nvzn fold stopped; projections no longer advance"),
                    }
                    folded.and(fenced)
                }
                fenced = &mut fence => {
                    let fenced = fenced.map_err(|e| Error::TaskJoin(e.to_string()))?;
                    // The fence ends cleanly only on `cancel`, which here came
                    // from `FoldState::halt`: it made what was committed
                    // durable, and the fold says why it stopped.
                    if fenced.is_ok() {
                        return (&mut fold).await.map_err(|e| Error::TaskJoin(e.to_string()))?;
                    }
                    // The fence failing strands the fold: nothing it folds
                    // from here can be made durable, so readers are owed the
                    // same answer as if the fold itself had stopped.
                    fold_state.stop(StopReason::StoreError);
                    cancel.cancel();
                    fenced
                }
            }
        });

        if let Some(target) = replay_target {
            info!(
                target = ?target,
                folded = ?*folded.borrow(),
                "waiting for the fold to replay the stable log"
            );
            // A closed watch means the fold dropped its sender: report why it
            // stopped rather than the channel mechanics, as `Syncer::sync`.
            if let Err(closed) = folded.wait_for(|f| *f >= Some(target)).await {
                gate_state.check()?;
                return Err(closed.into());
            }
            info!(target = ?target, "fold replayed the stable log");
        }

        Ok(dispatcher_handle)
    }

    /// The part of the dispatcher the fold owns; the rest is dropped here.
    pub(super) fn fold_inputs(self) -> FoldInputs {
        let Dispatcher {
            store,
            handlers,
            tx,
            fold_state,
            subscribers,
            fold_commit,
            ..
        } = self;
        FoldInputs {
            store: Arc::downgrade(&store),
            handlers,
            watermark: tx,
            fold_state,
            subscribers,
            fold_commit,
        }
    }
}
