//! The stable fold: the stable log, applied in index order to every tenant's
//! projections, one store commit per batch.
//!
//! ```text
//!   txn = store.write();  w = txn.watermark()
//!   entries ready, one at a time, until txn holds `fold_commit` bytes:
//!     Appended{index <= w.index}     ─> skip           (replayed)
//!     Appended{data_id <= w.data_id} ─> stop           (the stable log is corrupt)
//!     Appended{blob}                 ─> apply: tenant_id ‖ payload ─> tenant handler
//!     Watermark(hlc)                 ─> w.hlc = max(w.hlc, hlc)
//!   txn.commit(w) ─> send written refs to subscribers ─> advance FoldState + watch
//! ```
//!
//! A batch is cut by what it writes, not by what it reads: a commit holds every
//! row it writes in memory until it lands, and one small event can write
//! thousands of rows (a map box's fill names its days; each becomes a day, a
//! month and a timeline row and their index entries). Cut by the events'
//! payload instead, a burst of such events was one commit of ~240k rows. The
//! transaction's staged bytes are looked at after each entry: an entry (one
//! write) is never split, so a lone entry above the limit commits alone.
//!
//! The watermark commits atomically with the batch, so the store always covers
//! exactly the applied prefix and a restart resumes above it. Subscribers are
//! sent refs only after the commit, so every ref they get is readable.
//!
//! Every exit is a [`FoldStop`] returned to [`run`], which records it in one
//! place: the metric, the reader-visible [`FoldState`] and the subscribers'
//! cancel are the same fact, and they are wrong the moment they can disagree.
//! The fold holds the store weakly, so dropping the dispatcher's store ends it.

use super::{FoldState, InnerHandler, Subscribers};
use crate::ids::APP_ID_LEN;
use crate::metrics::{self, EntryOutcome, StopReason};
use crate::{Error, ReadTransaction, Store, TenantId, Watermark, WriteTransactionImpl};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Weak;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace};
use zslg::{Channels, Hlc, LogIndex, StableLogEvent};

type LogItem = Result<StableLogEvent, zslg::Error>;

/// What the fold runs on, split off the dispatcher by `finalise`.
pub(super) struct FoldInputs {
    pub(super) store: Weak<dyn Store>,
    pub(super) handlers: BTreeMap<TenantId, InnerHandler>,
    /// The HLC committed through, for async waiters; [`FoldState`] carries the
    /// same value for blocking ones.
    pub(super) watermark: watch::Sender<Option<Hlc>>,
    pub(super) fold_state: FoldState,
    pub(super) subscribers: Subscribers,
    /// What one commit holds in memory, at most (plus the entry that crosses
    /// it): [`WriteTransactionImpl::staged_bytes`] against
    /// [`StoreConfig::fold_commit`](crate::StoreConfig::fold_commit).
    pub(super) fold_commit: usize,
}

/// Why the fold ended, and the error [`run`] returns for it, if any.
pub(super) struct FoldStop {
    reason: StopReason,
    error: Option<Error>,
}

impl FoldStop {
    /// An end that is not a fault: `run` returns `Ok`.
    fn clean(reason: StopReason) -> Self {
        Self {
            reason,
            error: None,
        }
    }

    fn failed(reason: StopReason, error: impl Into<Error>) -> Self {
        Self {
            reason,
            error: Some(error.into()),
        }
    }

    /// [`failed`](Self::failed) as a `map_err` argument.
    fn on<E: Into<Error>>(reason: StopReason) -> impl FnOnce(E) -> Self {
        move |error| Self::failed(reason, error)
    }
}

/// Fold `ch` until it ends, `cancel` fires, the store goes, or an entry
/// cannot be folded; `Ok` for an end that is not a fault.
pub(super) fn run(
    mut inputs: FoldInputs,
    mut ch: Channels<LogItem>,
    cancel: &CancellationToken,
) -> Result<(), Error> {
    let _span = tracing::info_span!("nvzn_fold").entered();
    let stop = loop {
        match inputs.step(&mut ch, cancel) {
            Ok(()) => continue,
            Err(stop) => break stop,
        }
    };
    inputs.stop(stop)
}

impl FoldInputs {
    fn stop(self, FoldStop { reason, error }: FoldStop) -> Result<(), Error> {
        metrics::fold_stopped(reason);
        self.fold_state.stop(reason);
        // The recorded reason, not `reason`: a fence failure that cancels the
        // fold has already recorded `store_error`, and subscribers are owed
        // that, not a cancel. Fired before `subscribers` drops with `self`, so
        // their streams see the cancel first.
        self.subscribers.cancel_if(self.fold_state.reason());
        error.map_or(Ok(()), Err)
    }

    fn step(
        &mut self,
        ch: &mut Channels<LogItem>,
        cancel: &CancellationToken,
    ) -> Result<(), FoldStop> {
        if cancel.is_cancelled() {
            info!("dispatcher cancelled; fold loop stopping");
            return Err(FoldStop::clean(StopReason::Cancelled));
        }
        let Ok(first) = ch.recv() else {
            return Err(ended(ch.end_reason()));
        };
        self.fold_batch(first, ch)
    }

    /// `first`, then whatever is ready without waiting, in one commit, until
    /// the commit holds `fold_commit` bytes.
    fn fold_batch(&mut self, first: LogItem, ch: &mut Channels<LogItem>) -> Result<(), FoldStop> {
        let Some(store) = self.store.upgrade() else {
            info!("dispatcher store dropped; fold loop stopping");
            return Err(FoldStop::clean(StopReason::StoreDropped));
        };

        let started = std::time::Instant::now();
        let mut entries = 0usize;
        let mut txn = store
            .write()
            .map_err(FoldStop::on(StopReason::StoreError))?;
        let mut watermark = txn
            .watermark()
            .map_err(FoldStop::on(StopReason::StoreError))?;

        let mut next = Some(first);
        while let Some(item) = next.take() {
            entries = entries.saturating_add(1);
            match item.map_err(FoldStop::on(StopReason::Upstream))? {
                StableLogEvent::Appended { index, data } => {
                    self.fold_appended(&mut txn, &mut watermark, index, data)?;
                }
                StableLogEvent::Watermark(hlc) => {
                    metrics::fold_entry(EntryOutcome::Watermark);
                    watermark.hlc = watermark.hlc.max(Some(hlc));
                }
            }
            if txn.staged_bytes() < self.fold_commit {
                next = ch.try_recv();
            }
        }

        let bytes = txn.staged_bytes();
        let written = txn.written();
        let Watermark { index, hlc, .. } = watermark;
        txn.commit(watermark)
            .map_err(FoldStop::on(StopReason::StoreError))?;
        self.subscribers.publish(&written);

        metrics::fold_batch(entries, bytes, started.elapsed());
        metrics::fold_watermark(index, hlc);

        if let Some(hlc) = hlc {
            self.fold_state.advance(hlc);
            let _ = self.watermark.send(Some(hlc));
        }
        Ok(())
    }

    fn fold_appended(
        &self,
        txn: &mut WriteTransactionImpl,
        watermark: &mut Watermark,
        index: LogIndex,
        zslg::Data { data_id, blob }: zslg::Data,
    ) -> Result<(), FoldStop> {
        if Some(index) <= watermark.index {
            metrics::fold_entry(EntryOutcome::Replayed);
            return Ok(());
        }

        if let Some(max_data_id) = watermark.data_id
            && data_id <= max_data_id
        {
            error!(
                ?data_id,
                ?max_data_id,
                log_index = ?index,
                "data ID went backwards: the stable log is corrupt and cannot be \
                 folded past this point"
            );
            return Err(FoldStop::failed(
                StopReason::NonMonotoneDataId,
                Error::NonMonotoneDataId {
                    max_data_id,
                    data_id,
                    index,
                },
            ));
        }

        watermark.index = watermark.index.max(Some(index));
        watermark.data_id = watermark.data_id.max(Some(data_id));
        watermark.hlc = watermark.hlc.max(Some(data_id.hlc()));
        apply(&self.handlers, txn, blob).map_err(FoldStop::on(StopReason::PoisonEntry))
    }
}

/// The stop for a subscription that has ended: only an abort is a fault.
fn ended(end: zslg::ChannelEnd) -> FoldStop {
    match end {
        zslg::ChannelEnd::Closed => {
            info!("stable log subscription closed; fold loop finished");
            FoldStop::clean(StopReason::SubscriptionClosed)
        }
        zslg::ChannelEnd::Cancelled => {
            info!("stable log subscription cancelled; fold loop stopping");
            FoldStop::clean(StopReason::Cancelled)
        }
        zslg::ChannelEnd::Aborted { what } => {
            // `what` names the leaf that ended — where to look upstream.
            info!(what, "stable log subscription ended; fold loop stopping");
            FoldStop::failed(StopReason::SubscriptionAborted, Error::SubscriptionAborted)
        }
    }
}

fn apply(
    handlers: &BTreeMap<TenantId, InnerHandler>,
    txn: &mut WriteTransactionImpl,
    payload: Bytes,
) -> Result<(), Error> {
    let Some((tenant_id, payload)) = payload.split_first_chunk() else {
        error!(
            len = payload.len(),
            "log entry is too short to carry an tenant ID; a non-nvzn writer shares this log"
        );
        return Err(Error::PayloadTooShort(payload.len(), APP_ID_LEN));
    };

    let tenant_id = TenantId::from_slice(*tenant_id);
    let Some(handler) = handlers.get(&tenant_id) else {
        trace!(
            tenant_id = tenant_id.as_u128(),
            "no handler registered; skipping"
        );
        metrics::fold_entry(EntryOutcome::ForeignTenant);
        return Ok(());
    };

    handler(tenant_id, txn, payload)?;
    metrics::fold_entry(EntryOutcome::Applied);

    Ok(())
}
