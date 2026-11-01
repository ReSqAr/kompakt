//! The fold's keyed fan-out to [`Reader::subscribe_stable_log`] streams.
//!
//! ```text
//!   Reader::subscribe_stable_log ──subscribe((tenant, type))──> KeyedRegistry
//!                                                                    ▲
//!   fold, after each commit: tx.send(Arc::new((tenant, type, id))) ──┘
//! ```
//!
//! The registry owns the distribution thread, the subscriber set, and the
//! routing. The fold owns the data sender and the single [`Shutdown`]; readers
//! hold a clone of the registry. There is no pending-registration queue and no
//! `_keep_armed`: the bounded registry queue is the admission bound.
//!
//! A stream ends with `Aborted(ProducerGone)` when the fan-out stops. A caller
//! that wants `Cancelled` binds its own `Cancellation` with `until`.

use crate::metrics::StopReason;
use crate::{ProjectionId, ProjectionTypeId, TenantId};
use crossbeam_combinators::{CancelHandle, Cancellation, KeyedRegistry, Shutdown};
use std::sync::Arc;

/// A registration key.
pub(crate) type SubscriptionKey = (TenantId, ProjectionTypeId);
/// A written ref: the key plus the projection id.
pub(crate) type SubscriptionItem = (TenantId, ProjectionTypeId, ProjectionId);
/// The reader side: the subscribe registry plus the live-view cancellation.
/// The cancellation is not the fan-out's; it stops live views when the fold
/// stops. The armed handle keeps the cancellation from firing during teardown.
#[derive(Clone)]
pub(crate) struct Subscriptions {
    pub(crate) registry: KeyedRegistry<SubscriptionKey, SubscriptionItem>,
    pub(crate) cancelled: Cancellation,
    _keep_armed: Arc<CancelHandle>,
}

impl Subscriptions {
    pub(crate) fn new(
        registry: KeyedRegistry<SubscriptionKey, SubscriptionItem>,
        cancelled: Cancellation,
        keep_armed: Arc<CancelHandle>,
    ) -> Self {
        Self {
            registry,
            cancelled,
            _keep_armed: keep_armed,
        }
    }
}

/// Registrations waiting in the bounded queue.
pub(crate) const PENDING_SUBSCRIBERS: usize = 1024;
/// Per-subscriber stream depth.
pub(crate) const SUBSCRIBER_CAPACITY: usize = 1024;

/// The fold's side: the data sender and the single shutdown owner.
pub(crate) struct Subscribers {
    tx: crossbeam_channel::Sender<Arc<SubscriptionItem>>,
    shutdown: Shutdown,
    cancel: Arc<CancelHandle>,
}

impl Subscribers {
    pub(crate) fn new(
        tx: crossbeam_channel::Sender<Arc<SubscriptionItem>>,
        shutdown: Shutdown,
        cancel: Arc<CancelHandle>,
    ) -> Self {
        Self {
            tx,
            shutdown,
            cancel,
        }
    }

    /// Publish every written ref. `tx` is the bounded data queue; `Block`
    /// backpressures the fold while a subscriber catches up.
    pub(super) fn publish(&mut self, written: &[(TenantId, ProjectionTypeId, ProjectionId)]) {
        for &(tenant, ptid, pid) in written {
            let _ = self.tx.send(Arc::new((tenant, ptid, pid)));
        }
    }

    /// Stop the fan-out. A deliberate stop ends subscriber streams
    /// `ProducerGone`; dropping `Subscribers` stops it the same way.
    pub(super) fn cancel_if(&mut self, recorded: Option<StopReason>) {
        if recorded == Some(StopReason::Cancelled) {
            self.cancel.cancel();
        }
        self.shutdown.cancel();
    }
}
