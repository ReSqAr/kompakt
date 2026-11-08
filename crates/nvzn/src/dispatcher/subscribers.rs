//! The fold's keyed fan-out to [`Reader::subscribe_stable_log`] streams.
//!
//! ```text
//!   Reader::subscribe_stable_log ──subscribe((tenant, type))──> KeyedRegistry
//!                                                                    ▲
//!   fold, after each commit: tx.send((tenant, type, id)) ──┘
//! ```
//!
//! The registry owns the distribution thread, the subscriber set, and the
//! routing. The fold owns the data sender and the single [`Shutdown`]; readers
//! hold a clone of the registry. There is no pending-registration queue and no
//! `_keep_armed`: the bounded registry queue is the admission bound.
//!
//! A stream ends with `Aborted(ProducerGone)` when the fan-out stops. A caller
//! that wants `Cancelled` binds its own `Cancellation` with `until`.

use crate::StopReason;
use crate::{ProjectionId, ProjectionTypeId, SubscriptionItem, TenantId};
use crossbeam_combinators::{CancelHandle, Fanout};
use std::sync::Arc;

/// Registrations waiting in the bounded queue.
pub(crate) const PENDING_SUBSCRIBERS: usize = 1024;
/// Per-subscriber stream depth.
pub(crate) const SUBSCRIBER_CAPACITY: usize = 1024;
/// How long a stopping fold waits for subscribers to take their queued refs.
const DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// The fold's side: the fan-out producer (until stopped) and the live-view
/// cancellation.
pub(crate) struct Subscribers {
    /// `None` once [`cancel_if`](Self::cancel_if) stopped it.
    fanout: Option<Fanout<SubscriptionItem>>,
    cancel: Arc<CancelHandle>,
    /// Whether a stopped fan-out has been reported; it is reported once.
    warned: bool,
}

impl Subscribers {
    pub(crate) fn new(fanout: Fanout<SubscriptionItem>, cancel: Arc<CancelHandle>) -> Self {
        Self {
            fanout: Some(fanout),
            cancel,
            warned: false,
        }
    }

    /// Publish every written ref. The data queue is bounded and the fan-out
    /// is `Block`, so this backpressures the fold while a subscriber catches up.
    ///
    /// A stopped fan-out is warned about once and otherwise ignored:
    /// subscribers are observers, and the fold's correctness does not depend
    /// on them.
    pub(super) fn publish(&mut self, written: &[(TenantId, ProjectionTypeId, ProjectionId)]) {
        let Some(fanout) = &self.fanout else {
            return;
        };
        for &(tenant, ptid, pid) in written {
            if fanout.publish((tenant, ptid, pid)).is_err() && !self.warned {
                self.warned = true;
                tracing::warn!(
                    "subscriber fan-out stopped; stable-log subscribers see no more refs"
                );
            }
        }
    }

    /// Stop the fan-out: subscriber streams end `ProducerGone`. Dropping
    /// `Subscribers` stops it at once, discarding what is queued.
    ///
    /// A stop that is not a cancel first delivers every ref already published
    /// (they are committed; a subscriber is owed them), for at most
    /// [`DRAIN_GRACE`] so a subscriber that never reads cannot hold the fold's
    /// exit. A cancel stops at once.
    pub(super) fn cancel_if(&mut self, recorded: Option<StopReason>) {
        let Some(fanout) = self.fanout.take() else {
            return;
        };
        if recorded == Some(StopReason::Cancelled) {
            self.cancel.cancel();
            drop(fanout);
        } else {
            fanout.drain(DRAIN_GRACE);
        }
    }
}
