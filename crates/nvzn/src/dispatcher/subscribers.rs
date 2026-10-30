//! [`Reader::subscribe_stable_log`](crate::Reader::subscribe_stable_log)
//! registrations: how a reader hands the fold a channel, and the cancellation
//! that ends those channels.
//!
//! ```text
//!   Reader::subscribe_stable_log ──register──> (unbounded) ──> fold, after each commit:
//!                                                              send every written ref of
//!                                                              (tenant, projection type)
//! ```
//!
//! Every stream is bound to one [`Cancellation`]. The fold fires it only when
//! it stops as [`StopReason::Cancelled`](crate::StopReason::Cancelled), so
//! those streams end [`crossbeam_combinators::End::Cancelled`]; when it stops for any
//! other reason it drops its senders and the streams end `Aborted`.

use crate::metrics::StopReason;
use crate::{ProjectionId, ProjectionTypeId, TenantId};
use crossbeam_combinators::{CancelHandle, Cancellation};
use std::sync::Arc;

/// A [`Reader::subscribe_stable_log`](crate::Reader::subscribe_stable_log) registration.
pub(crate) type Subscriber = (
    TenantId,
    ProjectionTypeId,
    crossbeam_channel::Sender<ProjectionId>,
);

/// What a [`Reader`](crate::Reader) needs to hand out
/// [`subscribe_stable_log`](crate::Reader::subscribe_stable_log) streams: where to register them,
/// and the cancellation that ends them when the fold is cancelled.
#[derive(Clone)]
pub(crate) struct Subscriptions {
    pub(crate) register: crossbeam_channel::Sender<Subscriber>,
    pub(crate) cancelled: Cancellation,
    /// Held, never used: a handle dropped unfired ends every bound stream at
    /// once, and the fold drops its copy whenever it stops. Readers keeping
    /// it alive is what lets a fold that stopped for any other reason still
    /// deliver what its subscribers hold, then end them `Aborted`.
    pub(crate) _keep_armed: Arc<CancelHandle>,
}

impl Subscriptions {
    /// The reader side and the fold side of one set of subscriptions.
    pub(crate) fn new() -> (Subscriptions, Subscribers) {
        let (register, registered) = crossbeam_channel::unbounded();
        let (cancel, cancelled) = CancelHandle::new();
        let cancel = Arc::new(cancel);
        let subscriptions = Subscriptions {
            register,
            cancelled,
            _keep_armed: Arc::clone(&cancel),
        };
        let subscribers = Subscribers {
            registered,
            cancel,
            live: Vec::new(),
        };
        (subscriptions, subscribers)
    }
}

/// The fold's side: registrations not yet taken up, the subscribers taken up
/// so far, and the handle that cancels their streams. Dropping it drops the
/// fold's copy of that handle and every sender it holds.
pub(crate) struct Subscribers {
    registered: crossbeam_channel::Receiver<Subscriber>,
    cancel: Arc<CancelHandle>,
    live: Vec<Subscriber>,
}

impl Subscribers {
    /// Take up pending registrations, then send each subscriber the refs of
    /// its `(tenant, projection type)` in `written`. Call only after the
    /// commit, so every ref sent is readable. A dropped subscriber fails its
    /// send and is removed.
    pub(super) fn publish(&mut self, written: &[(TenantId, ProjectionTypeId, ProjectionId)]) {
        self.live.extend(self.registered.try_iter());
        self.live.retain(|(tenant_id, ptid, tx)| {
            written
                .iter()
                .filter(|(t, p, _)| (t, p) == (tenant_id, ptid))
                .all(|(_, _, pid)| tx.send(*pid).is_ok())
        });
    }

    /// Cancel every stream iff the fold's *recorded* stop reason is
    /// [`StopReason::Cancelled`]. Any other reason leaves them to end
    /// `Aborted` once the senders drop.
    pub(super) fn cancel_if(&self, recorded: Option<StopReason>) {
        if recorded == Some(StopReason::Cancelled) {
            self.cancel.cancel();
        }
    }
}

/// Registrations the fold has not yet taken up. Checked in `subscribe_stable_log`, not a
/// `bounded` channel: only the unbounded flavour drops queued registrations
/// (ending their channels) when the fold's receiver goes.
pub(crate) const PENDING_SUBSCRIBERS: usize = 128;
