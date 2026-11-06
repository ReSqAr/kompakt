//! Cancellation signals: [`Cancellation`](crate::Cancellation) and its [`CancelHandle`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::Receiver;

/// A cancellation that can end a blocked [`Channel::recv`](crate::Channel::recv).
///
/// A tokio [`CancellationToken`](tokio_util::sync::CancellationToken) cannot
/// wake a crossbeam `Select`, but a disconnecting receiver can, and every clone
/// of a crossbeam receiver sees the disconnect. So one [`CancelHandle`] owns
/// the only sender and every bound stream holds a receiver clone: firing is one
/// drop, however many streams are bound, and no per-stream task exists to
/// leak when a consumer walks away early.
///
/// The disconnect always ends the bound streams at once — that is the point:
/// whatever holds a stream may be what keeps its producer alive, so nothing but
/// the guard can be relied on to wake a parked consumer. `fired` only decides
/// the verdict. It is set *before* the sender drops, so a bound stream that
/// ends once it is set reports [`End::Aborted { reason: AbortReason::Cancelled }`]. A handle dropped without
/// [`CancelHandle::cancel`] ends them `Aborted` instead: that is teardown (the
/// runtime running [`from_token`](Self::from_token)'s task shut down), not a
/// request to stop. An owner whose streams should outlive it — and deliver
/// what they hold — keeps its handle alive until it means to cancel.
#[derive(Clone)]
pub struct Cancellation {
    /// `None` for [`never`](Self::never): nothing to race against.
    pub(super) rx: Option<Receiver<()>>,
    pub(super) fired: Arc<AtomicBool>,
}

/// The firing half of a [`Cancellation`](crate::Cancellation).
pub struct CancelHandle {
    /// `Mutex` only so `cancel` can take `&self`; never held across an
    /// `.await` (§4).
    tx: std::sync::Mutex<Option<crossbeam_channel::Sender<()>>>,
    fired: Arc<AtomicBool>,
}

impl CancelHandle {
    /// A fresh, unfired cancellation and the handle that fires it.
    #[must_use]
    pub fn new() -> (Self, Cancellation) {
        let (tx, rx) = crossbeam_channel::bounded::<()>(0);
        let fired = Arc::new(AtomicBool::new(false));
        (
            Self {
                tx: std::sync::Mutex::new(Some(tx)),
                fired: Arc::clone(&fired),
            },
            Cancellation {
                rx: Some(rx),
                fired,
            },
        )
    }

    /// Fire: every stream bound to this cancellation ends with
    /// [`End::Aborted { reason: AbortReason::Cancelled }`]. Idempotent, and synchronous — a consumer woken after
    /// this returns sees the cancel, which is what lets a caller order it
    /// before tearing down whatever feeds those streams.
    pub fn cancel(&self) {
        // Before the drop: a guard reads the flag only after observing the
        // disconnect, so it must already be set by then.
        self.fired.store(true, Ordering::SeqCst);
        drop(
            self.tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
    }
}

impl Cancellation {
    /// A cancellation that fires when `token` is cancelled.
    ///
    /// Spawns one task, which lives until the token fires, so call it once per
    /// token and clone the result rather than once per stream. Must be called
    /// from within a Tokio runtime.
    #[cfg(feature = "tokio")]
    #[must_use]
    pub fn from_token(token: &tokio_util::sync::CancellationToken) -> Self {
        let (handle, cancellation) = CancelHandle::new();
        let token = token.clone();
        tokio::spawn(async move {
            token.cancelled().await;
            handle.cancel();
        });
        cancellation
    }

    /// A cancellation that never fires: [`Channel::until`](crate::Channel::until) with it is a
    /// no-op. For a caller with nothing to cancel on.
    #[must_use]
    pub fn never() -> Self {
        Self {
            rx: None,
            fired: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether [`CancelHandle::cancel`] has run.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}
