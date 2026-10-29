//! What a reader can know about the fold without talking to it: once, why it
//! stopped.
//!
//! A stopped fold is not a failure a reader can see any other way: the store
//! keeps answering, from a watermark that no longer moves. The stop reason is
//! set once — the first reason to arrive is the one that ended the fold; later
//! paths unwinding behind it would only overwrite it with a symptom.
//!
//! Progress is the dispatcher's `watch` (`Syncer::watermark`), not kept here.

use crate::Error;
use crate::metrics::StopReason;
use std::sync::Arc;

/// The fold's terminal state, shared with every reader built from
/// the same dispatcher.
#[derive(Clone)]
pub(crate) struct FoldState(Arc<FoldShared>);

struct FoldShared {
    stopped: std::sync::OnceLock<StopReason>,
    /// The dispatcher's cancellation: a child of the node's shutdown token
    /// (`Dispatcher::new`), so a node shutdown stops the fold while ending the
    /// fold leaves the node up. The fold and its fence stop on it; the
    /// supervisor fires it when the fold ends, and [`FoldState::halt`] when
    /// something outside the fold needs it stopped.
    cancel: tokio_util::sync::CancellationToken,
}

impl FoldState {
    pub(super) fn new(cancel: tokio_util::sync::CancellationToken) -> Self {
        Self(Arc::new(FoldShared {
            stopped: std::sync::OnceLock::new(),
            cancel,
        }))
    }

    pub(super) fn stop(&self, reason: StopReason) {
        let _ = self.0.stopped.set(reason);
    }

    /// Stop the dispatcher from outside the fold (a live view whose tail
    /// ended): record `reason` (unless the fold already stopped for another),
    /// then cancel. The fold takes the recorded reason as why it stopped, and
    /// as a failure, so the dispatcher's task ends with [`Error::FoldStopped`]
    /// and the process notices as for any fold stop.
    pub(crate) fn halt(&self, reason: StopReason) {
        self.stop(reason);
        self.0.cancel.cancel();
    }

    /// The dispatcher's cancellation, for the supervisor.
    pub(super) fn cancellation(&self) -> tokio_util::sync::CancellationToken {
        self.0.cancel.clone()
    }

    /// Why the fold stopped, once it has: the first reason recorded.
    pub(super) fn reason(&self) -> Option<StopReason> {
        self.0.stopped.get().copied()
    }

    /// `Err` once the fold has stopped, naming why.
    pub(crate) fn check(&self) -> Result<(), Error> {
        match self.0.stopped.get() {
            Some(reason) => Err(Error::FoldStopped(*reason)),
            None => Ok(()),
        }
    }
}
