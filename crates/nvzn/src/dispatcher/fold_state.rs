//! What a reader can know about the fold without talking to it: how far it has
//! committed, and — once — why it stopped.
//!
//! A stopped fold is not a failure a reader can see any other way: the store
//! keeps answering, from a watermark that no longer moves. The stop reason is
//! set once — the first reason to arrive is the one that ended the fold; later
//! paths unwinding behind it would only overwrite it with a symptom.
//!
//! Progress is the HLC the fold has committed through, the same value the
//! dispatcher's `watch` carries. It is kept here as well because
//! [`Reader::unstable_snapshot`](crate::Reader::unstable_snapshot) is a
//! blocking call and needs a blocking wait on it; a `watch` only offers an
//! async one.

use crate::Error;
use crate::metrics::StopReason;
use std::sync::Arc;
use zslg::Hlc;

/// The fold's progress and terminal state, shared with every reader built from
/// the same dispatcher.
#[derive(Clone)]
pub(crate) struct FoldState(Arc<FoldShared>);

struct FoldShared {
    stopped: std::sync::OnceLock<StopReason>,
    /// Never held across an `.await` (§4): every critical section on it is
    /// synchronous. Poisoning is ignored throughout — a plain `Option<Hlc>`
    /// cannot be left half-written by a panic.
    folded: std::sync::Mutex<Option<Hlc>>,
    changed: std::sync::Condvar,
}

impl FoldState {
    pub(super) fn new(folded: Option<Hlc>) -> Self {
        Self(Arc::new(FoldShared {
            stopped: std::sync::OnceLock::new(),
            folded: std::sync::Mutex::new(folded),
            changed: std::sync::Condvar::new(),
        }))
    }

    fn folded(&self) -> std::sync::MutexGuard<'_, Option<Hlc>> {
        self.0
            .folded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn stop(&self, reason: StopReason) {
        let _ = self.0.stopped.set(reason);
        // Taking the lock orders this against a waiter between its `check`
        // and its `wait`, so the wake-up cannot be lost.
        let _guard = self.folded();
        self.0.changed.notify_all();
    }

    /// Record that the fold has committed through `hlc`.
    pub(super) fn advance(&self, hlc: Hlc) {
        let mut folded = self.folded();
        *folded = (*folded).max(Some(hlc));
        self.0.changed.notify_all();
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

    /// Block until the fold has committed through `target`, or fail once it
    /// has stopped and never will.
    ///
    /// Blocks the calling thread: on a `current_thread` runtime, whatever
    /// feeds the fold's subscription must not need that thread to run.
    pub(crate) fn wait_folded(&self, target: Hlc) -> Result<(), Error> {
        let mut folded = self.folded();
        loop {
            if *folded >= Some(target) {
                return Ok(());
            }
            self.check()?;
            folded = self
                .0
                .changed
                .wait(folded)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}
