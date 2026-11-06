//! A phase whose channel comes from an async opener, started on first read.

use crossbeam_channel::{Receiver, Select, TryRecvError};

use super::{AbortReason, End, Poll, Source};

/// A phase whose producer starts on first use: [`Channel::defer_async`].
///
/// Until the future resolves, the source registers the hand-off receiver, so
/// it stays selectable (an enclosing merge or chain does not block). The
/// future is spawned the first time the source is registered or drained —
/// never at construction — so a `chain_all` of deferred phases opens them one
/// at a time. Dropping the source while it waits aborts the task.
pub(super) struct Deferred<T> {
    pub(super) state: DeferredState<T>,
}

pub(super) enum DeferredState<T> {
    Waiting {
        handoff: Receiver<Box<dyn Source<T>>>,
        start: Option<Box<dyn FnOnce() -> tokio::task::AbortHandle + Send>>,
        task: Option<tokio::task::AbortHandle>,
    },
    Running(Box<dyn Source<T>>),
    Ended(End),
}

pub(super) const DEFERRED: &str = "defer_async";

impl<T: Send + 'static> Deferred<T> {
    fn ensure_started(&mut self) {
        if let DeferredState::Waiting { start, task, .. } = &mut self.state
            && let Some(start) = start.take()
        {
            *task = Some(start());
        }
    }

    /// Take the resolved channel if it is there. `true` when the state moved.
    fn try_resolve(&mut self) -> bool {
        let DeferredState::Waiting { handoff, .. } = &self.state else {
            return false;
        };
        match handoff.try_recv() {
            Ok(source) => {
                self.state = DeferredState::Running(source);
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => {
                self.state = DeferredState::Ended(End::Aborted {
                    reason: AbortReason::ProducerGone { what: DEFERRED },
                });
                true
            }
        }
    }
}

impl<T: Send + 'static> Source<T> for Deferred<T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        self.ensure_started();
        match &mut self.state {
            DeferredState::Waiting { handoff, .. } => {
                sel.recv(handoff);
                1
            }
            DeferredState::Running(src) => src.register(sel),
            DeferredState::Ended(_) => 0,
        }
    }

    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        match &mut self.state {
            DeferredState::Waiting { .. } => {
                if !self.try_resolve() {
                    return Poll::Empty;
                }
                match self.state {
                    // Resolved: the new leaves are registered on the next round.
                    DeferredState::Running(_) => Poll::Filtered,
                    _ => Poll::Dead,
                }
            }
            DeferredState::Running(src) => src.poll(ordinal),
            DeferredState::Ended(_) => Poll::Dead,
        }
    }

    fn drain_first(&mut self) -> Option<T> {
        self.ensure_started();
        self.try_resolve();
        match &mut self.state {
            DeferredState::Running(src) => src.drain_first(),
            _ => None,
        }
    }

    fn leaf_count(&self) -> usize {
        match &self.state {
            DeferredState::Waiting { .. } => 1,
            DeferredState::Running(src) => src.leaf_count(),
            DeferredState::Ended(_) => 0,
        }
    }

    fn end_reason(&mut self) -> End {
        match &mut self.state {
            // Asked before the phase ended: fail closed, as a leaf does.
            DeferredState::Waiting { .. } => End::Aborted {
                reason: AbortReason::ProducerGone { what: DEFERRED },
            },
            DeferredState::Running(src) => src.end_reason(),
            DeferredState::Ended(end) => *end,
        }
    }
}

impl<T> Drop for Deferred<T> {
    fn drop(&mut self) {
        if let DeferredState::Waiting {
            task: Some(task), ..
        } = &self.state
        {
            task.abort();
        }
    }
}
