//! A source raced against a cancellation or liveness signal.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::Select;

use super::leaf::LeafState;
use super::{AbortReason, Cancellation, Channel, End, Poll, Source};
use crate::kind::Kind;

/// An inner source raced against a [`Cancellation`]: whichever ends first ends
/// the stream ([`Channel::until`]).
///
/// Not [`MergeState`], which keeps the survivor when one branch closes cleanly
/// — that would make a finite source never terminate. Not [`ChainState`]
/// either, which is sequential. The guard carries no items: its only event is
/// disconnecting. Whatever holds the stream may be what keeps its producer
/// alive, so the guard is the one thing that can be relied on to wake the
/// consumer; `fired` only picks the verdict — `Cancelled` for a cancel,
/// `CancelHandleDropped` for a handle dropped without cancelling.
pub(super) struct Guarded<T> {
    pub(super) inner: Box<dyn Source<T>>,
    pub(super) alive: LeafState<()>,
    /// Set before the guard's sender is dropped by a deliberate cancel.
    pub(super) fired: Arc<AtomicBool>,
}

impl<T> Guarded<T> {
    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}

impl<T: Send + 'static> Source<T> for Guarded<T> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        // Either side finishing finishes the source, so once one is gone
        // nothing is registered and `recv` falls through to `end_reason`.
        // Registering the survivor instead would block forever: on the guard
        // after a finite source completed, or on the source after the consumer
        // left — the two bugs this type exists to prevent.
        if self.leaf_count() == 0 {
            return 0;
        }
        // Registration order (inner then guard) defines the ordinals, as in
        // MergeState; the guard's leaf gets the highest one.
        let inner = self.inner.register(sel);
        let alive = self.alive.register(sel);
        inner.saturating_add(alive)
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        debug_assert!(
            ordinal < self.leaf_count(),
            "ordinal {ordinal} out of range for guarded source with {} leaves",
            self.leaf_count()
        );
        if ordinal < self.inner.leaf_count() {
            self.inner.poll(ordinal)
        } else {
            debug_assert_eq!(
                ordinal,
                self.inner.leaf_count(),
                "only the guard leaf lives above the inner source"
            );
            match self.alive.poll(0) {
                // The guard never carries items, so the only thing that can
                // happen to it is dying — and that ends the stream.
                Poll::Dead => Poll::Dead,
                _ => Poll::Filtered,
            }
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        if self.leaf_count() == 0 {
            return None;
        }
        self.inner.drain_first()
    }
    /// Zero as soon as *either* side is gone — the count must match what
    /// `register` actually registers, since an enclosing source derives its
    /// ordinal split from it.
    fn leaf_count(&self) -> usize {
        let (inner, alive) = (self.inner.leaf_count(), self.alive.leaf_count());
        if inner == 0 || alive == 0 {
            return 0;
        }
        inner.saturating_add(alive)
    }
    fn end_reason(&mut self) -> End {
        // A declared completion stands whatever happened afterwards: the
        // producer closed and every item it sent was drained, so the stream is
        // whole.
        if self.inner.leaf_count() == 0 && self.inner.end_reason() == End::Complete {
            return End::Complete;
        }
        // Read the flag, not the guard leaf: the leaf learns of the disconnect
        // only when polled, and the select may have woken on the inner side
        // first. `fired` is set before anything the cancel tears down, so an
        // inner end seen once it is set is the cancel's doing.
        if self.fired() {
            return End::Aborted {
                reason: AbortReason::Cancelled,
            };
        }
        if self.alive.leaf_count() == 0 {
            // A cancel handle went away unfired. The guard's own disconnect is
            // the reason, not the inner source's.
            return End::Aborted {
                reason: AbortReason::CancelHandleDropped,
            };
        }
        self.inner.end_reason()
    }
}

impl<T: Send + 'static, K: Kind> Channel<T, K> {
    /// End this stream as soon as `cancel` fires, even while `recv` is
    /// blocked waiting for an item: the stream then reports
    /// [`End::Aborted { reason: AbortReason::Cancelled }`]. Items already buffered when it fires may still be
    /// delivered first, or not: the stream stops at the first wake-up that
    /// sees the cancel.
    ///
    /// A race, not a [`merge`](Self::merge): merged with a cancellation, a
    /// finite stream that closes would leave the merge waiting on the
    /// survivor — the cancellation — and never end. Here either side ending
    /// ends the stream, and a clean [`End::Complete`] keeps its verdict.
    #[must_use]
    pub fn until(self, cancel: &Cancellation) -> Channel<T, K> {
        let Some(rx) = &cancel.rx else {
            return self;
        };
        Channel::wrap(Box::new(Guarded {
            inner: self.source,
            alive: LeafState::Alive {
                rx: rx.clone(),
                close_rx: None,
                what: "cancellation",
            },
            fired: Arc::clone(&cancel.fired),
        }))
    }
}
