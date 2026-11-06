//! The one resume loop behind `Resumable` and `resync`.

use crossbeam_channel::Select;

use super::{AbortReason, Channel, End, Poll, Source};
use crate::kind::Infinite;
use crate::resume::Opening;

/// The one resume loop behind [`crate::Resumable`] and [`crate::resync`]: a
/// stream of segments, each opened by `make` from the cursor.
///
/// The rule (D18): a new segment opens only after `Aborted(ProducerGone)` —
/// the fan-out hang-up, or a dead producer that a fresh cycle repairs. Every
/// other end (`Cancelled`, `OrderViolation`, ...) ends the stream unchanged,
/// so a corrupt log cannot spin the loop and `until` is safe on either side.
/// `make` may decline with `None`; a decline after a segment ends the stream
/// with that segment's reason, a decline of the *first* open ends it
/// `Aborted(ProducerGone { what })`, never `Complete`.
///
/// The cursor `C` is updated from every item the segment emits (`track`), so
/// `make` always sees the last emitted position. [`crate::resync`] has none
/// (`C = ()`).
///
/// A segment is materialised eagerly — at construction, and again the instant
/// its predecessor dies — never lazily inside `register`, and never while a
/// registration is outstanding. An enclosing [`MergeState`] splits the ordinal
/// space by `leaf_count()` *at poll time*, against a registration built before
/// `Select::ready` blocked, so the count has to hold across that whole window.
/// A segment swapped in mid-window would misalign every index to its right.
/// This is the one source whose leaf count is meant to change.
pub(super) struct Resume<T, C> {
    pub(super) what: &'static str,
    pub(super) cursor: C,
    pub(super) track: fn(&T, &mut C),
    #[allow(clippy::type_complexity, reason = "one boxed closure, named inline")]
    pub(super) make: Box<dyn FnMut(&C, Opening) -> Option<Channel<T, Infinite>> + Send>,
    pub(super) current: Option<Box<dyn Source<T>>>,
    /// The end of the segment before `current`, reported once by `end_reason`
    /// when the stream stops.
    pub(super) last_end: Option<End>,
}

impl<T: Send + 'static, C: Send + 'static> Resume<T, C> {
    pub(super) fn open(&mut self, opening: Opening) {
        self.current = (self.make)(&self.cursor, opening).map(|ch| ch.source);
    }

    /// The current segment is dead with `end`: open the next one if the rule
    /// allows it.
    fn ended(&mut self, end: End) {
        self.last_end = Some(end);
        self.current = None;
        if let End::Aborted {
            reason: AbortReason::ProducerGone { what },
        } = end
        {
            self.open(Opening::AfterProducerGone { what });
        }
    }
}

impl<T: Send + 'static, C: Send + 'static> Source<T> for Resume<T, C> {
    fn register<'a>(&'a mut self, sel: &mut Select<'a>) -> usize {
        self.current.as_mut().map_or(0, |src| src.register(sel))
    }
    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        let Some(src) = &mut self.current else {
            return Poll::Dead;
        };
        debug_assert!(
            ordinal < src.leaf_count(),
            "ordinal {ordinal} out of range for resume segment with {} leaves",
            src.leaf_count()
        );
        match src.poll(ordinal) {
            Poll::Got(v) => {
                (self.track)(&v, &mut self.cursor);
                Poll::Got(v)
            }
            Poll::Dead => {
                let end = src.end_reason();
                self.ended(end);
                // Retry rather than Dead: the new segment (if any) has not
                // been registered yet, and an enclosing Chain/Merge must not
                // pop this source while it can still produce.
                match self.current {
                    Some(_) => Poll::Filtered,
                    None => Poll::Dead,
                }
            }
            other => other,
        }
    }
    fn drain_first(&mut self) -> Option<T> {
        loop {
            let src = self.current.as_mut()?;
            if let Some(v) = src.drain_first() {
                (self.track)(&v, &mut self.cursor);
                return Some(v);
            }
            if src.leaf_count() > 0 {
                return None; // alive, just empty
            }
            let end = src.end_reason();
            self.ended(end);
        }
    }
    fn leaf_count(&self) -> usize {
        self.current.as_ref().map_or(0, |src| src.leaf_count())
    }
    fn end_reason(&mut self) -> End {
        match &mut self.current {
            Some(src) => src.end_reason(),
            // The last segment's own verdict is the stream's; with none, the
            // first open was declined, which is still an abort (the stream is
            // infinite, a clean end would claim a finish it never had).
            None => self.last_end.unwrap_or(End::Aborted {
                reason: AbortReason::ProducerGone { what: self.what },
            }),
        }
    }
}
