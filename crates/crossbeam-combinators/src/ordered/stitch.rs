//! `stitch`: fill the gaps of a strictly increasing stream from a fallback.

use std::marker::PhantomData;

use super::{Contiguous, Key, OrderedChannel, StrictlyIncreasing, Successor};
use crate::channel::{AbortReason, Channel, End, Poll, Source};
use crate::kind::{Finite, Kind};

/// A gap in an ordered stream: everything in `(after, got)` was absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gap<K> {
    /// The last position delivered before the gap (`None`: none yet).
    pub after: Option<K>,
    /// The first position past the gap.
    pub got: K,
}

impl<T, Pos, K> OrderedChannel<T, StrictlyIncreasing<Pos>, K>
where
    T: Send + 'static,
    Pos: Key<T> + Successor,
    K: Kind,
{
    /// Fill forward gaps from `fallback`, strengthening `StrictlyIncreasing` to
    /// `Contiguous`.
    ///
    /// The fallback covers `(gap.after, gap.got)`; the primary item at
    /// `gap.got` is buffered and emitted after the fallback. A short, overlong,
    /// gapped, or aborted fallback fails the stitched stream.
    #[must_use]
    pub fn stitch<F>(self, fallback: F) -> OrderedChannel<T, Contiguous<Pos>, K>
    where
        F: FnMut(Gap<Pos>) -> Channel<T, Finite> + Send + 'static,
    {
        let stitched = StitchSource {
            primary: Some(self.inner.source),
            fallback,
            fallback_source: None,
            pending: None,
            after: self.after,
            last: None,
            fallback_last: None,
            gap_next: None,
            gap_got: None,
            done: None,
            _key: PhantomData,
        };
        let inner = Channel::wrap(Box::new(stitched));
        // `StitchSource` already returns a gapless sequence. The outer
        // Contiguous check is deliberate defence-in-depth: do not remove it as
        // "redundant" without preserving the same guarantee another way.
        OrderedChannel::<T, Contiguous<Pos>, K>::named(inner, self.after, self.what)
    }
}

pub(super) struct StitchSource<T, K, F>
where
    K: Key<T> + Successor,
{
    primary: Option<Box<dyn Source<T>>>,
    fallback: F,
    fallback_source: Option<Box<dyn Source<T>>>,
    pending: Option<T>,
    after: Option<K>,
    last: Option<K>,
    fallback_last: Option<K>,
    /// The next key the active fallback owes.
    gap_next: Option<K>,
    gap_got: Option<K>,
    done: Option<End>,
    _key: PhantomData<K>,
}

pub(super) enum PrimaryStep<T> {
    Emit(T),
    Gap,
    Failed,
}

impl<T, K, F> StitchSource<T, K, F>
where
    T: Send + 'static,
    K: Key<T> + Successor,
    F: FnMut(Gap<K>) -> Channel<T, Finite> + Send + 'static,
{
    fn expected(&self) -> K {
        K::next_after(self.last.or(self.after))
    }

    fn fail(&mut self, what: &'static str) {
        self.done = Some(End::Aborted {
            reason: AbortReason::OrderViolation { what },
        });
        self.primary = None;
        self.fallback_source = None;
        self.pending = None;
    }

    fn primary_item(&mut self, item: T) -> PrimaryStep<T> {
        let Some(got) = K::key(&item) else {
            return PrimaryStep::Emit(item);
        };
        let expected = self.expected();
        if got < expected {
            self.fail("stitch primary");
            return PrimaryStep::Failed;
        }
        if got > expected {
            let after = self.last.or(self.after);
            self.pending = Some(item);
            self.gap_next = Some(expected);
            self.gap_got = Some(got);
            self.fallback_last = None;
            self.fallback_source = Some((self.fallback)(Gap { after, got }).source);
            return PrimaryStep::Gap;
        }
        self.last = Some(got);
        PrimaryStep::Emit(item)
    }

    fn fallback_item(&mut self, item: T) -> Result<T, ()> {
        let Some(got) = K::key(&item) else {
            // No-key items (errors, watermarks) pass through; they do not
            // advance coverage, so the endpoint check still has to pass.
            return Ok(item);
        };
        if self.gap_got.is_some_and(|end| got >= end) || Some(got) != self.gap_next {
            self.fail("stitch fallback");
            return Err(());
        }
        self.fallback_last = Some(got);
        self.gap_next = Some(K::next_after(Some(got)));
        Ok(item)
    }

    fn finish_fallback(&mut self) -> Option<T> {
        let Some(got) = self.gap_got else {
            self.fail("stitch fallback");
            return None;
        };
        let covered = self
            .fallback_last
            .is_some_and(|last| K::next_after(Some(last)) == got);
        if !covered {
            self.fail("stitch fallback");
            return None;
        }
        let item = self.pending.take();
        if item.is_none() {
            self.fail("stitch fallback");
            return None;
        }
        self.last = Some(got);
        self.gap_next = None;
        self.gap_got = None;
        self.fallback_last = None;
        self.fallback_source = None;
        item
    }

    fn primary_end(&mut self, end: End) -> Poll<T> {
        self.done = Some(end);
        self.primary = None;
        self.pending = None;
        Poll::Dead
    }

    fn fallback_end(&mut self, end: End) -> Poll<T> {
        match end {
            End::Complete => match self.finish_fallback() {
                Some(item) => Poll::Got(item),
                None => Poll::Dead,
            },
            End::Aborted { .. } => {
                self.done = Some(end);
                self.primary = None;
                self.fallback_source = None;
                self.pending = None;
                Poll::Dead
            }
        }
    }
}

impl<T, K, F> Source<T> for StitchSource<T, K, F>
where
    T: Send + 'static,
    K: Key<T> + Successor,
    F: FnMut(Gap<K>) -> Channel<T, Finite> + Send + 'static,
{
    fn register<'a>(&'a mut self, sel: &mut crossbeam_channel::Select<'a>) -> usize {
        if let Some(fallback) = self.fallback_source.as_mut() {
            fallback.register(sel)
        } else {
            self.primary.as_mut().map_or(0, |src| src.register(sel))
        }
    }

    fn poll(&mut self, ordinal: usize) -> Poll<T> {
        if let Some(mut fallback) = self.fallback_source.take() {
            let poll = match fallback.poll(ordinal) {
                Poll::Got(item) => match self.fallback_item(item) {
                    Ok(item) => {
                        self.fallback_source = Some(fallback);
                        Poll::Got(item)
                    }
                    Err(()) => Poll::Dead,
                },
                Poll::Filtered => {
                    self.fallback_source = Some(fallback);
                    Poll::Filtered
                }
                Poll::Empty => {
                    self.fallback_source = Some(fallback);
                    Poll::Empty
                }
                Poll::Dead => {
                    let end = fallback.end_reason();
                    self.fallback_end(end)
                }
            };
            return poll;
        }

        let Some(mut primary) = self.primary.take() else {
            return Poll::Dead;
        };
        match primary.poll(ordinal) {
            Poll::Got(item) => match self.primary_item(item) {
                PrimaryStep::Emit(item) => {
                    self.primary = Some(primary);
                    Poll::Got(item)
                }
                PrimaryStep::Gap => {
                    self.primary = Some(primary);
                    // A fallback can be born with no live leaves, e.g.
                    // `chain_all([])`. `recv` would then see leaf_count 0 and
                    // read the empty fallback's Complete as the stream's end,
                    // silently dropping the buffered item. Close it through the
                    // same path as a drained fallback so coverage is checked.
                    let Some(mut fallback) = self.fallback_source.take() else {
                        return Poll::Filtered;
                    };
                    if fallback.leaf_count() == 0 {
                        let end = fallback.end_reason();
                        self.fallback_end(end)
                    } else {
                        self.fallback_source = Some(fallback);
                        Poll::Filtered
                    }
                }
                PrimaryStep::Failed => Poll::Dead,
            },
            Poll::Filtered => {
                self.primary = Some(primary);
                Poll::Filtered
            }
            Poll::Empty => {
                self.primary = Some(primary);
                Poll::Empty
            }
            Poll::Dead => {
                let end = primary.end_reason();
                self.primary_end(end)
            }
        }
    }

    fn drain_first(&mut self) -> Option<T> {
        loop {
            if self.fallback_source.is_some() {
                let mut fallback = self.fallback_source.take()?;
                match fallback.drain_first() {
                    Some(item) => match self.fallback_item(item) {
                        Ok(item) => {
                            self.fallback_source = Some(fallback);
                            return Some(item);
                        }
                        Err(()) => return None,
                    },
                    None => {
                        if fallback.leaf_count() == 0 {
                            let end = fallback.end_reason();
                            return match self.fallback_end(end) {
                                Poll::Got(item) => Some(item),
                                _ => None,
                            };
                        }
                        self.fallback_source = Some(fallback);
                        return None;
                    }
                }
            }

            let mut primary = self.primary.take()?;
            match primary.drain_first() {
                Some(item) => match self.primary_item(item) {
                    PrimaryStep::Emit(item) => {
                        self.primary = Some(primary);
                        return Some(item);
                    }
                    PrimaryStep::Gap => {
                        self.primary = Some(primary);
                        // The fallback is now active; drain it on the next
                        // iteration rather than polling the primary past the
                        // buffered gap-triggering item. If it has no live
                        // leaves, close it now so its Complete cannot be read
                        // as the stitched stream's end.
                        let Some(mut fallback) = self.fallback_source.take() else {
                            continue;
                        };
                        if fallback.leaf_count() == 0 {
                            let end = fallback.end_reason();
                            return match self.fallback_end(end) {
                                Poll::Got(item) => Some(item),
                                _ => None,
                            };
                        }
                        self.fallback_source = Some(fallback);
                        continue;
                    }
                    PrimaryStep::Failed => return None,
                },
                None => {
                    if primary.leaf_count() == 0 {
                        let end = primary.end_reason();
                        self.primary_end(end);
                        return None;
                    }
                    self.primary = Some(primary);
                    return None;
                }
            }
        }
    }

    fn leaf_count(&self) -> usize {
        self.fallback_source
            .as_ref()
            .or(self.primary.as_ref())
            .map_or(0, |src| src.leaf_count())
    }

    fn end_reason(&mut self) -> End {
        match self.done {
            Some(end) => end,
            None => self
                .fallback_source
                .as_mut()
                .or(self.primary.as_mut())
                .map_or(End::Complete, |src| src.end_reason()),
        }
    }
}
