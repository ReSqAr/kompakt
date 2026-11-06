//! The read side: `recv`, `try_recv`, `end_reason` and `ready_batch`.

use crossbeam_channel::Select;

use super::{Channel, End, Poll};
use crate::kind::Kind;

impl<T: Send + 'static, K: Kind> Channel<T, K> {
    /// Block for the next item; `Err` is the stream's verdict once it ended.
    pub fn recv(&mut self) -> Result<T, K::End> {
        self.recv_end().map_err(K::end)
    }

    /// `recv` with the internal verdict, before the kind narrows it.
    pub(crate) fn recv_end(&mut self) -> Result<T, End> {
        loop {
            let leaf_count = self.source.leaf_count();
            if leaf_count == 0 {
                return Err(self.source.end_reason());
            }
            let mut sel = Select::new();
            let registered = self.source.register(&mut sel);
            debug_assert_eq!(
                registered, leaf_count,
                "register pushed {registered} ops but leaf_count() is {leaf_count}"
            );
            let idx = sel.ready();
            debug_assert!(
                idx < registered,
                "ready index {idx} exceeds {registered} registered operations"
            );
            match self.source.poll(idx) {
                Poll::Got(v) => return Ok(v),
                Poll::Filtered | Poll::Empty => continue,
                Poll::Dead => {
                    debug_assert_eq!(
                        self.source.leaf_count(),
                        0,
                        "a Dead source still counts leaves; recv would spin"
                    );
                    continue;
                }
            }
        }
    }

    pub fn try_recv(&mut self) -> Option<T> {
        self.source.drain_first()
    }

    /// Why the stream finished, for a stream already known to have stopped.
    /// `recv` and `ready_batch` return the same verdict in their `Err`.
    pub fn end_reason(&mut self) -> K::End {
        K::end(self.source.end_reason())
    }

    /// Block for the first item, then non-blocking drain up to `max` items.
    /// `Err` carries the verdict when the stream has ended, exactly as
    /// [`recv`](Self::recv) does; the returned iterator yields at least one
    /// item. Each subsequent `next()` calls `try_recv()`, so items that arrive
    /// while you iterate are still captured.
    pub fn ready_batch(&mut self, max: usize) -> Result<ReadyBatch<'_, T, K>, K::End> {
        let first = self.recv()?;
        Ok(ReadyBatch {
            channel: self,
            head: Some(first),
            remaining: max.saturating_sub(1),
        })
    }
}

pub struct ReadyBatch<'a, T, K> {
    channel: &'a mut Channel<T, K>,
    head: Option<T>,
    remaining: usize,
}

impl<T: Send + 'static, K: Kind> Iterator for ReadyBatch<'_, T, K> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.head.take().or_else(|| {
            if self.remaining == 0 {
                return None;
            }
            self.remaining = self.remaining.saturating_sub(1);
            self.channel.try_recv()
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self
            .remaining
            .saturating_add(if self.head.is_some() { 1 } else { 0 });
        (0, Some(n))
    }
}
