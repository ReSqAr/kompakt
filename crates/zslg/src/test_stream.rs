//! Test-only reads of a channel, for tests that only look at items.
//!
//! The crate has no lossy reads (no `IntoIterator`, no async `Stream` bridge):
//! production code reads with `recv`/`ready_batch`, which return the verdict.
//! Tests that only compare items use these instead. `into_test_stream` keeps
//! the old bridge's liveness guard — the drain is bound to a cancellation the
//! stream owns — so dropping a stream on a quiet infinite channel cannot leave a
//! blocking thread parked in `recv` and hang runtime shutdown.

use crossbeam_combinators::{CancelHandle, Channel, Key, OrderPolicy, OrderedChannel};
use futures::StreamExt;
use futures::stream::BoxStream;

pub(crate) trait ChannelTestExt<T> {
    /// The items until the stream ends, verdict ignored.
    fn items(self) -> std::vec::IntoIter<T>;
    /// The items as an async stream, drained on the blocking pool.
    fn into_test_stream(self) -> BoxStream<'static, T>;
}

impl<T: Send + 'static> ChannelTestExt<T> for Channel<T> {
    fn items(mut self) -> std::vec::IntoIter<T> {
        std::iter::from_fn(|| self.recv().ok())
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn into_test_stream(self) -> BoxStream<'static, T> {
        let (handle, cancellation) = CancelHandle::new();
        let mut ch = self.until(&cancellation);
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        tokio::task::spawn_blocking(move || {
            while let Ok(item) = ch.recv() {
                if tx.blocking_send(item).is_err() {
                    break;
                }
            }
        });
        // The handle rides with the stream: dropping the stream drops it, and
        // the drain's `until` ends.
        futures::stream::unfold((rx, handle), |(mut rx, handle)| async move {
            rx.recv().await.map(|item| (item, (rx, handle)))
        })
        .boxed()
    }
}

impl<T: Send + 'static, P> ChannelTestExt<T> for OrderedChannel<T, P>
where
    P: OrderPolicy,
    P::Key: Key<T>,
{
    fn items(self) -> std::vec::IntoIter<T> {
        self.into_channel().items()
    }

    fn into_test_stream(self) -> BoxStream<'static, T> {
        self.into_channel().into_test_stream()
    }
}
