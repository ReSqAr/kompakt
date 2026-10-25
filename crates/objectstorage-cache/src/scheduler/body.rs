//! Helpers around a body: deadlines, the first chunk, the slot that travels
//! with it, and what is worth a second try.

use crate::{Download, Error};
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::OwnedSemaphorePermit;

/// `f`, or a deadline error after `limit` (none: wait as long as it takes).
pub(super) async fn within<T>(
    limit: Option<Duration>,
    f: impl Future<Output = T>,
    what: &'static str,
) -> Result<T, Error> {
    match limit {
        None => Ok(f.await),
        Some(limit) => tokio::time::timeout(limit, f)
            .await
            .map_err(|_| Error::Deadline(what)),
    }
}

/// Wait for the first chunk (the first byte) and put it back in front.
pub(super) async fn first_chunk(
    attempt: impl Future<Output = Result<Download, Error>>,
) -> Result<Download, Error> {
    let (size, mut body) = attempt.await?;
    let first = body.next().await;
    Ok((size, futures::stream::iter(first).chain(body).boxed()))
}

/// The body, holding the slot until it ends, and failing if it goes `stall`
/// without a chunk.
pub(super) fn guarded(
    body: futures::stream::BoxStream<'static, Result<bytes::Bytes, Error>>,
    permit: OwnedSemaphorePermit,
    stall: Option<Duration>,
) -> futures::stream::BoxStream<'static, Result<bytes::Bytes, Error>> {
    futures::stream::unfold(Some((body, permit)), move |state| async move {
        let (mut body, permit) = state?;
        let next = match stall {
            None => body.next().await,
            Some(limit) => match tokio::time::timeout(limit, body.next()).await {
                Ok(next) => next,
                Err(_) => return Some((Err(Error::Deadline("the next chunk")), None)),
            },
        };
        next.map(|chunk| (chunk, Some((body, permit))))
    })
    .boxed()
}

/// Worth one more try: the store or the disk had a moment. Not a missing
/// object, a refused one, or our own deadline.
pub(super) fn retryable(e: &Error) -> bool {
    match e {
        Error::Store(e) => !matches!(
            e,
            object_store::Error::NotFound { .. }
                | object_store::Error::PermissionDenied { .. }
                | object_store::Error::Unauthenticated { .. }
                | object_store::Error::InvalidPath { .. }
                | object_store::Error::Precondition { .. }
                | object_store::Error::NotModified { .. }
                | object_store::Error::AlreadyExists { .. }
        ),
        Error::Io(_) => true,
        _ => false,
    }
}
