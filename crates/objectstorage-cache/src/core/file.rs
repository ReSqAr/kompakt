//! What a reader gets: a complete cached file (pinned), or one that is still
//! arriving, followed as it is written.

use super::Inner;
use crate::disk::Id;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;

/// An object as a stream: complete in the cache, or still arriving.
pub enum Streamed {
    Complete(CachedFile),
    /// Reading a `.part` file while the download writes it. The descriptor
    /// stays valid across the rename (and any later eviction).
    Following {
        file: tokio::fs::File,
        progress: watch::Receiver<(u64, u64)>,
    },
}

impl Streamed {
    /// The total size, as announced by the store.
    #[must_use]
    pub fn len(&self) -> u64 {
        match self {
            Streamed::Complete(f) => f.len(),
            Streamed::Following { progress, .. } => progress.borrow().0,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes `start..end` (MVP design §9); a complete file stays pinned until
    /// the last of them is read. A download in progress is read
    /// from `start` at once: bytes already written go out immediately, and
    /// only the ones not yet written are waited for.
    pub async fn into_range(
        self,
        start: u64,
        end: u64,
    ) -> std::io::Result<BoxStream<'static, std::io::Result<Bytes>>> {
        use tokio::io::AsyncSeekExt;
        match self {
            Streamed::Complete(pinned) => {
                let mut f = tokio::fs::File::open(pinned.path()).await?;
                f.seek(std::io::SeekFrom::Start(start)).await?;
                Ok(follow(f, None, start, end)
                    .map(move |c| {
                        let _pinned = &pinned;
                        c
                    })
                    .boxed())
            }
            Streamed::Following { mut file, progress } => {
                file.seek(std::io::SeekFrom::Start(start)).await?;
                Ok(follow(file, Some(progress), start, end))
            }
        }
    }
}

/// Read `file` from `start` to `end` (exclusive; the file is positioned at
/// `start`). With `progress`, a writer is still filling it: read up to what it
/// has published, then wait for more. Without, the file is complete.
fn follow(
    file: tokio::fs::File,
    progress: Option<watch::Receiver<(u64, u64)>>,
    start: u64,
    end: u64,
) -> BoxStream<'static, std::io::Result<Bytes>> {
    use tokio::io::AsyncReadExt;
    const CHUNK: u64 = 64 * 1024;
    let closed = progress.is_none();
    futures::stream::unfold(
        (file, progress, start, closed),
        move |(mut file, mut progress, at, closed)| async move {
            loop {
                if at >= end {
                    return None;
                }
                let written = match progress.as_mut() {
                    Some(p) => p.borrow_and_update().1,
                    None => end,
                };
                if at < written || closed {
                    let want = end.saturating_sub(at).min(CHUNK);
                    let mut buf = vec![0u8; usize::try_from(want).unwrap_or(0)];
                    return match file.read(&mut buf).await {
                        // Short of `end`: not written yet — or, once the
                        // download has ended, never: a truncated file.
                        Ok(0) if !closed => {
                            let ended = match progress.as_mut() {
                                Some(p) => p.changed().await.is_err(),
                                None => true,
                            };
                            Some((Ok(Bytes::new()), (file, progress, at, ended)))
                        }
                        Ok(0) => Some((
                            Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!("the file ended at byte {at} of {end}"),
                            )),
                            (file, progress, end, true),
                        )),
                        Ok(n) => {
                            buf.truncate(n);
                            let at = at.saturating_add(u64::try_from(n).unwrap_or(0));
                            Some((Ok(Bytes::from(buf)), (file, progress, at, closed)))
                        }
                        Err(e) => Some((Err(e), (file, progress, end, true))),
                    };
                }
                // Nothing new yet: wait for the writer. A closed channel means
                // the download ended: read whatever is on disk to its end.
                let ended = match progress.as_mut() {
                    Some(p) => p.changed().await.is_err(),
                    None => true,
                };
                if ended {
                    return Some((Ok(Bytes::new()), (file, progress, at, true)));
                }
            }
        },
    )
    .boxed()
}

/// A cached object on local disk, pinned for as long as this value lives.
pub struct CachedFile {
    pub(super) path: PathBuf,
    pub(super) len: u64,
    pub(super) _pin: Pin,
}

impl CachedFile {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

pub(super) struct Pin {
    pub(super) inner: Arc<Inner>,
    pub(super) id: Id,
}

impl Drop for Pin {
    fn drop(&mut self) {
        let over = {
            let mut st = self.inner.state.lock();
            st.update(&self.id, |e| e.pins = e.pins.saturating_sub(1));
            st.over_soft()
        };
        self.inner.room.notify_waiters();
        self.inner.wake_if(over);
    }
}
