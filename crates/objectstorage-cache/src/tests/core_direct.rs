//! The core with bodies the test controls: cancellation, failure, following.

use crate::Error;
use crate::core::{Core, Limits, Streamed};
use crate::disk::{Disk, Group, Id, Scan};
use crate::shape::Shape;
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const GROUP: Group = Group {
    app: "app",
    kind: "kind",
};

fn id(n: u8) -> Id {
    Id {
        group: GROUP,
        hash: [n; 32],
    }
}

fn core(dir: &Path, soft: u64, hard: u64) -> Core {
    let shape = Arc::new(Shape::default());
    shape.add_kind(GROUP.app, GROUP.kind, 1);
    Core::new(
        Disk::claim(dir).unwrap(),
        Scan::default(),
        Limits { soft, hard },
        shape,
    )
}

type Body = futures::stream::BoxStream<'static, Result<Bytes, Error>>;

/// Three 1000-byte chunks; the last two only when the test says so.
fn gated() -> (tokio::sync::mpsc::Sender<()>, Body) {
    let (release, gate) = tokio::sync::mpsc::channel::<()>(8);
    let slow = futures::stream::unfold((0u8, gate), |(i, mut gate)| async move {
        if i >= 3 {
            return None;
        }
        if i > 0 {
            gate.recv().await?;
        }
        Some((Ok(Bytes::from(vec![i; 1000])), (i + 1, gate)))
    })
    .boxed();
    (release, slow)
}

/// A caller that goes away mid-download (a browser cancelling an image)
/// must not take the download with it: the next caller gets the file.
#[tokio::test]
async fn a_cancelled_caller_does_not_break_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let core = core(dir.path(), 1000, 2000);
    let (tx, rx) = futures::channel::mpsc::unbounded::<Result<Bytes, Error>>();
    let first = {
        let core = core.clone();
        tokio::spawn(async move {
            core.open(id(1), "a", move || {
                async move { Ok((100u64, rx.boxed())) }.boxed()
            })
            .await
            .map(|f| f.len())
        })
    };
    tx.unbounded_send(Ok(Bytes::from(vec![7u8; 40]))).unwrap();
    while core.downloads() == 0 {
        tokio::task::yield_now().await;
    }
    first.abort();
    let _ = first.await;
    tx.unbounded_send(Ok(Bytes::from(vec![7u8; 60]))).unwrap();
    drop(tx);

    let again = core
        .open(id(1), "a", || {
            async { Err(Error::NotFound("a second download".into())) }.boxed()
        })
        .await
        .unwrap();
    assert_eq!(again.len(), 100);
    assert_eq!(core.downloads(), 1);
}

#[tokio::test]
async fn a_reader_follows_a_download_while_it_arrives() {
    let dir = tempfile::tempdir().unwrap();
    let core = core(dir.path(), 10_000, 20_000);
    let (release, slow) = gated();
    let streamed = core
        .stream(id(1), "video.mp4", move || {
            async move { Ok((3000u64, slow)) }.boxed()
        })
        .await
        .unwrap();
    assert_eq!(streamed.len(), 3000);
    let mut body = streamed.into_range(0, 3000).await.unwrap();

    let first = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await
        .expect("the first bytes must not wait for the whole download")
        .unwrap()
        .unwrap();
    assert!(!first.is_empty());

    release.send(()).await.unwrap();
    release.send(()).await.unwrap();
    let mut all = first.to_vec();
    while let Some(chunk) = body.next().await {
        all.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(all.len(), 3000);
    assert!(all[..1000].iter().all(|b| *b == 0) && all[2000..].iter().all(|b| *b == 2));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(core.contains(&id(1)), "and the cache kept it");
}

/// A range of a download in progress: bytes already written are served at
/// once, and only the rest waits.
#[tokio::test]
async fn a_range_of_a_growing_file_serves_what_is_written_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let core = core(dir.path(), 10_000, 20_000);
    let (release, slow) = gated();
    let streamed = core
        .stream(id(2), "clip.mp4", move || {
            async move { Ok((3000u64, slow)) }.boxed()
        })
        .await
        .unwrap();
    assert!(matches!(
        streamed,
        Streamed::Following { .. } | Streamed::Complete(_)
    ));
    // 500..1500: half of chunk 0 (written) and half of chunk 1 (not yet).
    let mut body = streamed.into_range(500, 1500).await.unwrap();
    let mut got = Vec::new();
    while got.len() < 500 {
        let chunk = tokio::time::timeout(Duration::from_secs(2), body.next())
            .await
            .expect("written bytes must not wait")
            .unwrap()
            .unwrap();
        got.extend_from_slice(&chunk);
    }
    assert!(got.iter().all(|b| *b == 0));
    release.send(()).await.unwrap();
    release.send(()).await.unwrap();
    while let Some(chunk) = body.next().await {
        got.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(got.len(), 1000, "stops at the end of the range");
    assert!(got[..500].iter().all(|b| *b == 0) && got[500..].iter().all(|b| *b == 1));
}

/// A download that fails — at once, or joined in flight — fails the stream.
#[tokio::test]
async fn a_failed_download_fails_the_stream() {
    let dir = tempfile::tempdir().unwrap();
    let core = core(dir.path(), 10_000, 20_000);
    let fail = || async { Err(Error::Unavailable) }.boxed();
    for i in 0..20u8 {
        let r = tokio::time::timeout(
            Duration::from_secs(2),
            core.stream(id(10 + i), "missing", fail),
        )
        .await
        .expect("the stream returns");
        assert!(r.is_err(), "{i}");
    }

    // Joined: a demand download is in flight (slow, then failing) when the
    // stream asks for the same file.
    let (go, gate) = tokio::sync::oneshot::channel::<()>();
    let first = {
        let core = core.clone();
        tokio::spawn(async move {
            core.open(id(99), "joined", move || {
                async move {
                    let _ = gate.await;
                    Err(Error::Unavailable)
                }
                .boxed()
            })
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let streamed = {
        let core = core.clone();
        tokio::spawn(async move { core.stream(id(99), "joined", fail).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = go.send(());
    assert!(first.await.unwrap().is_err());
    let r = tokio::time::timeout(Duration::from_secs(2), streamed)
        .await
        .expect("the joined stream returns")
        .unwrap();
    assert!(r.is_err(), "the joined download's failure is the stream's");
}
