//! The evictor: soft as retention, hard as the in-use budget, and held
//! predictions.

use crate::Error;
use crate::Ticket;
use crate::core::{Core, Limits};
use crate::disk::{Disk, Group, Id, Scan};
use crate::shape::Shape;
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

const GROUP: Group = Group {
    app: "app",
    kind: "kind",
};

fn id(n: u16) -> Id {
    let mut hash = [0u8; 32];
    hash[..2].copy_from_slice(&n.to_be_bytes());
    Id { group: GROUP, hash }
}

fn core_with(dir: &Path, soft: u64, hard: u64, scan: Scan) -> Core {
    let shape = Arc::new(Shape::default());
    shape.add_kind(GROUP.app, GROUP.kind, 1);
    Core::new(
        Disk::claim(dir).unwrap(),
        scan,
        Limits { soft, hard },
        shape,
    )
}

fn core(dir: &Path, soft: u64, hard: u64) -> Core {
    core_with(dir, soft, hard, Scan::default())
}

type Fetched = futures::future::BoxFuture<
    'static,
    Result<
        (
            u64,
            futures::stream::BoxStream<'static, Result<Bytes, Error>>,
        ),
        Error,
    >,
>;

/// A body of `size` bytes.
fn body(size: usize) -> impl FnOnce() -> Fetched + Send + 'static {
    move || {
        async move {
            let chunk: Result<Bytes, Error> = Ok(Bytes::from(vec![7u8; size]));
            Ok((
                u64::try_from(size).unwrap(),
                futures::stream::iter([chunk]).boxed(),
            ))
        }
        .boxed()
    }
}

async fn read(core: &Core, n: u16, size: usize) {
    drop(core.open(id(n), "f", body(size)).await.unwrap());
}

/// The split worker's shape: nothing outlives its last pin.
#[tokio::test]
async fn at_soft_zero_no_file_outlives_its_pin() {
    let dir = tempfile::tempdir().unwrap();
    let core = core(dir.path(), 0, 1000);
    for n in 0..5 {
        read(&core, n, 100).await;
    }
    core.settled().await;
    assert_eq!(core.used(), 0);
    for n in 0..5 {
        assert!(!core.path_of(&id(n)).exists(), "file {n} deleted");
    }
}

/// Many readers at once, each holding its file a moment: what is on disk
/// or in flight never passes hard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hard_is_never_exceeded_under_a_burst() {
    let dir = tempfile::tempdir().unwrap();
    let core = core(dir.path(), 0, 1000);
    let peak = Arc::new(AtomicU64::new(0));
    let watch = {
        let (core, peak) = (core.clone(), peak.clone());
        tokio::spawn(async move {
            loop {
                peak.fetch_max(core.committed(), Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        })
    };
    let readers: Vec<_> = (0..60u16)
        .map(|n| {
            let core = core.clone();
            tokio::spawn(async move {
                let f = core.open(id(n), "f", body(100)).await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
                drop(f);
            })
        })
        .collect();
    for r in readers {
        r.await.unwrap();
    }
    core.settled().await;
    watch.abort();
    assert!(peak.load(Ordering::SeqCst) <= 1000, "{peak:?}");
    assert_eq!(core.used(), 0);
}

/// A room request that arrives while the drain is working through a long
/// backlog is answered before the drain's next batch, not after the backlog.
#[tokio::test]
async fn a_room_request_is_served_before_the_next_drain_batch() {
    let dir = tempfile::tempdir().unwrap();
    // 4,096 files from the start-up scan, soft 0: four batches to drain
    // (the files are not on disk: deleting them fails quietly).
    let files = (0..4096u16).map(|n| (id(n), 1u64, None)).collect();
    let core = core_with(
        dir.path(),
        0,
        1 << 20,
        Scan {
            files,
            ..Scan::default()
        },
    );
    while core.entries() == 4096 {
        tokio::task::yield_now().await;
    }
    core.ask_room(1).await;
    assert!(
        core.entries() >= 4096 - 2 * 1024,
        "at most the batch under way and the next: {} left",
        core.entries()
    );
    core.settled().await;
    assert_eq!(core.entries(), 0);
}

fn predict(core: &Core, n: u16, size: usize, until: SystemTime) {
    core.predict(id(n), "p", until, Arc::new(Ticket::default()), body(size));
}

async fn arrived(core: &Core, n: u16) {
    while !core.contains(&id(n)) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// A prediction is admitted over soft (under hard and its share), the drain
/// passes it by, and room under hard takes it only once nothing else may go.
#[tokio::test]
async fn a_held_prediction_survives_the_drain_and_goes_last_for_room() {
    let dir = tempfile::tempdir().unwrap();
    let core = core(dir.path(), 1000, 1500);
    read(&core, 1, 400).await;
    read(&core, 2, 400).await;
    predict(&core, 3, 300, SystemTime::now() + Duration::from_secs(3600));
    arrived(&core, 3).await;
    core.settled().await;
    assert!(
        !core.contains(&id(1)),
        "the drain takes a recent file instead"
    );
    assert!(core.contains(&id(2)) && core.contains(&id(3)));
    assert_eq!(core.used(), 700);

    // 900 pinned: room for it takes the kept file, not the prediction.
    let big = core.open(id(4), "f", body(900)).await.unwrap();
    assert!(!core.contains(&id(2)) && core.contains(&id(3)));
    // Nothing else may go: now the prediction does.
    read(&core, 5, 400).await;
    assert!(
        !core.contains(&id(3)),
        "held predictions go last, but they go"
    );
    drop(big);
}

/// At the tick after its deadline, an unused prediction is an ordinary file
/// again — the oldest one, since it was never used.
#[tokio::test]
async fn an_expired_prediction_is_an_ordinary_file_after_the_tick() {
    let dir = tempfile::tempdir().unwrap();
    let core = core(dir.path(), 500, 2000);
    read(&core, 1, 400).await;
    predict(&core, 2, 200, SystemTime::now() + Duration::from_millis(50));
    arrived(&core, 2).await;
    core.settled().await;
    assert!(!core.contains(&id(1)) && core.contains(&id(2)), "held");

    read(&core, 3, 400).await;
    core.settled().await;
    assert!(!core.contains(&id(3)), "still held: the kept file goes");
    assert!(core.contains(&id(2)));

    tokio::time::sleep(Duration::from_millis(60)).await;
    core.tick_now().await;
    read(&core, 3, 400).await;
    core.settled().await;
    assert!(!core.contains(&id(2)), "expired: the least recently used");
    assert!(core.contains(&id(3)));
}
