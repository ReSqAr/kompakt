//! Quorum reads, read-your-own-write, and the node clock.

use super::*;

/// `hlc_now` reads the node's clock, the one its writes are minted from: it
/// is above every HLC that clock issued before the call.
#[tokio::test]
async fn hlc_now_is_above_every_hlc_the_node_issued_before() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stable_log = Arc::new(FakeStableLog::new());
    let local_store = crate::testing::local_store_at(dir.path().join("local.stable"))
        .await
        .expect("from_db");
    let clock = test_clock();
    let debouncer = Arc::new(QuorumReadDebouncer::for_test(
        Arc::new(NoOpCheck),
        Duration::ZERO,
        Duration::from_secs(5),
    ));
    let archive: Arc<dyn zslg_log::StableArchiveReader> = Arc::new(ObjectStoreArchiveReader::new(
        Arc::new(ScriptedSnapshotStore::default()),
        Arc::new(RaftStateHandle::default()),
    ));
    let reader = ReaderImpl::new(
        local_store,
        stable_log,
        archive,
        debouncer,
        Cancellation::never(),
        clock.clone(),
    );

    let minted: Vec<Hlc> = (0..100).map(|_| clock.now()).collect();
    let now = reader.hlc_now();
    assert!(
        minted.iter().all(|h| *h < now),
        "hlc_now is above every earlier HLC"
    );
}

#[tokio::test]
async fn quorum_read_returns_for_local_check() {
    let dir = tempfile::tempdir().expect("tempdir");
    let local_path = dir.path().join("local.db");
    let stable_log = Arc::new(FakeStableLog::new());
    let local_log = crate::testing::local_store_at(local_path.with_extension("stable"))
        .await
        .expect("from_db");

    let reader = local_reader(local_log, stable_log, Duration::from_millis(5));
    reader.sync().await.expect("ok");
}

#[tokio::test]
async fn read_your_own_write_via_quorum_then_unstable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let local_path = dir.path().join("local.db");
    let stable_log = Arc::new(FakeStableLog::new());
    let local_log = crate::testing::local_store_at(local_path.with_extension("stable"))
        .await
        .expect("from_db");

    let d = make_data(1, 100, b"hi");
    integrate_batch(&local_log, upstream(1), vec![(LogIndex(0), d.clone())])
        .await
        .expect("integrate");

    let reader = local_reader(
        local_log.clone(),
        stable_log.clone(),
        Duration::from_millis(5),
    );
    reader.sync().await.expect("quorum");
    let mut stream = reader.unstable_tail().into_test_stream();
    let first = stream.next().await.expect("item").expect("ok");
    assert_eq!(first, d);
}
