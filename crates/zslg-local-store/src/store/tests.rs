//! Local-store tests.

use crossbeam_combinators::testing::ChannelTestExt as _;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use tempfile::TempDir;

use super::flush::flush_if_dirty;
use super::*;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use zslg_log::LogIndex;
use zslg_log::{LocalStoreMut, LocalStoreView};

use zslg_consensus::NodeId;
use zslg_log::DataId;
use zslg_time::Hlc;

use zslg_consensus_fake::upstream;
use zslg_log_fake::make_data;

async fn integrate_batch(
    store: &LocalStore,
    source: NodeId,
    records: Vec<(LogIndex, Data)>,
) -> Result<(), crate::Error> {
    if records.is_empty() {
        return Ok(());
    }
    let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
    for (idx, rec) in records {
        let _ = tx.send(Ok((source, idx, rec)));
    }
    let (ch, closer) = crossbeam_combinators::Channel::from(rx, "test integrate");
    // Close before the last sender drops, or the consumer forms its
    // verdict as "disconnected, not yet closed" — an abort.
    closer.close();
    drop(tx);
    store.integrate_loop(ch).await.map_err(Into::into)
}

/// One `Storage` backing both this store and the stable log it reads the
/// watermark from — the production shape.
async fn shared_storage(root: &Path) -> (Arc<Storage>, Arc<dyn StableLogSnapshot>) {
    let storage = crate::testutil::open_storage(root).expect("open storage");
    let stable_log: Arc<dyn StableLogSnapshot> = Arc::new(zslg_log_fake::FakeStableLog::new());
    (storage, stable_log)
}

async fn fresh_store() -> (TempDir, Arc<LocalStore>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.db");
    let store = crate::testutil::simple_at(path.with_extension("stable"))
        .await
        .expect("from_db");
    (dir, store)
}

/// A store whose integration worker can be drained — required before
/// reopening the same root (see [`super::open_test_local_store`]).
async fn store_at(path: &Path) -> (Arc<LocalStore>, crate::testutil::TestWorker) {
    crate::testutil::open_test_local_store(path)
        .await
        .expect("open")
}

/// Integrate a single full body via the batch API.
async fn put_full(store: &LocalStore, source: NodeId, index: LogIndex, data: Data) {
    integrate_batch(store, source, vec![(index, data)])
        .await
        .expect("integrate_batch");
}

/// The integration signal is **level-triggered**: it says "state moved, go
/// look", so a waiter that was not draining while a burst landed must still
/// be woken, and woken without an error it has to interpret. An
/// edge-triggered channel reports a lag here instead, and the guarantee then
/// rests on every consumer remembering to treat that lag as a wakeup —
/// which is exactly the invariant `apply_post_fence` calls load-bearing
/// (`sync_quorum`'s last-job-cleared edge).
#[tokio::test]
async fn the_integration_signal_survives_a_burst_nobody_drained() {
    let (_dir, store) = fresh_store().await;
    let mut integrated = store.subscribe_integrated();

    // Far more integrations than a buffered channel would hold, with no
    // consumer draining any of them.
    for i in 0..64u64 {
        let seed = u8::try_from(i % 251).expect("fits");
        put_full(
            &store,
            upstream(1),
            LogIndex(i),
            make_data(seed, 100 + i, b"x"),
        )
        .await;
    }

    tokio::time::timeout(Duration::from_secs(5), integrated.changed())
        .await
        .expect("a waiter must be woken by the burst")
        .expect("waking must not be an error the caller has to decode");
    assert_eq!(
        store
            .subscription_cursor(upstream(1))
            .await
            .expect("cursor"),
        Some(LogIndex(63)),
        "and the state it points at is the state after the burst"
    );
}

#[tokio::test]
async fn integrate_full_writes_one_row() {
    let (_dir, store) = fresh_store().await;
    let d = make_data(1, 100, b"hello");
    put_full(&store, upstream(1), LogIndex(7), d.clone()).await;
    // Verify the data is present via stream.
    use futures::TryStreamExt as _;
    let all: Vec<Data> = store
        .stream_above_hlc(None)
        .into_test_stream()
        .try_collect()
        .await
        .expect("ok");
    assert!(all.iter().any(|item| item.data_id == d.data_id));
    assert_eq!(
        store.subscription_cursor(upstream(1)).await.expect("ok"),
        Some(LogIndex(7))
    );
}

/// A store over a stable log already at `watermark`, with its own config.
async fn store_with(
    dir: &Path,
    config: &crate::LocalStoreConfig,
    watermark: Option<Hlc>,
) -> Arc<LocalStore> {
    let (storage, stable_log) = shared_storage(dir).await;
    if let Some(hlc) = watermark {
        stable_log
            .record_end_hlc(hlc)
            .await
            .expect("record_end_hlc");
    }
    let (store, integrate_rx) = create_local_store(config, storage, stable_log)
        .await
        .expect("create");
    tokio::spawn(run_integrate_worker(
        Arc::downgrade(&store),
        integrate_rx,
        CancellationToken::new(),
    ));
    store
}

/// Only a first-time integration is news: the copy of the same write that
/// arrives from a second replica is deduped and not announced again.
#[tokio::test]
async fn unstable_subscribers_see_first_time_integrations_only() {
    let (_dir, store) = fresh_store().await;
    let mut live = store.subscribe_unstable();

    let d = make_data(1, 100, b"d");
    let e = make_data(2, 200, b"e");
    put_full(&store, upstream(1), LogIndex(0), d.clone()).await;
    put_full(&store, upstream(2), LogIndex(0), d.clone()).await;
    integrate_batch(
        &store,
        upstream(1),
        vec![(LogIndex(1), e.clone()), (LogIndex(2), e.clone())],
    )
    .await
    .expect("integrate");

    assert_eq!(live.recv().expect("d"), d);
    assert_eq!(live.recv().expect("e"), e);
    assert_eq!(live.try_recv(), None, "deduped copies are not published");
    assert_eq!(store.skipped_duplicates(), 2);
}

/// An entry at or below the stable watermark belongs to the stable log;
/// announcing it would duplicate the stable half (CLAUDE.md §12).
#[tokio::test]
async fn entries_at_or_below_the_watermark_are_not_published() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = crate::LocalStoreConfig::for_test();
    let store = store_with(dir.path(), &config, Some(Hlc(100))).await;
    let mut live = store.subscribe_unstable();

    let fresh = make_data(2, 101, b"fresh");
    integrate_batch(
        &store,
        upstream(1),
        vec![
            (LogIndex(0), make_data(1, 100, b"settled")),
            (LogIndex(1), fresh.clone()),
        ],
    )
    .await
    .expect("integrate");

    assert_eq!(live.recv().expect("fresh"), fresh);
    assert_eq!(live.try_recv(), None);
}

#[tokio::test]
async fn integrate_skips_full_record_at_or_below_end_hlc() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (storage, stable_log) = shared_storage(dir.path()).await;
    stable_log
        .record_end_hlc(Hlc(100))
        .await
        .expect("record_end_hlc");
    let (store, integrate_rx) =
        create_local_store(&crate::LocalStoreConfig::for_test(), storage, stable_log)
            .await
            .expect("create");
    tokio::spawn(run_integrate_worker(
        Arc::downgrade(&store),
        integrate_rx,
        CancellationToken::new(),
    ));

    // At/below end_hlc → already stable → skipped (not written), but the
    // cursor still advances so it is never re-pulled from upstream.
    let stale = make_data(1, 100, b"stale");
    put_full(&store, upstream(1), LogIndex(5), stale.clone()).await;
    assert_eq!(
        store.subscription_cursor(upstream(1)).await.expect("ok"),
        Some(LogIndex(5)),
        "cursor must advance over skipped records"
    );

    // Strictly above end_hlc → integrated as normal.
    let fresh = make_data(1, 101, b"fresh");
    put_full(&store, upstream(1), LogIndex(6), fresh.clone()).await;
    use futures::TryStreamExt as _;
    let all: Vec<Data> = store
        .stream_above_hlc(None)
        .into_test_stream()
        .try_collect()
        .await
        .expect("ok");
    assert!(
        all.iter().any(|item| item.data_id == fresh.data_id),
        "record above end_hlc must be integrated"
    );
    assert_eq!(
        store.subscription_cursor(upstream(1)).await.expect("ok"),
        Some(LogIndex(6))
    );
}

#[tokio::test]
async fn integrate_skips_meta_only_at_or_below_end_hlc() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (storage, stable_log) = shared_storage(dir.path()).await;
    stable_log
        .record_end_hlc(Hlc(200))
        .await
        .expect("record_end_hlc");
    let (store, integrate_rx) =
        create_local_store(&crate::LocalStoreConfig::for_test(), storage, stable_log)
            .await
            .expect("create");
    tokio::spawn(run_integrate_worker(
        Arc::downgrade(&store),
        integrate_rx,
        CancellationToken::new(),
    ));

    // Meta-only at/below end_hlc: data is stale, cursor still advances.
    let data_id = DataId::from_parts(Hlc(150), NodeId::from_uuid(uuid::Uuid::nil()), 2);
    integrate_batch(
        &store,
        upstream(1),
        vec![(
            LogIndex(3),
            Data {
                data_id,
                blob: Bytes::from_static(b""),
            },
        )],
    )
    .await
    .expect("integrate_batch");
    assert_eq!(
        store.subscription_cursor(upstream(1)).await.expect("ok"),
        Some(LogIndex(3)),
        "cursor must advance over skipped meta-only records"
    );
}

#[tokio::test]
async fn integrate_dedups_on_data_id() {
    let (_dir, store) = fresh_store().await;
    let d = make_data(3, 300, b"first");
    put_full(&store, upstream(1), LogIndex(5), d.clone()).await;
    put_full(&store, upstream(2), LogIndex(11), d.clone()).await;
    // Same DataId from two upstreams: stored once, both cursors advance.
    assert_eq!(
        store.subscription_cursor(upstream(1)).await.expect("ok"),
        Some(LogIndex(5))
    );
    assert_eq!(
        store.subscription_cursor(upstream(2)).await.expect("ok"),
        Some(LogIndex(11))
    );
}

/// Multi-source variant of `integrate_batch`: every record lands in ONE
/// batch, which is what exercises the batch-level half of the filter.
async fn integrate_mixed_batch(
    store: &LocalStore,
    records: Vec<(NodeId, LogIndex, Data)>,
) -> Result<(), crate::Error> {
    let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
    for (src, idx, rec) in records {
        let _ = tx.send(Ok((src, idx, rec)));
    }
    let (ch, closer) = crossbeam_combinators::Channel::from(rx, "test integrate");
    // Close before the last sender drops, or the consumer forms its
    // verdict as "disconnected, not yet closed" — an abort.
    closer.close();
    drop(tx);
    store.integrate_loop(ch).await.map_err(Into::into)
}

/// Cross-batch (the global ring): the three subscriber streams each deliver
/// every write, so the same `data_id` arrives once per replica in
/// *separate* batches. Only the first may reach the engine.
///
/// Counting rows would prove nothing — the LSM dedups by `DataId` anyway —
/// so this asserts the filter actually skipped the later copies.
#[tokio::test]
async fn later_batches_skip_an_already_committed_data_id() {
    let (_dir, store) = fresh_store().await;
    let d = make_data(4, 400, b"payload");
    for (i, peer) in [upstream(1), upstream(2), upstream(3)]
        .into_iter()
        .enumerate()
    {
        let idx = LogIndex(u64::try_from(i).expect("small") + 1);
        put_full(&store, peer, idx, d.clone()).await;
    }
    assert_eq!(
        store.skipped_duplicates(),
        2,
        "copies 2 and 3 must be dropped before the engine write"
    );
    // Every peer's cursor advanced: dedup must never stall replication, or
    // the skipped peers would resubscribe from a stale position.
    for (i, peer) in [upstream(1), upstream(2), upstream(3)]
        .into_iter()
        .enumerate()
    {
        let want = LogIndex(u64::try_from(i).expect("small") + 1);
        assert_eq!(
            store.subscription_cursor(peer).await.expect("ok"),
            Some(want),
            "peer cursor must advance even though its copy was skipped"
        );
    }
}

/// Batch level: the same `data_id` twice inside ONE batch is written once,
/// and both sources' cursors still advance after that write commits.
#[tokio::test]
async fn one_batch_writes_a_repeated_data_id_once_and_acks_both_sources() {
    let (_dir, store) = fresh_store().await;
    let d = make_data(5, 500, b"payload");
    integrate_mixed_batch(
        &store,
        vec![
            (upstream(1), LogIndex(1), d.clone()),
            (upstream(2), LogIndex(7), d.clone()),
        ],
    )
    .await
    .expect("integrate");

    assert_eq!(store.skipped_duplicates(), 1, "one copy collapsed in-batch");
    assert_eq!(
        store.subscription_cursor(upstream(1)).await.expect("ok"),
        Some(LogIndex(1))
    );
    assert_eq!(
        store.subscription_cursor(upstream(2)).await.expect("ok"),
        Some(LogIndex(7))
    );
}

/// A failed fence ends the worker, so `Shutdown` takes the node down.
///
/// The same claim `a_failed_batch_ends_the_worker_and_leaves_the_filter_clean`
/// makes for the integrate worker. Retrying instead would keep the store
/// accepting integrations and acking peers against a durability claim it
/// could no longer honour.
#[tokio::test]
async fn a_failed_fence_ends_the_flush_worker() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (storage, stable_log) = shared_storage(dir.path()).await;
    let cfg = crate::LocalStoreConfig::for_test();
    let (store, _rx) = create_local_store(&cfg, Arc::clone(&storage), stable_log)
        .await
        .expect("create");

    // Something for the tick to fence.
    store.dirty.store(true, Ordering::Release);
    storage.fail_fences_from_now_on();

    let worker = tokio::spawn(run_local_store_flush_worker(
        Arc::downgrade(&store),
        Duration::from_millis(10),
        CancellationToken::new(),
    ));

    let outcome = tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker ends promptly")
        .expect("worker joins");

    assert!(
        outcome.is_err(),
        "a failed fence must end the worker so the node shuts down"
    );
    assert!(
        store.dirty.load(Ordering::Acquire),
        "a failed fence must leave the store still owing an fsync"
    );
}

/// **The safety test.** A batch that fails to commit must leave the filter
/// untouched, or the records it did not write would be skipped forever
/// when the peer re-delivers them — silent data loss.
///
/// With one shared worker the failure no longer returns to the caller: it
/// ends the worker, and `Shutdown` turns that into a node shutdown, the
/// same contract as `replica:log_flush`. Driven straight through the
/// shared channel so nothing waits on a fence a dead worker cannot answer.
#[tokio::test]
async fn a_failed_batch_ends_the_worker_and_leaves_the_filter_clean() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (storage, stable_log) = shared_storage(dir.path()).await;
    let cfg = crate::LocalStoreConfig::for_test();
    let (store, rx) = create_local_store(&cfg, storage, stable_log)
        .await
        .expect("create");
    let worker = tokio::spawn(run_integrate_worker(
        Arc::downgrade(&store),
        rx,
        CancellationToken::new(),
    ));

    let d = make_data(6, 600, b"payload");
    store.fail_next_append();
    store
        .integrate_tx
        .send(IntegrateItem::Record(upstream(1), LogIndex(1), d.clone()))
        .expect("worker alive");

    let outcome = worker.await.expect("worker joins");
    assert!(
        outcome.is_err(),
        "an append failure must end the worker so the node shuts down"
    );
    assert!(
        store
            .subscription_cursor(upstream(1))
            .await
            .expect("ok")
            .is_none(),
        "nothing may be acked when the data was never written"
    );
    assert!(
        !store.recent.lock().contains(d.data_id),
        "a failed batch must leave the filter untouched, or the retry is \
         skipped forever"
    );
}

#[tokio::test]
async fn subscription_cursor_uses_max() {
    let (_dir, store) = fresh_store().await;
    put_full(&store, upstream(1), LogIndex(10), make_data(1, 10, b"a")).await;
    put_full(&store, upstream(1), LogIndex(5), make_data(2, 11, b"b")).await;
    assert_eq!(
        store.subscription_cursor(upstream(1)).await.expect("ok"),
        Some(LogIndex(10))
    );
}

#[tokio::test]
async fn stream_above_hlc_returns_all_matching_entries() {
    let (_dir, store) = fresh_store().await;
    for (seed, hlc) in [(2u8, 200u64), (1, 100), (3, 300)] {
        put_full(
            &store,
            upstream(1),
            LogIndex(u64::from(seed)),
            make_data(seed, hlc, &[seed]),
        )
        .await;
    }
    use futures::TryStreamExt as _;
    use std::collections::BTreeSet;
    let all: Vec<Data> = store
        .stream_above_hlc(None)
        .into_test_stream()
        .try_collect()
        .await
        .expect("ok");
    let all_hlcs: BTreeSet<u64> = all.iter().map(|d| d.data_id.hlc().raw()).collect();
    assert_eq!(all_hlcs, BTreeSet::from([100, 200, 300]));

    let above: Vec<Data> = store
        .stream_above_hlc(Some(Hlc(150)))
        .into_test_stream()
        .try_collect()
        .await
        .expect("ok");
    let above_hlcs: BTreeSet<u64> = above.iter().map(|d| d.data_id.hlc().raw()).collect();
    assert_eq!(above_hlcs, BTreeSet::from([200, 300]));
}

#[tokio::test]
async fn stream_window_sorts_by_hlc_then_data_id() {
    let (_dir, store) = fresh_store().await;
    // Build entries with known (HLC, random) values, where the random part
    // is in decreasing order — the HLC should dominate the sort.
    let mut expected: Vec<(u64, [u8; 32])> = Vec::new();
    for i in 0u8..8 {
        let hlc = 100u64.checked_add(u64::from(i)).expect("hlc");
        let r = u64::from(0xF0u8.checked_sub(i).expect("random"));
        let data_id = DataId::from_parts(Hlc(hlc), NodeId::from_uuid(uuid::Uuid::nil()), r);
        expected.push((hlc, data_id.0));
        put_full(
            &store,
            upstream(1),
            LogIndex(u64::from(i)),
            Data {
                data_id,
                blob: Bytes::copy_from_slice(&[i]),
            },
        )
        .await;
    }
    // Two entries at the same HLC: random part breaks the tie.
    for (slot, r) in [(8u8, 0x30u64), (9u8, 0x20u64)] {
        let data_id = DataId::from_parts(Hlc(200), NodeId::from_uuid(uuid::Uuid::nil()), r);
        expected.push((200, data_id.0));
        put_full(
            &store,
            upstream(1),
            LogIndex(u64::from(slot)),
            Data {
                data_id,
                blob: Bytes::copy_from_slice(&[slot]),
            },
        )
        .await;
    }
    expected.sort();

    let got: Vec<Data> = store
        .stream_window(None, Hlc(1_000))
        .items()
        .filter_map(Result::ok)
        .collect();
    let got_keys: Vec<(u64, [u8; 32])> = got
        .iter()
        .map(|d| (d.data_id.hlc().raw(), d.data_id.0))
        .collect();
    assert_eq!(got_keys, expected, "must be (HLC, data_id) ordered");
}

#[tokio::test]
async fn stream_window_respects_bounds() {
    let (_dir, store) = fresh_store().await;
    for (seed, hlc) in [(1u8, 100u64), (2, 200), (3, 300)] {
        put_full(
            &store,
            upstream(1),
            LogIndex(u64::from(seed)),
            make_data(seed, hlc, &[seed]),
        )
        .await;
    }
    // Lower bound exclusive, upper bound inclusive: (100, 300] → 200, 300.
    let got: Vec<Data> = store
        .stream_window(Some(Hlc(100)), Hlc(300))
        .items()
        .filter_map(Result::ok)
        .collect();
    let hlcs: Vec<u64> = got.iter().map(|d| d.data_id.hlc().raw()).collect();
    assert_eq!(hlcs, vec![200, 300]);
}

/// The cap must never split an HLC. The next chunk's lower bound is
/// `Excluded(DataId::from_parts(hlc, NodeId::MAX, u64::MAX))`, which skips
/// every entry at that HLC — so a chunk that stopped mid-HLC would silently
/// drop the rest of it. `take_at_least::<Hlc>` over the real scan makes the
/// cap a floor, not a ceiling.
#[tokio::test]
async fn a_capped_window_never_splits_an_hlc() {
    let (_dir, store) = fresh_store().await;
    // Five entries at HLC 100, then five at HLC 101.
    for (hlc, r) in (0u64..5)
        .map(|r| (100u64, r))
        .chain((0u64..5).map(|r| (101u64, r)))
    {
        let data_id = DataId::from_parts(Hlc(hlc), NodeId::from_uuid(uuid::Uuid::nil()), r);
        put_full(
            &store,
            upstream(1),
            LogIndex(r),
            Data {
                data_id,
                blob: Bytes::copy_from_slice(&[0u8]),
            },
        )
        .await;
    }

    // A cap of 2 lands inside the HLC-100 run; all five must still come out.
    let cap = std::num::NonZeroUsize::new(2).expect("nonzero");
    let mut window = store
        .stream_window(None, Hlc(200))
        .into_channel()
        .take_at_least::<Hlc>(cap);
    let mut hlcs = Vec::new();
    let end = loop {
        match window.recv() {
            Ok(Ok(d)) => hlcs.push(d.data_id.hlc()),
            Ok(Err(e)) => panic!("scan error: {e}"),
            Err(end) => break end,
        }
    };
    assert_eq!(
        hlcs,
        vec![Hlc(100); 5],
        "the cap must extend to the end of the HLC it lands in, and stop there",
    );
    assert_eq!(end, crossbeam_combinators::End::Complete);
}

/// A cap larger than the window is not a truncation.
#[tokio::test]
async fn a_cap_above_the_window_size_streams_everything() {
    let (_dir, store) = fresh_store().await;
    for r in 0u64..4 {
        let hlc = 100u64.checked_add(r).expect("hlc");
        let data_id = DataId::from_parts(Hlc(hlc), NodeId::from_uuid(uuid::Uuid::nil()), r);
        put_full(
            &store,
            upstream(1),
            LogIndex(r),
            Data {
                data_id,
                blob: Bytes::copy_from_slice(&[0u8]),
            },
        )
        .await;
    }
    let cap = std::num::NonZeroUsize::new(100).expect("nonzero");
    let entries: Vec<Data> = store
        .stream_window(None, Hlc(200))
        .into_channel()
        .take_at_least::<Hlc>(cap)
        .items()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(
        entries.len(),
        4,
        "a cap the scan never reached cuts nothing"
    );
}

/// Without a cap the window is the whole range, and it closes cleanly.
#[tokio::test]
async fn no_cap_streams_the_whole_window() {
    let (_dir, store) = fresh_store().await;
    for r in 0u64..6 {
        let hlc = 100u64.checked_add(r).expect("hlc");
        let data_id = DataId::from_parts(Hlc(hlc), NodeId::from_uuid(uuid::Uuid::nil()), r);
        put_full(
            &store,
            upstream(1),
            LogIndex(r),
            Data {
                data_id,
                blob: Bytes::copy_from_slice(&[0u8]),
            },
        )
        .await;
    }
    let mut window = store.stream_window(None, Hlc(200));
    let mut entries = 0;
    let end = loop {
        match window.recv() {
            Ok(_) => entries += 1,
            Err(end) => break end,
        }
    };
    assert_eq!(entries, 6);
    assert_eq!(end, crossbeam_combinators::End::Complete);
}

#[tokio::test]
async fn purge_below_hlc_drops_matching_entries() {
    let (_dir, store) = fresh_store().await;
    for (seed, hlc) in [(1u8, 100u64), (2, 200), (3, 300)] {
        put_full(
            &store,
            upstream(1),
            LogIndex(u64::from(seed)),
            make_data(seed, hlc, &[seed]),
        )
        .await;
    }
    store.purge_below_hlc(Hlc(200)).await.expect("ok");
    use futures::TryStreamExt as _;
    // `purge_below_hlc` is a durable `DeleteRange`: rows with HLC <= 200
    // are gone the moment it returns, not merely hidden pending a later
    // compaction (CLAUDE.md §8).
    let remaining: Vec<Data> = store
        .stream_above_hlc(None)
        .into_test_stream()
        .try_collect()
        .await
        .expect("ok");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].data_id.hlc(), Hlc(300));
}

/// **The win one DB unlocks.** A record and the cursor that covers it are
/// one `WriteBatch` across two column families, so after a fence they are
/// both durable or neither is. They were separate DBs before, where no
/// batch could span them: a crash between the two writes could leave a
/// durable cursor claiming coverage of a record that was never written, and
/// the peer would never re-deliver it.
///
/// The failure half of the claim is
/// `a_failed_batch_ends_the_worker_and_leaves_the_filter_clean`, which
/// asserts the cursor did *not* advance when the write failed.
#[tokio::test]
async fn data_and_cursor_commit_atomically() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("db");
    let d = make_data(9, 900, b"payload");
    {
        let (store, worker) = store_at(&root).await;
        put_full(&store, upstream(1), LogIndex(4), d.clone()).await;
        // The §8 fence: everything written before this returns is durable.
        flush_if_dirty(&store).await.expect("fence");
        drop(store);
        worker.stop().await;
    }

    let (store, worker) = store_at(&root).await;
    use futures::TryStreamExt as _;
    let has_data = store
        .stream_above_hlc(None)
        .into_test_stream()
        .try_collect::<Vec<Data>>()
        .await
        .expect("stream")
        .iter()
        .any(|item| item.data_id == d.data_id);
    let has_cursor = store
        .subscription_cursor(upstream(1))
        .await
        .expect("cursor")
        .is_some();
    assert_eq!(
        has_data, has_cursor,
        "a record and the cursor covering it must survive together or not at all",
    );
    assert!(has_data, "the fenced batch must be durable");
    drop(store);
    worker.stop().await;
}

/// Purge is durable. The in-memory clamp it replaced started empty at
/// every open, so purged entries reappeared until a compaction happened
/// to rewrite their SST.
#[tokio::test]
async fn purged_prefix_stays_gone_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("db");
    let old = make_data(1, 100, b"old");
    let new = make_data(2, 300, b"new");
    {
        let (store, worker) = store_at(&root).await;
        put_full(&store, upstream(1), LogIndex(1), old.clone()).await;
        put_full(&store, upstream(1), LogIndex(2), new.clone()).await;
        store.purge_below_hlc(Hlc(200)).await.expect("purge ok");
        drop(store);
        worker.stop().await;
    }

    let (store, worker) = store_at(&root).await;
    use futures::TryStreamExt as _;
    let remaining: Vec<Data> = store
        .stream_above_hlc(None)
        .into_test_stream()
        .try_collect()
        .await
        .expect("stream");
    assert!(
        !remaining.iter().any(|item| item.data_id == old.data_id),
        "an entry purged below the threshold must not come back after a reopen"
    );
    assert!(
        remaining.iter().any(|item| item.data_id == new.data_id),
        "an entry above the threshold survives"
    );
    drop(store);
    worker.stop().await;
}

#[tokio::test]
async fn subscription_persists_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("db");
    {
        let (store, worker) = store_at(&path).await;
        for i in 1u8..=3 {
            put_full(
                &store,
                upstream(1),
                LogIndex(u64::from(i)),
                make_data(i, u64::from(i) * 100, &[i]),
            )
            .await;
        }
        // Drain before reopening the root: a worker mid-batch holds the
        // DB's LOCK.
        drop(store);
        worker.stop().await;
    }
    let (store, worker) = store_at(&path).await;
    assert_eq!(
        store.subscription_cursor(upstream(1)).await.expect("ok"),
        Some(LogIndex(3))
    );
    drop(store);
    worker.stop().await;
}

#[tokio::test]
async fn local_store_subscription_cursor_persists_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.db");

    {
        let (store, worker) = crate::testutil::open_test_local_store(&path)
            .await
            .expect("open");
        for i in 1..=5u8 {
            integrate_batch(
                &store,
                upstream(1),
                vec![(LogIndex(u64::from(i)), make_data(i, u64::from(i), &[i]))],
            )
            .await
            .expect("integrate");
        }
        // Drop the store and DRAIN its worker before reopening the root:
        // a worker mid-batch holds the DB's LOCK (`open_test_local_store`).
        drop(store);
        worker.stop().await;
    }

    let (store, worker) = crate::testutil::open_test_local_store(&path)
        .await
        .expect("reopen");
    assert_eq!(
        store.subscription_cursor(upstream(1)).await.expect("ok"),
        Some(LogIndex(5))
    );
    drop(store);
    worker.stop().await;
}
