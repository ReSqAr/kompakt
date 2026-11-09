//! Tests for `log`.

use crossbeam_combinators::testing::ChannelTestExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use tempfile::TempDir;

use super::codec::KEY_WIDTH;
use super::codec::VALUE_PREFIX_WIDTH;
use super::flush::flush_one_batch_blocking;
use super::*;

use zslg_consensus::NodeId;
use zslg_log::{DataId, LogIndex};
use zslg_time::Hlc;

use tokio_util::sync::CancellationToken;

/// Create a guard for a test HLC. A fresh `HlcGate` has no cutoff, so
/// `check_hlc` always succeeds.
fn test_guard(_hlc: Hlc) -> Admission {
    Box::new(())
}

/// Open a store at `dir` with its flush loop, at the default sizing —
/// [`ReplicaLogStore::open_with_flush`] under a name the tests already use.
/// The `Storage` (and so the transitional root, budget and CF list) comes
/// from the production helper, so tests cannot drift from it.
async fn open_for_test(
    dir: PathBuf,
) -> Result<
    (
        Arc<ReplicaLogStore>,
        tokio::task::JoinHandle<Result<(), Error>>,
    ),
    Error,
> {
    ReplicaLogStore::open_with_flush(
        crate::testing::open_for_test(&dir)?,
        CancellationToken::new(),
    )
    .await
}

/// A point read, for the tests that assert the raw stored shape. Production
/// has no point-read path: the log is scanned, never probed.
fn get_entry(storage: &Storage, index: LogIndex) -> Result<Option<(DataId, Bytes)>, Error> {
    let mut found = None;
    for_each_entry(storage, index, index, &mut |_, data_id, payload| {
        // PINNING RULE: `payload` borrows the iterator's pinned block, so it
        // is copied here rather than escaping the callback.
        found = Some((data_id, Bytes::copy_from_slice(payload)));
        false
    })?;
    Ok(found)
}

/// The oldest surviving index — the assertion a durable purge needs, since
/// there is no production accessor for it (the log is scanned from a
/// cursor, never probed for its floor).
fn first_live_index(storage: &Storage) -> Result<Option<LogIndex>, Error> {
    let mut first = None;
    for_each_entry(
        storage,
        LogIndex::ZERO,
        LogIndex(u64::MAX),
        &mut |idx, _, _| {
            first = Some(idx);
            false
        },
    )?;
    Ok(first)
}

/// Every stored key, decoded. Key-only: no value copies, and the secondary
/// is what the monotonicity assertions are about.
fn scan_keys(storage: &Storage) -> Result<Vec<(LogIndex, Hlc)>, Error> {
    let cf = storage.cf(REPLICA_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek_to_first();
    let mut out = Vec::new();
    while let Some(key) = it.key() {
        out.push(decode_key(key).ok_or_else(|| undecodable(key))?);
        it.next();
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb key scan (replica log): {e}"
        )))
    })?;
    Ok(out)
}

use zslg_log_fake::make_data;

async fn fresh_store() -> (TempDir, Arc<ReplicaLogStore>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replica-log");
    let (store, _flush) = open_for_test(path).await.expect("open_for_test");
    (dir, store)
}

async fn store_at(path: &Path) -> Arc<ReplicaLogStore> {
    let (store, _flush) = open_for_test(path.to_path_buf())
        .await
        .expect("open_for_test");
    store
}

/// CLAUDE.md §20: a subscriber stream that goes backwards is the
/// log-corruption signal, and `subscribe_writer_loop` closes the connection
/// on it. So the broadcast order has to be the index order, however many
/// writers are pushing.
///
/// The single appender gives this by construction — it assigns indexes and
/// broadcasts on one thread, so there is no window. The test exists because
/// the alternative shape does not: when writers committed and published
/// their own batches, two of them could become contiguous in order and
/// still reach the fan-out out of order, and this fired ~100-130 times per
/// 6 s loadgen rep at `--inflight 128`. Keep this pinned against any future
/// attempt to move publishing back onto the writers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_broadcast_in_index_order() {
    const WRITERS: u8 = 24;

    let (_dir, store) = fresh_store().await;
    let mut ch = Arc::clone(&store).subscribe_from(None);

    // Keep the completion receivers alive so an ACK is not counted dropped.
    let mut keep = Vec::new();
    let mut writers = Vec::new();
    for i in 1..=WRITERS {
        let (tx, rx) = crossbeam_channel::bounded(1);
        keep.push(rx);
        let store = Arc::clone(&store);
        writers.push(tokio::task::spawn_blocking(move || {
            store.submit(
                make_data(i, u64::from(i) * 100, &[i]),
                test_guard(Hlc::ZERO),
                Arc::new(tx),
            );
        }));
    }
    for w in writers {
        w.await.expect("writer");
    }

    let mut seen: Vec<u64> = Vec::new();
    for _ in 0..WRITERS {
        let (idx, _) = ch.recv().expect("record").expect("ok");
        seen.push(idx.raw());
    }
    let mut sorted = seen.clone();
    sorted.sort_unstable();
    assert_eq!(
        seen, sorted,
        "broadcast went backwards — a subscriber closes the stream on this \
         (CLAUDE.md §20)",
    );
    assert_eq!(
        seen,
        (0..u64::from(WRITERS)).collect::<Vec<_>>(),
        "the log must be gapless and start at 0",
    );
}

#[tokio::test]
async fn append_assigns_zero_and_advances_frontier() {
    let (_dir, store) = fresh_store().await;
    assert!(store.frontier().is_none());

    let idx0 = store
        .append(
            make_data(1, u64::from(1u8) * 100, b"hello"),
            test_guard(Hlc::ZERO),
        )
        .await
        .expect("append");
    assert_eq!(idx0, LogIndex::ZERO);
    assert_eq!(store.frontier(), Some(LogIndex::ZERO));

    let idx1 = store
        .append(
            make_data(2, u64::from(2u8) * 100, b"world"),
            test_guard(Hlc::ZERO),
        )
        .await
        .expect("append");
    assert_eq!(idx1, LogIndex(1));
    assert_eq!(store.frontier(), Some(LogIndex(1)));
}

/// A connection whose outcome channel is full must not stall the flush loop:
/// it is shared by every connection on the node, so blocking there would stop
/// appends cluster-wide for one slow consumer. The completion is dropped
/// (counted as `zslg_replica_outcome_dropped_total`) and the writes still commit —
/// the writer resends un-acked writes on reconnect.
#[tokio::test]
async fn outcome_channel_full_does_not_block_the_flush_loop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, flush_handle) = open_for_test(dir.path().join("replica-log"))
        .await
        .expect("open_for_test");

    // A completion channel with no room at all: every send would block.
    let (full_tx, full_rx) = crossbeam_channel::bounded(1);
    full_tx
        .send((DataId([0u8; 32]), Ok(LogIndex(0))))
        .expect("prefill");
    let stuck: Completion = Arc::new(full_tx);

    for i in 0..8u8 {
        store.submit(
            make_data(i, 100u64.saturating_add(u64::from(i)), b"x"),
            test_guard(Hlc::ZERO),
            stuck.clone(),
        );
    }

    // The flush loop must keep making progress: a later write on a healthy
    // channel still completes, and the frontier advanced past all 8.
    let idx = store
        .append(make_data(200, 5_000, b"healthy"), test_guard(Hlc::ZERO))
        .await
        .expect("flush loop must still serve other connections");
    assert_eq!(idx, LogIndex(8), "all 8 stuck writes committed before it");
    assert!(!flush_handle.is_finished(), "flush loop must stay alive");
    drop(full_rx);
}

/// An over-cap payload is rejected **on its own**: the writes queued beside
/// it in the same batch still commit, the flush loop stays alive, and the
/// rejected write consumes no `LogIndex` (the log stays gapless). Letting one
/// bad record abort the batch would strand every good write and fail-stop the
/// node — a `WriteBatch` is all-or-nothing.
#[tokio::test]
async fn oversized_write_is_rejected_without_failing_the_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, flush_handle) = open_for_test(dir.path().join("replica-log"))
        .await
        .expect("open_for_test");

    // Cap is `max_payload_bytes - FRAME_OVERHEAD_SLACK`; overshoot it.
    let too_big = vec![0u8; 64 * 1024 * 1024];
    let (ok_a, bad, ok_b) = tokio::join!(
        store.append(make_data(1, 100, b"a"), test_guard(Hlc::ZERO)),
        store.append(make_data(2, 200, &too_big), test_guard(Hlc::ZERO)),
        store.append(make_data(3, 300, b"b"), test_guard(Hlc::ZERO)),
    );

    let err = bad.expect_err("over-cap write must be rejected");
    assert!(
        matches!(err, Error::Log(LogError::Codec(ref m)) if m.contains("exceeds the replica log frame cap")),
        "got: {err:?}"
    );
    let a = ok_a.expect("neighbouring write must still commit");
    let b = ok_b.expect("neighbouring write must still commit");

    // The rejection consumed no index: the two survivors are consecutive.
    let (lo, hi) = if a.raw() < b.raw() { (a, b) } else { (b, a) };
    assert_eq!(
        hi.raw(),
        lo.raw().saturating_add(1),
        "a rejected write must not consume a LogIndex ({lo:?} → {hi:?})"
    );

    // The flush loop is still running (fail-stop would have ended it).
    assert!(!flush_handle.is_finished(), "flush loop must stay alive");
}

/// A storage write failure is a FATAL storage fault: the flush loop must
/// fail-stop (return `Err`, which the `Shutdown` supervisor turns into a
/// node shutdown) rather than swallow the error and keep accepting writes.
/// Carrying on is unsafe — on Linux a failed fsync can be reported only once
/// (fsyncgate), so a later "successful" flush could leave a silent durable
/// gap. This pins the fail-stop so it can't silently regress.
#[tokio::test]
async fn append_failure_fails_the_flush_loop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replica-log");
    let (store, flush_handle) = open_for_test(path.clone()).await.expect("open_for_test");

    store
        .append(make_data(1, 100, b"a"), test_guard(Hlc::ZERO))
        .await
        .expect("first append succeeds");

    // The failure is injected rather than provoked through the filesystem:
    // RocksDB writes into an already-open WAL and buffers in the memtable,
    // so no permission trick (the read-only directory this test used to
    // rely on) fails a write deterministically. The claim is unchanged.
    store.storage.fail_writes_from_now_on();
    let err = store
        .append(make_data(2, 200, b"a"), test_guard(Hlc::ZERO))
        .await
        .expect_err("a storage write failure must surface to the caller");
    assert!(err.to_string().contains("append failed"), "got: {err}");

    // ...and the tracked flush loop terminates with `Err` (fail-stop)
    // rather than hanging or continuing to run.
    let joined = tokio::time::timeout(std::time::Duration::from_secs(5), flush_handle)
        .await
        .expect("flush loop must terminate (fail-stop), not hang");
    assert!(
        matches!(joined, Ok(Err(_))),
        "a storage write failure must fail the flush loop, got {joined:?}"
    );
}

#[tokio::test]
async fn frontier_persists_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replica-log");
    {
        let (store, flush) = open_for_test(path.clone()).await.expect("open_for_test");
        for i in 1..=3u8 {
            store
                .append(
                    make_data(i, u64::from(i) * 100, &[i]),
                    test_guard(Hlc::ZERO),
                )
                .await
                .expect("ok");
        }
        drop(store);
        let _ = flush.await;
    }
    let store = store_at(&path).await;
    assert_eq!(store.frontier(), Some(LogIndex(2)));

    let idx = store
        .append(
            make_data(99, u64::from(99u8) * 100, b"after-reopen"),
            test_guard(Hlc::ZERO),
        )
        .await
        .expect("ok");
    assert_eq!(idx, LogIndex(3));
}

// ── max_hlc: the membership drain bound ──────────────────────────────
//
// `max_hlc` must be an upper bound on the HLC of *every* entry in the log,
// which is what makes it safe to sample as a joint-config exit bound
// (`leader::membership_change::arm_exit_condition`). The frontier entry's
// own HLC is not such a bound, because concurrent writers append out of
// HLC order.

#[tokio::test]
async fn max_hlc_exact_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replica-log");
    {
        let (store, flush) = open_for_test(path.clone()).await.expect("open_for_test");
        // Largest HLC in the middle, so recovery cannot get the right answer
        // by reading the frontier entry's own HLC.
        for (seed, hlc) in [(1u8, 100u64), (2, 900), (3, 300)] {
            store
                .append(make_data(seed, hlc, &[seed]), test_guard(Hlc::ZERO))
                .await
                .expect("ok");
        }
        assert_eq!(store.max_hlc(), Some(Hlc(900)));
        drop(store);
        let _ = flush.await;
    }
    let store = store_at(&path).await;
    assert_eq!(
        store.max_hlc(),
        Some(Hlc(900)),
        "the bound is durable, recovered from the frontier record"
    );
    // And it keeps accumulating after the reopen.
    store
        .append(make_data(4, 950, b"d"), test_guard(Hlc::ZERO))
        .await
        .expect("ok");
    assert_eq!(store.max_hlc(), Some(Hlc(950)));
    store
        .append(make_data(5, 400, b"e"), test_guard(Hlc::ZERO))
        .await
        .expect("ok");
    assert_eq!(store.max_hlc(), Some(Hlc(950)));
}

/// The case that would break the bound if recovery read anything but the
/// running max: the largest HLC sits on the *oldest* entry, and purge then
/// tries to sweep most of the log before a restart.
///
/// It survives because the secondary is a running maximum — every entry
/// after the 9000 carries 9000 too — so a threshold below it covers
/// nothing, and the bound is still the last key's secondary after a reopen.
/// An under-report here would let a joint config collapse before the
/// outgoing set had drained (CLAUDE.md §8), which is why this is asserted
/// rather than reasoned about.
#[tokio::test]
async fn max_hlc_survives_purge_of_the_entry_that_carries_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replica-log");
    {
        let (store, flush) = open_for_test(path.clone()).await.expect("open_for_test");
        // Entry 0 carries the max HLC; everything after it is lower, so a
        // purge that swept by *index* or by arrival order would drop it.
        for (seed, hlc) in [(0u8, 9000u64), (1, 100), (2, 200), (3, 300), (4, 400)] {
            store
                .append(
                    make_data(seed, hlc, &vec![seed; 1024]),
                    test_guard(Hlc::ZERO),
                )
                .await
                .expect("append");
        }
        assert_eq!(store.max_hlc(), Some(Hlc(9000)));
        store.storage.compact(REPLICA_LOG_CF).expect("compact");

        // A threshold above every *low* entry. It covers nothing: entry 0's
        // running max is 9000, and so is every later entry's.
        store.set_purge_threshold(Hlc(400)).expect("purge");
        store.storage.compact(REPLICA_LOG_CF).expect("compact");
        drop(store);
        let _ = flush.await;
    }
    let reopened = store_at(&path).await;
    assert_eq!(
        reopened.max_hlc(),
        Some(Hlc(9000)),
        "the running max floors every later secondary, so nothing is covered",
    );
}

/// The raw on-disk value layout round-trips and recovery reads the prefix.
#[tokio::test]
async fn raw_value_round_trips_and_recovery_recovers_the_bound() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replica-log");
    let (store, flush) = {
        let (store, flush) = open_for_test(path.clone()).await.expect("open");
        // Out-of-order HLCs so max_hlc != frontier hlc.
        for (seed, hlc) in [(1u8, 100u64), (2, 900), (3, 300)] {
            store
                .append(make_data(seed, hlc, &[seed]), test_guard(Hlc::ZERO))
                .await
                .expect("append");
        }
        assert_eq!(store.max_hlc(), Some(Hlc(900)));
        (store, flush)
    };
    // The store holds RocksDB's LOCK on the root until the last `Arc`
    // drops, so it must be released before reopening.
    drop(store);
    let _ = flush.await;

    let (reopened, flush2) = open_for_test(path).await.expect("reopen");
    // The stored value is `data_id ‖ blob`, and `decode_value` splits it
    // back apart. Each append above awaits its completion before the next
    // submits, so the records land at consecutive indices starting at 0
    // (seed 1 here).
    let (secondary, value) = get_entry(&reopened.storage, LogIndex(0))
        .expect("ok")
        .expect("present");
    assert_eq!(secondary, make_data(1, 100, &[1]).data_id);
    assert_eq!(&value[..], &[1u8]);
    assert_eq!(
        reopened.max_hlc(),
        Some(Hlc(900)),
        "bound recovered from the last key's running-max secondary",
    );
    flush2.abort();
}

/// The ordering the drain bound depends on: `max_hlc` covers an HLC *before*
/// the writer is told the write succeeded. Otherwise step 2b could sample a
/// bound that misses a write the writer already counted as acknowledged.
#[tokio::test]
async fn max_hlc_advances_before_the_write_ack() {
    let (_dir, store) = fresh_store().await;
    // `append` resolves off the completion channel, which the flush loop
    // sends only after `frontier.advance` (see `flush_one_batch_blocking`).
    store
        .append(make_data(7, 777, b"g"), test_guard(Hlc::ZERO))
        .await
        .expect("ok");
    assert_eq!(
        store.max_hlc(),
        Some(Hlc(777)),
        "the bound must already cover an acked write"
    );
}

#[tokio::test]
async fn broadcast_fires_after_commit() {
    let (_dir, store) = fresh_store().await;
    let mut ch = Arc::clone(&store).subscribe_from(None);

    let record = make_data(42, u64::from(42u8) * 100, b"broadcast");
    let expected = record.clone();
    let hlc = record.data_id.hlc();
    store.append(record, test_guard(hlc)).await.expect("ok");

    let (idx, data) = ch.recv().unwrap().unwrap();
    assert_eq!(idx, LogIndex::ZERO);
    assert_eq!(data.data_id, expected.data_id);
}

/// A subscriber already at the frontier asks for `frontier + 1`: an empty
/// historic window, which is the routine state of a peer that reconnects
/// with nothing to catch up on. It must hand straight over to the live
/// tail, not panic on the `from <= to` precondition of `pump_range`.
#[tokio::test]
async fn subscribe_from_frontier_serves_live_tail() {
    let (_dir, store) = fresh_store().await;
    for i in 1..=3u8 {
        store
            .append(
                make_data(i, u64::from(i) * 100, &[i]),
                test_guard(Hlc::ZERO),
            )
            .await
            .expect("ok");
    }
    let frontier = store.frontier().expect("frontier");

    let mut ch = Arc::clone(&store).subscribe_from(Some(frontier));

    // Nothing historic to replay; the next append arrives on the live tail.
    let record = make_data(9, 900, b"after");
    let expected = record.data_id;
    let hlc = record.data_id.hlc();
    store.append(record, test_guard(hlc)).await.expect("ok");

    let (idx, rec) = ch.recv().unwrap().unwrap();
    assert_eq!(idx, LogIndex(frontier.0 + 1));
    assert_eq!(rec.data_id, expected);
}

/// The handover between the historic scan and the live tail must not drop a
/// record — the delicate part of `subscribe_from`, and the reason the live
/// tail is opened before anything else.
///
/// The window is a couple of instructions wide, so a merely-concurrent test
/// passes with the ordering reversed and proves nothing (verified: it did).
/// `SUBSCRIBE_HANDOFF_PAUSE_MS` parks execution *inside* the window so an
/// entry can be committed there on purpose.
///
/// Why the current order is correct: the flush loop advances the frontier
/// **before** it broadcasts, so an entry broadcast too early to reach a
/// subscriber that has just registered is necessarily at or below the
/// frontier this call then snapshots — i.e. inside the scan's range. Register
/// first, snapshot second, and every entry is served by exactly one of the
/// two phases (overlap is filtered; §8 makes duplicates benign anyway).
///
/// Reversed, the entry committed in the window is above `cutover` (outside
/// the scan) and was broadcast before this subscriber existed (never on the
/// live tail). It is lost, and downstream it looks like a benign purge gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_record_is_lost_in_the_historic_to_live_handover() {
    const SEEDED: u8 = 3; // indices 0..=2
    const PAUSE_MS: u64 = 400;

    let (_dir, store) = fresh_store().await;
    for i in 1..=SEEDED {
        store
            .append(
                make_data(i, u64::from(i) * 100, &[i]),
                test_guard(Hlc::ZERO),
            )
            .await
            .expect("seed append");
    }

    SUBSCRIBE_HANDOFF_PAUSE_MS.store(PAUSE_MS, std::sync::atomic::Ordering::Relaxed);
    let subscribing = {
        let store = Arc::clone(&store);
        tokio::task::spawn_blocking(move || Arc::clone(&store).subscribe_from(None))
    };

    // Land squarely inside the paused window, then commit index 3 there.
    tokio::time::sleep(std::time::Duration::from_millis(PAUSE_MS / 4)).await;
    let raced = make_data(9, 900, b"committed inside the handover window");
    let raced_id = raced.data_id;
    let raced_index = store
        .append(raced, test_guard(Hlc::ZERO))
        .await
        .expect("append inside the window");

    let mut ch = subscribing.await.expect("subscribe_from");
    SUBSCRIBE_HANDOFF_PAUSE_MS.store(0, std::sync::atomic::Ordering::Relaxed);

    let want = usize::from(SEEDED).saturating_add(1);
    let collected = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            let mut got = Vec::new();
            while got.len() < want {
                match ch.recv() {
                    Ok(Ok((idx, data))) => got.push((idx, data.data_id)),
                    Ok(Err(e)) => panic!("stream error: {e}"),
                    Err(_end) => break, // stream ended early — the record is gone
                }
            }
            got
        }),
    )
    .await
    .expect("the racing record was never delivered — it fell into the handover gap")
    .expect("collector");

    let indices: Vec<u64> = collected.iter().map(|(i, _)| i.raw()).collect();
    assert_eq!(
        indices,
        vec![0, 1, 2, 3],
        "every index must be served exactly once across the handover",
    );
    assert_eq!(
        collected[3].1, raced_id,
        "the entry committed inside the handover window must be delivered",
    );
    assert_eq!(raced_index, LogIndex(3));
}

/// A subscriber that drains promptly must survive a full-size flush batch.
/// The arithmetic invariant lives in `ReplicaLogConfig::validate`; this is
/// the behaviour it exists to protect, and it is what regressed when the
/// capacity (256) sat below the batch cap (1024).
#[tokio::test]
async fn full_flush_batch_does_not_shed_a_draining_subscriber() {
    let (_dir, store) = fresh_store().await;
    let mut ch = Arc::clone(&store).subscribe_from(None);

    // One batch's worth of records, appended before anything is drained.
    let batch = crate::ReplicaLogConfig::for_test(zslg_consensus::NodeId::new_v4())
        .max_flush_batch
        .get();
    for i in 0..batch {
        let seed = u8::try_from(i % 251).expect("mod 251 fits u8");
        store
            .append(
                make_data(seed, u64::try_from(i).expect("small") + 1, &[seed]),
                test_guard(Hlc::ZERO),
            )
            .await
            .expect("append");
    }

    let mut seen = 0usize;
    while seen < batch {
        let Ok(items) = ch.ready_batch(batch) else {
            panic!("subscriber was shed after {seen}/{batch} records");
        };
        seen = seen.saturating_add(items.count());
    }
    assert_eq!(seen, batch, "every record reached the subscriber");
}

#[tokio::test]
async fn subscribe_from_returns_in_index_order() {
    let (_dir, store) = fresh_store().await;
    for i in 1..=5u8 {
        store
            .append(
                make_data(i, u64::from(i) * 100, &[i]),
                test_guard(Hlc::ZERO),
            )
            .await
            .expect("ok");
    }
    let ch = Arc::clone(&store).subscribe_from(None);
    let entries: Vec<_> = ch
        .into_channel()
        .items()
        .take(5)
        .filter_map(|r| match r {
            Ok((idx, data)) => Some((idx, data)),
            _ => None,
        })
        .collect();
    assert_eq!(entries.len(), 5);
    for (n, (idx, _)) in entries.iter().enumerate() {
        let expected = u64::try_from(n).expect("fits");
        assert_eq!(idx.raw(), expected, "out-of-order at slot {n}");
    }
}

/// Purge retires by HLC, clamped strictly below the frontier entry: covered
/// entries vanish from every read and the newest entry always survives, so
/// `next_index` recovers across a reopen. Pins the hard case — an idle
/// cluster whose threshold has passed *every* entry.
#[tokio::test]
async fn purge_keeps_frontier_even_under_full_coverage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replica-log");

    let (store, flush) = open_for_test(path.clone()).await.expect("open_for_test");
    for i in 0..9u64 {
        let d = make_data(
            u8::try_from(i).expect("fits"),
            i * 100,
            &vec![u8::try_from(i).expect("fits"); 1024],
        );
        store
            .append(d, test_guard(Hlc::ZERO))
            .await
            .expect("append");
    }
    assert_eq!(store.frontier(), Some(LogIndex(8)));
    // Rewrite the SSTs so the purge below is exercised against files: a
    // DeleteRange tombstone written over keys already resident in an SST
    // is a different shape than one that only ever touches the memtable.
    store.storage.compact(REPLICA_LOG_CF).expect("compact");

    // Threshold covers entries 0..=3 (running max <= 300); the frontier's
    // max_hlc is 800, so the clamp leaves the threshold untouched. Purge
    // is a durable `DeleteRange`, fenced synchronously by this call — the
    // compact() above is only to exercise the SST-resident path.
    store.set_purge_threshold(Hlc(300)).expect("purge");
    store.storage.compact(REPLICA_LOG_CF).expect("compact");
    {
        // `subscribe_from(Some(LogIndex(3)))` → start = 4: entries 4..=8
        // survive (5). The live tail is infinite, so bound the drain.
        let ch = Arc::clone(&store).subscribe_from(Some(LogIndex(3)));
        let entries: Vec<_> = ch
            .into_channel()
            .items()
            .filter_map(|r| r.ok())
            .take(5)
            .collect();
        assert_eq!(entries.len(), 5, "indices 4..=8 should survive");
        for (n, (idx, _)) in entries.iter().enumerate() {
            let expected = LogIndex(u64::try_from(n).expect("fits").saturating_add(4));
            assert_eq!(*idx, expected, "expected {expected:?} at slot {n}");
        }
    }
    // Before reopening the root: join the flush loop, which exits once the
    // dropped store closes its write channel. The node's shutdown path
    // drains `replica:log_flush`.
    drop(store);
    let _ = flush.await;

    let store = store_at(&path).await;
    assert_eq!(store.frontier(), Some(LogIndex(8)));
    {
        let ch = Arc::clone(&store).subscribe_from(Some(LogIndex(3)));
        let entries: Vec<_> = ch
            .into_channel()
            .items()
            .filter_map(|r| r.ok())
            .take(5)
            .collect();
        assert_eq!(entries.len(), 5, "indices 4..=8 should survive reopen");
    }

    // A threshold past EVERY entry clamps to `max_hlc - 1`: the frontier
    // entry itself survives — including across a reopen.
    store.set_purge_threshold(Hlc(u64::MAX)).expect("purge");
    store.storage.compact(REPLICA_LOG_CF).expect("compact");
    assert_eq!(store.frontier(), Some(LogIndex(8)));
    {
        // start = 8 (next_after 7): only the frontier entry survives.
        let ch = Arc::clone(&store).subscribe_from(Some(LogIndex(7)));
        let entries: Vec<_> = ch
            .into_channel()
            .items()
            .filter_map(|r| r.ok())
            .take(1)
            .collect();
        assert_eq!(entries.len(), 1, "only the frontier entry survives");
        assert_eq!(entries[0].0, LogIndex(8));
    }
    drop(store);
    let store = store_at(&path).await;
    assert_eq!(
        store.frontier(),
        Some(LogIndex(8)),
        "frontier survives reopen after full-coverage purge"
    );
    let next = store
        .append(
            make_data(99, u64::from(99u8) * 100, b"post-purge"),
            test_guard(Hlc::ZERO),
        )
        .await
        .expect("ok");
    assert_eq!(next, LogIndex(9), "append continues from frontier + 1");
}

/// Two entries in ONE flush batch, staged high-HLC-first.
///
/// The fold that `flush_one_batch_blocking` used to do took only the
/// batch's *last* entry, so this reported 200 while the log held 900 — an
/// under-report of the bound the leader samples for the joint-drain exit
/// (CLAUDE.md §8). Co-batching cannot be forced through `append`, which
/// awaits each completion, so this drives one batch by hand: the store is
/// opened without a flush loop and `flush_one_batch_blocking` is called
/// directly, with the second write already queued behind the first.
#[tokio::test(flavor = "multi_thread")]
async fn max_hlc_folds_every_entry_of_one_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = crate::testing::open_for_test(&dir.path().join("replica-log")).expect("storage");
    let (tx, rx) = crossbeam_channel::bounded(4);
    let store = Arc::new(
        ReplicaLogStore::open_sized(storage, tx.clone(), 16, 16)
            .await
            .expect("open"),
    );
    let (completion_tx, _completion_rx) = crossbeam_channel::bounded(4);
    let completion = Arc::new(completion_tx);
    let pending = |seed: u8, hlc: u64| PendingWrite {
        data: make_data(seed, hlc, &[seed]),
        completion: Arc::clone(&completion),
        guard: Some(test_guard(Hlc::ZERO)),
        enqueued_at: std::time::Instant::now(),
    };
    // Queued first, so `BatchBuilder::build` drains it into the same batch
    // *behind* the high-HLC entry passed as `first`.
    tx.send(pending(2, 200)).expect("queue");
    let first = pending(1, 900);
    let store_cl = Arc::clone(&store);
    // The commit fence coalesces through a runtime task, so the blocking
    // body must not sit on a worker thread that task needs.
    tokio::task::spawn_blocking(move || flush_one_batch_blocking(&store_cl, first, &rx))
        .await
        .expect("join")
        .expect("flush");

    assert_eq!(store.frontier(), Some(LogIndex(1)), "both entries landed");
    assert_eq!(
        store.max_hlc(),
        Some(Hlc(900)),
        "the fold must cover every entry of the batch, not just its last"
    );
    // And the key ordering the durable purge depends on survives it: the
    // low-HLC entry inherits the running max rather than lowering it.
    assert_eq!(
        scan_keys(&store.storage).expect("scan"),
        vec![(LogIndex(0), Hlc(900)), (LogIndex(1), Hlc(900))],
    );
}

/// Purge is durable. The in-memory clamp it replaced started empty at every
/// open, so an un-compacted purged prefix became readable again — this is
/// the assertion that fails under that design and passes under a
/// `DeleteRange`.
#[tokio::test]
async fn purged_prefix_stays_gone_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replica-log");
    {
        let (store, flush) = open_for_test(path.clone()).await.expect("open");
        for (seed, hlc) in [(1u8, 100u64), (2, 200), (3, 300)] {
            store
                .append(make_data(seed, hlc, &[seed]), test_guard(Hlc::ZERO))
                .await
                .expect("append");
        }
        store.set_purge_threshold(Hlc(150)).expect("purge");
        // Rewrite the purged prefix's tombstone into an SST before the
        // reopen below, so "stays gone" is checked against the
        // SST-resident shape, not only the WAL/memtable one.
        store.storage.compact(REPLICA_LOG_CF).expect("compact");
        assert_eq!(
            first_live_index(&store.storage).expect("scan"),
            Some(LogIndex(1)),
            "index 0 is covered by the threshold"
        );
        drop(store);
        let _ = flush.await;
    }
    let store = store_at(&path).await;
    assert_eq!(
        first_live_index(&store.storage).expect("scan"),
        Some(LogIndex(1)),
        "index 0 was purged and must not come back after a reopen"
    );
}

#[test]
fn proptest_gapless_across_reopen() {
    use proptest::prelude::*;

    // Arbitrary HLCs, deliberately NOT index-ordered: writers append out of
    // HLC order, which is the whole reason the secondary is a running max
    // rather than the entry's own HLC. A monotone generator would make the
    // monotonicity assertion below vacuous.
    proptest!(ProptestConfig::with_cases(16), |(records in proptest::collection::vec((any::<Vec<u8>>(), any::<u64>()), 1..16))| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");
        let count = records.len();

        rt.block_on(async {
            let (store, flush) = open_for_test(path.clone())
                .await
                .expect("open_for_test");
            for (i, (blob, raw_hlc)) in records.iter().enumerate() {
                let hlc = Hlc(*raw_hlc);
                let record = Data {
                    // `i` as the tiebreaker keeps every `data_id` distinct
                    // even when two entries draw the same HLC.
                    data_id: DataId::test_id(hlc, u64::try_from(i).expect("fits")),
                    blob: Bytes::copy_from_slice(blob),
                };
                store.append(record, test_guard(hlc)).await.expect("append ok");
            }
            // Interleave a compaction pass so reopen recovers from
            // rewritten SSTs, not only the WAL/memtable.
            store.storage.compact(REPLICA_LOG_CF).expect("compact");
            drop(store);
            let _ = flush.await;
        });

        let (frontier_val, entries, recovered_keys) = rt.block_on(async {
            let store = store_at(&path).await;
            let frontier = store.frontier();
            let keys = scan_keys(&store.storage).expect("key scan");
            let mut entries: Vec<(LogIndex, Data)> = Vec::new();
            let mut ch = Arc::clone(&store).subscribe_from(None);
            for _ in 0..count {
                let (idx, data) = ch.recv().unwrap().unwrap();
                entries.push((idx, data));
            }
            (frontier, entries, keys)
        });

        let last_expected = u64::try_from(count)
            .expect("fits")
            .checked_sub(1)
            .expect("count >= 1");
        prop_assert_eq!(frontier_val, Some(LogIndex(last_expected)));
        prop_assert_eq!(entries.len(), count);
        for (n, (idx, _)) in entries.iter().enumerate() {
            let expected = u64::try_from(n).expect("fits");
            prop_assert_eq!(idx.raw(), expected);
        }

        // The secondary is a RUNNING MAX, so it never decreases across the log.
        // This is what makes the retired set a key-order prefix, and it is the
        // assertable form of "max_hlc folds every entry": a batch that staged a
        // high HLC before a low one would write a decreasing secondary here.
        prop_assert_eq!(recovered_keys.len(), count);
        let mut prev: Option<Hlc> = None;
        for (_, secondary) in &recovered_keys {
            if let Some(p) = prev {
                prop_assert!(
                    *secondary >= p,
                    "replica-log secondary must be non-decreasing: {p:?} then {secondary:?}"
                );
            }
            prev = Some(*secondary);
        }
    });
}

#[tokio::test]
async fn replica_log_frontier_resumes_at_next_free_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replica-log");

    {
        let (store, flush) = ReplicaLogStore::open_with_flush(
            crate::testing::open_for_test(&path).expect("storage"),
            CancellationToken::new(),
        )
        .await
        .expect("open_for_test");
        for i in 1..=5u8 {
            store
                .append(
                    make_data(i, u64::from(i), &[i]),
                    test_guard(Hlc(u64::from(i))),
                )
                .await
                .expect("append");
        }
        drop(store);
        let _ = flush.await;
    }

    let (store, _flush) = ReplicaLogStore::open_with_flush(
        crate::testing::open_for_test(&path).expect("storage"),
        CancellationToken::new(),
    )
    .await
    .expect("open_for_test");
    assert_eq!(store.frontier(), Some(LogIndex(4)));

    let appended = store
        .append(make_data(99, 100, b"after-recovery"), test_guard(Hlc(100)))
        .await
        .expect("append");
    assert_eq!(appended, LogIndex(5));
    assert_eq!(store.frontier(), Some(LogIndex(5)));
}

// ── key codec ───────────────────────────────────────────────────────

#[test]
fn replica_key_roundtrips_and_primary_dominates() {
    let a = encode_key(LogIndex(1), Hlc(u64::MAX));
    let b = encode_key(LogIndex(2), Hlc(1));
    assert!(a < b, "primary must dominate the byte ordering");
    assert_eq!(decode_key(&a), Some((LogIndex(1), Hlc(u64::MAX))));
    assert_eq!(
        encode_primary(LogIndex(0xDEAD_BEEF)),
        0xDEAD_BEEFu64.to_be_bytes()
    );
}

/// The value carries the `data_id` the key no longer does.
#[test]
fn replica_value_roundtrips() {
    let id = DataId::from_parts(Hlc(42), NodeId::new_v4(), 7);
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&id.0);
    encoded.extend_from_slice(b"payload");
    assert_eq!(decode_value(&encoded), Some((id, &b"payload"[..])));
    // An empty blob is a legal record; a short value is not a value at all.
    assert_eq!(decode_value(&id.0), Some((id, &[][..])));
    assert_eq!(decode_value(&[0u8; VALUE_PREFIX_WIDTH - 1]), None);
}

#[test]
fn replica_key_decode_rejects_wrong_length() {
    assert_eq!(decode_key(&[]), None);
    assert_eq!(decode_key(&[0u8; 4]), None);
    // Exactly `encode_primary`'s width (`PRIMARY_WIDTH`) — the shape a
    // seek bound actually has, so it's the more interesting rejection
    // case than an arbitrary short length.
    assert_eq!(decode_key(&[0u8; 8]), None);
    assert_eq!(decode_key(&[0u8; KEY_WIDTH - 1]), None);
}

/// Purge is a `DeleteRange` over encoded bounds, so a byte compare has to
/// be the `Ord` compare or retirement covers the wrong records.
#[test]
fn replica_key_order_matches_tuple_order() {
    use proptest::prelude::*;

    proptest!(|(
        (i1, h1) in (any::<u64>(), any::<u64>()),
        (i2, h2) in (any::<u64>(), any::<u64>()),
    )| {
        let k1 = encode_key(LogIndex(i1), Hlc(h1));
        let k2 = encode_key(LogIndex(i2), Hlc(h2));
        prop_assert_eq!(k1.cmp(&k2), (i1, h1).cmp(&(i2, h2)));
    });
}
