//! Stable-log tests.

use zslg_log::LogError;

use crossbeam_combinators::{Channel, Finite};
use zslg_log::LogIndex;
use zslg_log::{Data, SnapshotEntry, StableLogEvent};
use zslg_time::Hlc;

use super::*;
use crate::appender::STABLE_LOAD_BATCH;
use crate::engine::index_is_live;
use crate::key::{
    KEY_WIDTH, PRIMARY_WIDTH, decode_key, encode_key, encode_primary, encode_secondary,
};
use crossbeam_combinators::testing::ChannelTestExt as _;
use zslg_log::StableLogSnapshot;
use zslg_log_fake::make_data;

/// Build a closed `Channel` pre-loaded with `items` — test convenience
/// for the channel-based [`StableLogSnapshot::append`] signature (mirrors how
/// `integrate_manifest`'s decode worker feeds the channel, then drops its
/// sender at end-of-stream).
fn rx_of(
    items: impl IntoIterator<Item = Result<(LogIndex, Data), Error>>,
) -> Channel<Result<SnapshotEntry, Error>, Finite> {
    Channel::from_vec(
        items
            .into_iter()
            .map(|item| item.map(|(global_index, data)| SnapshotEntry { global_index, data }))
            .collect(),
    )
}

/// Open a [`StableLog`] rooted at `dir` with the default sizing.
async fn open_at(dir: std::path::PathBuf) -> StableLog {
    StableLog::open_engine(dir).await.expect("open stable log")
}

/// A terminal error arriving in the **same** `ready_batch` as good entries
/// must not discard them.
///
/// `ready_batch` boundaries are arbitrary: whether the error lands beside the
/// entries before it or in the next batch is a producer/consumer race, so
/// returning early made how much progress a load makes depend on that race —
/// which is what made `snapshot_loader::integrate_rejects_gap_between_snapshots`
/// flaky under load. `from_vec` puts everything in one batch, so this
/// reproduces the losing side deterministically.
#[tokio::test]
async fn error_in_the_same_batch_still_commits_the_prefix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = open_at(dir.path().join("stable")).await;

    let err = log
        .append(
            (rx_of([
                Ok((LogIndex(0), make_data(1, 100, b"a"))),
                Ok((LogIndex(1), make_data(2, 200, b"b"))),
                Err(Error::Log(LogError::StableLogGap(
                    "archive stream: expected index 5".into(),
                ))),
            ]))
            .err_into(),
        )
        .await
        .expect_err("the terminal error must still surface");
    assert!(
        matches!(err, zslg_log::Error::Log(LogError::StableLogGap(_))),
        "expected the original error, got: {err:?}"
    );
    assert_eq!(
        log.end_index(),
        Some(LogIndex(1)),
        "the contiguous prefix before the error must be durable"
    );
}

#[tokio::test]
async fn append_advances_frontier_and_emits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = open_at(dir.path().join("stable")).await;
    assert!(log.end_index().is_none());

    let mut rx = log.subscribe_events();
    let d = make_data(1, 100, b"a");
    log.append(rx_of([Ok((LogIndex(0), d.clone()))]).err_into())
        .await
        .expect("ok");
    assert_eq!(log.end_index(), Some(LogIndex(0)));

    let event = rx
        .recv()
        .expect("the awaited append above queued this event");
    match event {
        StableLogEvent::Appended { index, data } => {
            assert_eq!(index, LogIndex(0));
            assert_eq!(data, d);
        }
        other => panic!("expected Appended event, got {other:?}"),
    }
}

/// Index order == HLC order in the stable log: entries are index-contiguous
/// and HLC-ascending in lockstep, so an HLC-keyed retire threshold drops
/// exactly an index prefix. This is the invariant the purge policy leans on.
#[tokio::test]
async fn invariant_stable_log_index_order_is_hlc_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = open_at(dir.path().join("stable")).await;

    for (idx, hlc) in [(0u64, 100u64), (1, 150), (2, 200), (3, 275), (4, 300)] {
        log.append(
            (rx_of([Ok((
                LogIndex(idx),
                make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
            ))]))
            .err_into(),
        )
        .await
        .expect("append");
    }

    let all: Vec<(u64, u64)> = log
        .range(None, LogIndex(5))
        .items()
        .filter_map(|r| r.ok().map(|(i, d)| (i.raw(), d.data_id.hlc().raw())))
        .collect();
    // Strictly ascending in both index and HLC, in lockstep.
    assert_eq!(all, vec![(0, 100), (1, 150), (2, 200), (3, 275), (4, 300)]);

    // An HLC threshold drops exactly the index prefix at or below it.
    log.set_purge_threshold(Hlc(200)).expect("purge");
    let kept: Vec<u64> = log
        .range(None, LogIndex(5))
        .items()
        .filter_map(|r| r.ok().map(|(i, _)| i.raw()))
        .collect();
    assert_eq!(kept, vec![3, 4], "hlc<=200 → index prefix 0,1,2 dropped");
}

/// The purge threshold is always clamped strictly below the frontier entry's
/// HLC, so even a threshold of `Hlc::MAX` keeps the frontier — the recovery
/// anchor `max_key`/`end_index` reads from — and it survives a reopen.
#[tokio::test]
async fn stable_purge_keeps_frontier_even_under_full_coverage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("stable");
    {
        let log = open_at(path.clone()).await;
        for (idx, hlc) in [(0u64, 100u64), (1, 200), (2, 300)] {
            log.append(
                (rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .err_into(),
            )
            .await
            .expect("append");
        }

        // A threshold that covers every entry must still spare the frontier.
        log.set_purge_threshold(Hlc(u64::MAX)).expect("purge");
        assert_eq!(
            log.end_index(),
            Some(LogIndex(2)),
            "frontier index is unaffected by purge",
        );
        assert!(
            index_is_live(&log.storage, LogIndex(2)).expect("probe"),
            "frontier entry is not clamped away",
        );
    }

    // Reopen: the frontier must still recover — the purge already
    // durably deleted the covered prefix, and the frontier run was never
    // covered in the first place.
    let reopened = open_at(path).await;
    assert_eq!(reopened.end_index(), Some(LogIndex(2)));
    // Just below the frontier entry: its HLC group may be partial.
    assert_eq!(reopened.end_hlc(), Some(Hlc(299)));
}

/// After physical retirement (set a max threshold, seal + maintain to
/// sweep), the frontier entry — the recovery anchor `max_key`/`end_index`
/// reads from — is still present: the threshold is clamped below it.
#[tokio::test]
async fn frontier_survives_after_retired_runs_are_swept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("stable");
    {
        let log = open_at(path.clone()).await;
        for (idx, hlc) in [(0u64, 100u64), (1, 200), (2, 300)] {
            log.append(
                (rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .err_into(),
            )
            .await
            .expect("append");
        }
        // The range delete is durable and fenced synchronously — no
        // separate compaction step needed to physically retire the runs.
        log.set_purge_threshold(Hlc(u64::MAX)).expect("purge");
    }

    // Reopen: the frontier recovers even though all non-frontier
    // entries were physically swept.
    let reopened = open_at(path).await;
    assert_eq!(reopened.end_index(), Some(LogIndex(2)));
    assert_eq!(reopened.end_hlc(), Some(Hlc(299)));
    assert!(
        index_is_live(&reopened.storage, LogIndex(2)).expect("probe"),
        "frontier entry survives physical retirement",
    );
}

/// Purge is durable, so the prefix stays gone across a reopen without
/// anything re-establishing a clamp first. This replaces
/// `arm_from_watermark_reclamps_prefix_after_open`, which pinned the
/// opposite requirement: that an in-memory clamp be re-armed
/// synchronously, before the reader existed, or `learner::read` would
/// serve a purged prefix as live.
#[tokio::test]
async fn purged_prefix_stays_gone_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("stable");
    {
        let log = open_at(path.clone()).await;
        for (idx, hlc) in [(0u64, 100u64), (1, 101), (2, 102)] {
            log.append(
                (rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .err_into(),
            )
            .await
            .expect("append");
        }
        log.set_purge_threshold(Hlc(101)).expect("purge");
    }

    let reopened = open_at(path).await;
    let live: Vec<u64> = reopened
        .range(None, LogIndex(3))
        .items()
        .filter_map(|r| r.ok().map(|(i, _)| i.raw()))
        .collect();
    assert_eq!(
        live,
        vec![2],
        "indices 0..=1 were purged and must not come back after a reopen"
    );
    assert_eq!(
        reopened.end_index(),
        Some(LogIndex(2)),
        "the frontier entry is never covered by a purge"
    );
}

#[tokio::test]
async fn loaded_hlc_advances_with_append_and_persists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stable = dir.path().join("stable");
    let log = open_at(stable.clone()).await;
    assert!(log.end_hlc().is_none());

    let mut rx = log.subscribe_events();
    log.append(rx_of([Ok((LogIndex(0), make_data(0, 123, b"a")))]).err_into())
        .await
        .expect("ok");
    assert_eq!(log.end_hlc(), Some(Hlc(122)));
    // The broadcast reflects the watermark after the entries.
    // Skip the Appended event first, then read the Watermark.
    loop {
        let ev = rx
            .recv()
            .expect("the awaited append above queued this event");
        match ev {
            StableLogEvent::Watermark(hlc) => {
                assert_eq!(hlc, Hlc(122));
                break;
            }
            _ => continue,
        }
    }

    drop(log);
    let log = open_at(stable).await;
    assert_eq!(
        log.end_hlc(),
        Some(Hlc(122)),
        "watermark re-derived from the recovered log tail"
    );
}

/// The append stream is HLC-sorted by construction; feeding an unsorted
/// stream is refused with `Error::StableLogGap` at the point of use
/// instead of silently committing a watermark that recovery would derive
/// differently.
#[tokio::test]
async fn append_rejects_unsorted_hlc_stream() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = open_at(dir.path().join("stable")).await;

    log.append(rx_of([Ok((LogIndex(0), make_data(0, 200, b"a")))]).err_into())
        .await
        .expect("ok");
    assert_eq!(log.end_hlc(), Some(Hlc(199)));

    // HLC 100 after 200 violates the sorted-stream contract.
    let err = log
        .append(rx_of([Ok((LogIndex(1), make_data(1, 100, b"b")))]).err_into())
        .await
        .expect_err("unsorted stream must be refused");
    assert!(
        matches!(err, zslg_log::Error::Log(LogError::StableLogGap(ref m)) if m.contains("HLC-sorted")),
        "expected a StableLogGap naming the HLC ordering violation, got: {err:?}"
    );
    assert_eq!(log.end_hlc(), Some(Hlc(199)), "watermark unchanged");
}

/// `record_end_hlc` advances `end_hlc` above the last appended HLC
/// (watermark-only advance) — in memory only: a reopen re-derives the
/// watermark from the log tail, and the manifest event re-raises it.
#[tokio::test]
async fn loaded_hlc_advances_past_appended_via_record_loaded_up_to() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stable = dir.path().join("stable");
    let log = open_at(stable.clone()).await;

    log.append(rx_of([Ok((LogIndex(0), make_data(0, 200, b"a")))]).err_into())
        .await
        .expect("ok");
    assert_eq!(log.end_hlc(), Some(Hlc(199)));

    // Watermark-only advance past the appended data.
    log.record_end_hlc(Hlc(5000)).await.expect("record");
    assert_eq!(log.end_hlc(), Some(Hlc(5000)));

    // The empty-window advance is deliberately NOT durable: after a
    // restart the watermark reflects only local entries — it must never
    // claim coverage the log cannot back (the manifest event re-raises
    // it once integration confirms the coverage).
    drop(log);
    let log = open_at(stable).await;
    assert_eq!(log.end_hlc(), Some(Hlc(199)));
}

/// Contiguous ingest fence (docs/STABLE_LOG_GAP_FIX.md): an input stream that
/// JUMPS past `frontier + 1` must be rejected before anything commits — a
/// committed gap would be hidden forever by the manifest-integration skip.
#[tokio::test]
async fn invariant_append_rejects_gap_in_global_indices() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = open_at(dir.path().join("stable")).await;
    log.append(
        (rx_of([
            Ok((LogIndex(0), make_data(1, 100, b"a"))),
            Ok((LogIndex(1), make_data(2, 200, b"b"))),
        ]))
        .err_into(),
    )
    .await
    .expect("contiguous prefix ok");

    // Index 3 with no index 2 — must be refused, nothing committed.
    let err = log
        .append(rx_of([Ok((LogIndex(3), make_data(4, 400, b"d")))]).err_into())
        .await
        .expect_err("gap must be rejected");
    assert!(
        matches!(err, zslg_log::Error::Log(LogError::StreamTruncated { ref what }) if what.contains("stable log loader")),
        "typed gap error expected, got: {err}"
    );

    assert_eq!(log.end_index(), Some(LogIndex(1)), "frontier unchanged");
    let entries: Vec<(LogIndex, Data)> = log
        .range(None, LogIndex(10))
        .items()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(entries.len(), 2, "the gapped entry must not be committed");
}

/// Re-delivering entries at or below the frontier is legitimate (a re-read
/// of a partially-loaded snapshot overlaps what is already present): the
/// overlap is skipped idempotently and the stream extends the log from
/// `frontier + 1` onwards.
#[tokio::test]
async fn append_tolerates_overlapping_reappend() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = open_at(dir.path().join("stable")).await;
    log.append(
        (rx_of([
            Ok((LogIndex(0), make_data(1, 100, b"a"))),
            Ok((LogIndex(1), make_data(2, 200, b"b"))),
            Ok((LogIndex(2), make_data(3, 300, b"c"))),
        ]))
        .err_into(),
    )
    .await
    .expect("prefix");

    // Overlapping resume: 1..=2 already present (skipped), 3..=4 appended.
    let n = log
        .append(
            (rx_of([
                Ok((LogIndex(1), make_data(2, 200, b"b"))),
                Ok((LogIndex(2), make_data(3, 300, b"c"))),
                Ok((LogIndex(3), make_data(4, 400, b"d"))),
                Ok((LogIndex(4), make_data(5, 500, b"e"))),
            ]))
            .err_into(),
        )
        .await
        .expect("overlapping re-append is legitimate");
    assert_eq!(n, 2, "only the genuinely-new entries count as appended");
    assert_eq!(log.end_index(), Some(LogIndex(4)));

    let entries: Vec<(LogIndex, Data)> = log
        .range(None, LogIndex(10))
        .items()
        .filter_map(|r| r.ok())
        .collect();
    let indices: Vec<u64> = entries.iter().map(|(i, _)| i.raw()).collect();
    assert_eq!(indices, vec![0, 1, 2, 3, 4]);
}

/// `record_end_hlc` must broadcast a `Watermark` event only on a genuine
/// advance — a redundant re-record or a rejected regression emits nothing.
#[tokio::test]
async fn record_end_hlc_emits_watermark_only_on_advance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = open_at(dir.path().join("stable")).await;
    let mut rx = log.subscribe_events();

    log.record_end_hlc(Hlc(100)).await.expect("first advance");
    let ev = rx
        .recv()
        .expect("the awaited append above queued this event");
    assert!(matches!(ev, StableLogEvent::Watermark(Hlc(100))));

    // Redundant re-record: Ok, but no event.
    log.record_end_hlc(Hlc(100)).await.expect("redundant ok");
    // Regression: rejected, no event.
    log.record_end_hlc(Hlc(50))
        .await
        .expect_err("regression rejected");

    // The very next event must be the next genuine advance — nothing was
    // queued for the redundant or regressed records.
    log.record_end_hlc(Hlc(200)).await.expect("second advance");
    let ev = rx
        .recv()
        .expect("the awaited append above queued this event");
    assert!(matches!(ev, StableLogEvent::Watermark(Hlc(200))));
}

/// Startup integrity: a foreign/manufactured WAL file in the log dir must
/// NOT be silently accepted. The stable log is re-derivable from
/// object-store snapshots, but it is still opened as a durability source
/// of truth — so instead of self-healing by discarding (which would hide
/// the bug that produced the file), recovery REFUSES to open, and the
/// corrupt file is preserved for forensics.
#[tokio::test]
async fn startup_refuses_open_on_corrupt_committed_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stable_dir = dir.path().join("stable");
    {
        let log = open_at(stable_dir.clone()).await;
        log.append(
            (rx_of([
                Ok((LogIndex(0), make_data(1, 100, b"a"))),
                Ok((LogIndex(1), make_data(2, 200, b"b"))),
            ]))
            .err_into(),
        )
        .await
        .expect("prefix");
    }

    // Manufacture the corruption: the DB's MANIFEST, the committed-state
    // record RocksDB validates at open. The claim is CLAUDE.md §8 —
    // genuine corruption of committed state refuses to open rather than
    // self-healing by discard. The DB root is the log's own directory.
    let db_root = stable_dir.as_path();
    let manifest = std::fs::read_dir(db_root)
        .expect("read db root")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("MANIFEST-"))
        })
        .expect("a MANIFEST must exist once the db has been opened");
    std::fs::write(&manifest, b"not-a-manifest").expect("corrupt the manifest");

    let err = match StableLog::open_engine(stable_dir.clone()).await {
        Ok(_) => panic!("corrupt committed state must refuse to open, not be absorbed"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("open rocksdb"),
        "the error must name the failing open: {err}"
    );
    assert!(
        manifest.exists(),
        "the corrupt file is preserved for forensics"
    );
}

#[tokio::test]
async fn range_returns_in_index_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = open_at(dir.path().join("stable")).await;
    for i in 0..5u8 {
        log.append(
            (rx_of([Ok((
                LogIndex(u64::from(i)),
                make_data(i, 100 + u64::from(i), &[i]),
            ))]))
            .err_into(),
        )
        .await
        .expect("ok");
    }
    let entries: Vec<(zslg_log::LogIndex, zslg_log::Data)> = log
        .range(None, LogIndex(5))
        .items()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(entries.len(), 5);
    for (n, (idx, _)) in entries.iter().enumerate() {
        assert_eq!(idx.raw(), u64::try_from(n).expect("fits"));
    }
}

// ── key codec ───────────────────────────────────────────────────────

use proptest::prelude::{any, prop_assert_eq, proptest};

#[test]
fn stable_key_roundtrips_and_primary_dominates() {
    let idx = LogIndex(0xDEAD_BEEF);
    let hlc = Hlc(0xCAFE_BABE);
    let k = encode_key(idx, hlc);
    assert_eq!(k.len(), KEY_WIDTH);
    assert_eq!(decode_key(&k), Some((idx, hlc)));
    // The primary half is the seek prefix: a key's first PRIMARY_WIDTH
    // bytes are exactly `encode_primary`, which is what makes an index
    // seek land on the right entry.
    assert_eq!(&k[..PRIMARY_WIDTH], &encode_primary(idx)[..]);
    assert_eq!(&k[PRIMARY_WIDTH..], &encode_secondary(hlc)[..]);
}

/// Retirement is a `DeleteRange` over **encoded bytes**, so a byte compare
/// has to be the `Ord` compare or `set_purge_threshold` drops the wrong
/// records — the same property `learner::read`'s object-store fallback
/// relies on to know which reads fall below the purged prefix
/// (CLAUDE.md §8).
#[test]
fn stable_key_order_matches_tuple_order() {
    proptest!(|(
        (i1, h1) in (any::<u64>(), any::<u64>()),
        (i2, h2) in (any::<u64>(), any::<u64>()),
    )| {
        let k1 = encode_key(LogIndex(i1), Hlc(h1));
        let k2 = encode_key(LogIndex(i2), Hlc(h2));
        prop_assert_eq!(k1.cmp(&k2), (i1, h1).cmp(&(i2, h2)));
    });
}

/// The secondary encoding is what the retire threshold is expressed in, so
/// its byte order must be the `Hlc` order on its own too.
#[test]
fn stable_secondary_order_matches_hlc_order() {
    proptest!(|(a in any::<u64>(), b in any::<u64>())| {
        prop_assert_eq!(
            encode_secondary(Hlc(a)).cmp(&encode_secondary(Hlc(b))),
            a.cmp(&b),
        );
    });
}

#[test]
fn stable_key_decode_rejects_wrong_length() {
    assert_eq!(decode_key(&[0u8; 4]), None);
    assert_eq!(decode_key(&[]), None);
}

/// Entries sharing an HLC can straddle a load batch: `ready_batch` cuts
/// wherever it cuts. A `Watermark` claims everything at or below it is
/// loaded, so none may cover the group's HLC until the whole group is in —
/// otherwise the unloaded rest is in neither half of a blended read (the
/// unstable tail starts strictly above the watermark).
#[tokio::test]
async fn a_watermark_never_covers_an_hlc_group_split_across_batches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = crate::testing::open_for_test(&dir.path().join("stable")).expect("storage");
    // Deep enough to hold every event of the load without a hang-up.
    let log = StableLog::open_sized(storage, bytesize::nonzero(1024))
        .await
        .expect("open");
    let mut events = log.subscribe_events();

    // One more entry than a batch holds: the first batch ends on the
    // group's first entry, the second batch carries the rest of it.
    let batch = u64::try_from(STABLE_LOAD_BATCH).expect("fits");
    let group_hlc = 5_000u64;
    let hlc_of = |i: u64| {
        if i.saturating_add(1) >= batch {
            group_hlc
        } else {
            1_000u64.saturating_add(i)
        }
    };
    let entries: Vec<_> = (0..=batch)
        .map(|i| {
            let seed = u8::try_from(i.checked_rem(200).expect("nonzero")).expect("fits");
            Ok((LogIndex(i), make_data(seed, hlc_of(i), b"")))
        })
        .collect();
    log.append(rx_of(entries).err_into()).await.expect("append");

    // The fan-out delivers on its own thread, so `append` returning does not
    // mean the events have arrived: wait for every entry (with a deadline)
    // rather than draining with `try_recv` once.
    let want = usize::try_from(batch).expect("fits").saturating_add(1);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut emitted: Vec<Hlc> = Vec::new();
    let mut watermarks = 0usize;
    while emitted.len() < want {
        assert!(
            std::time::Instant::now() < deadline,
            "only {} of {want} entries arrived",
            emitted.len()
        );
        let Some(event) = events.try_recv() else {
            std::thread::sleep(std::time::Duration::from_millis(1));
            continue;
        };
        match event {
            StableLogEvent::Appended { data, .. } => emitted.push(data.data_id.hlc()),
            StableLogEvent::Watermark(wm) => {
                watermarks = watermarks.saturating_add(1);
                let owed = (0..=batch).filter(|&i| hlc_of(i) <= wm.raw()).count();
                assert!(
                    emitted.len() >= owed,
                    "Watermark({wm:?}) went out after only {} of the {owed} entries at or \
                     below it",
                    emitted.len(),
                );
            }
        }
    }
    assert!(watermarks > 0, "the load must have advanced the watermark");
    assert_eq!(
        emitted.len(),
        usize::try_from(batch).expect("fits").saturating_add(1)
    );
    assert!(
        log.end_hlc() < Some(Hlc(group_hlc)),
        "the group is settled by the manifest's `record_end_hlc`, not by append"
    );

    log.record_end_hlc(Hlc(group_hlc)).await.expect("record");
    assert_eq!(log.end_hlc(), Some(Hlc(group_hlc)));
}
