//! The stable tail and the stable-log subscription: history, live, resync, and holes filled from the archive.

use super::*;

#[tokio::test]
async fn stable_tail_emits_historical_then_live() {
    let dir = tempfile::tempdir().expect("tempdir");
    let local_path = dir.path().join("local.db");
    let stable_log = Arc::new(FakeStableLog::new());
    let local_log = crate::testing::local_store_at(local_path.with_extension("stable"))
        .await
        .expect("from_db");

    stable_log
        .append(rx_of([Ok((LogIndex(0), make_data(1, 100, b"a")))]).err_into())
        .await
        .expect("append");
    stable_log
        .append(rx_of([Ok((LogIndex(1), make_data(2, 200, b"b")))]).err_into())
        .await
        .expect("append");

    let reader = local_reader(
        local_log.clone(),
        stable_log.clone(),
        Duration::from_millis(5),
    );
    let mut stream = reader
        .subscribe_stable_log(None)
        .into_channel()
        .filter_map(|item| match item {
            Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
            Ok(StableLogEvent::Watermark(_)) => None,
            Err(e) => Some(Err(e)),
        })
        .into_test_stream();

    let (i0, d0) = stream.next().await.expect("item0").expect("ok");
    assert_eq!(i0, LogIndex(0));
    assert_eq!(d0.data_id.hlc(), Hlc(100));

    let (i1, d1) = stream.next().await.expect("item1").expect("ok");
    assert_eq!(i1, LogIndex(1));
    assert_eq!(d1.data_id.hlc(), Hlc(200));

    stable_log
        .append(rx_of([Ok((LogIndex(2), make_data(3, 300, b"c")))]).err_into())
        .await
        .expect("append");
    let live = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("recv timeout")
        .expect("item")
        .expect("ok");
    assert_eq!(live.0, LogIndex(2));
    assert_eq!(live.1.data_id.hlc(), Hlc(300));
}

#[tokio::test]
async fn stable_tail_filters_below_from() {
    let dir = tempfile::tempdir().expect("tempdir");
    let local_path = dir.path().join("local.db");
    let stable_log = Arc::new(FakeStableLog::new());
    let local_log = crate::testing::local_store_at(local_path.with_extension("stable"))
        .await
        .expect("from_db");

    for (idx, hlc) in [(0u64, 100u64), (1, 200), (2, 300)] {
        stable_log
            .append(
                (rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .err_into(),
            )
            .await
            .expect("append");
    }

    let reader = local_reader(local_log, stable_log.clone(), Duration::from_millis(5));

    let mut stream = reader
        .subscribe_stable_log(Some(LogIndex(0)))
        .into_channel()
        .filter_map(|item| match item {
            Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
            Ok(StableLogEvent::Watermark(_)) => None,
            Err(e) => Some(Err(e)),
        })
        .into_test_stream();
    let mut hlcs = Vec::new();
    for _ in 0..2 {
        let item = stream.next().await.expect("item").expect("ok");
        hlcs.push(item.1.data_id.hlc().raw());
    }
    assert_eq!(hlcs, vec![200, 300]);
}

/// After the local prefix is purged, `stable_tail` transparently serves the
/// purged indices from object-store snapshots and the retained suffix from
/// the local log — one seamless, gapless, index-ordered stream (design
/// doc §794). The split is discovered by peeking the first index the local
/// scan yields, not by reading a separate purge-floor value.
#[tokio::test]
async fn stable_tail_serves_purged_prefix_from_object_store() {
    use zslg_consensus::RaftManifestState;
    use zslg_log::SnapshotEntry;
    use zslg_snapshot::SnapshotId;
    use zslg_snapshot::SnapshotMetadata;

    let dir = tempfile::tempdir().expect("tempdir");
    let stable_log = Arc::new(FakeStableLog::new());
    let local_log = crate::testing::local_store_at(dir.path().join("local.stable"))
        .await
        .expect("from_db");

    // Append indices 0..=4 with HLCs 100..=104.
    for (idx, hlc) in [(0u64, 100u64), (1, 101), (2, 102), (3, 103), (4, 104)] {
        stable_log
            .append(
                (rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .err_into(),
            )
            .await
            .expect("append");
    }

    // Purge everything at or below HLC 102 → indices 0,1,2 gone locally
    // (the engine clamps them out of `range` immediately); 3,4 retained.
    stable_log.set_purge_threshold(Hlc(102)).expect("purge");
    let local_only: Vec<u64> = stable_log
        .range(None, LogIndex(5))
        .items()
        .map(|r| r.expect("ok").0.raw())
        .collect();
    assert_eq!(
        local_only,
        vec![3, 4],
        "local range clamps the purged prefix"
    );

    // Object store holds a snapshot covering the full 0..=4 range.
    let snap_id = SnapshotId::from_u128(1);
    let store = Arc::new(ScriptedSnapshotStore::default());
    store.script(
        snap_id,
        [(0u64, 100u64), (1, 101), (2, 102), (3, 103), (4, 104)]
            .into_iter()
            .map(|(idx, hlc)| SnapshotEntry {
                global_index: LogIndex(idx),
                data: make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
            })
            .collect(),
    );
    let handle = Arc::new(RaftStateHandle::default());
    let mut rs = zslg_consensus::RaftState::empty();
    rs.manifest_state = RaftManifestState {
        manifest: vec![SnapshotMetadata {
            start_hlc: Hlc(100),
            end_hlc: Hlc(104),
            start_index: LogIndex(0),
            end_index: LogIndex(4),
            snapshot_id: snap_id,
        }],
        manifest_watermark: Some(Hlc(104)),
        hlc_cutoff: Some(Hlc(104)),
    };
    handle.replace(rs);

    let reader = reader_with_source(
        local_log,
        stable_log.clone(),
        Duration::from_millis(5),
        store,
        handle,
    );

    // The caller asks from index 0 and gets a contiguous 0..=4: 0,1,2 from
    // object store (purged locally → the scan starts at 3, so [0,3) is
    // filled from the archive), 3,4 from the local log.
    let mut stream = reader
        .subscribe_stable_log(None)
        .into_channel()
        .filter_map(|item| match item {
            Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
            Ok(StableLogEvent::Watermark(_)) => None,
            Err(e) => Some(Err(e)),
        })
        .into_test_stream();
    let mut got: Vec<(u64, u64)> = Vec::new();
    for _ in 0..5 {
        let (idx, data) = stream.next().await.expect("item").expect("ok");
        got.push((idx.raw(), data.data_id.hlc().raw()));
    }
    assert_eq!(
        got,
        vec![(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)],
        "purged prefix + local suffix merge seamlessly and in order",
    );
}

/// A consumer slower than the broadcast ring must not see the lag. The
/// ring is 4 deep and 12 entries are appended while the stream is not
/// polled, so the subscriber is lapped several times over; the stream must
/// still deliver 0..=11 exactly once each, in order, with no error — it
/// re-enters historical replay from its own cursor instead of surfacing
/// `RecvError::Lagged`.
#[tokio::test]
async fn subscribe_stable_log_replays_instead_of_lagging() {
    let log = Arc::new(ScriptedStableLog::with_broadcast_capacity(
        vec![(LogIndex(0), make_data(0, 100, b"0"))],
        4,
    ));
    let (_dir, reader) = scripted_reader(
        log.clone(),
        Arc::new(ScriptedSnapshotStore::default()),
        Arc::new(RaftStateHandle::default()),
    )
    .await;

    let mut stream = reader.subscribe_stable_log(None).into_test_stream();

    // Lap the ring many times over without polling the stream.
    for i in 1u64..12 {
        log.push(LogIndex(i), make_data(1, 100 + i, b"x"));
    }

    let mut seen: Vec<u64> = Vec::new();
    while seen.len() < 12 {
        let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended early")
            .expect("a lagged consumer must never see an error");
        if let zslg_log::StableLogEvent::Appended { index, .. } = ev {
            seen.push(index.raw());
        }
    }
    assert_eq!(
        seen,
        (0..12).collect::<Vec<u64>>(),
        "every index exactly once, in order, across the resync"
    );
}

/// A `Watermark` lost to a hang-up must not stay lost. It is published
/// *after* the fan-out has already dropped this subscriber, so the only way
/// the consumer can learn it is the next cycle re-reading it from the log —
/// which it must, or a consumer that fell behind once would sit on a stale
/// watermark for as long as the log stayed quiet.
#[tokio::test]
async fn a_watermark_lost_to_a_hang_up_is_redelivered_by_the_next_cycle() {
    let log = Arc::new(ScriptedStableLog::with_broadcast_capacity(
        vec![(LogIndex(0), make_data(0, 100, b"0"))],
        4,
    ));
    let (_dir, reader) = scripted_reader(
        log.clone(),
        Arc::new(ScriptedSnapshotStore::default()),
        Arc::new(RaftStateHandle::default()),
    )
    .await;

    let mut stream = reader.subscribe_stable_log(None).into_test_stream();

    // Overrun the 4-deep queue, so the subscriber is hung up on ...
    for i in 1u64..8 {
        log.push(LogIndex(i), make_data(1, 100 + i, b"x"));
    }
    // ... and only then publish the watermark, which therefore reaches
    // nobody: it exists solely in the log's state.
    log.push_watermark(Hlc(5000));

    let mut appended = 0usize;
    let mut watermarks: Vec<Hlc> = Vec::new();
    for _ in 0..20 {
        let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended early")
            .expect("a consumer that fell behind must never see an error");
        match ev {
            zslg_log::StableLogEvent::Appended { .. } => appended += 1,
            zslg_log::StableLogEvent::Watermark(hlc) => watermarks.push(hlc),
        }
        if appended == 8 && watermarks.iter().any(|hlc| *hlc >= Hlc(5000)) {
            return;
        }
    }
    panic!(
        "expected all 8 entries and a watermark >= 5000; got {appended} entries \
         and {watermarks:?}"
    );
}

/// The same guarantee on the other public entry point: `stable_tail` is
/// the entry-only view of the same stream, so a consumer that falls behind
/// gets one gapless index-ordered run, not an error.
#[tokio::test]
async fn stable_tail_replays_instead_of_erroring_when_it_falls_behind() {
    let log = Arc::new(ScriptedStableLog::with_broadcast_capacity(
        vec![(LogIndex(0), make_data(0, 100, b"0"))],
        4,
    ));
    let (_dir, reader) = scripted_reader(
        log.clone(),
        Arc::new(ScriptedSnapshotStore::default()),
        Arc::new(RaftStateHandle::default()),
    )
    .await;

    let mut stream = reader
        .subscribe_stable_log(None)
        .into_channel()
        .filter_map(|item| match item {
            Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
            Ok(StableLogEvent::Watermark(_)) => None,
            Err(e) => Some(Err(e)),
        })
        .into_test_stream();
    for i in 1u64..12 {
        log.push(LogIndex(i), make_data(1, 100 + i, b"x"));
    }

    let mut seen: Vec<u64> = Vec::new();
    while seen.len() < 12 {
        let (index, _) = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended early")
            .expect("a consumer that fell behind must never see an error");
        seen.push(index.raw());
    }
    assert_eq!(seen, (0..12).collect::<Vec<u64>>());
}

/// After a cycle the stream must be genuinely live again, not one append
/// away from being hung up on afresh. Each cycle takes a *new* subscription
/// with an empty queue, so it starts with the full depth of headroom; the
/// old one is gone (that is what a hang-up is). The entries look identical
/// however often it cycles, because the cursor filter hides the duplicate
/// work — so what this asserts is the cost: one replay per hang-up and not
/// one more.
#[tokio::test]
async fn a_resync_settles_back_into_live_delivery() {
    use std::sync::atomic::Ordering;

    let log = Arc::new(ScriptedStableLog::with_broadcast_capacity(
        vec![(LogIndex(0), make_data(0, 100, b"0"))],
        2,
    ));
    let (_dir, reader) = scripted_reader(
        log.clone(),
        Arc::new(ScriptedSnapshotStore::default()),
        Arc::new(RaftStateHandle::default()),
    )
    .await;

    let mut stream = reader.subscribe_stable_log(None).into_test_stream();

    // Lap the 2-deep ring, then drain everything it dropped.
    for i in 1u64..6 {
        log.push(LogIndex(i), make_data(1, 100 + i, b"x"));
    }
    let mut seen = 0usize;
    while seen < 6 {
        let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended early")
            .expect("no error");
        if matches!(ev, zslg_log::StableLogEvent::Appended { .. }) {
            seen += 1;
        }
    }
    let after_lag = log.ranges.load(Ordering::Relaxed);
    assert_eq!(after_lag, 2, "the opening cycle plus one for the lag");

    // A later append must arrive over the live tail, with no further replay.
    log.push(LogIndex(6), make_data(1, 200, b"y"));
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended early")
            .expect("no error");
        if let zslg_log::StableLogEvent::Appended { index, .. } = ev {
            assert_eq!(index, LogIndex(6));
            break;
        }
    }
    assert_eq!(
        log.ranges.load(Ordering::Relaxed),
        after_lag,
        "a live append after a resync must not trigger another replay — \
         the resync left the receiver at the ring's head, not behind it"
    );
}

/// A hole in the *interior*, not just a missing leading prefix: the local
/// scan yields 0,1 then 4,5 (2,3 retired mid-stream) while the archive holds
/// the full range. `stable_tail` must fill the interior 2,3 from object store
/// and deliver one contiguous, in-order 0..=5. Splitting on a single purge
/// floor cannot serve this; discovering the gap from the data can.
#[tokio::test]
async fn stable_tail_fills_interior_hole_from_object_store() {
    use zslg_log::SnapshotEntry;
    use zslg_snapshot::SnapshotId;

    let local: Vec<(LogIndex, Data)> = [(0u64, 100u64), (1, 101), (4, 104), (5, 105)]
        .into_iter()
        .map(|(i, h)| {
            (
                LogIndex(i),
                make_data(u8::try_from(i + 1).expect("fits"), h, b""),
            )
        })
        .collect();
    let stable_log = Arc::new(ScriptedStableLog::new(local));

    let snap_id = SnapshotId::from_u128(1);
    let store = Arc::new(ScriptedSnapshotStore::default());
    store.script(
        snap_id,
        (0..=5u64)
            .map(|i| SnapshotEntry {
                global_index: LogIndex(i),
                data: make_data(u8::try_from(i + 1).expect("fits"), 100 + i, b""),
            })
            .collect(),
    );
    let handle = Arc::new(RaftStateHandle::default());
    handle.replace(covering_manifest(snap_id, 5));

    let (_dir, reader) = scripted_reader(stable_log, store, handle).await;
    let mut stream = reader
        .subscribe_stable_log(None)
        .into_channel()
        .filter_map(|item| match item {
            Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
            Ok(StableLogEvent::Watermark(_)) => None,
            Err(e) => Some(Err(e)),
        })
        .into_test_stream();
    let mut got: Vec<(u64, u64)> = Vec::new();
    for _ in 0..6 {
        let (idx, data) = stream.next().await.expect("item").expect("ok");
        got.push((idx.raw(), data.data_id.hlc().raw()));
    }
    assert_eq!(
        got,
        vec![(0, 100), (1, 101), (2, 102), (3, 103), (4, 104), (5, 105)],
        "interior hole 2,3 filled from object store — seamless, gapless, in order",
    );
}

/// If the archive cannot tile a hole (manifest covers only 0..=2 but the
/// local scan skips to 4), the reader must surface a `StableLogGap` rather
/// than silently jumping the gap: the served prefix stops and errors.
#[tokio::test]
async fn stable_tail_errors_when_archive_cannot_fill_hole() {
    use zslg_log::SnapshotEntry;
    use zslg_snapshot::SnapshotId;

    let local: Vec<(LogIndex, Data)> = [(0u64, 100u64), (1, 101), (4, 104), (5, 105)]
        .into_iter()
        .map(|(i, h)| {
            (
                LogIndex(i),
                make_data(u8::try_from(i + 1).expect("fits"), h, b""),
            )
        })
        .collect();
    let stable_log = Arc::new(ScriptedStableLog::new(local));

    // Archive covers only 0..=2 — index 3 of the [2,4) hole is untiled.
    let snap_id = SnapshotId::from_u128(1);
    let store = Arc::new(ScriptedSnapshotStore::default());
    store.script(
        snap_id,
        (0..=2u64)
            .map(|i| SnapshotEntry {
                global_index: LogIndex(i),
                data: make_data(u8::try_from(i + 1).expect("fits"), 100 + i, b""),
            })
            .collect(),
    );
    let handle = Arc::new(RaftStateHandle::default());
    handle.replace(covering_manifest(snap_id, 2));

    let (_dir, reader) = scripted_reader(stable_log, store, handle).await;
    let mut ch = reader
        .subscribe_stable_log(None)
        .into_channel()
        .filter_map(|item| match item {
            Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
            Ok(StableLogEvent::Watermark(_)) => None,
            Err(e) => Some(Err(e)),
        });

    // 0,1 (local) then 2 (archive) come through; then the untiled 3 ends
    // the stream: the archive's window is short, and a short window is not
    // a hang-up, so the resync loop does not reopen it.
    let (got, end) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::task::spawn_blocking(move || {
            let mut got = Vec::new();
            let end = loop {
                match ch.recv() {
                    Ok(Ok((idx, data))) => got.push((idx.raw(), data.data_id.hlc().raw())),
                    Ok(Err(e)) => panic!("unexpected error item: {e}"),
                    Err(end) => break end,
                }
            };
            (got, end)
        }),
    )
    .await
    .expect("the stream ends")
    .expect("drain");
    assert_eq!(got, vec![(0, 100), (1, 101), (2, 102)]);
    assert!(
        matches!(
            end,
            crossbeam_combinators::AbortReason::OrderViolation {
                what: "archive window"
            }
        ),
        "an untiled hole ends the stream short, got: {end:?}"
    );
}

/// `subscribe_stable_log(after)` replays history after `after` before going live.
#[tokio::test]
async fn subscribe_stable_log_from_replays_history() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stable_log = Arc::new(FakeStableLog::new());
    let local_log = crate::testing::local_store_at(dir.path().join("local.stable"))
        .await
        .expect("from_db");

    stable_log
        .append(rx_of([Ok((LogIndex(0), make_data(1, 100, b"a")))]).err_into())
        .await
        .expect("append");
    stable_log
        .append(rx_of([Ok((LogIndex(1), make_data(2, 200, b"b")))]).err_into())
        .await
        .expect("append");

    let reader = local_reader(local_log, stable_log.clone(), Duration::from_millis(5));
    let mut stream = reader.subscribe_stable_log(None).into_test_stream();

    for expected in [0u64, 1] {
        let ev = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("recv timeout")
            .expect("item")
            .expect("ok");
        match ev {
            zslg_log::StableLogEvent::Appended { index, .. } => {
                assert_eq!(index.raw(), expected);
            }
            other => panic!("expected historical Appended({expected}), got {other:?}"),
        }
    }
}

/// Node shutdown ends a stable-log subscription parked on a log that has
/// gone quiet with `AbortReason::Cancelled`. Nothing else would wake it: the reader
/// still holds the stable log, so its fan-out never hangs up.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cancels_a_quiet_stable_log_subscription() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stable_log = Arc::new(FakeStableLog::new());
    let local_log = crate::testing::local_store_at(dir.path().join("local.stable"))
        .await
        .expect("from_db");
    stable_log
        .append(rx_of([Ok((LogIndex(0), make_data(1, 100, b"a")))]).err_into())
        .await
        .expect("append");

    let (handle, shutdown) = crossbeam_combinators::CancelHandle::new();
    let reader = reader_shut_down_by(
        local_log,
        stable_log.clone(),
        Duration::from_millis(5),
        Arc::new(ScriptedSnapshotStore::default()),
        Arc::new(RaftStateHandle::default()),
        shutdown,
    );
    let mut tail = reader.subscribe_stable_log(None);
    let blocked = tokio::task::spawn_blocking(move || {
        loop {
            // The one historical entry comes first; then the log is quiet.
            if let Err(end) = tail.recv() {
                return end;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!blocked.is_finished(), "the tail ended on its own");

    handle.cancel();
    let end = tokio::time::timeout(Duration::from_secs(5), blocked)
        .await
        .expect("shutdown did not wake the subscription")
        .expect("recv panicked");
    assert_eq!(end, crossbeam_combinators::AbortReason::Cancelled);
    drop(stable_log);
}
