//! The unstable tail and its subscription: snapshot, replay, resync, watermark, eviction and shutdown.

use super::*;

#[tokio::test]
async fn unstable_tail_returns_snapshot_above_watermark() {
    let dir = tempfile::tempdir().expect("tempdir");
    let local_path = dir.path().join("local.db");
    let stable_log = Arc::new(FakeStableLog::new());
    let local_log = crate::testing::local_store_at(local_path.with_extension("stable"))
        .await
        .expect("from_db");

    stable_log
        .append((rx_of([Ok((LogIndex(0), make_data(0, 150, b"stable-seed")))])).err_into())
        .await
        .expect("watermark via append");

    integrate_batch(
        &local_log,
        upstream(1),
        vec![(LogIndex(0), make_data(1, 100, b"old"))],
    )
    .await
    .expect("integrate");
    integrate_batch(
        &local_log,
        upstream(1),
        vec![(LogIndex(1), make_data(2, 200, b"new"))],
    )
    .await
    .expect("integrate");

    let reader = local_reader(
        local_log.clone(),
        stable_log.clone(),
        Duration::from_millis(5),
    );

    let mut stream = reader.unstable_tail().into_test_stream();
    let first = stream.next().await.expect("item").expect("ok");
    assert_eq!(first.data_id.hlc(), Hlc(200));
    assert_eq!(first.blob.as_ref(), b"new");
    assert!(
        stream.next().await.is_none(),
        "unstable_tail is one-shot; no further items"
    );

    integrate_batch(
        &local_log,
        upstream(1),
        vec![(LogIndex(2), make_data(3, 300, b"newer"))],
    )
    .await
    .expect("integrate");

    let mut stream2 = reader.unstable_tail().into_test_stream();
    let a = stream2.next().await.expect("a").expect("ok");
    let b = stream2.next().await.expect("b").expect("ok");
    assert_eq!(a.data_id.hlc(), Hlc(200));
    assert_eq!(b.data_id.hlc(), Hlc(300));
    assert!(stream2.next().await.is_none());
}

/// The cycle opens with the watermark, replays what is already above it,
/// then follows live integrations.
#[tokio::test]
async fn subscribe_unstable_tail_replays_then_follows_live() {
    let (_dir, local, _stable, reader) = unstable_rig(16, Hlc(50), Cancellation::never()).await;
    let a = make_data(1, 100, b"a");
    let b = make_data(2, 200, b"b");
    integrate_one(&local, 0, a.clone()).await;
    integrate_one(&local, 1, b.clone()).await;

    let mut stream = reader.subscribe_unstable_tail();
    assert_eq!(
        next_unstable(&mut stream).await,
        zslg_log::UnstableEvent::Watermark(Hlc(50))
    );
    assert_eq!(
        next_unstable(&mut stream).await,
        zslg_log::UnstableEvent::Appended(a)
    );
    assert_eq!(
        next_unstable(&mut stream).await,
        zslg_log::UnstableEvent::Appended(b)
    );

    let c = make_data(3, 300, b"c");
    integrate_one(&local, 2, c.clone()).await;
    assert_eq!(
        next_unstable(&mut stream).await,
        zslg_log::UnstableEvent::Appended(c)
    );
    assert_quiet(&mut stream).await;
}

/// A consumer that overruns its queue is hung up on and silently resynced:
/// the next cycle opens with the watermark again and its rescan delivers
/// everything the hang-up cost, with no error.
#[tokio::test]
async fn a_hung_up_unstable_subscription_resyncs_without_loss() {
    let (_dir, local, _stable, reader) = unstable_rig(2, Hlc(50), Cancellation::never()).await;
    let mut stream = reader.subscribe_unstable_tail();
    assert_eq!(
        next_unstable(&mut stream).await,
        zslg_log::UnstableEvent::Watermark(Hlc(50))
    );

    let written: Vec<Data> = (0..6u8)
        .map(|i| make_data(i, 100 + u64::from(i), b"x"))
        .collect();
    for (i, data) in (0u64..).zip(&written) {
        integrate_one(&local, i, data.clone()).await;
    }

    let mut seen = std::collections::BTreeSet::new();
    let mut watermarks = 0usize;
    while seen.len() < written.len() {
        match next_unstable(&mut stream).await {
            zslg_log::UnstableEvent::Appended(data) => {
                seen.insert(data.data_id);
            }
            zslg_log::UnstableEvent::Watermark(hlc) => {
                assert_eq!(hlc, Hlc(50));
                watermarks += 1;
            }
        }
    }
    assert!(
        seen.iter().eq(written.iter().map(|d| &d.data_id)),
        "every entry, across the resync"
    );
    assert_eq!(watermarks, 1, "the resync cycle opens with the watermark");
}

/// A `Watermark` follows every advance of the stable watermark, even with
/// no entry traffic to carry it.
#[tokio::test]
async fn the_unstable_watermark_follows_each_advance() {
    let (_dir, _local, stable, reader) = unstable_rig(16, Hlc(50), Cancellation::never()).await;
    let mut stream = reader.subscribe_unstable_tail();
    assert_eq!(
        next_unstable(&mut stream).await,
        zslg_log::UnstableEvent::Watermark(Hlc(50))
    );
    for hlc in [60, 70] {
        stable.record_end_hlc(Hlc(hlc)).await.expect("advance");
        assert_eq!(
            next_unstable(&mut stream).await,
            zslg_log::UnstableEvent::Watermark(Hlc(hlc))
        );
    }
    assert_quiet(&mut stream).await;
}

/// An entry the watermark passes while it waits in the queue is the stable
/// log's to deliver by the time it would be emitted, so it is dropped.
#[tokio::test]
async fn an_entry_the_watermark_passed_while_queued_is_not_emitted() {
    let (_dir, local, stable, reader) = unstable_rig(16, Hlc(50), Cancellation::never()).await;
    let mut stream = reader.subscribe_unstable_tail();
    assert_eq!(
        next_unstable(&mut stream).await,
        zslg_log::UnstableEvent::Watermark(Hlc(50))
    );

    integrate_one(&local, 0, make_data(1, 100, b"settles")).await;
    stable.record_end_hlc(Hlc(150)).await.expect("advance");

    assert_eq!(
        next_unstable(&mut stream).await,
        zslg_log::UnstableEvent::Watermark(Hlc(150))
    );
    assert_quiet(&mut stream).await;
}

#[tokio::test]
async fn an_evicted_node_refuses_the_unstable_subscription() {
    let (_dir, _local, _stable, reader) = unstable_rig(16, Hlc(50), Cancellation::never()).await;
    let denied = zslg_consensus::EvictionStatus::new();
    denied.set(true);
    let gated = super::GatedReader::new(Arc::new(reader), denied, upstream(9));
    let mut stream = gated.subscribe_unstable_tail();
    assert!(matches!(
        stream.recv(),
        Ok(Err(zslg_log::Error::Lifecycle(LifecycleError::NotReady(_))))
    ));
}

/// Node shutdown ends a quiet unstable subscription with `AbortReason::Cancelled`.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cancels_a_quiet_unstable_subscription() {
    let (handle, shutdown) = crossbeam_combinators::CancelHandle::new();
    let (_dir, _local, _stable, reader) = unstable_rig(16, Hlc(50), shutdown).await;
    let mut tail = reader.subscribe_unstable_tail();
    let blocked = tokio::task::spawn_blocking(move || {
        loop {
            // The opening watermark comes first; then the store is quiet.
            if let Err(end) = tail.recv() {
                return end;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!blocked.is_finished(), "the subscription ended on its own");

    handle.cancel();
    let end = tokio::time::timeout(Duration::from_secs(5), blocked)
        .await
        .expect("shutdown did not wake the subscription")
        .expect("recv panicked");
    assert_eq!(end, crossbeam_combinators::AbortReason::Cancelled);
}

/// Across arbitrary interleavings of integrations, duplicates, watermark
/// advances and hang-ups (a queue two deep, overrun by any three
/// undrained integrations), the subscription ends up having delivered
/// everything `unstable_tail` holds, and never an entry at or below the
/// watermark current when it was emitted.
#[test]
fn proptest_unstable_subscription_delivers_the_unstable_tail() {
    use proptest::prelude::*;

    proptest!(ProptestConfig::with_cases(24), |(ops in proptest::collection::vec(unstable_op(), 1..40))| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let (_dir, local, stable, reader) =
                unstable_rig(2, Hlc(1000), Cancellation::never()).await;
            let mut stream = reader.subscribe_unstable_tail();
            let mut delivered = std::collections::BTreeSet::new();
            let mut last_watermark = None;
            let mut written: Vec<Data> = Vec::new();
            let mut next_index = 0u64;

            for op in ops {
                let watermark = stable.end_hlc().expect("set").raw();
                match op {
                    UnstableOp::Integrate(above) => {
                        let seed = u8::try_from(written.len()).expect("fits");
                        let data = make_data(seed, watermark + u64::from(above), b"p");
                        integrate_one(&local, next_index, data.clone()).await;
                        written.push(data);
                        next_index += 1;
                    }
                    UnstableOp::Duplicate(pick) => {
                        if !written.is_empty() {
                            let data = written[pick % written.len()].clone();
                            integrate_batch(
                                &local,
                                upstream(2),
                                vec![(LogIndex(next_index), data)],
                            )
                            .await
                            .expect("integrate");
                            next_index += 1;
                        }
                    }
                    UnstableOp::Advance(by) => {
                        stable
                            .record_end_hlc(Hlc(watermark + u64::from(by)))
                            .await
                            .expect("advance");
                    }
                    UnstableOp::Drain => {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        drain_ready(&mut stream, &stable, &mut delivered, &mut last_watermark)?;
                    }
                }
            }

            let expected: std::collections::BTreeSet<zslg_log::DataId> = reader
                .unstable_tail()
                .into_test_stream()
                .map_ok(|data| data.data_id)
                .try_collect()
                .await
                .expect("unstable_tail");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while !delivered.is_superset(&expected) || last_watermark != stable.end_hlc() {
                prop_assert!(
                    tokio::time::Instant::now() < deadline,
                    "missing {:?}; watermark {last_watermark:?} vs {:?}",
                    expected.difference(&delivered).collect::<Vec<_>>(),
                    stable.end_hlc()
                );
                tokio::time::sleep(Duration::from_millis(2)).await;
                drain_ready(&mut stream, &stable, &mut delivered, &mut last_watermark)?;
            }
            Ok(())
        })?;
    });
}

#[tokio::test]
async fn unstable_tail_hides_failed_writes_below_watermark() {
    let dir = tempfile::tempdir().expect("tempdir");
    let local = crate::testing::local_store_at(dir.path().join("local.stable"))
        .await
        .expect("from_db");
    let stable = Arc::new(FakeStableLog::new());

    // Two non-quorum "failed" writes (HLC 100, 200), accepted only here and
    // never snapshotted.
    for (i, hlc) in [(1u8, 100u64), (2, 200)] {
        integrate_batch(
            &local,
            upstream(1),
            vec![(LogIndex(u64::from(i)), make_data(i, hlc, &[i]))],
        )
        .await
        .expect("integrate");
    }

    let reader = local_reader(local.clone(), stable.clone(), Duration::from_millis(5));

    // Before the watermark passes them, the weak unstable view shows them.
    let before: Vec<Data> = reader
        .unstable_tail()
        .into_test_stream()
        .try_collect()
        .await
        .expect("collect");
    assert_eq!(
        before.len(),
        2,
        "recent unstable writes are visible at first"
    );

    // The cluster moves on: a watermark-only advance to 5000 (no snapshot data,
    // because the failed writes never reached quorum).
    stable.record_end_hlc(Hlc(5000)).await.expect("record");
    assert_eq!(reader.stable_watermark(), Some(Hlc(5000)));

    // Now they are below the watermark → they must disappear from the view.
    let after: Vec<Data> = reader
        .unstable_tail()
        .into_test_stream()
        .try_collect()
        .await
        .expect("collect");
    assert!(
        after.is_empty(),
        "failed writes below the watermark must disappear (convergence); got {after:?}"
    );

    // A genuinely recent write above the watermark is still shown.
    integrate_batch(
        &local,
        upstream(1),
        vec![(LogIndex(99), make_data(9, 6000, b"recent"))],
    )
    .await
    .expect("integrate");
    let recent: Vec<Data> = reader
        .unstable_tail()
        .into_test_stream()
        .try_collect()
        .await
        .expect("collect");
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].data_id.hlc(), Hlc(6000));
}
