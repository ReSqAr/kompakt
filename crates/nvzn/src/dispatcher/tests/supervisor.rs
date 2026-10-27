use super::*;

#[tokio::test]
async fn dispatcher_resumes_from_the_stored_watermark() {
    let store = inmemory();
    let log = TestLog::new();

    let (writer, syncer, handle) = run(&store, &log, &["user"]).await;
    writer.write(&[new_user(1, "holden")]).await.unwrap();
    syncer.sync().await.unwrap();
    assert!(committed_user(&store, 1).await.is_some());
    drop((writer, syncer));
    stop(&log, handle).await;

    let stored = store.read().unwrap().watermark().unwrap().index.unwrap();

    let (_writer, _syncer, handle) = run(&store, &log, &["user"]).await;
    assert_eq!(
        log.subscribed_from().last().copied(),
        Some(LogIndex::next_after(Some(stored))),
        "an unchanged config resumes above the stored watermark"
    );
    assert!(
        committed_user(&store, 1).await.is_some(),
        "resume must not wipe the projections"
    );
    stop(&log, handle).await;
}

#[tokio::test]
async fn dispatcher_resets_and_refolds_when_the_config_changes() {
    let store = inmemory();
    let log = TestLog::new();

    let (writer, syncer, handle) = run(&store, &log, &["user"]).await;
    writer.write(&[new_user(1, "holden")]).await.unwrap();
    syncer.sync().await.unwrap();
    assert!(committed_user(&store, 1).await.is_some());
    drop((writer, syncer));
    stop(&log, handle).await;

    let marker = crate::TenantId::from_string_hash("marker", 0);
    testsupport::commit_row(
        &store,
        marker,
        Ref::new(UserId(9).into()),
        &User::new(9, "x"),
    );

    let (_writer, _syncer, handle) = run(&store, &log, &["user", "todo"]).await;
    assert_eq!(
        log.subscribed_from().last().copied(),
        Some(LogIndex::ZERO),
        "a changed config refolds from the start"
    );

    let txn = store.read().unwrap();
    assert!(
        ReadTransactionExt::get::<User>(&*txn, tenant_id(), Ref::new(UserId(1).into()))
            .unwrap()
            .is_some(),
        "finalise returns only once the refold has restored the projections"
    );
    assert!(
        ReadTransactionExt::get::<User>(&*txn, marker, Ref::new(UserId(9).into()))
            .unwrap()
            .is_none(),
        "the reset must have wiped rows the refold does not rewrite"
    );
    drop(txn);
    stop(&log, handle).await;
}

#[tokio::test]
async fn dispatcher_fences_on_the_flush_interval() {
    let dir = tempfile::TempDir::new().unwrap();
    let rocks = testsupport::rocksdb_store(dir.path());
    let store: Arc<dyn Store> = Arc::new(rocks.clone());
    let log = TestLog::new();

    let (writer, syncer, handle) =
        run_with_interval(&store, &log, &["user"], Duration::from_millis(20)).await;
    writer.write(&[new_user(1, "fenced")]).await.unwrap();
    syncer.sync().await.unwrap();

    for _ in 0..500 {
        if !rocks.is_dirty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!rocks.is_dirty(), "the interval fence never ran");

    drop((writer, syncer));
    stop(&log, handle).await;
}

#[tokio::test]
async fn dispatcher_fences_once_more_when_it_stops() {
    let dir = tempfile::TempDir::new().unwrap();
    let rocks = testsupport::rocksdb_store(dir.path());
    let store: Arc<dyn Store> = Arc::new(rocks.clone());
    let log = TestLog::new();

    let (writer, syncer, handle) = run(&store, &log, &["user"]).await;
    writer.write(&[new_user(1, "fenced")]).await.unwrap();
    syncer.sync().await.unwrap();
    assert!(rocks.is_dirty(), "a commit owes an fsync");

    drop((writer, syncer));
    stop(&log, handle).await;

    assert!(
        !rocks.is_dirty(),
        "the shutdown fence must run even though the loop ended in an error"
    );
}

#[tokio::test]
async fn dispatcher_stops_when_the_fence_fails() {
    let store: Arc<dyn Store> = Arc::new(testsupport::UnfenceableStore::new(inmemory()));
    let log = TestLog::new();

    let (writer, syncer, handle) =
        run_with_interval(&store, &log, &["user"], Duration::from_millis(20)).await;
    writer.write(&[new_user(1, "doomed")]).await.unwrap();
    syncer.sync().await.unwrap();

    let result = timeout(SYNC, handle).await.unwrap().unwrap();
    assert!(
        matches!(result, Err(Error::Io(ref m)) if m == "fsync refused"),
        "unexpected outcome: {result:?}"
    );

    // Unpark the fold loop so the runtime can shut down.
    drop((writer, syncer));
    log.disconnect();
}

/// A reader handed out before `finalise` must not be served a store the
/// fold has not replayed into yet — after a reset that is an empty one.
#[tokio::test]
async fn finalise_waits_for_the_fold_to_replay_the_stable_log() {
    let store = inmemory();
    let log = TestLog::new();
    let (scripted, feed) = testsupport::ScriptedReader::new();
    scripted.set_stable_watermark(10);
    let (dispatcher, reader) = scripted_dispatcher(&store, &log, scripted).await;

    let finalise = tokio::spawn(dispatcher.finalise());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !finalise.is_finished(),
        "finalise must wait for the replay up to the stable watermark"
    );

    feed.send(Ok(StableLogEvent::Appended {
        index: LogIndex::ZERO,
        data: zslg::Data {
            data_id: testsupport::data_id(10, 1),
            blob: payload(tenant_id(), &[new_user(1, "holden")]),
        },
    }))
    .unwrap();
    feed.send(Ok(StableLogEvent::Watermark(testsupport::hlc(10))))
        .unwrap();

    let _handle = timeout(SYNC, finalise)
        .await
        .expect("finalise returns once the replay is done")
        .unwrap()
        .unwrap();
    let snapshot = reader.snapshot().unwrap();
    assert!(
        snapshot
            .get(Ref::<User>::new(UserId(1).into()))
            .unwrap()
            .is_some(),
        "the replayed history is visible without a sync"
    );
}

#[tokio::test]
async fn finalise_fails_when_the_fold_stops_during_replay() {
    let store = inmemory();
    let log = TestLog::new();
    let (scripted, feed) = testsupport::ScriptedReader::new();
    scripted.set_stable_watermark(10);
    let (dispatcher, _reader) = scripted_dispatcher(&store, &log, scripted).await;

    let finalise = tokio::spawn(dispatcher.finalise());
    drop(feed);

    let result = timeout(SYNC, finalise)
        .await
        .expect("finalise must not wait on a fold that has stopped")
        .unwrap();
    assert!(matches!(
        result,
        Err(Error::FoldStopped(StopReason::SubscriptionAborted))
    ));
}

/// The dispatcher's only cancellation is the node's shutdown: cancelling it
/// stops the fold, and the dispatcher ends cleanly.
#[tokio::test]
async fn the_nodes_shutdown_stops_the_dispatcher() {
    let shutdown = CancellationToken::new();
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::from_db(
        inmemory(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &shutdown,
    )
    .await
    .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<User>().unwrap();
    let (_writer, reader) = manager.finalise().unwrap();
    let handle = dispatcher.finalise().await.unwrap();

    shutdown.cancel();
    let ended = timeout(SYNC, handle).await.expect("it stops").unwrap();
    assert!(ended.is_ok(), "a shutdown, not a fault: {ended:?}");
    assert!(matches!(
        reader.snapshot().map(|_| ()),
        Err(Error::FoldStopped(StopReason::Cancelled))
    ));
}

/// A halt stops the dispatcher through its child of the node's token: the
/// node's own stays up (the host drains and exits on the failed fold).
#[tokio::test]
async fn a_halt_leaves_the_nodes_token_alone() {
    let shutdown = CancellationToken::new();
    let log = TestLog::new();
    let dispatcher = Dispatcher::from_db(
        inmemory(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &shutdown,
    )
    .await
    .unwrap();
    let fold_state = dispatcher.fold_state.clone();
    let handle = dispatcher.finalise().await.unwrap();

    fold_state.halt(StopReason::UnstableTailEnded);
    let ended = timeout(SYNC, handle).await.expect("it stops").unwrap();
    assert!(
        matches!(
            ended,
            Err(Error::FoldStopped(StopReason::UnstableTailEnded))
        ),
        "{ended:?}"
    );
    assert!(!shutdown.is_cancelled());
}
