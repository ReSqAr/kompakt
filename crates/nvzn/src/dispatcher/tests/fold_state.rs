use super::*;

/// A stopped fold is reported at the calls that cannot be served, rather
/// than left for a caller to infer from a store that quietly stopped
/// moving — the difference between an error and a hang.
#[tokio::test]
async fn a_stopped_fold_is_reported_to_every_read() {
    let store = inmemory();
    let log = TestLog::new();
    let mut dispatcher =
        Dispatcher::from_db(store.clone(), log.writer(), log.reader(), FLUSH_INTERVAL)
            .await
            .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<User>().unwrap();
    let (_writer, reader) = manager.finalise().unwrap();
    let handle = dispatcher.finalise().await.unwrap();

    reader.sync().await.expect("the fold is running");
    reader.snapshot().map(|_| ()).expect("the fold is running");

    stop(&log, handle).await;

    assert!(
        matches!(
            reader.sync().await,
            Err(Error::FoldStopped(StopReason::SubscriptionAborted))
        ),
        "sync waits on the fold's own progress; with no fold there is nothing to wait for"
    );
    assert!(matches!(
        reader.snapshot().map(|_| ()),
        Err(Error::FoldStopped(StopReason::SubscriptionAborted))
    ));
    assert!(matches!(
        reader.unstable_snapshot().map(|_| ()),
        Err(Error::FoldStopped(StopReason::SubscriptionAborted))
    ));
}

/// `unstable_snapshot` blocks on this; a fold that dies must release it
/// with the reason, never leave it parked.
#[tokio::test]
async fn a_blocked_fold_wait_wakes_on_progress_and_on_stop() {
    let state = FoldState::new(None);

    let reached = tokio::task::spawn_blocking({
        let state = state.clone();
        move || state.wait_folded(testsupport::hlc(10))
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!reached.is_finished());
    state.advance(testsupport::hlc(10));
    assert!(timeout(SYNC, reached).await.unwrap().unwrap().is_ok());

    let stopped = tokio::task::spawn_blocking({
        let state = state.clone();
        move || state.wait_folded(testsupport::hlc(20))
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!stopped.is_finished());
    state.stop(StopReason::Panicked);
    assert!(matches!(
        timeout(SYNC, stopped).await.unwrap().unwrap(),
        Err(Error::FoldStopped(StopReason::Panicked))
    ));
}
