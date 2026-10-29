use super::*;

/// A stopped fold is reported at the calls that cannot be served, rather
/// than left for a caller to infer from a store that quietly stopped
/// moving — the difference between an error and a hang.
#[tokio::test]
async fn a_stopped_fold_is_reported_to_every_read() {
    let store = inmemory();
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
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
        reader.live::<event::UserEvent<'static>>().map(|_| ()),
        Err(Error::FoldStopped(StopReason::SubscriptionAborted))
    ));
}
