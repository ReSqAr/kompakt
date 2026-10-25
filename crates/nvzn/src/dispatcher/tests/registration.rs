use super::*;

#[tokio::test]
async fn dispatcher_double_finalise_rejected() {
    let store = inmemory();
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::from_db(store, log.writer(), log.reader(), FLUSH_INTERVAL)
        .await
        .unwrap();

    let mut first = dispatcher.new_handler::<event::Event>(tenant());
    first.register::<User>().unwrap();
    drop(first.finalise().unwrap());

    let mut second = dispatcher.new_handler::<event::Event>(tenant());
    second.register::<User>().unwrap();
    let err = second.finalise().err().expect("double finalise must err");
    assert!(matches!(err, Error::TenantAlreadyRegistered(_)));
}
