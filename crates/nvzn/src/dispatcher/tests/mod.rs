//! Dispatcher tests, one file per production file, and the rigs they share:
//! a dispatcher over an in-memory store fed by a [`TestLog`], finalised and
//! running, or built and left for the test to finalise.

mod fold;
mod fold_state;
mod handles;
mod registration;
mod supervisor;

use super::*;
use crate::metrics::StopReason;
use crate::{
    ReadTransactionExt, Ref, Store, WithWireFormat,
    testsupport::{
        self, FLUSH_INTERVAL, TestLog, Todo, TodoId, User, UserId, event, inmemory, payload,
        tenant, tenant_id,
    },
};
use bytes::Bytes;
use crossbeam_combinators::CancelHandle;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use zslg::StableLogEvent;

pub(super) const SYNC: Duration = Duration::from_secs(5);

pub(super) fn new_user(id: u128, name: &str) -> event::Event {
    event::Event::NewUser(testsupport::NewUserV3 {
        id: UserId(id),
        name: name.to_string(),
        address: None,
        email: None,
    })
}

pub(super) async fn user_dispatcher(
    store: &Arc<dyn Store>,
) -> (
    TestLog,
    EventWriter<event::Event>,
    Syncer,
    JoinHandle<Result<(), Error>>,
) {
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<User>().unwrap();
    let (writer, _reader) = manager.finalise().unwrap();
    let syncer = dispatcher.syncer();
    let handle = dispatcher.finalise().await.unwrap();
    (log, writer, syncer, handle)
}

pub(super) async fn committed_user(store: &Arc<dyn Store>, id: u128) -> Option<User> {
    for _ in 0..500 {
        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<User>(&*txn, tenant_id(), Ref::new(UserId(id).into()))
            .unwrap();
        if got.is_some() {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("user {id} never committed");
}

pub(super) async fn run(
    store: &Arc<dyn Store>,
    log: &TestLog,
    projections: &[&str],
) -> (
    crate::EventWriter<event::Event>,
    crate::Syncer,
    tokio::task::JoinHandle<Result<(), Error>>,
) {
    run_with_interval(store, log, projections, FLUSH_INTERVAL).await
}

pub(super) async fn run_with_interval(
    store: &Arc<dyn Store>,
    log: &TestLog,
    projections: &[&str],
    flush_interval: Duration,
) -> (
    crate::EventWriter<event::Event>,
    crate::Syncer,
    tokio::task::JoinHandle<Result<(), Error>>,
) {
    let mut dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        log.reader(),
        flush_interval,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    for name in projections {
        match *name {
            "user" => {
                manager.register::<User>().unwrap();
            }
            "todo" => {
                manager.register::<Todo>().unwrap();
            }
            other => panic!("unknown projection {other}"),
        }
    }
    let (writer, _reader) = manager.finalise().unwrap();
    let syncer = dispatcher.syncer();
    let handle = dispatcher.finalise().await.unwrap();
    (writer, syncer, handle)
}

pub(super) async fn stop(log: &TestLog, handle: tokio::task::JoinHandle<Result<(), Error>>) {
    log.disconnect();
    let result = timeout(SYNC, handle).await.unwrap().unwrap();
    assert!(matches!(result, Err(Error::SubscriptionAborted)));
}

pub(super) async fn scripted_dispatcher(
    store: &Arc<dyn Store>,
    log: &TestLog,
    scripted: Arc<testsupport::ScriptedReader>,
) -> (Dispatcher, crate::Reader<event::Event>) {
    let mut dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        scripted,
        FLUSH_INTERVAL,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<User>().unwrap();
    let (_writer, reader) = manager.finalise().unwrap();
    (dispatcher, reader)
}
