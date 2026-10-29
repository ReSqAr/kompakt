use super::*;
use crate::dispatcher::fold;

#[tokio::test]
async fn dispatcher_routes_by_tenant_prefix() {
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

    let mut user_manager = dispatcher.new_handler::<event::Event>(tenant());
    user_manager.register::<User>().unwrap();
    let (user_writer, _user_reader) = user_manager.finalise().unwrap();

    let other_tenant = crate::Tenant::new("other", 0);
    let mut todo_manager = dispatcher.new_handler::<event::Event>(other_tenant);
    todo_manager.register::<Todo>().unwrap();
    let (todo_writer, _todo_reader) = todo_manager.finalise().unwrap();
    let syncer = dispatcher.syncer();
    let handle = dispatcher.finalise().await.unwrap();

    user_writer.write(&[new_user(1, "holden")]).await.unwrap();
    todo_writer
        .write(&[event::Event::NewTodo(testsupport::NewTodoV1 {
            id: TodoId(1),
            user_id: UserId(1),
            title: "t".to_string(),
        })])
        .await
        .unwrap();
    syncer.sync().await.unwrap();

    assert!(committed_user(&store, 1).await.is_some());
    let txn = store.read().unwrap();
    let got = ReadTransactionExt::get::<Todo>(&*txn, other_tenant.id(), Ref::new(TodoId(1).into()))
        .unwrap();
    assert_eq!(got.unwrap().title, "t");
    handle.abort();
}

#[tokio::test]
async fn dispatcher_skips_replayed_entries() {
    let store = inmemory();
    let (mut log, _writer, syncer, handle) = user_dispatcher(&store).await;

    let replayed = testsupport::data_id(1, 1);
    log.append_at(0, replayed, payload(tenant_id(), &[new_user(1, "first")]));
    // note: this may never happen in real life: the stable log of zslg is append-only and in particular any item in the stable log is immutable
    log.append_at(0, replayed, payload(tenant_id(), &[new_user(1, "second")]));
    log.append_at(
        1,
        testsupport::data_id(2, 2),
        payload(
            tenant_id(),
            &[event::Event::Pinged(testsupport::PingedV1 {
                id: UserId(1),
                ts: 7,
            })],
        ),
    );
    syncer.sync().await.unwrap();
    // Read once the fold has committed index 1 (the ping), not just the first
    // user: the two may land in separate commits.
    let at_ping = Some(zslg::LogIndex::from_bytes(1u64.to_be_bytes()));
    for _ in 0..500 {
        if store.read().unwrap().watermark().unwrap().index == at_ping {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let user = committed_user(&store, 1).await.unwrap();

    assert_eq!(user.name, "first");
    assert_eq!(user.last_ping, Some(7));

    let txn = store.read().unwrap();
    assert_eq!(
        txn.watermark().unwrap().index,
        Some(zslg::LogIndex::from_bytes(1u64.to_be_bytes()))
    );
    handle.abort();
}

#[tokio::test]
async fn dispatcher_watermark_event_advances_hlc() {
    let store = inmemory();
    let log = TestLog::new();
    let dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let hlc_rx = dispatcher.watermark();
    let handle = dispatcher.finalise().await.unwrap();
    log.watermark(42);
    hlc_rx
        .clone()
        .wait_for(|w| w >= &Some(zslg::Hlc::from_bytes(42u64.to_be_bytes())))
        .await
        .unwrap();

    let txn = store.read().unwrap();
    assert_eq!(
        txn.watermark().unwrap().hlc,
        Some(zslg::Hlc::from_bytes(42u64.to_be_bytes()))
    );
    handle.abort();
}

#[tokio::test]
async fn dispatcher_payload_too_short() {
    let store = inmemory();
    let (mut log, _writer, _syncer, handle) = user_dispatcher(&store).await;
    log.append(Bytes::from_static(&[1, 2, 3]));

    let result = timeout(SYNC, handle).await.unwrap().unwrap();
    assert!(matches!(result, Err(Error::PayloadTooShort(3, 16))));
}

#[tokio::test]
async fn dispatcher_unknown_tenant() {
    let store = inmemory();
    let (mut log, writer, syncer, handle) = user_dispatcher(&store).await;
    let other = crate::TenantId::from_string_hash("no-such-tenant", 0);
    log.append(payload(other, &[new_user(1, "foreign")]));

    timeout(SYNC, syncer.sync()).await.unwrap().unwrap();
    drop((store, log, writer, syncer));
    let result = timeout(SYNC, handle).await.unwrap().unwrap();
    assert!(matches!(result, Err(Error::SubscriptionAborted)));
}

#[tokio::test]
async fn dispatcher_channel_aborted() {
    let store = inmemory();
    let log = TestLog::new();
    let dispatcher = Dispatcher::from_db(
        store,
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let handle = dispatcher.finalise().await.unwrap();

    drop(log); // all senders gone without close(): Aborted
    let result = timeout(SYNC, handle).await.unwrap().unwrap();
    assert!(matches!(result, Err(Error::SubscriptionAborted)));
}

#[tokio::test]
async fn dispatcher_watermark_covers_exactly_the_applied_prefix() {
    let store = inmemory();
    let (mut log, _writer, syncer, handle) = user_dispatcher(&store).await;

    let d0 = testsupport::data_id(10, 10);
    let d1 = testsupport::data_id(20, 20);
    log.append_at(0, d0, payload(tenant_id(), &[new_user(1, "one")]));
    log.append_at(1, d1, payload(tenant_id(), &[new_user(2, "two")]));
    log.append_at(1, d1, payload(tenant_id(), &[new_user(3, "replay")]));
    log.watermark(30);
    syncer.sync().await.unwrap();
    assert!(committed_user(&store, 2).await.is_some());

    let wm = store.read().unwrap().watermark().unwrap();
    assert_eq!(wm.index, Some(LogIndex::from_bytes(1u64.to_be_bytes())));
    assert_eq!(wm.data_id, Some(d1));
    assert_eq!(wm.hlc, Some(zslg::Hlc::from_bytes(30u64.to_be_bytes())));

    let txn = store.read().unwrap();
    assert!(
        ReadTransactionExt::get::<User>(&*txn, tenant_id(), Ref::new(UserId(3).into()))
            .unwrap()
            .is_none(),
        "the replayed index must not be applied"
    );
    drop(txn);
    stop(&log, handle).await;
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Unreadable;

impl crate::Projection for Unreadable {
    type Event<'a> = event::TodoCreatedEvent<'a>;
    const TYPE_ID: crate::ProjectionTypeId = crate::ProjectionTypeId::from_u32(0xBAD);

    fn id(&self) -> Ref<Self> {
        Ref::new(crate::ProjectionId::from_u128(0))
    }

    fn next(
        _: &event::TodoCreatedEvent,
        _: &crate::View<'_>,
    ) -> Result<Vec<crate::Write<Self>>, Error> {
        Err(Error::Decode("store failed".to_string()))
    }
}

/// Any `Err` from `Projection::next` is a store failure: the entry is
/// poison, the fold stops, every read says so, and none of the entry commits.
#[tokio::test]
async fn next_error_stops_the_fold() {
    let store = inmemory();
    let mut log = TestLog::new();
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
    manager.register::<Unreadable>().unwrap();
    manager.register::<Todo>().unwrap();
    let (_writer, reader) = manager.finalise().unwrap();
    let handle = dispatcher.finalise().await.unwrap();

    log.append(payload(
        tenant_id(),
        &[event::Event::NewTodo(testsupport::NewTodoV1 {
            id: TodoId(1),
            user_id: UserId(1),
            title: "t".to_string(),
        })],
    ));

    let ended = timeout(SYNC, handle)
        .await
        .expect("the fold stops")
        .unwrap();
    assert!(matches!(ended, Err(Error::Decode(_))), "{ended:?}");
    assert!(matches!(
        reader.snapshot().map(|_| ()),
        Err(Error::FoldStopped(StopReason::PoisonEntry))
    ));
    // `Todo` accepts the same event; none of the entry is committed.
    let txn = store.read().unwrap();
    assert!(
        ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn dispatcher_survives_a_dropped_syncer() {
    let store = inmemory();
    let (mut log, _writer, syncer, handle) = user_dispatcher(&store).await;
    drop(syncer);

    log.append(payload(tenant_id(), &[new_user(1, "holden")]));
    assert!(committed_user(&store, 1).await.is_some());
    assert!(!handle.is_finished(), "the fold loop must still be running");
    stop(&log, handle).await;
}

#[tokio::test]
async fn dispatcher_rejects_a_non_monotone_data_id() {
    let store = inmemory();
    let (mut log, _writer, _syncer, handle) = user_dispatcher(&store).await;

    log.append_at(
        0,
        testsupport::data_id(5, 5),
        payload(tenant_id(), &[new_user(1, "first")]),
    );
    log.append_at(
        1,
        testsupport::data_id(3, 3),
        payload(tenant_id(), &[new_user(2, "second")]),
    );

    let result = timeout(SYNC, handle).await.unwrap().unwrap();
    assert!(
        matches!(result, Err(Error::NonMonotoneDataId { .. })),
        "got {result:?}"
    );
}

#[tokio::test]
async fn dispatcher_skips_a_foreign_tenant_and_keeps_going() {
    let store = inmemory();
    let (mut log, _writer, syncer, handle) = user_dispatcher(&store).await;
    let other = crate::TenantId::from_string_hash("no-such-tenant", 0);

    log.append(payload(tenant_id(), &[new_user(1, "before")]));
    log.append(payload(other, &[new_user(2, "foreign")]));
    log.append(payload(tenant_id(), &[new_user(3, "after")]));
    syncer.sync().await.unwrap();

    assert!(committed_user(&store, 1).await.is_some());
    assert!(
        committed_user(&store, 3).await.is_some(),
        "the loop kept going"
    );
    let txn = store.read().unwrap();
    assert!(
        ReadTransactionExt::get::<User>(&*txn, other, Ref::new(UserId(2).into()))
            .unwrap()
            .is_none(),
        "the foreign tenant's payload must not be folded"
    );
    drop(txn);
    stop(&log, handle).await;
}

#[tokio::test]
async fn dispatcher_batches_a_burst_into_one_commit() {
    let calls = testsupport::calls();
    let store: Arc<dyn Store> =
        Arc::new(testsupport::ObservingStore::new(inmemory(), calls.clone()));
    let (mut log, _writer, syncer, handle) = user_dispatcher(&store).await;

    const N: usize = 16;
    for i in 0..N {
        log.append(payload(
            tenant_id(),
            &[new_user(u128::try_from(i).unwrap(), "u")],
        ));
    }
    syncer.sync().await.unwrap();
    assert!(
        committed_user(&store, u128::try_from(N - 1).unwrap())
            .await
            .is_some()
    );

    let seen = testsupport::recorded(&calls)
        .iter()
        .filter(|c| **c == "commit")
        .count();
    assert!(
        (1..N).contains(&seen),
        "{N} entries took {seen} commits; expected at least one and fewer than {N}"
    );
    stop(&log, handle).await;
}

/// A row per (ping, n): a ping is a few bytes in the log and [`FANOUT`] rows
/// in the store — the shape of an event that names many rows (a map box's
/// fill naming its days).
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
struct Echo {
    id: crate::ProjectionId,
    filler: String,
}

const FANOUT: u128 = 1_000;

fn echo_id(ts: u128, n: u128) -> crate::ProjectionId {
    crate::ProjectionId::from_slice(((ts << 32) | n).to_be_bytes())
}

impl crate::Projection for Echo {
    type Event<'a> = event::UserEvent<'a>;
    const TYPE_ID: crate::ProjectionTypeId = crate::ProjectionTypeId::from_string_hash("Echo", 0);

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id)
    }

    fn next(
        event: &event::UserEvent,
        _: &crate::View<'_>,
    ) -> Result<Vec<crate::Write<Self>>, Error> {
        let event::UserEvent::Pinged(p) = event else {
            return Ok(Vec::new());
        };
        let mut writes = Vec::new();
        for n in 0..FANOUT {
            writes.extend(crate::Write::set(Self {
                id: echo_id(p.ts, n),
                filler: "x".repeat(100),
            })?);
        }
        Ok(writes)
    }
}

/// Fold a burst of [`PINGS`] entries, ready before the fold starts — tiny in
/// the log, ~20 MB in the store — with commits cut at `fold_commit` bytes;
/// every row lands. How many commits it took.
const PINGS: u128 = 100;

async fn commits_for_a_burst(fold_commit: usize) -> usize {
    let calls = testsupport::calls();
    let store: Arc<dyn Store> =
        Arc::new(testsupport::ObservingStore::new(inmemory(), calls.clone()));
    let mut log = TestLog::new();
    let mut dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    dispatcher.fold_commit = fold_commit;
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<Echo>().unwrap();
    let (_writer, _reader) = manager.finalise().unwrap();

    let mut payload_bytes = 0usize;
    for ts in 0..PINGS {
        let entry = payload(
            tenant_id(),
            &[event::Event::Pinged(testsupport::PingedV1 {
                id: UserId(1),
                ts,
            })],
        );
        payload_bytes = payload_bytes.saturating_add(entry.len());
        log.append(entry);
    }
    assert!(
        payload_bytes < 10_000,
        "the burst weighs {payload_bytes} bytes"
    );
    let commits = || {
        testsupport::recorded(&calls)
            .iter()
            .filter(|c| **c == "commit")
            .count()
    };
    let before = commits();
    let syncer = dispatcher.syncer();
    let handle = dispatcher.finalise().await.unwrap();
    syncer.sync().await.unwrap();

    let txn = store.read().unwrap();
    for ts in 0..PINGS {
        for n in [0, FANOUT - 1] {
            assert!(
                ReadTransactionExt::get::<Echo>(&*txn, tenant_id(), Ref::new(echo_id(ts, n)))
                    .unwrap()
                    .is_some(),
                "ping {ts}, row {n}"
            );
        }
    }
    drop(txn);
    let took = commits().saturating_sub(before);
    stop(&log, handle).await;
    took
}

/// A commit is cut by what it writes, not by what it weighs, at the
/// configured size: the same burst is one commit under a limit above it and
/// several under one below it, and never more commits than entries (an entry
/// is never split).
#[tokio::test]
async fn a_burst_that_writes_much_takes_several_commits_at_the_configured_size() {
    let entries = usize::try_from(PINGS).unwrap();
    let large = commits_for_a_burst(64 * 1024 * 1024).await;
    assert_eq!(large, 1, "~20 MB under a 64 MiB limit");
    let small = commits_for_a_burst(4 * 1024 * 1024).await;
    assert!(
        (4..=entries).contains(&small),
        "{PINGS} entries writing {FANOUT} rows each took {small} commits under 4 MiB"
    );
}

#[tokio::test]
async fn fold_loop_stops_when_cancelled_without_folding() {
    let store = inmemory();
    let mut log = TestLog::new();
    let dispatcher = Dispatcher::from_db(
        store.clone(),
        log.writer(),
        log.reader(),
        FLUSH_INTERVAL,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut dispatcher = dispatcher;
    let mut manager = dispatcher.new_handler::<event::Event>(tenant());
    manager.register::<User>().unwrap();
    let (writer, reader) = manager.finalise().unwrap();
    drop((writer, reader));

    log.append(payload(tenant_id(), &[new_user(1, "unfolded")]));
    let tail = log.reader().subscribe_stable_log(LogIndex::ZERO);
    log.disconnect();

    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = timeout(
        SYNC,
        tokio::task::spawn_blocking(move || fold::run(dispatcher.fold_inputs(), tail, &cancel)),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(result.is_ok(), "a cancelled fold loop stops cleanly");
    let txn = store.read().unwrap();
    assert!(
        ReadTransactionExt::get::<User>(&*txn, tenant_id(), Ref::new(UserId(1).into()))
            .unwrap()
            .is_none(),
        "a cancelled fold loop must not fold the batch that was waiting"
    );
}

/// A cancel from upstream reaches everything downstream of the fold as a
/// cancel: the stable-log subscription ends `Cancelled` (zslg's node
/// shutdown), so the fold stops cleanly as `StopReason::Cancelled`, and a
/// `Reader::subscribe_stable_log` stream ends `Cancelled` — not the `Aborted`
/// it gets when the fold stops for any other reason. The log stays quiet throughout:
/// nothing but the cancel can wake the fold.
#[tokio::test]
async fn an_upstream_cancel_stops_an_idle_fold_and_cancels_its_subscribers() {
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::from_db(
        inmemory(),
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
    let users = reader.subscribe_stable_log::<User>().unwrap();

    let (upstream, cancellation) = CancelHandle::new();
    let tail = log
        .reader()
        .subscribe_stable_log(LogIndex::ZERO)
        .until(&cancellation);
    let never = CancellationToken::new();
    let fold =
        tokio::task::spawn_blocking(move || fold::run(dispatcher.fold_inputs(), tail, &never));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!fold.is_finished(), "the fold stopped on its own");

    upstream.cancel();
    let folded = timeout(SYNC, fold).await.unwrap().unwrap();
    assert!(folded.is_ok(), "a cancelled fold stops cleanly: {folded:?}");
    assert!(matches!(
        reader.snapshot().map(|_| ()),
        Err(Error::FoldStopped(StopReason::Cancelled))
    ));

    let mut users = users;
    let end = timeout(
        SYNC,
        tokio::task::spawn_blocking(move || users.recv().map(|_| ())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(end, Err(zslg::ChannelEnd::Cancelled));
    drop(log);
}
