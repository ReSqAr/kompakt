//! Tests for `reader`.

use super::*;
use crate::{
    Dispatcher, Store,
    testing::{
        self, FLUSH_INTERVAL, TestLog, User, UserId, commit_row, event, inmemory, tenant, tenant_id,
    },
};
use std::sync::Arc;

fn new_user(id: u128, name: &str) -> event::Event {
    event::Event::NewUser(testing::NewUserV3 {
        id: UserId(id),
        name: name.to_string(),
        address: None,
        email: None,
    })
}

/// The contract of `folded_watermark` + `hlc_now`: once the fold has
/// passed `hlc_now()`, a stable snapshot includes every write made before.
#[tokio::test]
async fn the_fold_passing_hlc_now_puts_earlier_writes_in_the_stable_snapshot() {
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
    let (writer, reader) = manager.finalise().unwrap();
    let fold = dispatcher.finalise().await.unwrap();
    tokio::spawn(async move {
        let _ = fold.await;
    });

    writer.write(&[new_user(1, "first")]).await.unwrap();
    writer.write(&[new_user(2, "second")]).await.unwrap();
    let now = reader.hlc_now();
    let mut folded = reader.folded_watermark();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        folded.wait_for(|w| w.is_some_and(|w| w >= now)),
    )
    .await
    .expect("the fold passes now")
    .expect("the fold is running");

    let snapshot = reader.snapshot().unwrap();
    for id in [1, 2] {
        assert!(
            snapshot
                .get(Ref::<User>::new(UserId(id).into()))
                .unwrap()
                .is_some(),
            "user {id}, written before now, is in the stable snapshot"
        );
    }
}

async fn reader() -> (Arc<dyn Store>, TestLog, Reader<event::Event>) {
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
    (store, log, reader)
}

#[tokio::test]
async fn snapshot_get_select_from_slice() {
    let (store, _log, reader) = reader().await;
    commit_row(
        &store,
        tenant_id(),
        Ref::new(UserId(1).into()),
        &User::new(1, "holden"),
    );
    commit_row(
        &store,
        tenant_id(),
        Ref::new(UserId(2).into()),
        &User::new(2, "naomi"),
    );

    let snapshot = reader.snapshot().unwrap();
    assert_eq!(
        snapshot
            .get(Ref::<User>::new(UserId(1).into()))
            .unwrap()
            .unwrap()
            .name,
        "holden"
    );
    assert!(
        snapshot
            .get(Ref::<User>::new(UserId(3).into()))
            .unwrap()
            .is_none()
    );

    let mut names: Vec<String> = snapshot
        .select::<User>()
        .into_iter()
        .map(|r| r.unwrap().name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["holden", "naomi"]);

    let snapshot = reader.snapshot().unwrap();
    let echoed: Vec<u128> = snapshot
        .with_iter([7u128, 8])
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(echoed, vec![7, 8]);
}

#[tokio::test]
async fn a_snapshot_clone_reads_the_same_state() {
    let (store, _log, reader) = reader().await;
    let holden = User {
        id: UserId(1),
        name: "holden".into(),
        address: None,
        email: None,
        last_ping: None,
    };
    commit_row(&store, tenant_id(), Ref::new(UserId(1).into()), &holden);

    let snapshot = reader.snapshot().unwrap();
    let clone = snapshot.clone();
    let names = |s: Snapshot<'_>| -> Vec<String> {
        s.select::<User>()
            .into_iter()
            .map(|r| r.unwrap().name)
            .collect()
    };
    assert_eq!(names(snapshot), vec!["holden"]);
    assert_eq!(names(clone), vec!["holden"]);
}

mod subscribe_stable_log {
    use super::*;
    use crate::{
        EventWriter, StopReason,
        testing::{NewTodoV1, Todo, TodoDeletedV1, TodoId},
    };
    use crossbeam_combinators::{AbortReason, Infinite};
    use tokio::time::{Duration, timeout};

    const WAIT: Duration = Duration::from_secs(5);

    async fn rig() -> (
        TestLog,
        EventWriter<event::Event>,
        Reader<event::Event>,
        tokio::task::JoinHandle<Result<(), Error>>,
    ) {
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
        manager.register::<Todo>().unwrap();
        let (writer, reader) = manager.finalise().unwrap();
        let handle = dispatcher.finalise().await.unwrap();
        (log, writer, reader, handle)
    }

    fn new_todo(id: u128) -> event::Event {
        event::Event::NewTodo(NewTodoV1 {
            id: TodoId(id),
            user_id: UserId(100),
            title: format!("todo {id}"),
        })
    }

    fn todo_ref(id: u128) -> Ref<Todo> {
        Ref::new(TodoId(id).into())
    }

    /// The next `n` receives, off the runtime: `recv` blocks.
    async fn recv_n<T: Send + 'static>(
        ch: Channel<T, Infinite>,
        n: usize,
    ) -> (Channel<T, Infinite>, Vec<Result<T, AbortReason>>) {
        timeout(
            WAIT,
            tokio::task::spawn_blocking(move || {
                let mut ch = ch;
                let got = (0..n).map(|_| ch.recv()).collect();
                (ch, got)
            }),
        )
        .await
        .expect("the subscription delivered in time")
        .unwrap()
    }

    #[tokio::test]
    async fn delivers_only_new_refs_of_its_own_type() {
        let (_log, writer, reader, _handle) = rig().await;
        writer.write(&[new_todo(1)]).await.unwrap();
        reader.sync().await.unwrap();

        let todos = reader.subscribe_stable_log::<Todo>().unwrap();
        writer
            .write(&[new_user(100, "holden"), new_todo(2)])
            .await
            .unwrap();
        writer.write(&[new_todo(3)]).await.unwrap();

        let (_todos, got) = recv_n(todos, 2).await;
        let got: Vec<Ref<Todo>> = got.into_iter().map(Result::unwrap).collect();
        assert_eq!(
            got,
            vec![todo_ref(2), todo_ref(3)],
            "todo 1 predates the subscription; user 100 is another type"
        );
        let todo = reader.snapshot().unwrap().get(got[0]).unwrap();
        assert_eq!(todo.expect("already written").title, "todo 2");
    }

    #[tokio::test]
    async fn a_deletion_is_a_ref_whose_row_is_gone() {
        let (_log, writer, reader, _handle) = rig().await;
        writer.write(&[new_todo(1)]).await.unwrap();
        reader.sync().await.unwrap();

        let todos = reader.subscribe_stable_log::<Todo>().unwrap();
        let deleted = event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(1) });
        writer.write(&[deleted]).await.unwrap();

        let (_todos, got) = recv_n(todos, 1).await;
        assert_eq!(got, vec![Ok(todo_ref(1))]);
        let snapshot = reader.snapshot().unwrap();
        assert!(snapshot.get(todo_ref(1)).unwrap().is_none());
    }

    #[tokio::test]
    async fn a_stopped_fold_ends_the_channel_after_its_last_ref() {
        let (log, writer, reader, handle) = rig().await;
        let todos = reader.subscribe_stable_log::<Todo>().unwrap();
        writer.write(&[new_todo(1)]).await.unwrap();
        reader.sync().await.unwrap();

        log.disconnect();
        let _ = timeout(WAIT, handle).await.unwrap().unwrap();

        let (_todos, got) = recv_n(todos, 2).await;
        assert_eq!(got[0], Ok(todo_ref(1)), "what was sent is still delivered");
        assert!(got[1].is_err());
        assert!(matches!(
            reader.subscribe_stable_log::<Todo>().map(|_| ()),
            Err(Error::FoldStopped(StopReason::SubscriptionAborted))
        ));
    }

    #[tokio::test]
    async fn many_registrations_are_admitted_and_see_a_commit() {
        let (_log, writer, reader, _handle) = rig().await;
        let pending: Vec<_> = (0..PENDING_SUBSCRIBERS)
            .map(|_| reader.subscribe_stable_log::<Todo>().unwrap())
            .collect();

        writer.write(&[new_todo(1)]).await.unwrap();
        reader.sync().await.unwrap();
        let first = pending.into_iter().next().unwrap();
        let (_first, got) = recv_n(first, 1).await;
        assert_eq!(got, vec![Ok(todo_ref(1))]);
    }

    #[tokio::test]
    async fn a_dispatcher_dropped_unfinalised_ends_the_channel() {
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
        manager.register::<Todo>().unwrap();
        let (_writer, reader) = manager.finalise().unwrap();

        let before = reader.subscribe_stable_log::<Todo>().unwrap();
        drop(dispatcher);
        let after = reader.subscribe_stable_log::<Todo>().unwrap();

        for ch in [before, after] {
            let (_ch, got) = recv_n(ch, 1).await;
            assert!(got[0].is_err());
        }
    }

    #[tokio::test]
    async fn an_unregistered_projection_is_refused() {
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
        let _handle = dispatcher.finalise().await.unwrap();

        assert!(matches!(
            reader.subscribe_stable_log::<Todo>().map(|_| ()),
            Err(Error::ProjectionNotRegistered { tenant_id: t, .. })
                if t == tenant_id().as_u128()
        ));
        reader
            .subscribe_stable_log::<User>()
            .expect("a registered projection subscribes");
    }
}
