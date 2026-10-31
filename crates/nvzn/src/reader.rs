use crate::dispatcher::{FoldState, PENDING_SUBSCRIBERS, Subscriptions};
use crate::events::subset;
use crate::live::{self, Live, LiveLimits};
use crate::metrics::{self, ReadKind};
use crate::projection::Registered;
use crate::{
    ArcReadTransaction, ArcStore, Error, EventSet, Index, Projection, ProjectionTypeId,
    ReadTransactionExt, Ref, ResultSet, SubEventOf, Syncer, Tenant, TenantId, View, WithWireFormat,
};
use crossbeam_combinators::Channels;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::Arc;

impl Error {
    pub(crate) fn reference_not_found<P: Projection>(r: Ref<P>) -> Self {
        Error::ReferenceNotFound {
            pid: r.id(),
            type_name: std::any::type_name::<P>(),
        }
    }
}

/// Unread refs before the fold blocks on a subscriber.
const SUBSCRIBER_CAPACITY: usize = 1024;

/// One tenant's read side, typed by the tenant's event type `Ev` like the
/// [`EventWriter`](crate::EventWriter) handed out beside it.
#[must_use]
pub struct Reader<Ev> {
    tenant: Tenant,
    store: ArcStore,
    reader: Arc<dyn zslg::Reader>,
    syncer: Syncer,
    fold_state: FoldState,
    subscriptions: Subscriptions,
    registered: Arc<BTreeSet<ProjectionTypeId>>,
    /// Every registered projection's events and projector constructor: what
    /// [`live`](Self::live) picks its projections from.
    projections: Arc<BTreeMap<ProjectionTypeId, Registered<Ev>>>,
    // `fn() -> Ev`: the reader holds no `Ev`, so it is `Send + Sync` whatever `Ev` is.
    _event: PhantomData<fn() -> Ev>,
}

impl<Ev> Reader<Ev> {
    #[allow(
        clippy::too_many_arguments,
        reason = "crate-internal constructor called once, from finalise"
    )]
    pub(crate) fn new(
        tenant: Tenant,
        store: ArcStore,
        reader: Arc<dyn zslg::Reader>,
        syncer: Syncer,
        fold_state: FoldState,
        subscriptions: Subscriptions,
        registered: Arc<BTreeSet<ProjectionTypeId>>,
        projections: Arc<BTreeMap<ProjectionTypeId, Registered<Ev>>>,
    ) -> Self {
        Self {
            projections,
            tenant,
            store,
            reader,
            syncer,
            fold_state,
            subscriptions,
            registered,
            _event: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct Snapshot<'a> {
    tenant_id: TenantId,
    snapshot: ArcReadTransaction<'a>,
}

impl<Ev> Reader<Ev> {
    pub async fn sync(&self) -> Result<(), Error> {
        self.syncer.sync().await
    }

    /// The fold's progress: the HLC the folded state has been committed
    /// through, as a `watch` that changes with every commit. A caller that
    /// wants to see everything written before a moment takes
    /// [`hlc_now`](Self::hlc_now) and waits for this to reach it; a stable
    /// snapshot taken after that includes those writes:
    ///
    /// ```no_run
    /// # async fn f<Ev>(reader: &nvzn::Reader<Ev>) -> Result<(), nvzn::Error> {
    /// let now = reader.hlc_now();
    /// let mut folded = reader.folded_watermark();
    /// let _ = folded.wait_for(|w| w.is_some_and(|w| w >= now)).await;
    /// let snapshot = reader.snapshot()?;
    /// # Ok(()) }
    /// ```
    ///
    /// The watch ends (`wait_for` errs) when the fold stops; [`snapshot`]
    /// then reports why.
    ///
    /// [`snapshot`]: Self::snapshot
    #[must_use]
    pub fn folded_watermark(&self) -> tokio::sync::watch::Receiver<Option<zslg::Hlc>> {
        self.syncer.watermark()
    }

    /// This node's clock, read now: every write this node made before the
    /// call has an HLC at or below it (see [`folded_watermark`](Self::folded_watermark)).
    #[must_use]
    pub fn hlc_now(&self) -> zslg::Hlc {
        self.reader.hlc_now()
    }

    /// A read of the folded state.
    ///
    /// Fails with [`Error::FoldStopped`] once the fold has stopped: the store
    /// still answers, from a watermark that no longer moves, and a caller has
    /// no other way to tell that apart from a quiet cluster.
    pub fn snapshot(&self) -> Result<Snapshot<'_>, Error> {
        self.fold_state.check()?;
        metrics::read(self.tenant, ReadKind::Stable);
        Ok(Snapshot {
            tenant_id: self.tenant.id(),
            snapshot: self.store.read()?,
        })
    }

    /// Every stable `P` committed from now on, deletions included, as a
    /// [`Ref`] to look up. The fold blocks on a subscriber that stops reading;
    /// the channel ends only with the fold — with
    /// [`AbortReason::Cancelled`](crossbeam_combinators::AbortReason::Cancelled) when the fold was
    /// cancelled (its stop reason is [`StopReason::Cancelled`](crate::StopReason::Cancelled)),
    /// `Aborted` when it stopped for any other reason.
    ///
    /// `P` must be over this tenant's events, as for
    /// [`register`](crate::AssociatedProjectorManager::register), and fails
    /// with [`Error::ProjectionNotRegistered`] unless it was registered for
    /// this tenant — such a stream would never fire.
    ///
    /// A projection over unrelated events does not compile:
    ///
    /// ```compile_fail
    /// fn subscribe<Ev, P: nvzn::Projection>(reader: &nvzn::Reader<Ev>) {
    ///     let _ = reader.subscribe_stable_log::<P>();
    /// }
    /// ```
    ///
    /// whereas with the bound it does:
    ///
    /// ```
    /// fn subscribe<Ev, P>(reader: &nvzn::Reader<Ev>)
    /// where
    ///     P: nvzn::Projection,
    ///     for<'a> P::Event<'a>: nvzn::SubEventOf<'a, Ev>,
    /// {
    ///     let _ = reader.subscribe_stable_log::<P>();
    /// }
    /// ```
    pub fn subscribe_stable_log<P>(&self) -> Result<Channels<Ref<P>>, Error>
    where
        P: Projection,
        for<'a> P::Event<'a>: SubEventOf<'a, Ev>,
    {
        self.fold_state.check()?;
        if !self.registered.contains(&P::TYPE_ID) {
            return Err(Error::ProjectionNotRegistered {
                type_name: std::any::type_name::<P>(),
                type_id: P::TYPE_ID.as_u32(),
                tenant_id: self.tenant.id().as_u128(),
            });
        }
        let (tx, rx) = crossbeam_channel::bounded(SUBSCRIBER_CAPACITY);
        // Racy by a few concurrent subscribes, which is fine for this bound.
        if self.subscriptions.register.len() >= PENDING_SUBSCRIBERS {
            return Err(Error::TooManyPendingSubscriptions(PENDING_SUBSCRIBERS));
        }
        // If the fold is gone, `tx` is dropped here and the channel ends.
        let _ = self
            .subscriptions
            .register
            .send((self.tenant.id(), P::TYPE_ID, tx));
        Ok(Channels::from_infinite(rx, "nvzn-subscribe")
            .until(&self.subscriptions.cancelled)
            .map(Ref::new))
    }
}

impl<Ev: WithWireFormat> Reader<Ev> {
    /// A live view under the sub-event set `S` (any `EventSet`: an owned or a
    /// view subset of `define_events!`): every registered projection whose
    /// events are all in `S` is live in it; see [`Live`]. One per process:
    /// clone the [`Live`] to share it.
    ///
    /// Returns at once and starts itself once the fold has caught up
    /// ([`sync`](Self::sync)): until then it serves the stable snapshot and
    /// [`Live::wait_for`] waits through the start. An empty `S` never starts.
    /// Starting opens the unstable tail and scans the stable log from the
    /// store's watermark up to the stable one (the first snapshots wait for
    /// that, once).
    ///
    /// Needs a tokio runtime (it spawns its tasks); fails like
    /// [`snapshot`](Self::snapshot) once the fold has stopped.
    pub fn live<S: EventSet>(&self) -> Result<Live<Ev>, Error> {
        self.live_with::<S>(LiveLimits::default())
    }

    /// [`live`](Self::live) with explicit caps on what it holds.
    pub fn live_with<S: EventSet>(&self, limits: LiveLimits) -> Result<Live<Ev>, Error> {
        let view = self.not_started::<S>(limits)?;
        if S::EVENTS.is_empty() {
            // Nothing could ever be live: no tail, no scan, no tasks.
            return Ok(view);
        }
        tokio::spawn(live::begin_when_caught_up(
            view.clone(),
            self.syncer.clone(),
            Arc::clone(&self.reader),
            self.subscriptions.cancelled.clone(),
        ));
        Ok(view)
    }

    /// A live view started now, without waiting for the fold: for tests that
    /// drive F by hand.
    #[cfg(test)]
    pub(crate) fn live_started<S: EventSet>(&self, limits: LiveLimits) -> Result<Live<Ev>, Error> {
        let view = self.not_started::<S>(limits)?;
        live::begin(
            &view,
            &self.reader,
            &self.subscriptions.cancelled,
            self.syncer.watermark(),
        )?;
        Ok(view)
    }

    pub(crate) fn not_started<S: EventSet>(&self, limits: LiveLimits) -> Result<Live<Ev>, Error> {
        self.fold_state.check()?;
        let mut projectors = BTreeMap::new();
        for (type_id, registered) in self.projections.iter() {
            if subset(registered.events, S::EVENTS) {
                projectors.insert(*type_id, (registered.make)()?);
            }
        }
        live::not_started(
            self.tenant,
            self.store.clone(),
            self.fold_state.clone(),
            projectors,
            S::EVENTS,
            limits,
        )
    }
}

impl<'a> Snapshot<'a> {
    pub(crate) fn from_txn(tenant_id: TenantId, snapshot: ArcReadTransaction<'a>) -> Self {
        Self {
            tenant_id,
            snapshot,
        }
    }

    #[must_use]
    pub fn view(&self) -> View<'_> {
        View::new(self.tenant_id, &*self.snapshot)
    }

    pub fn get<P>(&self, r: Ref<P>) -> Result<Option<P>, Error>
    where
        P: Projection + Send + Sync,
    {
        self.view().get(r)
    }

    pub fn select<P: Projection>(&self) -> ResultSet<'_, P> {
        let rows = self.snapshot.scan::<P>(self.tenant_id);
        ResultSet::new(self.tenant_id, &*self.snapshot, rows)
    }

    pub fn with_iter<'s, I>(&'s self, rows: I) -> ResultSet<'s, I::Item>
    where
        I: IntoIterator,
        I::IntoIter: 's,
        I::Item: 's,
    {
        ResultSet::new(
            self.tenant_id,
            &*self.snapshot,
            Box::new(rows.into_iter().map(Ok)),
        )
    }

    pub fn lookup<P: Projection, Q: Projection>(
        &self,
        idx: Index<P, Q>,
        key: impl Into<Ref<Q>>,
    ) -> ResultSet<'_, P> {
        let view = self.view();
        let rows = view.lookup(idx, key).map(move |r| view.resolve(r?));
        ResultSet::new(self.tenant_id, &*self.snapshot, Box::new(rows))
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::{
        Dispatcher, Store,
        testsupport::{
            self, FLUSH_INTERVAL, TestLog, User, UserId, commit_row, event, inmemory, tenant,
            tenant_id,
        },
    };
    use std::sync::Arc;

    fn new_user(id: u128, name: &str) -> event::Event {
        event::Event::NewUser(testsupport::NewUserV3 {
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
            testsupport::{NewTodoV1, Todo, TodoDeletedV1, TodoId},
        };
        use crossbeam_combinators::End;
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
            ch: Channels<T>,
            n: usize,
        ) -> (Channels<T>, Vec<Result<T, End>>) {
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
            assert!(matches!(got[1], Err(End::Aborted { .. })));
            assert!(matches!(
                reader.subscribe_stable_log::<Todo>().map(|_| ()),
                Err(Error::FoldStopped(StopReason::SubscriptionAborted))
            ));
        }

        #[tokio::test]
        async fn a_full_registration_queue_fails_rather_than_blocks() {
            let (_log, writer, reader, _handle) = rig().await;
            // An idle fold takes up no registrations: fill the queue.
            let pending: Vec<_> = (0..PENDING_SUBSCRIBERS)
                .map(|_| reader.subscribe_stable_log::<Todo>().unwrap())
                .collect();
            assert!(matches!(
                reader.subscribe_stable_log::<Todo>().map(|_| ()),
                Err(Error::TooManyPendingSubscriptions(PENDING_SUBSCRIBERS))
            ));

            // A commit drains the queue, and the queued ones see it.
            writer.write(&[new_todo(1)]).await.unwrap();
            reader.sync().await.unwrap();
            let todos = reader
                .subscribe_stable_log::<Todo>()
                .expect("the queue drained");
            drop(todos);
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
                assert!(matches!(got[0], Err(End::Aborted { .. })));
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
}
