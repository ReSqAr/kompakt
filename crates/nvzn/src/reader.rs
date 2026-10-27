use crate::dispatcher::{FoldState, InnerHandler, PENDING_SUBSCRIBERS, Subscriptions};
use crate::events::subset;
use crate::ids::APP_ID_LEN;
use crate::live::{self, Live, LiveLimits};
use crate::metrics::{self, ReadKind};
use crate::projection::Registered;
use crate::{
    ArcReadTransaction, ArcStore, Error, EventSet, Index, Projection, ProjectionTypeId,
    ReadTransaction, ReadTransactionExt, Ref, ResultSet, SubEventOf, Syncer, Tenant, TenantId,
    View, WithWireFormat, WriteTransactionImpl,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::Arc;
use zslg::{Channels, Data, DataId, UnstableEvent};

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
    handler: InnerHandler,
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
        handler: InnerHandler,
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
            handler,
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
    /// [`ChannelEnd::Cancelled`](zslg::ChannelEnd::Cancelled) when the fold was
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

    /// The folded state with the unstable tail applied on top. Fails like
    /// [`snapshot`](Self::snapshot) once the fold has stopped — the tail is
    /// only the part of the view above the watermark the fold stopped at.
    ///
    /// Blocking: it drains the tail, and it may wait for the fold. The tail
    /// starts above zslg's stable watermark `S` as of the call, the store
    /// covers up to the fold's own watermark `F`, and `S` moves first when a
    /// snapshot is loaded — entries in `(F, S]` would be in neither half. So
    /// the order is: take the tail (cut at some `S_t`), read `S` again
    /// (`S_t <= S`, since it only rises), wait for `F >= S`, then open the
    /// store. The wait is empty unless a load is in flight. The overlap
    /// `(S_t, F]` is dropped from the tail (`Watermark::covers`): what is
    /// stable there is already folded, and what is not never will be.
    pub fn unstable_snapshot(&self) -> Result<Snapshot<'_>, Error> {
        self.fold_state.check()?;
        metrics::read(self.tenant, ReadKind::Unstable);
        let tail = self.reader.unstable_tail();
        if let Some(cut) = self.reader.stable_watermark() {
            self.fold_state.wait_folded(cut)?;
        }

        let mut txn = self.store.write()?;
        let folded = txn.watermark()?;
        let mut last_applied: Option<DataId> = None;
        let mut applied = 0usize;

        for item in tail {
            let Data { data_id, blob } = item?;
            // The tail is `DataId`-ordered, so `last_applied` drops a repeat.
            if folded.covers(data_id) || last_applied.is_some_and(|last| data_id <= last) {
                continue;
            }
            last_applied = Some(data_id);
            applied = applied.saturating_add(1);

            apply(self.tenant.id(), &self.handler, &mut txn, blob)?;
        }
        metrics::unstable_fold(applied);

        Ok(Snapshot {
            tenant_id: self.tenant.id(),
            snapshot: Arc::new(txn),
        })
    }
}

impl<Ev: WithWireFormat> Reader<Ev> {
    /// A live view under the sub-event set `S` (a subset enum of
    /// `define_events!`, as `LiveEvent<'static>`): every registered projection
    /// whose events are all in `S` is live in it; see [`Live`]. Opens the
    /// unstable tail, scans the stable log from the store's watermark up to the
    /// stable one (the first snapshots wait for that, once), and integrates
    /// from then on. One per process: clone the [`Live`] to share it.
    ///
    /// Needs a tokio runtime (it spawns its tasks); fails like
    /// [`snapshot`](Self::snapshot) once the fold has stopped.
    pub fn live<S: EventSet>(&self) -> Result<Live<Ev>, Error> {
        self.live_with::<S>(LiveLimits::default())
    }

    /// [`live`](Self::live) with explicit caps on what it holds.
    pub fn live_with<S: EventSet>(&self, limits: LiveLimits) -> Result<Live<Ev>, Error> {
        self.fold_state.check()?;
        let mut projectors = BTreeMap::new();
        for (type_id, registered) in self.projections.iter() {
            if subset(registered.events, S::EVENTS) {
                projectors.insert(*type_id, (registered.make)()?);
            }
        }
        live::start(
            self.tenant,
            self.store.clone(),
            &self.reader,
            self.fold_state.clone(),
            &self.subscriptions.cancelled,
            self.syncer.watermark(),
            projectors,
            S::EVENTS,
            limits,
        )
    }

    /// A trigger, not a feed: this tenant's events as they reach the node's
    /// unstable tail, one item per write that carries any. Every write
    /// integrated after the call is announced at least once, in arrival
    /// order; the answer to an item is to read
    /// [`unstable_snapshot`](Self::unstable_snapshot), which alone says what
    /// is true. A write that never converges is announced and then absent
    /// from a later snapshot. A caller that cares about a subset of the
    /// events filters with [`SubEventOf::project`].
    ///
    /// A payload that cannot be decoded is an `Err` item and the stream goes
    /// on. Ends `Cancelled` when the dispatcher is cancelled, independently of
    /// the fold.
    #[must_use]
    pub fn subscribe_unstable_tail(&self) -> Channels<Result<Vec<Ev>, Error>> {
        let tenant = self.tenant;
        self.reader
            .subscribe_unstable_tail()
            .filter_map(move |item| match item {
                Ok(UnstableEvent::Appended(data)) => announce::<Ev>(tenant, &data.blob),
                Ok(UnstableEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e.into())),
            })
            .until(&self.subscriptions.cancelled)
    }
}

/// `Ev`'s events of `blob`, if `blob` is this tenant's and carries any.
fn announce<Ev: WithWireFormat>(tenant: Tenant, blob: &[u8]) -> Option<Result<Vec<Ev>, Error>> {
    let Some((tenant_id, payload)) = blob.split_first_chunk() else {
        metrics::unstable_subscription_undecodable(tenant);
        return Some(Err(Error::PayloadTooShort(blob.len(), APP_ID_LEN)));
    };
    if TenantId::from_slice(*tenant_id) != tenant.id() {
        return None;
    }
    match Ev::decode(payload) {
        Ok(events) => (!events.is_empty()).then_some(Ok(events)),
        Err(e) => {
            metrics::unstable_subscription_undecodable(tenant);
            Some(Err(e.into()))
        }
    }
}

fn apply(
    tenant_id: TenantId,
    handler: &InnerHandler,
    txn: &mut WriteTransactionImpl,
    payload: Bytes,
) -> Result<(), Error> {
    let Some((payload_tenant_id, payload)) = payload.split_first_chunk() else {
        return Err(Error::PayloadTooShort(payload.len(), APP_ID_LEN));
    };

    let payload_tenant_id = TenantId::from_slice(*payload_tenant_id);
    if payload_tenant_id != tenant_id {
        tracing::trace!(
            tenant_id = payload_tenant_id.as_u128(),
            "not this reader's tenant; skipping"
        );
        return Ok(());
    }

    handler(tenant_id, txn, payload)?;

    Ok(())
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
        Dispatcher, Store, Watermark,
        testsupport::{
            self, FLUSH_INTERVAL, TestLog, User, UserId, commit_row, event, inmemory, payload,
            tenant, tenant_id,
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

    fn committed_user(store: &Arc<dyn Store>, id: u128) -> Option<User> {
        let txn = store.read().unwrap();
        ReadTransactionExt::get::<User>(&*txn, tenant_id(), Ref::new(UserId(id).into())).unwrap()
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
    async fn unstable_snapshot_applies_own_payloads() {
        let (store, mut log, reader) = reader().await;
        log.push_unstable(payload(tenant_id(), &[new_user(1, "holden")]));
        drop(log);

        let snapshot = reader.unstable_snapshot().unwrap();
        assert_eq!(
            snapshot
                .get(Ref::<User>::new(UserId(1).into()))
                .unwrap()
                .unwrap()
                .name,
            "holden"
        );

        assert!(committed_user(&store, 1).is_none());
    }

    #[tokio::test]
    async fn unstable_snapshot_dedups_by_data_id() {
        let (store, log, reader) = reader().await;

        let seen = testsupport::data_id(1, 1);
        let txn = store.write().unwrap();
        txn.commit(Watermark {
            index: None,
            data_id: Some(seen),
            hlc: None,
        })
        .unwrap();

        log.push_unstable_at(seen, payload(tenant_id(), &[new_user(1, "first")]));
        let newer = testsupport::data_id(2, 2);
        log.push_unstable_at(newer, payload(tenant_id(), &[new_user(1, "second")]));
        drop(log);

        let snapshot = reader.unstable_snapshot().unwrap();
        assert_eq!(
            snapshot
                .get(Ref::<User>::new(UserId(1).into()))
                .unwrap()
                .unwrap()
                .name,
            "second"
        );
    }

    #[tokio::test]
    async fn unstable_snapshot_skips_foreign_tenant_payloads() {
        let (_store, mut log, reader) = reader().await;
        let other_tenant = crate::TenantId::from_string_hash("other", 0);
        log.push_unstable(payload(other_tenant, &[new_user(1, "foreign")]));
        log.push_unstable(payload(tenant_id(), &[new_user(2, "own")]));
        drop(log);

        let snapshot = reader.unstable_snapshot().unwrap();
        assert!(
            snapshot
                .get(Ref::<User>::new(UserId(1).into()))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            snapshot
                .get(Ref::<User>::new(UserId(2).into()))
                .unwrap()
                .unwrap()
                .name,
            "own"
        );
    }

    #[tokio::test]
    async fn unstable_snapshot_payload_too_short() {
        let (_store, mut log, reader) = reader().await;
        log.push_unstable(bytes::Bytes::from_static(&[1, 2, 3]));
        drop(log);

        let err = reader.unstable_snapshot().err().expect("must error");
        assert!(matches!(err, Error::PayloadTooShort(_, _)));
    }

    #[tokio::test]
    async fn unstable_snapshot_can_be_taken_twice() {
        let (_store, mut log, reader) = reader().await;
        log.push_unstable(payload(tenant_id(), &[new_user(1, "holden")]));

        for attempt in 0..3 {
            let snapshot = reader.unstable_snapshot().unwrap();
            assert_eq!(
                snapshot
                    .get(Ref::<User>::new(UserId(1).into()))
                    .unwrap()
                    .expect("attempt {attempt}")
                    .name,
                "holden",
                "attempt {attempt}"
            );
        }
    }

    #[tokio::test]
    async fn unstable_snapshot_reads_the_tail_before_opening_the_store() {
        let calls = testsupport::calls();
        let store: Arc<dyn Store> =
            Arc::new(testsupport::ObservingStore::new(inmemory(), calls.clone()));
        let log = TestLog::new();
        let zslg_reader: Arc<dyn zslg::Reader> = Arc::new(testsupport::ObservingReader::new(
            log.reader(),
            calls.clone(),
        ));

        let mut dispatcher = Dispatcher::from_db(
            store,
            log.writer(),
            zslg_reader,
            FLUSH_INTERVAL,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
        let mut manager = dispatcher.new_handler::<event::Event>(tenant());
        manager.register::<User>().unwrap();
        let (_writer, reader) = manager.finalise().unwrap();

        calls.lock().unwrap().clear();
        let _snapshot = reader.unstable_snapshot().unwrap();

        let seen: Vec<&str> = testsupport::recorded(&calls)
            .into_iter()
            .filter(|c| ["unstable_tail", "write"].contains(c))
            .collect();
        assert_eq!(seen, vec!["unstable_tail", "write"]);
    }

    fn user_name(snapshot: &Snapshot<'_>, id: u128) -> Option<String> {
        snapshot
            .get(Ref::<User>::new(UserId(id).into()))
            .unwrap()
            .map(|u| u.name)
    }

    /// The seam: zslg's stable watermark moves as soon as a snapshot is
    /// loaded, and the tail is cut there, but the fold has not folded that
    /// snapshot's entries yet. Read straight away, the entry would be in
    /// neither half.
    #[tokio::test(flavor = "multi_thread")]
    async fn unstable_snapshot_waits_for_the_fold_to_reach_the_stable_watermark() {
        let store = inmemory();
        let log = TestLog::new();
        let (scripted, feed) = testsupport::ScriptedReader::new();
        let mut dispatcher = Dispatcher::from_db(
            store,
            log.writer(),
            scripted.clone(),
            FLUSH_INTERVAL,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
        let mut manager = dispatcher.new_handler::<event::Event>(tenant());
        manager.register::<User>().unwrap();
        let (_writer, reader) = manager.finalise().unwrap();
        let _handle = dispatcher.finalise().await.unwrap();

        // Loaded at 10: the tail now starts above it and no longer carries
        // the entry; only the fresher write at 20 is left in it.
        scripted.set_stable_watermark(10);
        scripted.push_unstable(
            testsupport::data_id(20, 2),
            payload(tenant_id(), &[new_user(2, "naomi")]),
        );
        let reader = Arc::new(reader);
        let read = tokio::task::spawn_blocking({
            let reader = Arc::clone(&reader);
            move || {
                let snapshot = reader.unstable_snapshot().unwrap();
                (user_name(&snapshot, 1), user_name(&snapshot, 2))
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !read.is_finished(),
            "the read must wait while the fold is behind the stable watermark"
        );

        feed.send(Ok(zslg::StableLogEvent::Appended {
            index: zslg::LogIndex::ZERO,
            data: Data {
                data_id: testsupport::data_id(10, 1),
                blob: payload(tenant_id(), &[new_user(1, "holden")]),
            },
        }))
        .unwrap();
        feed.send(Ok(zslg::StableLogEvent::Watermark(testsupport::hlc(10))))
            .unwrap();

        let (stable, unstable) = tokio::time::timeout(std::time::Duration::from_secs(5), read)
            .await
            .expect("the read returns once the fold catches up")
            .unwrap();
        assert_eq!(stable.as_deref(), Some("holden"), "folded, below the cut");
        assert_eq!(
            unstable.as_deref(),
            Some("naomi"),
            "from the tail, above it"
        );
    }

    #[tokio::test]
    async fn unstable_snapshot_hides_tail_entries_at_or_below_the_folded_watermark() {
        let (store, log, reader) = reader().await;
        let txn = store.write().unwrap();
        txn.commit(Watermark {
            index: None,
            data_id: Some(testsupport::data_id(10, 1)),
            hlc: Some(testsupport::hlc(50)),
        })
        .unwrap();

        // Below the settled prefix but never part of it: a write that did not
        // make quorum. Above it: fresh.
        log.push_unstable_at(
            testsupport::data_id(30, 2),
            payload(tenant_id(), &[new_user(1, "doomed")]),
        );
        log.push_unstable_at(
            testsupport::data_id(60, 3),
            payload(tenant_id(), &[new_user(2, "fresh")]),
        );
        drop(log);

        let snapshot = reader.unstable_snapshot().unwrap();
        assert_eq!(user_name(&snapshot, 1), None);
        assert_eq!(user_name(&snapshot, 2).as_deref(), Some("fresh"));
    }

    #[tokio::test]
    async fn a_snapshot_clone_reads_the_same_state() {
        let (_store, mut log, reader) = reader().await;
        log.push_unstable(payload(tenant_id(), &[new_user(1, "holden")]));

        let snapshot = reader.unstable_snapshot().unwrap();
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
        use tokio::time::{Duration, timeout};
        use zslg::ChannelEnd;

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
        ) -> (Channels<T>, Vec<Result<T, ChannelEnd>>) {
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
            assert!(matches!(got[1], Err(ChannelEnd::Aborted { .. })));
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
                assert!(matches!(got[0], Err(ChannelEnd::Aborted { .. })));
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

    mod subscribe_unstable_tail {
        use super::*;
        use crate::testsupport::{NewTodoV1, TodoId};
        use tokio::time::{Duration, timeout};
        use zslg::ChannelEnd;

        const WAIT: Duration = Duration::from_secs(5);

        fn new_todo(id: u128) -> event::Event {
            event::Event::NewTodo(NewTodoV1 {
                id: TodoId(id),
                user_id: UserId(100),
                title: format!("todo {id}"),
            })
        }

        /// The next item, off the runtime: `recv` blocks.
        async fn next<T: Send + 'static>(ch: Channels<T>) -> (Channels<T>, Result<T, ChannelEnd>) {
            timeout(
                WAIT,
                tokio::task::spawn_blocking(move || {
                    let mut ch = ch;
                    let got = ch.recv();
                    (ch, got)
                }),
            )
            .await
            .expect("the subscription delivered in time")
            .unwrap()
        }

        #[tokio::test]
        async fn announces_this_tenants_events() {
            let (_store, mut log, reader) = reader().await;
            let all = reader.subscribe_unstable_tail();

            log.push_unstable(payload(tenant_id(), &[new_todo(1)]));
            log.push_unstable(payload(tenant_id(), &[new_todo(2), new_user(1, "holden")]));

            let (all, first) = next(all).await;
            assert_eq!(first.unwrap().unwrap().len(), 1);
            let (_all, second) = next(all).await;
            assert_eq!(second.unwrap().unwrap().len(), 2);
        }

        #[tokio::test]
        async fn skips_foreign_tenants_and_watermarks() {
            let (_store, mut log, reader) = reader().await;
            let users = reader.subscribe_unstable_tail();

            let other_tenant = crate::TenantId::from_string_hash("other", 0);
            log.push_unstable(payload(other_tenant, &[new_user(1, "amos")]));
            // Below the next write's HLC, so the write is still announced.
            log.watermark(0);
            log.push_unstable(payload(tenant_id(), &[new_user(2, "naomi")]));

            let (_users, got) = next(users).await;
            assert!(matches!(
                got.unwrap().unwrap().as_slice(),
                [event::Event::NewUser(u)] if u.name == "naomi"
            ));
        }

        #[tokio::test]
        async fn an_undecodable_payload_is_an_error_and_the_stream_goes_on() {
            let (_store, mut log, reader) = reader().await;
            let users = reader.subscribe_unstable_tail();

            let mut garbage = bytes::BytesMut::new();
            garbage.extend_from_slice(&tenant_id().as_slice());
            garbage.extend_from_slice(&[0xff; 4]);
            log.push_unstable(garbage.freeze());
            log.push_unstable(Bytes::from_static(b"short"));
            log.push_unstable(payload(tenant_id(), &[new_user(1, "holden")]));

            let (users, undecodable) = next(users).await;
            assert!(undecodable.unwrap().is_err());
            let (users, short) = next(users).await;
            assert!(matches!(short.unwrap(), Err(Error::PayloadTooShort(5, _))));
            let (_users, decoded) = next(users).await;
            assert_eq!(decoded.unwrap().unwrap().len(), 1);
        }

        /// The stream reads zslg, not the fold, so a fold that stopped for a
        /// reason other than a cancel leaves it running.
        #[tokio::test]
        async fn outlives_a_fold_that_stopped() {
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
            let handle = dispatcher.finalise().await.unwrap();
            let users = reader.subscribe_unstable_tail();

            log.disconnect();
            let folded = timeout(WAIT, handle).await.unwrap().unwrap();
            assert!(matches!(folded, Err(Error::SubscriptionAborted)));

            let mut log = log;
            log.push_unstable(payload(tenant_id(), &[new_user(1, "holden")]));
            let (_users, got) = next(users).await;
            assert_eq!(got.unwrap().unwrap().len(), 1);
        }
    }
}
