use crate::PENDING_SUBSCRIBERS;
use crate::Syncer;
use crate::live::{self, Live, LiveLimits};
use crate::metrics::{self, ReadKind};
use crate::subset;
use crate::{
    ArcStore, Error, EventSet, Projection, ProjectionTypeId, Ref, Snapshot, SubEventOf, Tenant,
    WithWireFormat,
};
use crate::{FoldState, Subscriptions};
use crossbeam_combinators::{Channel, Infinite, RegistryError};
use nvzn_projector::Registered;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::Arc;

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
    /// # async fn f<Ev>(reader: &nvzn_dispatcher::Reader<Ev>) -> Result<(), nvzn_types::Error> {
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
        Ok(Snapshot::from_txn(self.tenant.id(), self.store.read()?))
    }

    /// Every stable `P` committed from now on, deletions included, as a
    /// [`Ref`] to look up. The fold blocks on a subscriber that stops reading;
    /// the channel ends only with the fold, as
    /// [`AbortReason::ProducerGone`](crossbeam_combinators::AbortReason::ProducerGone).
    /// A caller that needs to distinguish a cancellation from a failure binds
    /// its own [`Cancellation`](crossbeam_combinators::Cancellation) with
    /// `until`.
    ///
    /// `P` must be over this tenant's events, as for
    /// [`register`](crate::AssociatedProjectorManager::register), and fails
    /// with [`Error::ProjectionNotRegistered`] unless it was registered for
    /// this tenant — such a stream would never fire.
    ///
    /// A projection over unrelated events does not compile:
    ///
    /// ```compile_fail
    /// fn subscribe<Ev, P: nvzn_projection::Projection>(reader: &nvzn_dispatcher::Reader<Ev>) {
    ///     let _ = reader.subscribe_stable_log::<P>();
    /// }
    /// ```
    ///
    /// whereas with the bound it does:
    ///
    /// ```
    /// fn subscribe<Ev, P>(reader: &nvzn_dispatcher::Reader<Ev>)
    /// where
    ///     P: nvzn_projection::Projection,
    ///     for<'a> P::Event<'a>: nvzn_types::SubEventOf<'a, Ev>,
    /// {
    ///     let _ = reader.subscribe_stable_log::<P>();
    /// }
    /// ```
    pub fn subscribe_stable_log<P>(&self) -> Result<Channel<Ref<P>, Infinite>, Error>
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
        let stream = match self
            .subscriptions
            .registry
            .subscribe((self.tenant.id(), P::TYPE_ID))
        {
            Ok(stream) => stream,
            Err(RegistryError::Full) => {
                return Err(Error::TooManyPendingSubscriptions(PENDING_SUBSCRIBERS));
            }
            Err(RegistryError::Closed) => Channel::aborted("nvzn-subscribe"),
        };
        Ok(stream.map(|entry| Ref::new(entry.2)))
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests;
