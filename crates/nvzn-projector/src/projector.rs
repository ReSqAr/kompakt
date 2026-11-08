//! Folds events into projections: one `Projector` per projection type, and the
//! manager that registers them.

use crate::metrics::{self, RowOp};
use bytes::Bytes;
use nvzn_projection::{AnyIndex, Projection, View, Write};
use nvzn_store::{WriteTransaction, WriteTransactionImpl};
use nvzn_types::{Error, IndexId, ProjectionTypeId, SubEventOf, Tenant};
use std::collections::{BTreeMap, BTreeSet};

pub trait ProjectorLike<Ev, Txn>: Sync + Send + 'static
where
    Txn: WriteTransaction,
{
    /// Runs this projection's `next` for `event` against `txn` as it is, and
    /// returns the step that stores the result. Nothing is written until the
    /// step runs, so every projection of an event reads the same state.
    fn plan<'p>(
        &'p self,
        tenant: Tenant,
        event: &Ev,
        txn: &Txn,
    ) -> Result<Option<Apply<'p, Txn>>, Error>;
}

/// The storing half of one projection's fold of one event.
pub type Apply<'p, Txn> = Box<dyn FnOnce(&mut Txn) -> Result<(), Error> + 'p>;

pub type BoxedProjectorLike<Ev> = Box<dyn for<'a> ProjectorLike<Ev, WriteTransactionImpl<'a>>>;

/// Folds one event into every projection. Each `next` reads the state before
/// the event — none sees another's writes for it — and the writes are stored
/// together afterwards, so the result does not depend on the order of the
/// projections.
pub fn fold_event<Ev: 'static>(
    projectors: &BTreeMap<ProjectionTypeId, BoxedProjectorLike<Ev>>,
    tenant: Tenant,
    event: &Ev,
    txn: &mut WriteTransactionImpl<'_>,
) -> Result<(), Error> {
    let mut steps = Vec::new();
    for projector in projectors.values() {
        steps.extend(projector.plan(tenant, event, txn)?);
    }
    for step in steps {
        step(txn)?;
    }
    Ok(())
}

/// Folds one projection type: runs its `next` and maintains its
/// [`Projection::INDICES`].
pub struct Projector<P: 'static> {
    indices: BTreeMap<IndexId, &'static dyn AnyIndex<P>>,
}

impl<P: Projection> Projector<P> {
    pub fn new() -> Result<Self, Error> {
        let mut indices = BTreeMap::new();
        for index in P::INDICES {
            if indices.insert(index.id(), *index).is_some() {
                return Err(Error::IndexAlreadyRegistered(index.id().as_u32()));
            }
        }
        Ok(Self { indices })
    }
}

/// An [`Index`] with its target type erased, so one projection's indices fit
/// one list whatever they point at. Implemented by every `Index<P, Q>`.
impl<P: Projection, Ev, Txn> ProjectorLike<Ev, Txn> for Projector<P>
where
    Txn: WriteTransaction,
    for<'a> P::Event<'a>: SubEventOf<'a, Ev>,
{
    fn plan<'p>(
        &'p self,
        tenant: Tenant,
        event: &Ev,
        txn: &Txn,
    ) -> Result<Option<Apply<'p, Txn>>, Error> {
        let Some(subevent) = <P::Event<'_> as SubEventOf<Ev>>::project(event) else {
            return Ok(None);
        };

        let writes = P::next(&subevent, &View::new(tenant.id(), txn))?;
        Ok(Some(Box::new(move |txn: &mut Txn| {
            for write in writes {
                self.apply(tenant, write, txn)?;
            }
            Ok(())
        })))
    }
}

impl<P: Projection> Projector<P> {
    /// Stores one row change and moves the row's index entries with it.
    fn apply<Txn: WriteTransaction>(
        &self,
        tenant: Tenant,
        write: Write<P>,
        store: &mut Txn,
    ) -> Result<(), Error> {
        let tenant_id = tenant.id();
        let (r, next) = match write {
            Write::Set(p) => (p.id(), Some(p)),
            Write::Delete(r) => (r, None),
        };

        let stored = store.get_raw(tenant_id, r.type_id(), r.id())?;
        let current = stored.as_ref().map(|b| P::decode(b)).transpose()?;
        // A write that stores the bytes already there changes neither the
        // row nor, since indices are a function of the row, its index entries.
        let encoded = next.as_ref().map(P::encode).transpose()?.map(Bytes::from);
        if stored == encoded {
            return Ok(());
        }
        let mut current_indices = current.as_ref().map_or_else(BTreeMap::default, |c| {
            self.indices
                .iter()
                .map(|(iid, ix)| (iid, ix.key(c)))
                .collect()
        });

        store.set_raw(tenant_id, r.type_id(), r.id(), encoded)?;
        metrics::projection_row(
            tenant,
            metrics::short_type_name::<P>(),
            if next.is_some() {
                RowOp::Upsert
            } else {
                RowOp::Delete
            },
        );

        for (iid, ix) in &self.indices {
            let current_i = current_indices.remove(iid);
            let next_i = next.as_ref().map(|n| ix.key(n));

            match (current_i, next_i) {
                (Some(current_i), Some(next_i)) => {
                    if current_i != next_i {
                        store.add_index(tenant_id, P::TYPE_ID, *iid, r.id(), next_i)?;
                        store.remove_index(tenant_id, P::TYPE_ID, *iid, r.id(), current_i)?;
                    }
                }
                (None, Some(next_i)) => {
                    store.add_index(tenant_id, P::TYPE_ID, *iid, r.id(), next_i)?;
                }
                (Some(current_i), None) => {
                    store.remove_index(tenant_id, P::TYPE_ID, *iid, r.id(), current_i)?;
                }
                (None, None) => {}
            }
        }

        Ok(())
    }
}

/// What a live view needs of a registered projection: the events it folds,
/// and how to build a projector of its own (the fold keeps the first one).
pub struct Registered<Ev> {
    pub events: &'static [u32],
    pub make: fn() -> Result<BoxedProjectorLike<Ev>, Error>,
}

fn make_projector<P, Ev>() -> Result<BoxedProjectorLike<Ev>, Error>
where
    P: Projection,
    for<'a> P::Event<'a>: SubEventOf<'a, Ev>,
{
    Ok(Box::new(Projector::<P>::new()?))
}

pub struct ProjectorManager<Ev> {
    pub tenant: Tenant,
    pub projectors: BTreeMap<ProjectionTypeId, BoxedProjectorLike<Ev>>,
    pub registered: BTreeMap<ProjectionTypeId, Registered<Ev>>,
    pub registered_projection_types: BTreeSet<ProjectionTypeId>,
    pub registered_indices: BTreeSet<(ProjectionTypeId, IndexId)>,
}

impl<Ev> ProjectorManager<Ev> {
    #[must_use]
    pub fn new(tenant: Tenant) -> Self {
        Self {
            tenant,
            projectors: BTreeMap::new(),
            registered: BTreeMap::new(),
            registered_projection_types: BTreeSet::new(),
            registered_indices: BTreeSet::new(),
        }
    }

    #[must_use]
    pub fn tenant(&self) -> Tenant {
        self.tenant
    }
}

impl<Ev> ProjectorManager<Ev>
where
    Ev: Send + Sync + 'static,
{
    /// Registers `P` and every index in its [`Projection::INDICES`].
    pub fn register<P>(&mut self) -> Result<(), Error>
    where
        P: Projection,
        for<'a> P::Event<'a>: SubEventOf<'a, Ev>,
    {
        if self.projectors.contains_key(&P::TYPE_ID) {
            return Err(Error::ProjectionAlreadyRegistered(P::TYPE_ID.as_u32()));
        }
        let p = Projector::<P>::new()?;

        self.registered_projection_types.insert(P::TYPE_ID);
        for iid in p.indices.keys() {
            self.registered_indices.insert((P::TYPE_ID, *iid));
        }

        self.projectors.insert(P::TYPE_ID, Box::new(p));
        self.registered.insert(
            P::TYPE_ID,
            Registered {
                events: P::EVENTS,
                make: make_projector::<P, Ev>,
            },
        );

        Ok(())
    }
}
