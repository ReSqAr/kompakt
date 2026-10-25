//! A projection is a row type plus [`Projection::next`]: the function from
//! *(event, the data so far)* to *the rows that change*.
//!
//! ```text
//!   entry = [e1, e2]     projectors: User, Todo
//!
//!   e1:  User.next(e1, view) ─┐                      both see all of e0, none of e1
//!        Todo.next(e1, view) ─┴─> writes applied     together, after both returned
//!   e2:  User.next(e2, view) ...                     sees e1 fully, both projectors
//! ```
//!
//! A `next` call reads the state after everything applied before it: earlier
//! events completely, and none of this event's writes — not even those of
//! another projection — so the order of the projections does not matter and
//! cannot be relied on. A projection that needs the effect of the event on
//! another projection's rows derives it from the event. Within one call every read sees the same state, because its writes apply
//! only after it returns, so two writes to one row inside a single `Vec`
//! fold themselves. The framework then moves each row's index entries with it.
//!
//! A `next` is usually a `match` on the event whose arms are one
//! [`Write`] helper each: `set`/`delete` blind, `insert`/`modify`/`update`
//! after reading the row. Anything else (fan-out, cascades, several rows per
//! event) builds the `Vec` by hand.

use crate::metrics::{self, RowOp};
use crate::{
    Error, IndexId, ProjectionId, ProjectionTypeId, SubEventOf, Tenant, View, WriteTransaction,
    WriteTransactionImpl,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

pub trait Projection:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    /// The events this projection folds: a subset enum borrowed from the
    /// event (`event::TodoEvent<'a>`), so folding copies nothing.
    type Event<'a>;
    const TYPE_ID: ProjectionTypeId;
    /// The indices the framework maintains for this projection; registering
    /// the projection registers them. Each is usually a named [`Index`] const
    /// (`&[&Todo::BY_USER]`), the same one `lookup` and `join_idx` take.
    const INDICES: Indices<Self> = &[];
    fn id(&self) -> Ref<Self>;

    /// The rows `event` changes. Reads go through `view`; nothing is written
    /// until this returns.
    ///
    /// `Err` means the *store* failed (it is what `?` on a `view` read yields)
    /// and stops the fold. An event that is irrelevant or invalid is
    /// `Ok(vec![])`: events live in the log forever, so an `Err` on one would
    /// halt the fold again at every restart.
    fn next(event: &Self::Event<'_>, view: &View<'_>) -> Result<Vec<Write<Self>>, Error>;

    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(postcard::to_stdvec(&self)?)
    }

    fn decode(data: &[u8]) -> Result<Self, Error> {
        Ok(postcard::from_bytes(data)?)
    }
}

/// One row change returned by [`Projection::next`]. A `Set` names its row by
/// [`Projection::id`], so a row cannot be stored under the wrong id.
///
/// The associated functions build a whole `next` result, so a simple match
/// arm is one call. The ones that read check the row they store against
/// `key` ([`Error::IncorrectProjectionID`]), since a row filed under another
/// id is one the next `get(key)` never finds. Their reads see the state
/// before this `next` call: two of them on one key inside one call both read
/// the old row, and the later write wins.
#[derive(Debug, Clone, PartialEq)]
pub enum Write<P: Projection> {
    Set(P),
    Delete(Ref<P>),
}

impl<P: Projection> Write<P> {
    /// Stores `row`, whatever was there.
    #[must_use = "a write does nothing until `next` returns it"]
    pub fn set(row: P) -> Result<Vec<Self>, Error> {
        Ok(vec![Self::Set(row)])
    }

    /// Deletes the row at `key`, if any.
    #[must_use = "a write does nothing until `next` returns it"]
    pub fn delete(key: impl Into<Ref<P>>) -> Result<Vec<Self>, Error> {
        Ok(vec![Self::Delete(key.into())])
    }

    /// Stores `create()` if no row exists at `key`; otherwise writes nothing.
    #[must_use = "a write does nothing until `next` returns it"]
    pub fn insert(
        view: &View<'_>,
        key: impl Into<Ref<P>>,
        create: impl FnOnce() -> P,
    ) -> Result<Vec<Self>, Error> {
        let key = key.into();
        match view.get(key)? {
            Some(_) => Ok(vec![]),
            None => Self::checked(key, create()),
        }
    }

    /// Stores `f(row)` if a row exists at `key`; otherwise writes nothing.
    #[must_use = "a write does nothing until `next` returns it"]
    pub fn modify(
        view: &View<'_>,
        key: impl Into<Ref<P>>,
        f: impl FnOnce(P) -> P,
    ) -> Result<Vec<Self>, Error> {
        let key = key.into();
        match view.get(key)? {
            Some(row) => Self::checked(key, f(row)),
            None => Ok(vec![]),
        }
    }

    /// Reads the row at `key` and writes what `f` returns: `Some` stores it,
    /// `None` deletes it. Absent in and absent out writes nothing.
    #[must_use = "a write does nothing until `next` returns it"]
    pub fn update(
        view: &View<'_>,
        key: impl Into<Ref<P>>,
        f: impl FnOnce(Option<P>) -> Option<P>,
    ) -> Result<Vec<Self>, Error> {
        let key = key.into();
        let current = view.get(key)?;
        let existed = current.is_some();
        match f(current) {
            Some(row) => Self::checked(key, row),
            None if existed => Ok(vec![Self::Delete(key)]),
            None => Ok(vec![]),
        }
    }

    /// `Set(row)`, if `row` files under `key`.
    fn checked(key: Ref<P>, row: P) -> Result<Vec<Self>, Error> {
        if row.id().id() != key.id() {
            return Err(Error::IncorrectProjectionID {
                type_name: std::any::type_name::<P>(),
                expected: key.id(),
                actual: row.id().id(),
            });
        }
        Ok(vec![Self::Set(row)])
    }
}

#[derive(Ord, Serialize, Deserialize, PartialOrd, Eq, PartialEq, Debug, Hash)]
pub struct Ref<P: Projection> {
    id: ProjectionId,
    phantom: PhantomData<P>,
}

impl<P: Projection> Ref<P> {
    #[must_use]
    pub fn new(id: ProjectionId) -> Self {
        Self {
            id,
            phantom: PhantomData,
        }
    }

    #[must_use]
    pub const fn type_id(&self) -> ProjectionTypeId {
        P::TYPE_ID
    }

    #[must_use]
    pub fn id(&self) -> ProjectionId {
        self.id
    }
}

impl<P: Projection> Clone for Ref<P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P: Projection> Copy for Ref<P> {}

pub(crate) trait ProjectorLike<Ev, Txn>: Sync + Send + 'static
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
pub(crate) type Apply<'p, Txn> = Box<dyn FnOnce(&mut Txn) -> Result<(), Error> + 'p>;

pub(crate) type BoxedProjectorLike<Ev> =
    Box<dyn for<'a> ProjectorLike<Ev, WriteTransactionImpl<'a>>>;

/// Folds one event into every projection. Each `next` reads the state before
/// the event — none sees another's writes for it — and the writes are stored
/// together afterwards, so the result does not depend on the order of the
/// projections.
pub(crate) fn fold_event<Ev: 'static>(
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
pub(crate) struct Projector<P: 'static> {
    indices: BTreeMap<IndexId, &'static dyn AnyIndex<P>>,
}

impl<P: Projection> Projector<P> {
    pub(crate) fn new() -> Result<Self, Error> {
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
pub trait AnyIndex<P>: Send + Sync {
    fn id(&self) -> IndexId;
    /// The id of the row `row` is filed under.
    fn key(&self, row: &P) -> ProjectionId;
}

/// What [`Projection::INDICES`] holds: `&[&Todo::BY_USER, &Todo::BY_LIST]`.
pub type Indices<P> = &'static [&'static dyn AnyIndex<P>];

#[derive(Debug)]
pub struct Index<P, Q: Projection> {
    id: IndexId,
    key: fn(&P) -> Ref<Q>,
}

impl<P, Q: Projection> Clone for Index<P, Q> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P, Q: Projection> Copy for Index<P, Q> {}

impl<P, Q: Projection> Index<P, Q> {
    /// The index `P -> Q` under `id`, filing each row under `key(row)`. A
    /// `const` of this type is the whole index: registration, a projection's
    /// `next` and the reader's joins all name it, so they cannot disagree
    /// about what it means. `key` is a plain `fn`: a pure function of the row.
    #[must_use]
    pub const fn new(id: IndexId, key: fn(&P) -> Ref<Q>) -> Self {
        Self { id, key }
    }

    pub(crate) fn id(&self) -> IndexId {
        self.id
    }
}

impl<P, Q: Projection> AnyIndex<P> for Index<P, Q> {
    fn id(&self) -> IndexId {
        self.id
    }

    fn key(&self, row: &P) -> ProjectionId {
        (self.key)(row).id()
    }
}

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
                        store.add_index(tenant_id, P::TYPE_ID, *iid, r.id, next_i)?;
                        store.remove_index(tenant_id, P::TYPE_ID, *iid, r.id, current_i)?;
                    }
                }
                (None, Some(next_i)) => {
                    store.add_index(tenant_id, P::TYPE_ID, *iid, r.id, next_i)?;
                }
                (Some(current_i), None) => {
                    store.remove_index(tenant_id, P::TYPE_ID, *iid, r.id, current_i)?;
                }
                (None, None) => {}
            }
        }

        Ok(())
    }
}

pub struct ProjectorManager<Ev> {
    pub(crate) tenant: Tenant,
    pub(crate) projectors: BTreeMap<ProjectionTypeId, BoxedProjectorLike<Ev>>,
    pub(crate) registered_projection_types: BTreeSet<ProjectionTypeId>,
    pub(crate) registered_indices: BTreeSet<(ProjectionTypeId, IndexId)>,
}

impl<Ev> ProjectorManager<Ev> {
    #[must_use]
    pub fn new(tenant: Tenant) -> Self {
        Self {
            tenant,
            projectors: BTreeMap::new(),
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

        Ok(())
    }
}

#[cfg(test)]
mod next_tests;

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
        ReadTransactionExt,
        testsupport::{
            NewTodoV1, NewUserV3, Todo, TodoDeletedV1, TodoId, User, UserId, event, inmemory,
            tenant, tenant_id,
        },
    };
    use std::sync::Arc;

    /// `Todo::BY_USER`'s id.
    const IID: IndexId = IndexId::from_u32(1);

    fn todo(id: u128, user_id: u128, title: &str) -> NewTodoV1 {
        NewTodoV1 {
            id: TodoId(id),
            user_id: UserId(user_id),
            title: title.to_string(),
        }
    }

    fn rig() -> (Arc<dyn crate::Store>, ProjectorManager<event::Event>) {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();
        (inmemory(), manager)
    }

    fn handle(
        store: &Arc<dyn crate::Store>,
        manager: &ProjectorManager<event::Event>,
        events: &[event::Event],
    ) {
        let mut txn = store.write().unwrap();

        for event in events {
            fold_event(&manager.projectors, tenant(), event, &mut txn).unwrap();
        }

        txn.commit(crate::Watermark::default()).unwrap();
    }

    fn indexed_under(store: &Arc<dyn crate::Store>, user_id: u128) -> Vec<ProjectionId> {
        let txn = store.read().unwrap();
        txn.scan_index(
            tenant_id(),
            Todo::TYPE_ID,
            IID,
            ProjectionId::from_u128(user_id),
        )
        .map(|r| r.unwrap())
        .collect()
    }

    /// The wiring, not just the helper: a fold through a real projector must
    /// label the row counter with this tenant and this projection type.
    #[test]
    fn project_counts_rows_by_tenant_and_projection() {
        use metrics_util::debugging::DebuggingRecorder;

        let store = inmemory();
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let mut txn = store.write().unwrap();
        // `crate::metrics` shadows the facade in this module.
        ::metrics::with_local_recorder(&recorder, || {
            for event in [
                event::Event::NewTodo(todo(1, 1, "t")),
                event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(1) }),
            ] {
                fold_event(&manager.projectors, tenant(), &event, &mut txn).unwrap();
            }
        });

        let snapshot = snapshotter.snapshot().into_hashmap();
        let ops: Vec<String> = snapshot
            .keys()
            .filter(|k| k.key().name() == "nvzn_projection_rows_total")
            .map(|k| {
                let mut ls: Vec<String> = k
                    .key()
                    .labels()
                    .map(|l| format!("{}={}", l.key(), l.value()))
                    .collect();
                ls.sort();
                ls.join(",")
            })
            .collect();
        assert!(
            ops.contains(&"op=upsert,projection=Todo,tenant=test".to_string()),
            "no upsert row counted: {ops:?}"
        );
        assert!(
            ops.contains(&"op=delete,projection=Todo,tenant=test".to_string()),
            "no delete row counted: {ops:?}"
        );
    }

    /// A fold that stores the row exactly as it is stored writes nothing:
    /// no row, no index entry, no counted upsert.
    #[test]
    fn identical_write_is_skipped() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let (store, manager) = rig();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let mut txn = store.write().unwrap();
        ::metrics::with_local_recorder(&recorder, || {
            for _ in 0..2 {
                let event = event::Event::NewTodo(todo(1, 1, "t"));
                fold_event(&manager.projectors, tenant(), &event, &mut txn).unwrap();
            }
        });
        let upserts: u64 = snapshotter
            .snapshot()
            .into_hashmap()
            .into_iter()
            .filter(|(k, _)| {
                k.key().name() == "nvzn_projection_rows_total"
                    && k.key().labels().any(|l| l.value() == "upsert")
            })
            .map(|(_, (_, _, v))| match v {
                DebugValue::Counter(n) => n,
                _ => 0,
            })
            .sum();
        assert_eq!(upserts, 1, "the second, identical write is a no-op");
    }

    #[test]
    fn register_sets_and_indexes() {
        let (store, manager) = rig();
        handle(&store, &manager, &[event::Event::NewTodo(todo(1, 42, "t"))]);

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert_eq!(got.unwrap().title, "t");
        assert_eq!(indexed_under(&store, 42), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn projector_rekey_moves_index() {
        let (store, manager) = rig();
        handle(&store, &manager, &[event::Event::NewTodo(todo(1, 42, "t"))]);
        handle(
            &store,
            &manager,
            &[event::Event::NewTodo(todo(1, 43, "t2"))],
        );

        assert!(indexed_under(&store, 42).is_empty());
        assert_eq!(indexed_under(&store, 43), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn projector_delete_removes_index_and_tombs() {
        let (store, manager) = rig();
        handle(&store, &manager, &[event::Event::NewTodo(todo(1, 42, "t"))]);
        handle(
            &store,
            &manager,
            &[event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(1) })],
        );

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert!(got.is_none());
        assert!(indexed_under(&store, 42).is_empty());
    }

    #[test]
    fn projector_reapply_is_idempotent() {
        let (store, manager) = rig();
        let ev = event::Event::NewTodo(todo(1, 42, "t"));
        handle(&store, &manager, &[ev.clone(), ev]);

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert_eq!(got.unwrap().title, "t");
        assert_eq!(indexed_under(&store, 42), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn manager_duplicate_type_id_rejected() {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();
        let err = manager.register::<Todo>().expect_err("duplicate must err");
        assert!(matches!(err, crate::Error::ProjectionAlreadyRegistered(_)));
    }

    #[test]
    fn projector_ignores_events_outside_projection() {
        let (store, manager) = rig();
        let user_event = event::Event::NewUser(NewUserV3 {
            id: UserId(1),
            name: "holden".to_string(),
            address: None,
            email: None,
        });
        handle(&store, &manager, &[user_event]);

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert!(got.is_none());
        assert!(indexed_under(&store, 1).is_empty());
    }

    fn rig_both() -> (Arc<dyn crate::Store>, ProjectorManager<event::Event>) {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();
        manager.register::<User>().unwrap();
        (inmemory(), manager)
    }

    #[test]
    fn projector_recreate_after_delete_in_one_batch() {
        let (store, manager) = rig();
        handle(
            &store,
            &manager,
            &[
                event::Event::NewTodo(todo(1, 42, "first")),
                event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(1) }),
                event::Event::NewTodo(todo(1, 43, "second")),
            ],
        );

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert_eq!(got.unwrap().title, "second");
        drop(txn);

        assert!(indexed_under(&store, 42).is_empty());
        assert_eq!(indexed_under(&store, 43), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn projector_handles_two_projection_types_in_one_batch() {
        let (store, manager) = rig_both();
        handle(
            &store,
            &manager,
            &[
                event::Event::NewUser(NewUserV3 {
                    id: UserId(42),
                    name: "holden".to_string(),
                    address: None,
                    email: None,
                }),
                event::Event::NewTodo(todo(1, 42, "t")),
            ],
        );

        let txn = store.read().unwrap();
        assert_eq!(
            ReadTransactionExt::get::<User>(&*txn, tenant_id(), Ref::new(UserId(42).into()))
                .unwrap()
                .unwrap()
                .name,
            "holden"
        );
        assert_eq!(
            ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
                .unwrap()
                .unwrap()
                .title,
            "t"
        );
        assert_eq!(
            ReadTransactionExt::scan::<Todo>(&*txn, tenant_id())
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            1,
            "a Todo scan must not see the User row"
        );
        drop(txn);

        assert_eq!(indexed_under(&store, 42), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn projector_index_moves_and_moves_back_in_one_batch() {
        let (store, manager) = rig();
        handle(
            &store,
            &manager,
            &[
                event::Event::NewTodo(todo(1, 42, "a")),
                event::Event::NewTodo(todo(1, 43, "b")),
                event::Event::NewTodo(todo(1, 42, "c")),
            ],
        );

        assert_eq!(indexed_under(&store, 42), vec![ProjectionId::from_u128(1)]);
        assert!(indexed_under(&store, 43).is_empty());
    }

    /// A user row indexed under its own id, with `Todo::BY_USER`'s index id.
    #[derive(Debug, Serialize, Deserialize)]
    struct UserById {
        id: UserId,
    }

    impl Projection for UserById {
        type Event<'a> = event::UserEvent<'a>;
        const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32(0x1D);
        const INDICES: Indices<Self> = &[&Index::new(IID, UserById::id)];

        fn id(&self) -> Ref<Self> {
            Ref::new(self.id.into())
        }

        fn next(ev: &event::UserEvent, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
            match ev {
                event::UserEvent::NewUser(n) => Write::set(UserById { id: n.id }),
                event::UserEvent::Pinged(_) => Ok(vec![]),
            }
        }
    }

    #[test]
    fn index_ids_may_repeat_across_projections() {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();
        manager.register::<UserById>().unwrap();
        let store = inmemory();

        handle(
            &store,
            &manager,
            &[
                event::Event::NewUser(NewUserV3 {
                    id: UserId(42),
                    name: "holden".to_string(),
                    address: None,
                    email: None,
                }),
                event::Event::NewTodo(todo(7, 42, "t")),
            ],
        );

        let txn = store.read().unwrap();
        let under = |ptid| {
            txn.scan_index(tenant_id(), ptid, IID, ProjectionId::from_u128(42))
                .map(Result::unwrap)
                .collect::<Vec<_>>()
        };
        assert_eq!(under(Todo::TYPE_ID), vec![ProjectionId::from_u128(7)]);
        assert_eq!(under(UserById::TYPE_ID), vec![ProjectionId::from_u128(42)]);
    }

    /// Lists two indices under one id.
    #[derive(Debug, Serialize, Deserialize)]
    struct TwoUnderOneId {
        id: TodoId,
        user_id: UserId,
    }

    impl Projection for TwoUnderOneId {
        type Event<'a> = event::TodoEvent<'a>;
        const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32(0xD0);
        const INDICES: Indices<Self> = &[
            &Index::<Self, User>::new(IID, |t| Ref::new(t.user_id.into())),
            &Index::<Self, Self>::new(IID, TwoUnderOneId::id),
        ];

        fn id(&self) -> Ref<Self> {
            Ref::new(self.id.into())
        }

        fn next(_: &event::TodoEvent, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
            Ok(vec![])
        }
    }

    #[test]
    fn register_rejects_two_indices_under_one_id() {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        let err = manager
            .register::<TwoUnderOneId>()
            .expect_err("duplicate index must err");
        assert!(
            matches!(err, crate::Error::IndexAlreadyRegistered(1)),
            "{err:?}"
        );
        assert!(manager.projectors.is_empty(), "nothing half-registered");
    }
}
