//! The contract of `Projection::next` and its adaptors, one claim per test.
//!
//! ```text
//!   event ──> projector A ──writes──> txn ──> projector B ──writes──> txn
//!                 reads state so far ─┘            reads A's same-event writes
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use super::*;
use crate::{
    ReadTransactionExt, Store, WriteTransactionExt,
    testsupport::{
        NewTodoV1, TodoId, User, UserDeletedV1, UserId, event, inmemory, tenant, tenant_id,
    },
};
use std::sync::Arc;

const IID: IndexId = IndexId::from_u32(1);

fn new_todo(id: u128, user_id: u128, title: &str) -> event::Event {
    event::Event::NewTodo(NewTodoV1 {
        id: TodoId(id),
        user_id: UserId(user_id),
        title: title.to_string(),
    })
}

fn user_deleted(id: u128) -> event::Event {
    event::Event::UserDeleted(UserDeletedV1 { id: UserId(id) })
}

fn run(
    store: &Arc<dyn Store>,
    manager: &ProjectorManager<event::Event>,
    events: &[event::Event],
) -> Result<(), Error> {
    let mut txn = store.write().unwrap();
    for event in events {
        fold_event(&manager.projectors, tenant(), event, &mut txn)?;
    }
    txn.commit(crate::Watermark::default())
}

fn row<P: Projection>(store: &Arc<dyn Store>, id: u128) -> Option<P> {
    let txn = store.read().unwrap();
    ReadTransactionExt::get::<P>(&*txn, tenant_id(), Ref::new(ProjectionId::from_u128(id))).unwrap()
}

fn staged<P: Projection>(txn: &WriteTransactionImpl<'_>, id: u128) -> Option<P> {
    ReadTransactionExt::get::<P>(txn, tenant_id(), Ref::new(ProjectionId::from_u128(id))).unwrap()
}

fn indexed_under<P: Projection>(store: &Arc<dyn Store>, user_id: u128) -> Vec<ProjectionId> {
    let txn = store.read().unwrap();
    txn.scan_index(
        tenant_id(),
        P::TYPE_ID,
        IID,
        ProjectionId::from_u128(user_id),
    )
    .map(Result::unwrap)
    .collect()
}

fn manager_of<P: Projection>() -> ProjectorManager<event::Event>
where
    for<'a> P::Event<'a>: SubEventOf<'a, event::Event>,
{
    let mut manager = ProjectorManager::<event::Event>::new(tenant());
    manager.register::<P>().unwrap();
    manager
}

/// Reacts to a foreign event (`UserDeleted`) by reading the reverse index.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct Pinned {
    id: TodoId,
    user_id: UserId,
}

impl Pinned {
    const BY_USER: Index<Pinned, User> = Index::new(IID, Pinned::user);

    fn user(&self) -> Ref<User> {
        Ref::new(self.user_id.into())
    }
}

impl Projection for Pinned {
    type Event<'a> = event::CascadeEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32(0xCA5C);
    const INDICES: Indices<Self> = &[&Pinned::BY_USER];

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(ev: &event::CascadeEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        match ev {
            event::CascadeEvent::NewTodo(n) => Ok(vec![Write::Set(Pinned {
                id: n.id,
                user_id: n.user_id,
            })]),
            event::CascadeEvent::UserDeleted(u) => view
                .lookup(Self::BY_USER, Ref::<User>::new(u.id.into()))
                .map(|r| r.map(Write::Delete))
                .collect(),
        }
    }
}

#[test]
fn next_cascades_deletes_via_lookup() {
    let store = inmemory();
    let manager = manager_of::<Pinned>();

    run(
        &store,
        &manager,
        &[
            new_todo(1, 42, "a"),
            new_todo(2, 42, "b"),
            new_todo(3, 7, "c"),
        ],
    )
    .unwrap();
    run(&store, &manager, &[user_deleted(42)]).unwrap();

    assert!(row::<Pinned>(&store, 1).is_none());
    assert!(row::<Pinned>(&store, 2).is_none());
    assert!(
        row::<Pinned>(&store, 3).is_some(),
        "another user's row stays"
    );
    assert!(indexed_under::<Pinned>(&store, 42).is_empty());
    assert_eq!(
        indexed_under::<Pinned>(&store, 7),
        vec![ProjectionId::from_u128(3)]
    );
}

macro_rules! probe {
    ($name:ident, $type_id:expr, $other:ident) => {
        /// Records whether the other probe's row for the same todo was
        /// visible when this one ran.
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct $name {
            id: TodoId,
            other_row_visible: bool,
        }

        impl Projection for $name {
            type Event<'a> = event::TodoEvent<'a>;
            const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32($type_id);

            fn id(&self) -> Ref<Self> {
                Ref::new(self.id.into())
            }

            fn next(ev: &event::TodoEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
                let event::TodoEvent::NewTodo(n) = ev else {
                    return Ok(vec![]);
                };
                let seen = view.get(Ref::<$other>::new(n.id.into()))?.is_some();
                Ok(vec![Write::Set($name {
                    id: n.id,
                    other_row_visible: seen,
                })])
            }
        }
    };
}

probe!(First, 1, Second);
probe!(Second, 2, First);

#[test]
fn no_projector_sees_another_projectors_writes_for_the_same_event() {
    let store = inmemory();
    let mut manager = ProjectorManager::<event::Event>::new(tenant());
    manager.register::<Second>().unwrap();
    manager.register::<First>().unwrap();

    run(&store, &manager, &[new_todo(1, 1, "t")]).unwrap();

    assert!(!row::<First>(&store, 1).unwrap().other_row_visible);
    assert!(!row::<Second>(&store, 1).unwrap().other_row_visible);

    // The next event reads both rows.
    run(&store, &manager, &[new_todo(1, 1, "u")]).unwrap();
    assert!(row::<First>(&store, 1).unwrap().other_row_visible);
    assert!(row::<Second>(&store, 1).unwrap().other_row_visible);
}

#[test]
fn a_next_call_reads_earlier_events_of_the_same_entry() {
    let store = inmemory();
    let manager = manager_of::<Pinned>();

    // The lookup for e2 must see e1's index entry, written in the same txn.
    run(&store, &manager, &[new_todo(1, 42, "a"), user_deleted(42)]).unwrap();

    assert!(row::<Pinned>(&store, 1).is_none());
}

/// Emits two writes to one row; the second must apply on top of the first,
/// index included.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct Twice {
    id: TodoId,
    user_id: UserId,
}

impl Twice {
    const BY_USER: Index<Twice, User> = Index::new(IID, Twice::user);

    fn user(&self) -> Ref<User> {
        Ref::new(self.user_id.into())
    }
}

impl Projection for Twice {
    type Event<'a> = event::TodoCreatedEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32(0x7771);
    const INDICES: Indices<Self> = &[&Twice::BY_USER];

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(ev: &event::TodoCreatedEvent, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        let event::TodoCreatedEvent::NewTodo(n) = ev;
        let at = |user: u128| Twice {
            id: n.id,
            user_id: UserId(user),
        };
        Ok(vec![Write::Set(at(1)), Write::Set(at(2))])
    }
}

#[test]
fn duplicate_ids_in_one_vec_apply_in_order() {
    let store = inmemory();
    let manager = manager_of::<Twice>();

    run(&store, &manager, &[new_todo(5, 0, "t")]).unwrap();

    assert_eq!(row::<Twice>(&store, 5).unwrap().user_id, UserId(2));
    assert!(indexed_under::<Twice>(&store, 1).is_empty());
    assert_eq!(
        indexed_under::<Twice>(&store, 2),
        vec![ProjectionId::from_u128(5)]
    );
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct Failing;

impl Projection for Failing {
    type Event<'a> = event::TodoCreatedEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32(0xFA11);

    fn id(&self) -> Ref<Self> {
        Ref::new(ProjectionId::from_u128(0))
    }

    fn next(_: &event::TodoCreatedEvent, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        Err(Error::Decode("store failed".to_string()))
    }
}

#[test]
fn next_error_surfaces_before_anything_of_the_event_is_written() {
    let store = inmemory();
    let mut manager = manager_of::<Pinned>();
    manager.register::<Failing>().unwrap();

    let mut txn = store.write().unwrap();
    let err = fold_event(
        &manager.projectors,
        tenant(),
        &new_todo(1, 1, "t"),
        &mut txn,
    )
    .expect_err("a next error must surface");
    assert!(matches!(err, Error::Decode(_)));

    // Writes are stored only once every `next` has returned, so the event
    // left nothing behind; discarding the entry is still the fold's job
    // (`dispatcher::tests::fold::next_error_stops_the_fold`).
    assert!(staged::<Pinned>(&txn, 1).is_none());
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct Note {
    id: TodoId,
    title: String,
}

impl Note {
    fn new(id: u128, title: &str) -> Self {
        Note {
            id: TodoId(id),
            title: title.to_string(),
        }
    }
}

impl Projection for Note {
    type Event<'a> = event::TodoCreatedEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32(0x2073);

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(ev: &event::TodoCreatedEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        let event::TodoCreatedEvent::NewTodo(n) = ev;
        Write::insert(view, Ref::new(n.id.into()), || Note {
            id: n.id,
            title: n.title.clone(),
        })
    }
}

fn note(id: u128) -> Ref<Note> {
    Ref::new(TodoId(id).into())
}

/// A store holding `Note` 1 ("old"), and nothing else.
fn with_note_1() -> Arc<dyn Store> {
    let store = inmemory();
    let mut txn = store.write().unwrap();
    let row = Note::new(1, "old");
    WriteTransactionExt::set(&mut txn, tenant_id(), row.id(), Some(&row)).unwrap();
    txn.commit(crate::Watermark::default()).unwrap();
    store
}

#[test]
fn insert_writes_only_for_an_absent_row() {
    let store = with_note_1();
    let txn = store.read().unwrap();
    let view = View::new(tenant_id(), &*txn);

    assert_eq!(
        Write::insert(&view, note(1), || Note::new(1, "new")).unwrap(),
        vec![]
    );
    assert_eq!(
        Write::insert(&view, note(2), || Note::new(2, "new")).unwrap(),
        vec![Write::Set(Note::new(2, "new"))]
    );
}

#[test]
fn modify_writes_only_for_a_present_row() {
    let store = with_note_1();
    let txn = store.read().unwrap();
    let view = View::new(tenant_id(), &*txn);
    let retitle = |n: Note| Note {
        title: format!("{}!", n.title),
        ..n
    };

    assert_eq!(
        Write::modify(&view, note(1), retitle).unwrap(),
        vec![Write::Set(Note::new(1, "old!"))]
    );
    assert_eq!(Write::modify(&view, note(2), retitle).unwrap(), vec![]);
}

#[test]
fn update_deletes_on_none_and_skips_absent_to_absent() {
    let store = with_note_1();
    let txn = store.read().unwrap();
    let view = View::new(tenant_id(), &*txn);

    assert_eq!(
        Write::update(&view, note(1), |_| None).unwrap(),
        vec![Write::Delete(note(1))]
    );
    assert_eq!(
        Write::<Note>::update(&view, note(2), |_| None).unwrap(),
        vec![]
    );
    assert_eq!(
        Write::update(&view, note(2), |cur| {
            assert!(cur.is_none());
            Some(Note::new(2, "made"))
        })
        .unwrap(),
        vec![Write::Set(Note::new(2, "made"))]
    );
}

#[test]
fn helpers_in_one_call_read_the_state_before_it() {
    let store = with_note_1();
    let txn = store.read().unwrap();
    let view = View::new(tenant_id(), &*txn);
    let append = |s: &'static str| move |n: Note| Note::new(1, &format!("{}{s}", n.title));

    let mut writes = Write::modify(&view, note(1), append("+a")).unwrap();
    writes.extend(Write::modify(&view, note(1), append("+b")).unwrap());

    // Both read "old"; applied in order, the second write wins.
    assert_eq!(
        writes,
        vec![
            Write::Set(Note::new(1, "old+a")),
            Write::Set(Note::new(1, "old+b"))
        ]
    );
}

#[test]
fn a_helper_rejects_a_row_whose_id_differs_from_its_key() {
    let store = with_note_1();
    let txn = store.read().unwrap();
    let view = View::new(tenant_id(), &*txn);

    for result in [
        Write::insert(&view, note(2), || Note::new(3, "x")),
        Write::modify(&view, note(1), |_| Note::new(3, "x")),
        Write::update(&view, note(2), |_| Some(Note::new(3, "x"))),
    ] {
        match result {
            Err(Error::IncorrectProjectionID {
                type_name, actual, ..
            }) => {
                assert!(type_name.contains("Note"), "got {type_name}");
                assert_eq!(actual, TodoId(3).into());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

#[test]
fn a_mismatched_id_stops_the_projector_and_stages_nothing() {
    let store = inmemory();
    let manager = manager_of::<Misfiled>();

    let mut txn = store.write().unwrap();
    let err = fold_event(
        &manager.projectors,
        tenant(),
        &new_todo(1, 1, "t"),
        &mut txn,
    )
    .expect_err("must be rejected");
    assert!(
        matches!(err, Error::IncorrectProjectionID { .. }),
        "{err:?}"
    );
    for id in [1, 2] {
        assert!(staged::<Misfiled>(&txn, id).is_none(), "id {id} staged");
    }
}

/// Keys row 1, builds row 2.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct Misfiled {
    id: u128,
}

impl Projection for Misfiled {
    type Event<'a> = event::TodoCreatedEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32(0xBAD);

    fn id(&self) -> Ref<Self> {
        Ref::new(ProjectionId::from_u128(self.id))
    }

    fn next(_: &event::TodoCreatedEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        Write::insert(view, Ref::new(ProjectionId::from_u128(1)), || Misfiled {
            id: 2,
        })
    }
}

#[test]
fn an_insert_projection_keeps_the_first_row_across_entries() {
    let store = inmemory();
    let manager = manager_of::<Note>();

    run(&store, &manager, &[new_todo(1, 1, "first")]).unwrap();
    run(
        &store,
        &manager,
        &[new_todo(1, 1, "second"), new_todo(2, 1, "other")],
    )
    .unwrap();

    assert_eq!(row::<Note>(&store, 1).unwrap().title, "first");
    assert_eq!(row::<Note>(&store, 2).unwrap().title, "other");
}
