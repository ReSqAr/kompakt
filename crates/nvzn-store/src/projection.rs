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

use crate::View;
use nvzn_types::{Error, EventSet, IndexId, ProjectionId, ProjectionTypeId};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

pub trait Projection:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    /// The events this projection folds: a subset enum borrowed from the
    /// event (`event::TodoEvent<'a>`), so folding copies nothing.
    type Event<'a>: EventSet;
    /// The numbers of the events it folds; what decides whether it is live
    /// under a live view's sub-event set ([`is_live`](crate::is_live)).
    const EVENTS: &'static [u32] = <Self::Event<'static> as EventSet>::EVENTS;
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

    #[must_use]
    pub fn id(&self) -> IndexId {
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
