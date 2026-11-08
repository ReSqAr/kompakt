//! Test data for the nvzn crates: one event set with a version-upgrade chain
//! and the two projections that consume it.
#![allow(clippy::unwrap_used, clippy::expect_used, reason = "test fixtures")]

use bytes::Bytes;
use nvzn_store::{
    Index, Indices, Projection, Ref, Store, View, Watermark, Write, WriteTransactionExt,
};
use nvzn_types::{
    Error, IndexId, ProjectionId, ProjectionTypeId, Tenant, TenantId, WithWireFormat,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[must_use]
pub fn tenant() -> Tenant {
    Tenant::new("test", 0)
}

/// The id half, for the many store-level helpers that key by it.
#[must_use]
pub fn tenant_id() -> TenantId {
    tenant().id()
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct UserId(pub u128);

impl From<UserId> for ProjectionId {
    fn from(val: UserId) -> Self {
        ProjectionId::from_u128(val.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct TodoId(pub u128);

impl From<TodoId> for ProjectionId {
    fn from(val: TodoId) -> Self {
        ProjectionId::from_u128(val.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NewUserV1 {
    pub id: UserId,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NewUserV2 {
    pub id: UserId,
    pub name: String,
    pub address: Option<String>,
}

impl From<NewUserV1> for NewUserV2 {
    fn from(NewUserV1 { id, name }: NewUserV1) -> Self {
        Self {
            id,
            name,
            address: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NewUserV3 {
    pub id: UserId,
    pub name: String,
    pub address: Option<String>,
    pub email: Option<String>,
}

impl From<NewUserV2> for NewUserV3 {
    fn from(NewUserV2 { id, name, address }: NewUserV2) -> Self {
        Self {
            id,
            name,
            address,
            email: None,
        }
    }
}

nvzn_types::chained_from!(from: NewUserV1, to: NewUserV3, via: NewUserV2);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PingedV1 {
    pub id: UserId,
    pub ts: u128,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NewTodoV1 {
    pub id: TodoId,
    pub user_id: UserId,
    pub title: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TodoDeletedV1 {
    pub id: TodoId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserDeletedV1 {
    pub id: UserId,
}

nvzn_types::define_events! {
    event::Event = {
        event 1 := NewUser {
            version 3 => NewUserV3
        } upgrades from {
            version 1 => NewUserV1,
            version 2 => NewUserV2,
        }

        event 2 := Pinged {
            version 1 => PingedV1
        }

        event 3 := NewTodo {
            version 1 => NewTodoV1
        }

        event 4 := TodoDeleted {
            version 1 => TodoDeletedV1
        }

        event 5 := UserDeleted {
            version 1 => UserDeletedV1
        }
    } subsets {
        UserEvent := NewUser | Pinged,
        TodoEvent := NewTodo | TodoDeleted,
        TodoCreatedEvent := NewTodo,
        CascadeEvent := NewTodo | UserDeleted,
    } owned {
        UserWrite := NewUser | Pinged,
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct User {
    pub id: UserId,
    pub name: String,
    pub address: Option<String>,
    pub email: Option<String>,
    pub last_ping: Option<u128>,
}

impl User {
    #[must_use]
    pub fn new(id: u128, name: &str) -> Self {
        Self {
            id: UserId(id),
            name: name.to_string(),
            address: None,
            email: None,
            last_ping: None,
        }
    }
}

impl Projection for User {
    type Event<'a> = event::UserEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash("User", 0);

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(event: &event::UserEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        match event {
            event::UserEvent::NewUser(n) => Write::set(Self {
                id: n.id,
                name: n.name.clone(),
                address: n.address.clone(),
                email: n.email.clone(),
                last_ping: None,
            }),
            event::UserEvent::Pinged(p) => Write::modify(view, Ref::new(p.id.into()), |u| Self {
                last_ping: Some(p.ts),
                ..u
            }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Todo {
    pub id: TodoId,
    pub user_id: UserId,
    pub title: String,
}

impl Todo {
    /// Index id 1, which the projection tests scan directly.
    pub const BY_USER: Index<Todo, User> = Index::new(IndexId::from_u32(1), Todo::user);

    #[must_use]
    pub fn user(&self) -> Ref<User> {
        Ref::new(self.user_id.into())
    }
}

impl Projection for Todo {
    type Event<'a> = event::TodoEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash("Todo", 0);
    const INDICES: Indices<Self> = &[&Todo::BY_USER];

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(event: &event::TodoEvent, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        match event {
            event::TodoEvent::NewTodo(n) => Write::set(Self {
                id: n.id,
                user_id: n.user_id,
                title: n.title.clone(),
            }),
            event::TodoEvent::TodoDeleted(d) => Write::delete(Ref::new(d.id.into())),
        }
    }
}

pub fn commit_row<P: Projection>(store: &Arc<dyn Store>, tenant_id: TenantId, r: Ref<P>, p: &P) {
    let mut txn = store.write().unwrap();
    WriteTransactionExt::set(&mut txn, tenant_id, r, Some(p)).unwrap();
    txn.commit(Watermark::default()).unwrap();
}

#[must_use]
pub fn payload(tenant_id: TenantId, events: &[event::Event]) -> Bytes {
    let mut buf = bytes::BytesMut::new();
    buf.extend_from_slice(&tenant_id.as_slice());
    let buf = event::Event::encode(events, buf).expect("encode events");
    buf.freeze()
}
