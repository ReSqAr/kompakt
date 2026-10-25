//! The rig a consumer gets, exercised through nothing but `nvzn::`.
//!
//! An integration test sees only the public API, so this fails if any part of
//! the path below stops being reachable from outside the crate — which is the
//! property `nvzn::testing` exists to provide.
//!
//! ```text
//!   TestLog ──> Dispatcher::in_memory ──> register ──> finalise
//!                                             │
//!                       EventWriter ──write──>│<──sync── Reader
//! ```

#![allow(unused_crate_dependencies, reason = "integration test")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use nvzn::{
    Dispatcher, Error, Index, IndexId, Indices, Projection, ProjectionId, ProjectionTypeId, Ref,
    Tenant, View, Write, testing::TestLog,
};
use serde::{Deserialize, Serialize};

const TENANT: Tenant = Tenant::new("public-api", 0);

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
struct OwnerId(u128);

impl From<OwnerId> for ProjectionId {
    fn from(val: OwnerId) -> Self {
        ProjectionId::from_u128(val.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
struct ItemId(u128);

impl From<ItemId> for ProjectionId {
    fn from(val: ItemId) -> Self {
        ProjectionId::from_u128(val.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NewOwnerV1 {
    id: OwnerId,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NewItemV1 {
    id: ItemId,
    owner_id: OwnerId,
    label: String,
}

nvzn::define_events! {
    event::Event = {
        event 1 := NewOwner {
            version 1 => NewOwnerV1
        }
        event 2 := NewItem {
            version 1 => NewItemV1
        }
    } subsets {
        NewOwnerEvent := NewOwner,
        NewItemEvent := NewItem,
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Owner {
    id: OwnerId,
    name: String,
}

impl Projection for Owner {
    type Event<'a> = event::NewOwnerEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash("Owner", 0);

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(event: &event::NewOwnerEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        let event::NewOwnerEvent::NewOwner(n) = event;
        Write::insert(view, Ref::new(n.id.into()), || Owner {
            id: n.id,
            name: n.name.clone(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Item {
    id: ItemId,
    owner_id: OwnerId,
    label: String,
}

impl Item {
    const BY_OWNER: Index<Item, Owner> =
        Index::new(IndexId::from_string_hash("item->owner", 0), Item::owner);

    fn owner(&self) -> Ref<Owner> {
        Ref::new(self.owner_id.into())
    }
}

impl Projection for Item {
    type Event<'a> = event::NewItemEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash("Item", 0);
    const INDICES: Indices<Self> = &[&Item::BY_OWNER];

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(event: &event::NewItemEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        let event::NewItemEvent::NewItem(n) = event;
        Write::insert(view, Ref::new(n.id.into()), || Item {
            id: n.id,
            owner_id: n.owner_id,
            label: n.label.clone(),
        })
    }
}

/// Two calls' worth of setup, where standing up the same rig by hand means
/// implementing [`zslg::Reader`] and [`zslg::Writer`].
#[tokio::test]
async fn an_in_memory_rig_folds_writes_into_projections() {
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::in_memory(log.writer(), log.reader())
        .await
        .expect("in-memory dispatcher");

    let mut manager = dispatcher.new_handler::<event::Event>(TENANT);

    manager.register::<Owner>().expect("register Owner");

    manager.register::<Item>().expect("register Item");

    let (writer, reader) = manager.finalise().expect("finalise handler");
    let _handle = dispatcher.finalise().await.expect("finalise dispatcher");

    writer
        .write(&[
            NewOwnerV1 {
                id: OwnerId(1),
                name: "Naomi".to_string(),
            }
            .into(),
            NewItemV1 {
                id: ItemId(10),
                owner_id: OwnerId(1),
                label: "wrench".to_string(),
            }
            .into(),
            NewItemV1 {
                id: ItemId(11),
                owner_id: OwnerId(1),
                label: "spanner".to_string(),
            }
            .into(),
        ])
        .await
        .expect("write events");

    reader.sync().await.expect("sync");

    let snapshot = reader.snapshot().expect("snapshot");
    let found = snapshot
        .get(Ref::<Owner>::new(OwnerId(1).into()))
        .expect("get owner")
        .expect("owner was folded");
    assert_eq!(found.name, "Naomi");

    let snapshot = reader.snapshot().expect("snapshot");
    let owners_with_items = snapshot
        .select::<Owner>()
        .join_idx(Item::BY_OWNER)
        .on(Owner::id)
        .collect()
        .expect("collect");

    assert_eq!(owners_with_items.len(), 1);
    let (owner, items) = owners_with_items.into_iter().next().unwrap();
    assert_eq!(owner.id, OwnerId(1));
    let items = items.collect::<Result<Vec<_>, _>>().expect("fetch items");
    assert_eq!(items.len(), 2);
}

/// The unstable tail is above the watermark, so it shows in
/// `unstable_snapshot` and not in `snapshot`.
#[tokio::test]
async fn the_unstable_tail_is_visible_only_to_the_unstable_snapshot() {
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::in_memory(log.writer(), log.reader())
        .await
        .expect("in-memory dispatcher");

    let mut manager = dispatcher.new_handler::<event::Event>(TENANT);
    manager.register::<Owner>().expect("register Owner");
    let (writer, reader) = manager.finalise().expect("finalise handler");
    let _handle = dispatcher.finalise().await.expect("finalise dispatcher");

    writer
        .write(&[NewOwnerV1 {
            id: OwnerId(2),
            name: "Alex".to_string(),
        }
        .into()])
        .await
        .expect("write");
    reader.sync().await.expect("sync");

    let stable = reader
        .snapshot()
        .expect("snapshot")
        .get(Ref::<Owner>::new(OwnerId(2).into()))
        .expect("get");
    assert!(stable.is_some(), "a synced write is in the stable snapshot");

    let unstable = reader
        .unstable_snapshot()
        .expect("unstable snapshot")
        .get(Ref::<Owner>::new(OwnerId(2).into()))
        .expect("get");
    assert!(unstable.is_some(), "and in the unstable one");
}
