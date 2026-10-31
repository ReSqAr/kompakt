//! nvzn's own fixtures: one event set with a version-upgrade chain, the two
//! projections that consume it, and the store wrappers a test reaches for when
//! it needs to observe or break one.
//!
//! The rig itself — [`TestLog`] and the store constructors — lives in
//! [`crate::testing`], which ships to consumers. What is here is test data, and
//! stays behind `cfg(test)`.
#![allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
#![allow(
    unreachable_pub,
    reason = "crate-internal fixtures; `pub` items are consumed within the crate"
)]

use crate::{
    Error, Index, IndexId, Indices, Projection, ProjectionId, ProjectionTypeId, Ref, Store, Tenant,
    TenantId, View, WithWireFormat, Write,
};
use bytes::Bytes;
use crossbeam_combinators::Channels;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use zslg::{Data, Hlc, LogIndex, StableLogEvent};

pub use crate::testing::{FLUSH_INTERVAL, TestLog, data_id, inmemory, rocksdb, rocksdb_store};
pub fn tenant() -> Tenant {
    Tenant::new("test", 0)
}

/// The id half, for the many store-level helpers that key by it.
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

crate::chained_from!(from: NewUserV1, to: NewUserV3, via: NewUserV2);

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

crate::define_events! {
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
    crate::WriteTransactionExt::set(&mut txn, tenant_id, r, Some(p)).unwrap();
    txn.commit(crate::Watermark::default()).unwrap();
}

pub fn payload(tenant_id: TenantId, events: &[event::Event]) -> Bytes {
    let mut buf = bytes::BytesMut::new();
    buf.extend_from_slice(&tenant_id.as_slice());
    let buf = event::Event::encode(events, buf).expect("encode events");
    buf.freeze()
}

pub type Calls = Arc<Mutex<Vec<&'static str>>>;

#[must_use]
pub fn calls() -> Calls {
    Arc::new(Mutex::new(Vec::new()))
}

#[must_use]
pub fn recorded(calls: &Calls) -> Vec<&'static str> {
    calls.lock().expect("test mutex").clone()
}

fn record(calls: &Calls, what: &'static str) {
    calls.lock().expect("test mutex").push(what);
}

pub struct ObservingStore {
    inner: Arc<dyn Store>,
    calls: Calls,
}

impl ObservingStore {
    #[must_use]
    pub fn new(inner: Arc<dyn Store>, calls: Calls) -> Self {
        Self { inner, calls }
    }
}

impl Store for ObservingStore {
    fn config_hash(&self) -> Result<Option<crate::ConfigHash>, crate::Error> {
        self.inner.config_hash()
    }
    fn set_config_hash(&self, hash: &crate::ConfigHash) -> Result<(), crate::Error> {
        self.inner.set_config_hash(hash)
    }
    fn read(&self) -> Result<crate::ArcReadTransaction<'_>, crate::Error> {
        record(&self.calls, "read");
        self.inner.read()
    }
    fn write(&self) -> Result<crate::WriteTransactionImpl<'_>, crate::Error> {
        record(&self.calls, "write");
        Ok(crate::WriteTransactionImpl::new(self.inner.read()?, self))
    }
    fn commit_batch(
        &self,
        batch: crate::CommitBatch,
        watermark: crate::Watermark,
    ) -> Result<(), crate::Error> {
        record(&self.calls, "commit");
        self.inner.commit_batch(batch, watermark)
    }
    fn flush(&self) -> Result<(), crate::Error> {
        self.inner.flush()
    }
    fn reset(&self) -> Result<(), crate::Error> {
        self.inner.reset()
    }
}

/// A zslg reader whose first `failures` syncs fail (a quorum read timing
/// out at startup); everything else is `inner`'s.
pub struct FailingSyncReader {
    inner: Arc<dyn zslg::Reader>,
    failures: Mutex<usize>,
}

impl FailingSyncReader {
    #[must_use]
    pub fn new(inner: Arc<dyn zslg::Reader>, failures: usize) -> Self {
        Self {
            inner,
            failures: Mutex::new(failures),
        }
    }
}

#[async_trait::async_trait]
impl zslg::Reader for FailingSyncReader {
    async fn sync(&self) -> Result<(), zslg::Error> {
        let failing = {
            let mut left = self.failures.lock().expect("test mutex");
            let failing = *left > 0;
            *left = left.saturating_sub(1);
            failing
        };
        if failing {
            return Err(zslg::Error::QuorumReadTimeout {
                timeout: std::time::Duration::from_millis(1),
            });
        }
        self.inner.sync().await
    }

    fn stable_watermark(&self) -> Option<Hlc> {
        self.inner.stable_watermark()
    }

    fn hlc_now(&self) -> Hlc {
        self.inner.hlc_now()
    }

    fn subscribe_stable_log(&self, from: LogIndex) -> zslg::StableStream {
        self.inner.subscribe_stable_log(from)
    }

    fn unstable_tail(&self) -> Channels<Result<Data, zslg::Error>> {
        self.inner.unstable_tail()
    }

    fn subscribe_unstable_tail(&self) -> Channels<Result<zslg::UnstableEvent, zslg::Error>> {
        self.inner.subscribe_unstable_tail()
    }
}

pub struct UnfenceableStore {
    inner: Arc<dyn Store>,
}

impl UnfenceableStore {
    #[must_use]
    pub fn new(inner: Arc<dyn Store>) -> Self {
        Self { inner }
    }
}

impl Store for UnfenceableStore {
    fn config_hash(&self) -> Result<Option<crate::ConfigHash>, crate::Error> {
        self.inner.config_hash()
    }
    fn set_config_hash(&self, hash: &crate::ConfigHash) -> Result<(), crate::Error> {
        self.inner.set_config_hash(hash)
    }
    fn read(&self) -> Result<crate::ArcReadTransaction<'_>, crate::Error> {
        self.inner.read()
    }
    fn write(&self) -> Result<crate::WriteTransactionImpl<'_>, crate::Error> {
        Ok(crate::WriteTransactionImpl::new(self.inner.read()?, self))
    }
    fn commit_batch(
        &self,
        batch: crate::CommitBatch,
        watermark: crate::Watermark,
    ) -> Result<(), crate::Error> {
        self.inner.commit_batch(batch, watermark)
    }
    fn flush(&self) -> Result<(), crate::Error> {
        Err(crate::Error::Io("fsync refused".to_string()))
    }
    fn reset(&self) -> Result<(), crate::Error> {
        self.inner.reset()
    }
}

pub type StableFeed = crossbeam_channel::Sender<Result<StableLogEvent, zslg::Error>>;

#[must_use]
pub fn hlc(ms: u64) -> Hlc {
    Hlc::from_bytes(ms.to_be_bytes())
}

/// A [`zslg::Reader`] whose halves the test drives separately, so the stable
/// watermark can run ahead of what the fold has been fed — the window between
/// zslg loading a snapshot and the fold catching up, which [`TestLog`] never
/// opens because it moves both together.
pub struct ScriptedReader {
    watermark: Mutex<Option<Hlc>>,
    unstable: Mutex<Vec<Data>>,
    stable: Mutex<Option<crossbeam_channel::Receiver<Result<StableLogEvent, zslg::Error>>>>,
}

impl ScriptedReader {
    /// The reader, and the sender that feeds its one stable-log subscription.
    /// Dropping the sender ends the subscription as `Aborted`.
    #[must_use]
    pub fn new() -> (Arc<Self>, StableFeed) {
        let (tx, rx) = crossbeam_channel::unbounded();
        let reader = Self {
            watermark: Mutex::new(None),
            unstable: Mutex::new(Vec::new()),
            stable: Mutex::new(Some(rx)),
        };
        (Arc::new(reader), tx)
    }

    pub fn set_stable_watermark(&self, hlc_ms: u64) {
        *self.watermark.lock().expect("test mutex") = Some(hlc(hlc_ms));
    }
}

#[async_trait::async_trait]
impl zslg::Reader for ScriptedReader {
    async fn sync(&self) -> Result<(), zslg::Error> {
        Ok(())
    }

    fn stable_watermark(&self) -> Option<Hlc> {
        *self.watermark.lock().expect("test mutex")
    }

    /// No writes go through a scripted reader: its "now" is its watermark.
    fn hlc_now(&self) -> Hlc {
        self.stable_watermark().unwrap_or_else(|| hlc(0))
    }

    fn subscribe_stable_log(&self, from: LogIndex) -> zslg::StableStream {
        let rx = self
            .stable
            .lock()
            .expect("test mutex")
            .take()
            .expect("a scripted reader serves one subscription");
        zslg::StableStream::new(Channels::from_infinite(rx, "scripted"), Some(from))
    }

    fn unstable_tail(&self) -> Channels<Result<Data, zslg::Error>> {
        let items = self
            .unstable
            .lock()
            .expect("test mutex")
            .iter()
            .cloned()
            .map(Ok)
            .collect();
        Channels::from_vec(items)
    }

    /// The staged unstable entries above the watermark, once: a scripted
    /// reader has no live leg, so the stream then ends `Aborted`.
    fn subscribe_unstable_tail(&self) -> Channels<Result<zslg::UnstableEvent, zslg::Error>> {
        let watermark = self.stable_watermark();
        let entries = self.unstable.lock().expect("test mutex").clone();
        let (tx, rx) = crossbeam_channel::unbounded();
        if let Some(w) = watermark {
            let _ = tx.send(Ok(zslg::UnstableEvent::Watermark(w)));
        }
        for data in entries {
            if watermark.is_none_or(|w| data.data_id.hlc() > w) {
                let _ = tx.send(Ok(zslg::UnstableEvent::Appended(data)));
            }
        }
        Channels::from_infinite(rx, "scripted unstable")
    }
}
