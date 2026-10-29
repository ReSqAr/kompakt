//! The dispatcher: one stable-log fold per node, shared by every tenant
//! registered on it.
//!
//! ```text
//!   Dispatcher::new / in_memory / from_db
//!     └─ new_handler(tenant).register::<P>()... .finalise() ─> (EventWriter, Reader)
//!   Dispatcher::finalise ─> fold task + fence task ─> JoinHandle
//! ```
//!
//! Registration only builds state; nothing folds until `finalise` consumes the
//! dispatcher. Every handle shares the one store, the fold's progress and its
//! stop reason, so a reader taken before `finalise` sees the same fold as one
//! taken after.

mod fold;
mod fold_state;
mod handles;
mod registration;
mod subscribers;
mod supervisor;
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests;

pub(crate) use fold_state::FoldState;
pub use handles::{Encoder, EventWriter, Syncer};
pub use registration::AssociatedProjectorManager;
use registration::compute_config_hash;
pub(crate) use subscribers::{PENDING_SUBSCRIBERS, Subscribers, Subscriptions};

use crate::{
    ArcStore, Error, IndexId, ProjectionTypeId, ProjectorManager, RocksDBStore, StoreConfig,
    Tenant, TenantId, WriteTransactionImpl,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zslg::{Hlc, LogIndex};

pub(crate) type InnerHandler = Arc<
    dyn for<'a> Fn(TenantId, &mut WriteTransactionImpl<'a>, &[u8]) -> Result<(), Error>
        + Send
        + Sync,
>;

pub struct Dispatcher {
    store: ArcStore,
    handlers: BTreeMap<TenantId, InnerHandler>,
    rx: watch::Receiver<Option<Hlc>>,
    tx: watch::Sender<Option<Hlc>>,
    writer: Arc<dyn zslg::Writer>,
    reader: Arc<dyn zslg::Reader>,
    registered_projection_types: BTreeSet<(TenantId, ProjectionTypeId)>,
    registered_indices: BTreeSet<(TenantId, ProjectionTypeId, IndexId)>,
    flush_interval: Duration,
    /// [`StoreConfig::fold_commit`], in bytes.
    fold_commit: usize,
    fold_state: FoldState,
    /// Cloned into every reader; dropped when the fold starts.
    subscriptions: Subscriptions,
    subscribers: Subscribers,
}

impl Dispatcher {
    /// A dispatcher over `writer` and `reader`, stopped by `shutdown`: the
    /// token that stops the node they come from (`ReplicaNode::shutdown_token`).
    /// The dispatcher has no cancellation of its own; it stops the fold
    /// through a child of this one, so ending the fold leaves the node up.
    pub async fn new(
        config: &StoreConfig,
        writer: Arc<dyn zslg::Writer>,
        reader: Arc<dyn zslg::Reader>,
        shutdown: &CancellationToken,
    ) -> Result<Self, Error> {
        let store = Arc::new(RocksDBStore::new(config)?);
        let mut dispatcher =
            Self::from_db(store, writer, reader, config.flush_interval, shutdown).await?;
        dispatcher.fold_commit = config.fold_commit_bytes();
        Ok(dispatcher)
    }

    /// A dispatcher over [`InMemoryStore`](crate::InMemoryStore), for tests.
    ///
    /// The projection side only: `writer` and `reader` are still whatever the
    /// caller supplies, so pair it with
    /// [`testing::TestLog`](crate::testing::TestLog) for a rig with no zslg in
    /// it at all.
    ///
    /// ```no_run
    /// # async fn f() -> Result<(), nvzn::Error> {
    /// let log = nvzn::testing::TestLog::new();
    /// let mut dispatcher = nvzn::Dispatcher::in_memory(log.writer(), log.reader()).await?;
    /// // ... register projections on `dispatcher`, then `finalise`.
    /// # Ok(()) }
    /// ```
    #[cfg(any(test, feature = "testing"))]
    pub async fn in_memory(
        writer: Arc<dyn zslg::Writer>,
        reader: Arc<dyn zslg::Reader>,
    ) -> Result<Self, Error> {
        Self::from_db(
            Arc::new(crate::InMemoryStore::default()),
            writer,
            reader,
            crate::testing::FLUSH_INTERVAL,
            // A test rig has no node to stop: its shutdown is the root.
            &CancellationToken::new(),
        )
        .await
    }

    pub(crate) async fn from_db(
        store: ArcStore,
        writer: Arc<dyn zslg::Writer>,
        reader: Arc<dyn zslg::Reader>,
        flush_interval: Duration,
        shutdown: &CancellationToken,
    ) -> Result<Self, Error> {
        let max_hlc = store.read()?.watermark()?.hlc;
        let (tx, rx) = watch::channel(max_hlc);
        let (subscriptions, subscribers) = Subscriptions::new();
        Ok(Self {
            store,
            handlers: BTreeMap::new(),
            rx,
            tx,
            writer,
            reader,
            registered_projection_types: BTreeSet::new(),
            registered_indices: BTreeSet::new(),
            flush_interval,
            fold_commit: crate::store::DEFAULT_FOLD_COMMIT
                .as_u64()
                .try_into()
                .unwrap_or(usize::MAX),
            fold_state: FoldState::new(shutdown.child_token()),
            subscriptions,
            subscribers,
        })
    }

    #[must_use]
    pub fn watermark(&self) -> watch::Receiver<Option<Hlc>> {
        self.rx.clone()
    }

    pub fn log_index_watermark(&self) -> Result<Option<LogIndex>, Error> {
        Ok(self.store.read()?.watermark()?.index)
    }

    #[must_use]
    pub fn syncer(&self) -> Syncer {
        Syncer::new(
            self.reader.clone(),
            self.watermark(),
            self.fold_state.clone(),
        )
    }

    pub fn new_handler<Ev: Send + Sync + 'static>(
        &'_ mut self,
        tenant: Tenant,
    ) -> AssociatedProjectorManager<'_, Ev> {
        AssociatedProjectorManager {
            dispatcher: self,
            manager: ProjectorManager::new(tenant),
        }
    }
}
