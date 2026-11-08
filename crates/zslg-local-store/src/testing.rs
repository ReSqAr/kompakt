//! Test wiring for the crate's own tests.

use std::path::Path;
use std::sync::Arc;

use bytesize::ByteSize;
use rocksdb_kit::{CfSpec, Storage, StorageConfig};
use tokio_util::sync::CancellationToken;
use zslg_log::StableLogSnapshot;
use zslg_log_fake::FakeStableLog;

use crate::store::{
    LOCAL_STORE_CURSORS_CF, LOCAL_STORE_DATA_CF, create_local_store, run_integrate_worker,
};
use crate::{Error, LocalStore, LocalStoreConfig};

/// A storage root carrying this store's two column families.
pub(crate) fn open_storage(root: &Path) -> Result<Arc<Storage>, Error> {
    let cfs: Vec<CfSpec> = [LOCAL_STORE_DATA_CF, LOCAL_STORE_CURSORS_CF]
        .into_iter()
        .map(|cf| CfSpec {
            cf,
            compress: false,
        })
        .collect();
    Ok(Storage::open(
        root,
        StorageConfig::new(ByteSize::from_mib(64)),
        &cfs,
    )?)
}

/// A store with a detached integration worker, for tests that use a fresh root
/// and never reopen it.
pub(crate) async fn simple_at(root: std::path::PathBuf) -> Result<Arc<LocalStore>, Error> {
    let storage = open_storage(&root)?;
    let stable_log: Arc<dyn StableLogSnapshot> = Arc::new(FakeStableLog::new());
    let (store, rx) =
        create_local_store(&LocalStoreConfig::for_test(), storage, stable_log).await?;
    tokio::spawn(run_integrate_worker(
        Arc::downgrade(&store),
        rx,
        CancellationToken::new(),
    ));
    Ok(store)
}

/// The integration worker of an [`open_test_local_store`] store.
pub(crate) struct TestWorker {
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), Error>>,
}

impl TestWorker {
    /// Cancel and drain — what `Shutdown` does for the production worker. Call
    /// it after dropping the store, before reopening its root.
    pub(crate) async fn stop(self) {
        self.cancel.cancel();
        let _ = self.handle.await;
    }
}

/// A store whose integration worker can be drained, for tests that reopen a root.
pub(crate) async fn open_test_local_store(
    root: &Path,
) -> Result<(Arc<LocalStore>, TestWorker), Error> {
    let storage = open_storage(root)?;
    let stable_log: Arc<dyn StableLogSnapshot> = Arc::new(FakeStableLog::new());
    let cancel = CancellationToken::new();
    let (store, rx) =
        create_local_store(&LocalStoreConfig::for_test(), storage, stable_log).await?;
    let handle = tokio::spawn(run_integrate_worker(
        Arc::downgrade(&store),
        rx,
        cancel.clone(),
    ));
    Ok((store, TestWorker { cancel, handle }))
}
