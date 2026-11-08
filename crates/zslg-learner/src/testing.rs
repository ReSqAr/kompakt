//! Test support for code that wires a real learner: engines opened on a root
//! carrying every learner column family.

use std::sync::Arc;
use zslg_consensus::{RaftManifestState, RaftStateView};
use zslg_log::StableLogSnapshot;
use zslg_stable_log::integrate_manifest;

use crate::Error;

/// Test-only ergonomic wrapper: build an `ObjectStoreArchiveReader` over a
/// handle seeded with `manifest_state` (the archive reader selects snapshots
/// from its raft manifest) and delegate. Lets leader/learner tests drive a load
/// from a bare snapshot store without constructing the reader themselves.
pub async fn integrate_manifest_for_test(
    snapshot_store: Arc<dyn zslg_snapshot::SnapshotStore>,
    stable_log: &dyn StableLogSnapshot,
    manifest_state: &RaftManifestState,
) -> Result<(), Error> {
    let archive = zslg_stable_archive::ObjectStoreArchiveReader::new(
        snapshot_store,
        seed_raft_state(manifest_state),
    );
    integrate_manifest(&archive, stable_log, manifest_state)
        .await
        .map_err(Error::from)
}

/// A `RaftStateView` handle whose manifest is `manifest_state` — the archive
/// reader reads its covering snapshots from here.
#[must_use]
pub fn seed_raft_state(manifest_state: &RaftManifestState) -> Arc<dyn RaftStateView> {
    let handle = zslg_consensus::RaftStateHandle::default();
    let mut rs = zslg_consensus::RaftState::empty();
    rs.manifest_state = manifest_state.clone();
    handle.replace(rs);
    Arc::new(handle)
}

/// Open a test stable log beside a store path (mirrors
/// `StorageConfig::stable_log_dir`'s sibling convention) with default sizing.
pub async fn stable_log_at(store_path: &std::path::Path) -> Arc<crate::StableLog> {
    Arc::new(
        stable_log_engine(store_path.with_extension("stable"))
            .await
            .expect("open stable log"),
    )
}

/// A storage root carrying every learner column family, so any learner test
/// store can share a root with any other.
pub fn open_for_test(root: &std::path::Path) -> Result<Arc<rocksdb_kit::Storage>, Error> {
    let cfs: Vec<rocksdb_kit::CfSpec> = [
        crate::STABLE_LOG_CF,
        crate::LOCAL_STORE_DATA_CF,
        crate::LOCAL_STORE_CURSORS_CF,
    ]
    .into_iter()
    .map(|cf| rocksdb_kit::CfSpec {
        cf,
        compress: false,
    })
    .collect();
    Ok(rocksdb_kit::Storage::open(
        root,
        rocksdb_kit::StorageConfig::new(bytesize::ByteSize::from_mib(64)),
        &cfs,
    )?)
}

/// A local store with a detached integration worker over a storage root
/// carrying every learner column family, for tests that use a fresh root.
pub async fn local_store_at(root: std::path::PathBuf) -> Result<Arc<crate::LocalStore>, Error> {
    let (store, _) =
        local_store_with(root, &zslg_local_store::LocalStoreConfig::for_test()).await?;
    Ok(store)
}

/// [`local_store_at`] with a config of the test's choosing, handing back the
/// stable log (a fake) the store reads its watermark from.
pub async fn local_store_with(
    root: std::path::PathBuf,
    config: &zslg_local_store::LocalStoreConfig,
) -> Result<(Arc<crate::LocalStore>, Arc<zslg_log_fake::FakeStableLog>), Error> {
    let storage = open_for_test(&root)?;
    let stable_log = Arc::new(zslg_log_fake::FakeStableLog::new());
    let watermark_source: Arc<dyn zslg_log::StableLogSnapshot> = stable_log.clone();
    let (store, rx) =
        zslg_local_store::create_local_store(config, storage, watermark_source).await?;
    tokio::spawn(zslg_local_store::run_integrate_worker(
        Arc::downgrade(&store),
        rx,
        tokio_util::sync::CancellationToken::new(),
    ));
    Ok((store, stable_log))
}

/// The integration worker of an [`open_test_local_store`] store.
pub struct TestWorker {
    cancel: tokio_util::sync::CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), zslg_local_store::Error>>,
}

impl TestWorker {
    /// Cancel and drain — what `Shutdown` does for the production worker. Call
    /// it after dropping the store, before reopening its root.
    pub async fn stop(self) {
        self.cancel.cancel();
        let _ = self.handle.await;
    }
}

/// A local store whose integration worker can be drained, for tests that
/// reopen the same root.
pub async fn open_test_local_store(
    root: &std::path::Path,
) -> Result<(Arc<crate::LocalStore>, TestWorker), Error> {
    let storage = open_for_test(root)?;
    let stable_log: Arc<dyn zslg_log::StableLogSnapshot> =
        Arc::new(zslg_log_fake::FakeStableLog::new());
    let cancel = tokio_util::sync::CancellationToken::new();
    let (store, rx) = zslg_local_store::create_local_store(
        &zslg_local_store::LocalStoreConfig::for_test(),
        storage,
        stable_log,
    )
    .await?;
    let handle = tokio::spawn(zslg_local_store::run_integrate_worker(
        Arc::downgrade(&store),
        rx,
        cancel.clone(),
    ));
    Ok((store, TestWorker { cancel, handle }))
}

/// A stable log on its own storage root carrying every learner column family.
pub async fn stable_log_engine(dir: std::path::PathBuf) -> Result<crate::StableLog, Error> {
    Ok(crate::StableLog::open_on(open_for_test(&dir)?).await?)
}
