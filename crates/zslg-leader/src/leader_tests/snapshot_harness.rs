/// Shared single-leader snapshot-pipeline harness: one real raft node plus
/// the local store / stable log / object store the snapshot pipeline runs
/// against. Hoisted from `tests/snapshot_lifecycle.rs` so the leadership-churn
/// and stable-order property tests can drive the same pipeline.
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use raft_kit::Config;
use zslg_consensus::NodeId;
use zslg_consensus::RaftStateHandle;
use zslg_consensus::ReplicatedRaftState;
use zslg_leader_snapshot::create::SnapshotResult;
use zslg_learner::LocalStore;
use zslg_learner::StableLog;
use zslg_object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use zslg_raft::Raft;

pub(crate) struct SnapshotHarness {
    pub(crate) _dir: tempfile::TempDir,
    pub(crate) local_store: Arc<LocalStore>,
    pub(crate) stable_log: Arc<StableLog>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) raft: Raft,
    pub(crate) raft_state: RaftStateHandle,
    pub(crate) leader_id: NodeId,
}

fn default_raft_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    )
}

pub(crate) async fn boot_snapshot_harness() -> SnapshotHarness {
    let n1 = NodeId::new_v4();
    let mut nodes = zslg_raft_setup::testing::start_cluster(&[n1], default_raft_config())
        .await
        .expect("cluster");
    nodes
        .get_mut(&n1)
        .expect("n1")
        .raft
        .initialize(zslg_raft_fake::test_membership_map(&[n1]))
        .await
        .expect("initialize");
    zslg_raft_setup::testing::wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;

    let sys = nodes.remove(&n1).expect("n1");

    let dir = tempfile::tempdir().expect("tempdir");
    let local_path = dir.path().join("local.db");
    let stable_path = dir.path().join("stable.db");
    let local_store = zslg_learner::testing::local_store_at(local_path.with_extension("stable"))
        .await
        .expect("from_db");
    let stable_log = zslg_learner::testing::stable_log_at(&stable_path).await;
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    SnapshotHarness {
        _dir: dir,
        local_store,
        stable_log,
        object_store,
        raft: sys.raft,
        raft_state: sys.raft_state,
        leader_id: n1,
    }
}

/// Commit a `SnapshotResult` exactly as the leader loop's snapshot tick would
/// (manifest + watermark + cutoff in one state write), fenced on the generation
/// read just before it.
pub(crate) async fn commit_snapshot_result_via(
    replicator: &dyn zslg_consensus::StateReplicator,
    raft_state: &RaftStateHandle,
    result: &SnapshotResult,
) {
    if let SnapshotResult::Committed {
        object,
        new_watermark,
        ..
    } = result
    {
        let current = raft_state.snapshot();
        let mut manifest = current.manifest_state.manifest.clone();
        if let Some(written) = object {
            manifest.push(written.entry.clone());
        }
        replicator
            .replicate_state(
                ReplicatedRaftState {
                    replica: current.replica.clone(),
                    manifest,
                    manifest_watermark: Some(*new_watermark),
                    hlc_cutoff: Some(*new_watermark),
                },
                current.generation,
                "test snapshot commit",
            )
            .await
            .expect("replicate state after snapshot");
    }
}

/// The snapshot pipeline's stores plus a single-node consensus fake: for the
/// tests that are about building and committing snapshots, not about Raft.
/// The fake enforces the same generation fence on the committed state; the
/// tests that need a real log (leadership changes, the apply fence itself) use
/// [`SnapshotHarness`].
pub(crate) struct FakeSnapshotHarness {
    pub(crate) _dir: tempfile::TempDir,
    pub(crate) local_store: Arc<LocalStore>,
    pub(crate) stable_log: Arc<StableLog>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) consensus: Arc<zslg_consensus_fake::SingleNodeConsensus>,
    pub(crate) raft_state: RaftStateHandle,
}

impl FakeSnapshotHarness {
    pub(crate) fn snapshot_store(&self) -> Arc<RemoteSnapshotStore> {
        Arc::new(RemoteSnapshotStore::new(
            SnapshotStoreConfig {
                object_store: self.object_store.clone(),
            },
            tokio_util::sync::CancellationToken::new(),
        ))
    }
}

pub(crate) async fn boot_fake_snapshot_harness() -> FakeSnapshotHarness {
    let consensus = Arc::new(zslg_consensus_fake::SingleNodeConsensus::new(
        NodeId::new_v4(),
    ));
    let dir = tempfile::tempdir().expect("tempdir");
    let local_store =
        zslg_learner::testing::local_store_at(dir.path().join("local.db").with_extension("stable"))
            .await
            .expect("from_db");
    let stable_log = zslg_learner::testing::stable_log_at(&dir.path().join("stable.db")).await;
    FakeSnapshotHarness {
        _dir: dir,
        local_store,
        stable_log,
        object_store: Arc::new(InMemory::new()),
        raft_state: consensus.state(),
        consensus,
    }
}

/// A storage root carrying the replica-log column family.
pub(super) fn open_replica_storage(
    root: &std::path::Path,
) -> Result<Arc<rocksdb_kit::Storage>, zslg_error::Error> {
    let cfs = [rocksdb_kit::CfSpec {
        cf: zslg_replica_log::REPLICA_LOG_CF,
        compress: false,
    }];
    Ok(rocksdb_kit::Storage::open(
        root,
        rocksdb_kit::StorageConfig::new(bytesize::ByteSize::from_mib(64)),
        &cfs,
    )?)
}
