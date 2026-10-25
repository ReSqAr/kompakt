/// Shared single-leader snapshot-pipeline harness: one real raft node plus
/// the local store / stable log / object store the snapshot pipeline runs
/// against. Hoisted from `tests/snapshot_lifecycle.rs` so the leadership-churn
/// and stable-order property tests can drive the same pipeline.
use std::sync::Arc;

use crate::api::ReplicatedRaftState;
use crate::hlc::HlcGate;
use crate::ids::NodeId;
use crate::leader::SnapshotResult;
use crate::learner::LocalStore;
use crate::learner::StableLog;
use crate::object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use crate::raft::Raft;
use crate::raft::RaftStateHandle;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use openraft::Config;

pub(crate) struct SnapshotHarness {
    pub(crate) _dir: tempfile::TempDir,
    pub(crate) local_store: Arc<LocalStore>,
    pub(crate) stable_log: Arc<StableLog>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) hlc_gate: HlcGate,
    pub(crate) raft: Raft,
    pub(crate) raft_state: RaftStateHandle,
    pub(crate) leader_id: NodeId,
}

impl SnapshotHarness {
    pub(crate) fn snapshot_store(&self) -> Arc<RemoteSnapshotStore> {
        Arc::new(RemoteSnapshotStore::new(
            SnapshotStoreConfig {
                object_store: self.object_store.clone(),
            },
            tokio_util::sync::CancellationToken::new(),
        ))
    }
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
    let mut nodes = super::start_cluster(&[n1], default_raft_config())
        .await
        .expect("cluster");
    nodes
        .get_mut(&n1)
        .expect("n1")
        .raft
        .initialize(super::test_membership_map(&[n1]))
        .await
        .expect("initialize");
    super::wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;

    let sys = nodes.remove(&n1).expect("n1");

    let dir = tempfile::tempdir().expect("tempdir");
    let local_path = dir.path().join("local.db");
    let stable_path = dir.path().join("stable.db");
    let local_store = LocalStore::simple_at(local_path.with_extension("stable"))
        .await
        .expect("from_db");
    let stable_log = crate::tests::stable_log_at(&stable_path).await;
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let hlc_gate = HlcGate::new();

    SnapshotHarness {
        _dir: dir,
        local_store,
        stable_log,
        object_store,
        hlc_gate,
        raft: sys.raft,
        raft_state: sys.raft_state,
        leader_id: n1,
    }
}

/// Commit a `SnapshotResult` through the real raft, exactly as the leader
/// loop's snapshot tick would (manifest + watermark + cutoff in one fenced
/// state write).
pub(crate) async fn commit_snapshot_result(
    raft: &Raft,
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
        super::write_state_fenced(
            raft,
            raft_state,
            ReplicatedRaftState {
                replica: current.replica.clone(),
                manifest,
                manifest_watermark: Some(*new_watermark),
                hlc_cutoff: Some(*new_watermark),
            },
        )
        .await
        .expect("client_write after snapshot");
    }
}
