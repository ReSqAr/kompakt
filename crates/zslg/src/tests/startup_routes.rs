#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_arguments,
    clippy::result_large_err,
    reason = "test code"
)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;

use crate::api::SnapshotStore as _;
use crate::object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use crate::replica_node::ReplicaNodeConfig;
use crate::{Error, NodeId, PeerAddr, ReplicaNode};

fn single_node_config(object_store: Arc<dyn ObjectStore>, db_path: PathBuf) -> ReplicaNodeConfig {
    let peers = vec![PeerAddr {
        addr: "127.0.0.1:0".into(),
        server_name: "localhost".into(),
    }];
    let mut cfg = ReplicaNodeConfig::new(
        "localhost".into(),
        peers,
        "localhost".into(),
        "127.0.0.1:0".parse().expect("listen addr"),
        db_path,
        object_store,
    )
    .expect("single-node config builds");
    cfg.snapshot_interval = Duration::from_secs(3600);
    cfg.snapshot_cutoff_timeout = Duration::from_millis(10);
    cfg.snapshot_compaction_tiers = vec![
        Duration::from_secs(3600).into(),
        Duration::from_secs(3600 + 3600).into(),
        Duration::from_secs(86_400).into(),
        Duration::from_secs(7 * 86_400).into(),
        Duration::from_secs(30 * 86_400).into(),
        Duration::from_secs(365 * 86_400).into(),
    ];
    cfg.drain_timeout = Duration::from_secs(30);
    cfg
}

#[tokio::test]
async fn bootstrap_route_writes_fence_and_initialises() {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let object_store_dir = tmpdir.path().join("objstore");
    std::fs::create_dir_all(&object_store_dir).expect("mkdir objstore");

    let _node_id = NodeId::new_v4();

    let data_dir = tmpdir.path().join("replica_data");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&object_store_dir).expect("ls"),
    );
    let mut cfg = single_node_config(store.clone(), data_dir.join("node.db"));
    cfg.snapshot_interval = Duration::from_secs(3600);
    let node = ReplicaNode::start(cfg)
        .await
        .expect("start")
        .wait_until_ready()
        .await
        .expect("ready");
    let node_id = node.node_id();

    let fence_store = RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    );
    assert!(fence_store.has_bootstrap_fence().await.expect("read fence"));

    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(15))
        .expect("Instant + 15s");
    loop {
        let snap = node.state_handle().snapshot();
        if snap.replica.current().contains(&node_id) {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("SetReplicaSet did not apply within deadline");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    node.shutdown_token().cancel();
    tokio::time::timeout(Duration::from_secs(30), node.run())
        .await
        .expect("shutdown timed out after 30s")
        .expect("clean shutdown");
}

/// **The whole point of the migration, measured on a real node.** Every
/// RocksDB instance puts exactly one LOCK file in its own root, so counting
/// them under the storage root counts instances.
///
/// A learner node used to be charged two to three times its declared budget:
/// the local store opened its own `<root>/data` sub-root, whose *parent* is a different
/// root, so it got a second DB with a second full-size block cache. Two more
/// arrived during the port, when each ported store had to run its own root.
///
/// This is also what makes `zslg_storage_memory_charged_bytes` exact: the gauge
/// reads one cache, and one LOCK file is the proof there is only one.
#[tokio::test]
async fn a_started_node_opens_exactly_one_rocksdb() {
    fn count_lock_files(dir: &std::path::Path) -> usize {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|e| {
                let path = e.path();
                if path.is_dir() {
                    count_lock_files(&path)
                } else {
                    usize::from(path.file_name().is_some_and(|n| n == "LOCK"))
                }
            })
            .sum()
    }

    let tmpdir = tempfile::tempdir().expect("tempdir");
    let object_store_dir = tmpdir.path().join("objstore");
    std::fs::create_dir_all(&object_store_dir).expect("mkdir objstore");
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&object_store_dir).expect("ls"),
    );
    let db_path = tmpdir.path().join("replica_data").join("node");
    let node = ReplicaNode::start(single_node_config(store, db_path.clone()))
        .await
        .expect("start");

    assert_eq!(
        count_lock_files(&db_path),
        1,
        "a node must open exactly one RocksDB instance under its storage root",
    );

    node.shutdown_token().cancel();
    tokio::time::timeout(Duration::from_secs(30), node.run())
        .await
        .expect("shutdown timed out after 30s")
        .expect("clean shutdown");
}

/// A clean restart of a node still in its own membership takes the Rejoin
/// route and must pass `wait_for_raft_recent` — waiting for the durable state
/// (instant) then catching up to the quorum's applied generation against a
/// self-quorum — coming back up successfully.
#[tokio::test]
async fn clean_restart_rejoins_through_freshness_gate() {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let data_dir = tmpdir.path().join("replica_data");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    let object_store_dir = tmpdir.path().join("objstore");
    std::fs::create_dir_all(&object_store_dir).expect("mkdir objstore");

    let db_path = data_dir.join("node.db");
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&object_store_dir).expect("ls"),
    );
    let cfg = single_node_config(store, db_path);

    // First boot: bootstrap, then wait until the initial replica set applied.
    let node = ReplicaNode::start(cfg.clone())
        .await
        .expect("first boot")
        .wait_until_ready()
        .await
        .expect("first boot ready");
    let node_id = node.node_id();
    wait_until_replica_contains(&node, node_id).await;
    node.shutdown_token().cancel();
    tokio::time::timeout(Duration::from_secs(30), node.run())
        .await
        .expect("shutdown timed out after 30s")
        .expect("clean shutdown");

    // Second boot: same db + membership → Rejoin route. The freshness gate must
    // complete (self-quorum) and `start` must succeed within the timeout.
    let node = tokio::time::timeout(Duration::from_secs(30), async {
        ReplicaNode::start(cfg).await?.wait_until_ready().await
    })
    .await
    .expect("rejoin start timed out after 30s")
    .expect("rejoin boot");
    wait_until_replica_contains(&node, node_id).await;
    node.shutdown_token().cancel();
    tokio::time::timeout(Duration::from_secs(30), node.run())
        .await
        .expect("shutdown timed out after 30s")
        .expect("clean shutdown");
}

/// Spin until the node's applied replica set contains `node_id`, or panic after
/// 15s.
async fn wait_until_replica_contains(node: &ReplicaNode, node_id: NodeId) {
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(15))
        .expect("Instant + 15s");
    loop {
        if node
            .state_handle()
            .snapshot()
            .replica
            .current()
            .contains(&node_id)
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("replica set did not contain node within deadline");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn hard_crash_restart_surfaces_fresh_node_id_error() {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let data_dir = tmpdir.path().join("replica_data");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    let object_store_dir = tmpdir.path().join("objstore");
    std::fs::create_dir_all(&object_store_dir).expect("mkdir objstore");

    let db_path = data_dir.join("node.db");
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&object_store_dir).expect("ls"),
    );
    let cfg = single_node_config(store, db_path.clone());
    let node = ReplicaNode::start(cfg.clone())
        .await
        .expect("first boot")
        .wait_until_ready()
        .await
        .expect("first boot ready");
    node.shutdown_token().cancel();
    tokio::time::timeout(Duration::from_secs(30), node.run())
        .await
        .expect("shutdown timed out after 30s")
        .expect("clean shutdown");

    // The raft store's DB lives under the storage root.
    rewrite_membership_to_other_id(data_dir.join("node.db"))
        .await
        .expect("rewrite membership");

    // The decision is made inside `run_raft_startup`, which now lives behind
    // `wait_until_ready` — `start` itself no longer inspects durable membership.
    let err = ReplicaNode::start(cfg)
        .await
        .expect("second boot binds")
        .wait_until_ready()
        .await
        .expect_err("second boot must surface HardCrashDataLoss");
    assert!(
        matches!(err, Error::HardCrashDataLoss),
        "expected HardCrashDataLoss; got {err:?}",
    );
}

/// Open the node's database, retrying on lock contention.
///
async fn rewrite_membership_to_other_id(
    storage_root: std::path::PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    crate::raft::seed_foreign_membership_for_test(storage_root).await?;
    Ok(())
}
