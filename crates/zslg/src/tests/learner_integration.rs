//! End-to-end: a free-standing [`LearnerNode`] subscribing to a *genuinely
//! joined* cluster of three real [`ReplicaNode`]s over the TCP transport, then serving reads
//! through its own [`Reader`](crate::Reader) API.
//!
//! This exercises the full learner path that has no Raft of its own: the learner
//! discovers the replica set via streaming `subscribe()` connections (the
//! [`RaftTracker`](crate::raft::RaftTracker) streamer), replicates data into its
//! local store, and answers quorum-gated reads including `unstable_tail`.

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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use object_store::memory::InMemory;
use tempfile::TempDir;

use crate::tests::{Cluster, ClusterConfig, assert_cluster_invariants};
use crate::{LearnerNode, LearnerNodeConfig, ObjectStoreConfig};

#[tokio::test(flavor = "multi_thread")]
async fn learner_node_starts_and_shuts_down_cleanly() {
    let cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        ..Default::default()
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(15)).await;
    cluster.wait_for_convergence(Duration::from_secs(45)).await;

    let learner_dir = TempDir::new().expect("tempdir for learner");
    let learner_db = learner_dir.path().join("learner.db");

    let learner_cfg = {
        let mut cfg = LearnerNodeConfig::new(
            cluster.peer_addrs(),
            learner_db,
            ObjectStoreConfig::InMemory,
        )
        .expect("learner config");
        cfg.startup_timeout = Duration::from_secs(30);
        cfg
    };
    let learner = LearnerNode::start(learner_cfg)
        .await
        .expect("learner start");

    learner.shutdown_token().cancel();
    learner.run().await.expect("learner clean shutdown");
    assert_cluster_invariants(&cluster);
    cluster.shutdown().await.expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn learner_node_reads_data_written_by_replica() {
    let cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        ..Default::default()
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(15)).await;
    cluster.wait_for_convergence(Duration::from_secs(45)).await;

    let learner_dir = TempDir::new().expect("tempdir for learner");
    let learner_db = learner_dir.path().join("learner.db");

    let learner_cfg = {
        let mut cfg = LearnerNodeConfig::new(
            cluster.peer_addrs(),
            learner_db,
            ObjectStoreConfig::InMemory,
        )
        .expect("learner config");
        cfg.startup_timeout = Duration::from_secs(30);
        cfg
    };
    let learner = LearnerNode::start(learner_cfg)
        .await
        .expect("learner start");

    // Write through a replica's embedded writer.
    let any_id = cluster
        .node_ids()
        .into_iter()
        .next()
        .expect("at least one node");
    let writer = cluster.node(any_id).writer();
    let payload = Bytes::from_static(b"hello-from-learner-integration-test");

    writer
        .write(payload.clone())
        .await
        .expect("quorum write succeeds");

    // Read back through the LearnerNode's reader.
    let reader = learner.reader();
    let found = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut stream = reader.unstable_tail().into_stream();
            while let Some(item) = stream.next().await {
                if let Ok(d) = item
                    && d.blob == payload
                {
                    return d;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("written entry never appeared in learner's unstable tail");

    assert_eq!(found.blob.as_ref(), b"hello-from-learner-integration-test");
    assert_eq!(
        found.data_id.source_node(),
        cluster.node(any_id).node_id(),
        "data must be stamped with the writing replica's node id",
    );

    learner.shutdown_token().cancel();
    learner.run().await.expect("learner clean shutdown");
    assert_cluster_invariants(&cluster);
    cluster.shutdown().await.expect("clean shutdown");
}
