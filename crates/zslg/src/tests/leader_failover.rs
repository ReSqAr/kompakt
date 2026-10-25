//! Leader failover: kill the leader, verify a new leader is elected and
//! writes continue to succeed.

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
use object_store::memory::InMemory;

use crate::tests::{Cluster, ClusterConfig, CutoffObserver, assert_cluster_invariants};

/// Kill the leader, wait for a new leader, and verify writes still succeed.
#[tokio::test(flavor = "multi_thread")]
async fn leader_failover_writes_recover() {
    let mut cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        ..Default::default()
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(20)).await;

    // Find the current leader.
    let leader_id = cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(|n| n.node_id())
        .expect("no leader");

    // Spawn the committed-cutoff observer before the failover, the case
    // that matters: a new leader may *propose* a lower cutoff (CLAUDE.md
    // §12, expected), but a lower one must never be *committed*.
    let cutoff_observer = CutoffObserver::spawn_for(&cluster);

    // Write before failover to advance the log.
    cluster
        .node(leader_id)
        .writer()
        .write(Bytes::from_static(b"before-failover"))
        .await
        .expect("pre-failover write");

    // Kill the leader.
    cluster.remove(leader_id).await.expect("remove leader");

    // A new leader must be elected within the remaining nodes.
    cluster.wait_for_leader(Duration::from_secs(15)).await;

    // Find the new leader.
    let new_leader = cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(|n| n.node_id())
        .expect("no leader after failover");
    assert_ne!(new_leader, leader_id, "new leader must be a different node");

    // Writes must still succeed.
    cluster
        .node(new_leader)
        .writer()
        .write(Bytes::from_static(b"after-failover"))
        .await
        .expect("post-failover write");

    assert_cluster_invariants(&cluster);
    cutoff_observer.assert_monotonic();
    cutoff_observer.stop().await;

    cluster.shutdown().await.expect("clean shutdown");
}

/// Kill a follower — writes through the leader must not be affected.
#[tokio::test(flavor = "multi_thread")]
async fn follower_loss_does_not_block_writes() {
    let mut cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        ..Default::default()
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(20)).await;

    let leader = cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(|n| n.node_id())
        .expect("no leader");

    // Find a follower (not the leader).
    let follower = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("no follower");

    // Kill the follower.
    cluster.remove(follower).await.expect("remove follower");

    // Writes through the leader must still succeed (quorum is 2/3, we have 2).
    cluster
        .node(leader)
        .writer()
        .write(Bytes::from_static(b"after-follower-loss"))
        .await
        .expect("write after follower loss");

    assert_cluster_invariants(&cluster);

    cluster.shutdown().await.expect("clean shutdown");
}
