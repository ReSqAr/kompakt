//! TCP transport failure-detection tests: the operational reason the transport
//! was moved to plain TCP (docs/PERFORMANCE.md entry 26).
//!
//! A Kubernetes StatefulSet pod keeps its DNS name but gets a new IP on restart.
//! Under the old transport the dead connection blackholed until the idle timeout;
//! under TCP the dead connection surfaces immediately and every reconnect
//! re-resolves DNS. These tests exercise that a node restarted **on the same
//! address** is reached again and the cluster keeps making progress.
//!
//! Fast connection-refused failure and socket-option application are covered by
//! unit tests in `src/network/tcp.rs` (`connection_refused_is_fast_typed_error`,
//! `socket_opts_are_applied`); true packet-level blackhole detection (kernel
//! keepalive / TCP_USER_TIMEOUT) is left to the docker harness.

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
    clippy::too_many_arguments,
    clippy::result_large_err,
    reason = "test code"
)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use object_store::memory::InMemory;

use crate::ids::NodeId;
use crate::tests::{Cluster, ClusterConfig, assert_cluster_invariants};

/// Find the id of the current leader (panics if there is none).
fn leader_of(cluster: &Cluster) -> NodeId {
    cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(|n| n.node_id())
        .expect("no leader")
}

/// k8s pod-restart simulation: replace a node **on the same address** (new
/// instance, new `NodeId`) and confirm the cluster keeps committing writes.
/// Under the old transport the leader's cached connection to the old instance
/// would blackhole; over TCP the reconnect re-resolves and re-dials the same address.
#[tokio::test(flavor = "multi_thread")]
async fn reconnect_after_server_restart() {
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

    let leader = leader_of(&cluster);
    cluster
        .node(leader)
        .writer()
        .write(Bytes::from_static(b"before-restart"))
        .await
        .expect("pre-restart write");

    // Restart a follower on the same listen address (a fresh node/IP behind the
    // stable address). `replace` drains the old node, waits for the TCP port to
    // free, and launches the new one; convergence proves the leader re-dialed it.
    let follower = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("a follower exists");
    let new_id = cluster.replace(follower).await.expect("replace follower");
    eprintln!("  replaced {follower:?} -> {new_id:?} on the same address");
    cluster.wait_for_convergence(Duration::from_secs(60)).await;

    // The cluster must still commit writes after the restart.
    let leader = leader_of(&cluster);
    cluster
        .node(leader)
        .writer()
        .write(Bytes::from_static(b"after-restart"))
        .await
        .expect("post-restart write must reach quorum");

    assert_cluster_invariants(&cluster);

    cluster.shutdown().await.expect("clean shutdown");
}

/// The hot pooled write path must survive a peer restart: sustained writes
/// through the leader keep succeeding while a follower is replaced, and the
/// replaced node catches up (proving the write pool reopened its connections
/// with a re-resolved address rather than wedging on a dead one).
#[tokio::test(flavor = "multi_thread")]
async fn pooled_write_survives_peer_restart() {
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

    // Write a batch, restart a follower, then write another batch — all writes
    // (pooled ReplicaWrite RPCs) must reach quorum across the restart.
    let write_batch = |cluster: &Cluster, tag: u8, n: usize| {
        let leader = leader_of(cluster);
        let writer = cluster.node(leader).writer();
        async move {
            for i in 0..n {
                let payload = Bytes::from(vec![tag, u8::try_from(i).unwrap_or(0)]);
                writer
                    .write(payload)
                    .await
                    .expect("pooled write must reach quorum");
            }
        }
    };

    write_batch(&cluster, 0xAA, 5).await;

    let follower = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader_of(&cluster))
        .expect("a follower exists");
    let new_id = cluster.replace(follower).await.expect("replace follower");
    cluster.wait_for_convergence(Duration::from_secs(60)).await;

    write_batch(&cluster, 0xBB, 5).await;

    // The replaced node must observe post-restart writes through its own tail —
    // it could only get them by having its inbound write connections re-established.
    let reader = cluster.node(new_id).reader();
    let seen = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut stream = reader.unstable_tail().into_stream();
            while let Some(item) = stream.next().await {
                if let Ok(d) = item
                    && d.blob.first() == Some(&0xBB)
                {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        seen.is_ok(),
        "replaced node never observed a post-restart write"
    );

    assert_cluster_invariants(&cluster);

    cluster.shutdown().await.expect("clean shutdown");
}
