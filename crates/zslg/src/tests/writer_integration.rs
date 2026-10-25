//! End-to-end: a free-standing [`WriterNode`] writing into a *genuinely joined*
//! cluster of three real [`ReplicaNode`]s over the TCP transport.
//!
//! This exercises the whole writer path that has no Raft of its own: the writer
//! learns the replica set + `NodeId → addr` mapping purely by polling the
//! replicas' `fetch_status`, then fans a write out to a quorum. Verification
//! reads the blob back through a replica's `unstable_tail` and confirms the
//! *writer's* ephemeral id is what stamped the data.

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

use crate::tests::{Cluster, ClusterConfig, assert_cluster_invariants};
use crate::{WriterNode, WriterNodeConfig};

const WRITER_NAME: &str = "writer";

#[tokio::test(flavor = "multi_thread")]
async fn writer_node_writes_against_three_replica_nodes() {
    let cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: true,
        object_store: Arc::new(InMemory::new()),
        ..Default::default()
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(15)).await;
    cluster.wait_for_convergence(Duration::from_secs(45)).await;

    // Start the external writer.
    let writer_cfg = {
        let mut cfg = WriterNodeConfig::new(cluster.peer_addrs());
        cfg.raft_tracker_min_backoff = Duration::from_millis(100);
        cfg
    };
    let writer = WriterNode::start(writer_cfg).await.expect("writer start");

    let payload = Bytes::from_static(b"from-external-writer");
    tokio::time::timeout(
        Duration::from_secs(10),
        writer.writer().write(payload.clone()),
    )
    .await
    .expect("write did not time out")
    .expect("quorum write succeeds");

    // The write must be observable through a replica's unstable tail, stamped
    // with the *writer's* ephemeral node id.
    let any_id = cluster
        .node_ids()
        .into_iter()
        .next()
        .expect("at least one node");
    let reader = cluster.node(any_id).reader();
    let writer_id = writer.node_id();
    let found = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let mut stream = reader.unstable_tail().into_stream();
            while let Some(item) = stream.next().await {
                if let Ok(d) = item
                    && d.data_id.source_node() == writer_id
                    && d.blob == payload
                {
                    return d;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("written entry never appeared in unstable tail");

    assert_eq!(found.blob.as_ref(), b"from-external-writer");
    assert_eq!(
        found.data_id.source_node(),
        writer_id,
        "data must be stamped with the external writer's ephemeral node id",
    );

    // Clean shutdown: writer first, then the replicas.
    writer.shutdown_token().cancel();
    writer.run().await.expect("writer clean shutdown");
    assert_cluster_invariants(&cluster);
    cluster.shutdown().await.expect("clean shutdown");
}
