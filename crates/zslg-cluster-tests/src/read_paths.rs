#![allow(
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
//! Every way of reading what the replicas committed, against one three-node cluster:
//!
//! - an external `WriterNode` writes; a replica's unstable tail shows the entry stamped
//!   with the *writer's* ephemeral node id;
//! - a `LearnerNode` (which also starts and shuts down cleanly) reads an entry written
//!   through a replica's embedded writer from its own unstable tail;
//! - once a snapshot exists, every node's stable log contains that entry, and the
//!   cluster-wide durability chain accepts the acked write.
//!
//! One bring-up serves all three; they used to be separate tests with a boot each.

use crossbeam_combinators::testing::ChannelTestExt as _;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use object_store::memory::InMemory;
use tempfile::TempDir;

use crate::{Cluster, ClusterConfig, assert_cluster_invariants_with_acks, default_fast_config};
use zslg::{LearnerNode, WriterNode, WriterNodeConfig};
use zslg_consensus::NodeId;
use zslg_log::{DataId, StableLogEvent};

/// Poll a node's unstable tail until an entry with exactly this payload shows up.
async fn unstable_tail_has(
    reader: &Arc<dyn zslg_log::Reader>,
    payload: &Bytes,
    within: Duration,
) -> DataId {
    tokio::time::timeout(within, async {
        loop {
            let mut stream = reader.unstable_tail().into_test_stream();
            while let Some(item) = stream.next().await {
                if let Ok(d) = item
                    && d.blob == *payload
                {
                    return d.data_id;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("written entry never appeared in the unstable tail")
}

#[tokio::test(flavor = "multi_thread")]
async fn every_read_path_sees_the_writes() {
    let mut base = default_fast_config();
    base.snapshot_interval = Duration::from_millis(200);
    let cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: true,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(base),
    })
    .await
    .expect("Cluster::new");
    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(45)).await;

    let any_id: NodeId = cluster
        .node_ids()
        .into_iter()
        .next()
        .expect("at least one node");

    // ── external writer → a replica's unstable tail ───────────────────
    let writer_node = WriterNode::start({
        let mut cfg = WriterNodeConfig::new(cluster.peer_addrs());
        cfg.raft_tracker_min_backoff = Duration::from_millis(100);
        cfg
    })
    .await
    .expect("writer start");
    let external = Bytes::from_static(b"from-external-writer");
    tokio::time::timeout(
        Duration::from_secs(10),
        writer_node.writer().write(external.clone()),
    )
    .await
    .expect("write did not time out")
    .expect("quorum write succeeds");
    let id = unstable_tail_has(
        &cluster.node(any_id).reader(),
        &external,
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(
        id.source_node(),
        writer_node.node_id(),
        "data must be stamped with the external writer's ephemeral node id",
    );
    writer_node.shutdown_token().cancel();
    writer_node.run().await.expect("writer clean shutdown");

    // ── replica writer → learner's unstable tail ─────────────────────
    let learner_dir = TempDir::new().expect("tempdir for learner");
    let learner = LearnerNode::start(cluster.learner_config(learner_dir.path().join("learner.db")))
        .await
        .expect("learner start");

    let payload = Bytes::from_static(b"read-paths");
    let acked_id = cluster
        .node(any_id)
        .writer()
        .write(payload.clone())
        .await
        .expect("quorum write succeeds");
    let id = unstable_tail_has(&learner.reader(), &payload, Duration::from_secs(60)).await;
    assert_eq!(
        id.source_node(),
        any_id,
        "data must be stamped with the writing replica's node id",
    );
    learner.shutdown_token().cancel();
    learner.run().await.expect("learner clean shutdown");

    // ── stable log on every node, once a snapshot covers the write ────
    crate::wait_until_sync("snapshot created", Duration::from_secs(15), || {
        cluster.nodes().any(|n| {
            n.state_handle()
                .snapshot()
                .manifest_state
                .manifest_watermark
                .is_some()
        })
    })
    .await;
    for node in cluster.nodes() {
        let found = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut stream = node
                    .reader()
                    .subscribe_stable_log(None)
                    .into_channel()
                    .filter_map(|item| match item {
                        Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                        Ok(StableLogEvent::Watermark(_)) => None,
                        Err(e) => Some(Err(e)),
                    })
                    .into_test_stream();
                while let Some(item) = stream.next().await {
                    if let Ok((_, d)) = item
                        && d.blob == payload
                    {
                        return d;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("entry not found in stable log");
        assert_eq!(found.blob.as_ref(), payload.as_ref());
    }

    let _ = assert_cluster_invariants_with_acks(&cluster, &[(acked_id, payload)]).await;
    cluster.shutdown().await.expect("clean shutdown");
}
