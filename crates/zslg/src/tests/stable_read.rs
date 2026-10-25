//! Stable-read: write data, wait for a snapshot to be created, then verify
//! the data is visible in the stable log across all nodes.

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

use crate::ids::LogIndex;
use std::sync::Arc;
use std::time::Duration;

use crate::StableLogEvent;
use crate::tests::{
    Cluster, ClusterConfig, assert_cluster_invariants_with_acks, default_fast_config,
};
use bytes::Bytes;
use futures::StreamExt;
use object_store::memory::InMemory;

/// Write data, wait for a snapshot (stable prefix), and verify the data
/// appears in every node's stable tail.
#[tokio::test(flavor = "multi_thread")]
async fn writes_appear_in_stable_log_across_all_nodes() {
    let mut base = default_fast_config();
    base.snapshot_interval = Duration::from_millis(200);

    let cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(base),
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

    let payload = Bytes::from_static(b"stable-read-test");
    let acked_id = cluster
        .node(leader)
        .writer()
        .write(payload.clone())
        .await
        .expect("write");

    // Wait for the snapshot to be created (stable prefix covers this write).
    crate::tests::wait_until_sync("snapshot created", Duration::from_secs(15), || {
        cluster.nodes().any(|n| {
            n.state_handle()
                .snapshot()
                .manifest_state
                .manifest_watermark
                .is_some()
        })
    })
    .await;

    // Every node's stable tail must contain the written entry.
    for node in cluster.nodes() {
        let found = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut stream = node
                    .reader()
                    .subscribe_stable_log(LogIndex::ZERO)
                    .filter_map(|item| match item {
                        Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                        Ok(StableLogEvent::Watermark(_)) => None,
                        Err(e) => Some(Err(e)),
                    })
                    .into_stream();
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

    let _ = assert_cluster_invariants_with_acks(&cluster, &[(acked_id, payload.clone())]).await;

    cluster.shutdown().await.expect("clean shutdown");
}

/// A wrong payload under a correct `DataId` must fail the durability chain —
/// the check is content, not presence.
///
/// This is the teeth check for [`assert_durability_chain`]'s payload
/// comparison: it feeds the chain a `DataId` that really is in every stable
/// log, paired with bytes that were never written, and requires the chain to
/// notice. Against a presence-only chain it passes silently.
#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "does not match what was acked")]
async fn durability_chain_rejects_wrong_payload() {
    let mut base = default_fast_config();
    base.snapshot_interval = Duration::from_millis(200);

    let cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(base),
    })
    .await
    .expect("Cluster::new");
    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(20)).await;

    // `find_map(.leader)` yields the leader's id. Any node would accept this
    // write, but naming the node correctly keeps the test honest about what it
    // is doing.
    let leader = cluster
        .nodes()
        .find_map(|n| n.state_handle().snapshot().leader)
        .expect("no leader");

    let payload = Bytes::from_static(b"correct-payload");
    let acked = cluster
        .node(leader)
        .writer()
        .write(payload.clone())
        .await
        .expect("write");

    // Corrupt the *expected* payload rather than the stored one: the stable
    // log holds `correct-payload` under `acked`, and the chain is told to
    // expect different bytes under that same id. Same length, so nothing but a
    // content comparison can catch it.
    let wrong = Bytes::from_static(b"WRONG!!-payload");
    assert_eq!(
        wrong.len(),
        payload.len(),
        "same length: only content differs"
    );
    let _ = assert_cluster_invariants_with_acks(&cluster, &[(acked, wrong)]).await;

    // Unreachable: the chain panics above. No `shutdown()` — the harness drops
    // the nodes. Same shape as `assert_monotonic_panics_on_decrease`.
}
