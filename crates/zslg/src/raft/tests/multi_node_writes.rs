//! One end-to-end smoke test through the full TCP stack: a three-node cluster
//! over real sockets, a leader write, and the payload appearing in the
//! unstable tail.
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

use crate::tests::{Cluster, ClusterConfig};

#[tokio::test]
async fn three_node_tcp_write_round_trip() {
    let cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        ..Default::default()
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(20)).await;

    let leader_id = cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(|n| n.node_id())
        .expect("no leader");

    let payload = Bytes::from_static(b"hello-tcp");
    cluster
        .node(leader_id)
        .writer()
        .write(payload.clone())
        .await
        .expect("write");

    let reader = cluster.node(leader_id).reader();
    let found = tokio::time::timeout(Duration::from_secs(20), async {
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
    .expect("written entry never appeared in unstable tail");

    assert_eq!(found.blob.as_ref(), payload.as_ref());

    cluster.shutdown().await.expect("clean shutdown");
}
