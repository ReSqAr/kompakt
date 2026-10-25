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
use std::time::Duration;

use std::path::PathBuf;

use bytes::Bytes;
use futures::StreamExt;
use tempfile::TempDir;

use crate::Error;
use crate::ReplicaNode;
use crate::hlc::Hlc;
use crate::ids::DataId;
use crate::ids::LogIndex;
use crate::ids::NodeId;
use crate::ids::PeerAddr;
use crate::leader::CompactionTier;
use crate::object_store::ObjectStoreConfig;
use crate::replica_node::ReplicaNodeConfig;
use crate::{Data, StableLogEvent};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

fn test_config_with_snapshot_interval(
    snapshot_interval: Duration,
    db_path: PathBuf,
) -> ReplicaNodeConfig {
    let tier0: CompactionTier = snapshot_interval.max(Duration::from_secs(60)).into();
    let compaction_tiers = vec![
        tier0,
        tier0 + Duration::from_secs(3600),
        tier0 + Duration::from_secs(86_400),
        tier0 + Duration::from_secs(7 * 86_400),
        tier0 + Duration::from_secs(30 * 86_400),
        tier0 + Duration::from_secs(365 * 86_400),
    ];
    let listen: std::net::SocketAddr = "127.0.0.1:0".parse().expect("valid addr");
    let peers = vec![PeerAddr {
        addr: listen.into(),
        server_name: "localhost".into(),
    }];
    let mut cfg = ReplicaNodeConfig::new(
        "localhost".into(),
        peers,
        "localhost".into(),
        listen,
        db_path,
        ObjectStoreConfig::InMemory,
    )
    .expect("ReplicaNodeConfig::new");
    cfg.snapshot_interval = snapshot_interval;
    cfg.snapshot_cutoff_timeout = Duration::from_millis(10);
    cfg.snapshot_compaction_tiers = compaction_tiers;
    cfg
}

fn test_config(db_path: PathBuf) -> ReplicaNodeConfig {
    test_config_with_snapshot_interval(Duration::from_secs(3600), db_path)
}

async fn start_node() -> (TempDir, ReplicaNode, NodeId) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("node.db");
    let config = test_config(db_path);
    let node = ReplicaNode::start(config)
        .await
        .expect("ReplicaNode::start")
        .wait_until_ready()
        .await
        .expect("ReplicaNode ready");
    let node_id = node.node_id();
    (dir, node, node_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn write_then_read_via_unstable_tail() {
    let (_dir, node, _) = start_node().await;

    node.writer()
        .write(Bytes::from_static(b"hello"))
        .await
        .expect("write");

    let reader = node.reader();
    let entry = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut stream = reader.unstable_tail().into_stream();
            let mut found = None;
            while let Some(item) = stream.next().await {
                if let Ok(d) = item {
                    found = Some(d);
                }
            }
            if let Some(d) = found {
                return d;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("unstable tail never returned data");

    assert_eq!(entry.blob.as_ref(), b"hello");
    assert_eq!(entry.data_id.source_node(), node.node_id());

    node.shutdown_token().cancel();
    node.run().await.expect("clean shutdown");
}

/// Regression: `LocalFrontierReplicaNode` previously held `Arc<ReplicaService>` which transitively
/// kept `Arc<ReplicaLogStore>` alive through the Reader chain. Holding a `Reader` after
/// shutdown would deadlock the flush loop because `write_tx` was never
/// dropped. This test deliberately keeps `reader` alive across shutdown.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_completes_even_when_reader_outlives_node() {
    let (_dir, node, _) = start_node().await;

    node.writer()
        .write(Bytes::from_static(b"x"))
        .await
        .expect("write");

    let reader = node.reader();

    node.shutdown_token().cancel();
    tokio::time::timeout(Duration::from_secs(20), node.run())
        .await
        .expect("shutdown timeout")
        .expect("shutdown must complete even with reader kept alive");

    drop(reader);
}

#[tokio::test(flavor = "multi_thread")]
async fn quorum_read_completes_for_single_node() {
    let (_dir, node, _) = start_node().await;
    node.reader().sync().await.expect("sync_quorum");
    node.shutdown_token().cancel();
    tokio::time::timeout(TEST_TIMEOUT, node.run())
        .await
        .expect("shutdown timed out")
        .expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn write_multiple_entries_observable_via_reader() {
    let (_dir, node, _) = start_node().await;

    let payloads: Vec<Bytes> = vec![
        Bytes::from_static(b"alpha"),
        Bytes::from_static(b"beta"),
        Bytes::from_static(b"gamma"),
    ];
    for p in &payloads {
        node.writer().write(p.clone()).await.expect("write");
    }

    let reader = node.reader();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut observed: Vec<Bytes> = Vec::new();
    while observed.len() < payloads.len() {
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "only observed {} of {} entries in time",
                observed.len(),
                payloads.len()
            );
        }
        let mut stream = reader.unstable_tail().into_stream();
        observed.clear();
        for _ in 0..payloads.len() {
            match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                Ok(Some(Ok(d))) => observed.push(d.blob.clone()),
                _ => break,
            }
        }
    }

    let observed_set: std::collections::HashSet<_> = observed.into_iter().collect();
    let expected_set: std::collections::HashSet<_> = payloads.into_iter().collect();
    assert_eq!(observed_set, expected_set);

    node.shutdown_token().cancel();
    node.run().await.expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_creation_tick_promotes_into_stable_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config =
        test_config_with_snapshot_interval(Duration::from_millis(500), dir.path().join("node.db"));
    let node = ReplicaNode::start(config)
        .await
        .expect("ReplicaNode::start")
        .wait_until_ready()
        .await
        .expect("ReplicaNode ready");

    for i in 0..3u32 {
        let payload = Bytes::from(format!("snap-{i}").into_bytes());
        node.writer().write(payload).await.expect("write");
    }

    // Wait on the promotion itself — an entry readable through `stable_tail` —
    // not on the watermark. `stable_watermark()` is the stable log's `end_hlc`,
    // and that advances over an **empty** window too (CLAUDE.md §12: the
    // watermark moves even when a snapshot covers nothing): if the first
    // snapshot tick fires before these writes have been integrated locally, its
    // cutoff lands below them, it promotes nothing, and the watermark is
    // nevertheless `Some`. The writes are then covered by a *later* tick, one
    // full `snapshot_interval` away — so gating on the watermark and reading the
    // tail on a single 500 ms budget asserts that race rather than the promotion
    // this test is named for. Re-opening the stream each round is what makes the
    // retry meaningful: `stable_tail` snapshots `end_index` when it is created.
    let reader = node.reader();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    let promoted = loop {
        let mut stream = reader
            .subscribe_stable_log(LogIndex::ZERO)
            .filter_map(|item| match item {
                Ok(StableLogEvent::Appended { index, data }) => Some(Ok((index, data))),
                Ok(StableLogEvent::Watermark(_)) => None,
                Err(e) => Some(Err(e)),
            })
            .into_stream();
        if matches!(
            tokio::time::timeout(Duration::from_millis(500), stream.next()).await,
            Ok(Some(Ok(_)))
        ) {
            break true;
        }
        if tokio::time::Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(promoted, "stable_tail should yield at least one entry");
    // Implied by the promotion, and free to state once it has happened: an
    // entry cannot be in the stable log without the watermark covering it.
    assert!(
        reader.stable_watermark().is_some(),
        "stable_watermark should advance after snapshot creation"
    );

    node.shutdown_token().cancel();
    node.run().await.expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn hlc_cutoff_persists_across_restart() {
    let node_id = NodeId::new_v4();
    let dir = tempfile::tempdir().expect("tempdir");

    let committed_cutoff = {
        let node = ReplicaNode::start(test_config_with_snapshot_interval(
            Duration::from_millis(500),
            dir.path().join("node.db"),
        ))
        .await
        .expect("first boot")
        .wait_until_ready()
        .await
        .expect("first boot ready");

        node.writer()
            .write(Bytes::from_static(b"seed"))
            .await
            .expect("write");

        let cutoff = tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                if let Some(c) = node.state_handle().snapshot().manifest_state.hlc_cutoff {
                    return c;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timed out waiting for snapshot cutoff to land in Raft");

        node.shutdown_token().cancel();
        node.run().await.expect("clean shutdown");
        cutoff
    };

    let node = ReplicaNode::start(test_config(dir.path().join("node.db")))
        .await
        .expect("restart")
        .wait_until_ready()
        .await
        .expect("restart ready");

    assert_eq!(
        node.state_handle().snapshot().manifest_state.hlc_cutoff,
        Some(committed_cutoff),
        "raft_state must carry the Raft-persisted hlc_cutoff after restart",
    );

    assert_eq!(
        node.replica()
            .fetch_status()
            .await
            .expect("no error")
            .effective_cutoff,
        Some(committed_cutoff),
        "HlcCutoff must be restored from Raft state on restart",
    );

    let stale = Data {
        data_id: DataId::from_parts(Hlc(committed_cutoff.raw().saturating_sub(1)), node_id, 0),
        blob: Bytes::from_static(b"stale"),
    };
    let err = crate::tests::common::replica_write(&*node.replica(), stale)
        .await
        .expect_err("write below cutoff must be rejected");
    assert!(
        matches!(err, Error::HlcCutoff(_)),
        "expected HlcCutoff, got {err:?}",
    );

    node.shutdown_token().cancel();
    node.run().await.expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_workers_cleanly() {
    let (_dir, node, _) = start_node().await;
    let started = tokio::time::Instant::now();
    node.shutdown_token().cancel();
    tokio::time::timeout(Duration::from_secs(20), node.run())
        .await
        .expect("shutdown timed out")
        .expect("clean shutdown");
    assert!(
        started.elapsed() < Duration::from_secs(25),
        "shutdown hung: {:?} elapsed",
        started.elapsed()
    );
}
