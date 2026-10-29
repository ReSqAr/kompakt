//! A follower reading a cluster it does not belong to — the shape of a CLI.
//!
//! ```text
//!   server process:  ReplicaNode  + nvzn ──> writes ──> snapshots ──┐
//!                         ^                                  object store
//!   CLI process:     FollowerNode + nvzn <── replica sub ──────────┘
//! ```
//!
//! A follower starts with nothing: no local store, no cursor, and a stable log
//! it has to fill from the cluster's snapshots before it can answer anything.
//! That makes its first sync the interesting one, and makes a *second* CLI run
//! different from the first — by then the cluster has a manifest watermark to
//! reach and a frontier to catch up to, where the first run had neither and
//! the quorum check simply early-outs.

#![allow(unused_crate_dependencies, reason = "integration test")]

mod common;

use common::{Cluster, init_tracing};
use std::time::Duration;

const PORT: u16 = 19347;

/// A follower joining a cluster whose data has already settled into the
/// stable prefix catches up through a snapshot load rather than the live
/// tail, and its first sync is bounded even so.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_syncs_against_data_it_never_saw_written() {
    init_tracing();
    let cluster = Cluster::start(PORT).await;

    let server = cluster.server("server").await;
    for id in 1..4u128 {
        server.write(id, &format!("user-{id}")).await;
    }
    // Past the cutoff lag and a snapshot interval, so the writes are in the
    // cluster's stable prefix before the follower exists.
    tokio::time::sleep(Duration::from_secs(12)).await;
    server.sync("server, after writing").await;
    assert!(
        server.sees_folded(1),
        "the cluster's data has not settled into the stable log yet",
    );

    let cli = cluster.follower("cli").await;
    cli.sync("a fresh follower's first sync").await;
    assert!(
        cli.sees(1).await,
        "sync returned but the cluster's existing data is not visible",
    );

    cli.write(9, "written-by-the-follower").await;
    cli.sync("after the follower's own write").await;
    assert!(cli.sees(9).await, "the follower cannot see its own write");

    assert!(
        !cli.fold.is_finished(),
        "the fold stopped while the follower was still serving",
    );

    cli.stop().await;
    server.stop().await;
    cluster.shutdown();
}

/// Two CLI runs against one cluster, sharing nothing but the cluster and the
/// object store: the second has to observe what the first wrote.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_invocation_syncs_against_what_the_first_wrote() {
    init_tracing();
    let cluster = Cluster::start(PORT + 1).await;

    let first = cluster.follower("cli-1").await;
    first.sync("first invocation").await;
    first.write(1, "holden").await;
    first.stop().await;

    // The gap between runs: long enough for the write to be snapshotted.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let second = cluster.follower("cli-2").await;
    second.sync("second invocation").await;
    assert!(
        second.sees(1).await,
        "the second invocation synced but cannot see what the first wrote",
    );

    second.stop().await;
    cluster.shutdown();
}
