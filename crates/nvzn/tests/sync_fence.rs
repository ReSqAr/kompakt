//! What `Reader::sync` promises a caller, against a real cluster.
//!
//! It is the fence a live view's read stands behind, and it has two
//! obligations that pull in opposite directions:
//!
//! ```text
//!   nothing to integrate  ──> return now        (never wait on a cadence)
//!   a write to observe    ──> return after it   (never return early)
//! ```
//!
//! Both are bounded by [`SYNC_BOUND`], which sits below every cadence in the
//! system, so a sync that starts waiting on one fails here rather than
//! surfacing as latency in an application.

#![allow(unused_crate_dependencies, reason = "integration test")]

mod common;

use common::{Cluster, SYNC_BOUND, init_tracing};
use std::time::Duration;

const PORT: u16 = 19345;

#[tokio::test(flavor = "multi_thread")]
async fn sync_returns_promptly_with_nothing_to_integrate() {
    init_tracing();
    let cluster = Cluster::start(PORT).await;
    let app = cluster.server("server").await;

    for round in 0..3 {
        app.sync(&format!("quiet round {round}")).await;
    }

    // Past one cutoff interval, so the cluster's stable watermark has advanced
    // on its own with no data behind it. A sync that waits for the *next*
    // advance rather than recognising the current one fails here.
    tokio::time::sleep(Duration::from_secs(6)).await;
    app.sync("after a watermark-only advance").await;

    app.stop().await;
    cluster.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_fences_a_write_for_the_live_view() {
    init_tracing();
    let cluster = Cluster::start(PORT + 1).await;
    let app = cluster.server("server").await;

    for id in 1..4u128 {
        app.write(id, &format!("user-{id}")).await;
        app.sync(&format!("after writing user-{id}")).await;
        assert!(
            app.sees(id).await,
            "user-{id}: sync returned but the write it fenced is not visible",
        );
    }

    // With a non-empty log the quorum check has a frontier to reach and a
    // watermark to wait on, so none of its empty-cluster early-outs apply.
    app.sync("quiet, with a non-empty log").await;
    assert!(
        SYNC_BOUND < Duration::from_secs(10),
        "the bound has to stay below the quorum-read timeout to mean anything",
    );

    app.stop().await;
    cluster.shutdown();
}
