//! `Live::wait_for` against a real cluster: a write is seen by the writing
//! process's live view, and by another process's, through the unstable tail.
//!
//! ```text
//!   server: write ──> replicas ──> its local store ──> tail ──> its Live   wait_for → Seen
//!   cli:                           its local store ──> tail ──> its Live   wait_for → Seen
//! ```

#![allow(unused_crate_dependencies, reason = "integration test")]

mod common;

use common::{Cluster, init_tracing};
use nvzn::Wait;
use std::time::Duration;

const PORT: u16 = 19355;

/// Well past one integrate batch and a fan-out hop; neither waits on a timer.
const WAIT_BOUND: Duration = Duration::from_secs(10);

/// Until the view has started (it waits for the fold to catch up).
async fn started(live: &nvzn::Live<common::event::Event>, d: zslg::DataId) -> Wait {
    loop {
        match live.wait_for(d, WAIT_BOUND).await {
            Wait::NotStarted => tokio::time::sleep(Duration::from_millis(20)).await,
            other => return other,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_is_seen_by_the_writer_and_by_another_process() {
    init_tracing();
    let cluster = Cluster::start(PORT).await;
    let server = cluster.server("server").await;
    let cli = cluster.follower("cli").await;
    server.sync("server").await;
    cli.sync("cli").await;

    let d = server.write(7, "user-7").await;
    assert_eq!(
        started(&server.live, d).await,
        Wait::Seen,
        "the writer's own view"
    );
    assert!(server.sees(7).await, "Seen means a snapshot now shows it");
    assert_eq!(
        started(&cli.live, d).await,
        Wait::Seen,
        "another process's view"
    );
    assert!(cli.sees(7).await);

    cli.stop().await;
    server.stop().await;
    cluster.shutdown();
}
