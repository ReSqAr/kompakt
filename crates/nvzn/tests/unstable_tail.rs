//! `Reader::subscribe_unstable_tail` against a real cluster: a trigger to
//! re-read, ended by the node's shutdown.
//!
//! ```text
//!   write ──> zslg local store ──> fan-out ──> nvzn: decode, project ──> Vec<Ev>
//!                                                 consumer ──> unstable_snapshot()
//!   node.shutdown_token().cancel() ──> stream ends ChannelEnd::Cancelled
//! ```

#![allow(unused_crate_dependencies, reason = "integration test")]

mod common;

use common::{Cluster, init_tracing};
use std::time::Duration;

const PORT: u16 = 19355;

/// Well past one integrate batch and a fan-out hop; neither waits on a timer.
const ANNOUNCE_BOUND: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
async fn a_write_is_announced_then_the_snapshot_shows_it_and_shutdown_cancels() {
    init_tracing();
    let cluster = Cluster::start(PORT).await;
    let server = cluster.server("server").await;

    let mut announced = server.reader.subscribe_unstable_tail();
    server.write(7, "user-7").await;

    let (rest, first) = tokio::time::timeout(
        ANNOUNCE_BOUND,
        tokio::task::spawn_blocking(move || {
            let first = announced.recv();
            (announced, first)
        }),
    )
    .await
    .expect("the write was not announced")
    .expect("recv panicked");
    let events = first.expect("stream ended").expect("decoded");
    assert_eq!(events.len(), 1, "one event in the write: {events:?}");
    assert!(
        server.sees(7),
        "the announced write is in the unstable snapshot"
    );

    cluster.shutdown();

    let end = tokio::time::timeout(
        ANNOUNCE_BOUND,
        tokio::task::spawn_blocking(move || {
            let mut rest = rest;
            loop {
                // A duplicate announcement may still be queued.
                if let Err(end) = rest.recv() {
                    return end;
                }
            }
        }),
    )
    .await
    .expect("the unstable subscription did not end after the node was cancelled")
    .expect("recv panicked");
    assert_eq!(end, zslg::ChannelEnd::Cancelled);
}
