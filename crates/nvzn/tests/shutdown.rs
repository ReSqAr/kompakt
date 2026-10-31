//! Cancelling the zslg node stops everything reading from it.
//!
//! ```text
//!   node.shutdown_token().cancel()
//!     └─> zslg reader's streams end End::Aborted { reason: crossbeam_combinators::AbortReason::Cancelled }   (even on a quiet log)
//!           └─> nvzn fold stops as StopReason::Cancelled    (dispatcher task: Ok)
//!                 └─> Reader::subscribe_stable_log streams end End::Aborted { reason: crossbeam_combinators::AbortReason::Cancelled }
//! ```
//!
//! The quiet log is the point. A fold parked in `recv` with nothing arriving
//! used to stay parked after the node was cancelled: the reader the dispatcher
//! holds kept the stable log alive, so nothing ever woke the fold, and the
//! blocking thread outlived the node.

#![allow(unused_crate_dependencies, reason = "integration test")]

mod common;

use common::{Cluster, User, init_tracing};
use nvzn::{Error, StopReason};
use std::time::Duration;

const PORT: u16 = 19351;

/// Well past anything the cancel path waits on: it is a token, one task and a
/// channel disconnect, not a timer.
const STOP_BOUND: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_node_stops_an_idle_fold_and_its_subscribers() {
    init_tracing();
    let cluster = Cluster::start(PORT).await;
    let server = cluster.server("server").await;

    server.write(1, "user-1").await;
    server.sync("before going quiet").await;

    let mut users = server
        .reader
        .subscribe_stable_log::<User>()
        .expect("subscribe");
    // Nothing else is written: the fold and the subscriber are both parked
    // on a log that has gone quiet when the node is cancelled.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!server.fold.is_finished(), "the fold stopped on its own");

    cluster.shutdown();

    let folded = tokio::time::timeout(STOP_BOUND, server.fold)
        .await
        .expect("the fold did not stop after the node was cancelled")
        .expect("the dispatcher task panicked");
    assert!(
        folded.is_ok(),
        "a cancelled fold is a clean stop: {folded:?}"
    );

    assert!(
        matches!(
            server.reader.snapshot(),
            Err(Error::FoldStopped(StopReason::Cancelled))
        ),
        "readers are told the fold was cancelled",
    );

    let end = tokio::time::timeout(
        STOP_BOUND,
        tokio::task::spawn_blocking(move || {
            loop {
                // A ref folded before the cancel may still be in the buffer.
                if let Err(end) = users.recv() {
                    return end;
                }
            }
        }),
    )
    .await
    .expect("the subscribe_stable_log stream did not end after the fold was cancelled")
    .expect("recv panicked");
    assert_eq!(
        end,
        crossbeam_combinators::End::Aborted {
            reason: crossbeam_combinators::AbortReason::Cancelled
        }
    );
}
