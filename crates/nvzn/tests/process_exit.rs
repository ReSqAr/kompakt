//! Dropping a runtime that still holds a node returns, without anyone having
//! cancelled the node.
//!
//! This is what a CLI does when it returns early — the work is done, the
//! `cancel()` on the way out is never reached, and the runtime is dropped with
//! the node's tasks still live. `Runtime::drop` waits for blocking tasks that
//! have already started, and one of the writer's used to never finish:
//!
//! ```text
//!   crossbeam_combinators::Channel::recv                     channel/mod.rs
//!   zslg_writer::coordinator::run_replica             zslg-writer/src/coordinator.rs
//!   tokio::runtime::blocking::pool::Task::run
//! ```
//!
//! `run_replica` is a `spawn_blocking` loop parked on the replica's response
//! stream. It only re-checks whether the coordinator is still alive *after* a
//! response arrives, so an idle peer keeps it parked for good — and the drop
//! that would end it cannot run, because shutdown is waiting on this very
//! task. Cancelling the node first is what avoids it today.
//!
//! Two things used to keep that drop from returning, and both are fixed at
//! the source rather than by cancelling first:
//!
//! - the writer's replica loops sat in a `recv` on an idle peer's response
//!   stream, so they are bound to a cancellation that fires when the
//!   coordinator drops;
//! - a subscription's blocking socket reader is only woken by a `shutdown`, so
//!   the task that owns the socket shuts it down when it is dropped.
//!
//! The drop runs on its own thread and the wait is bounded, so a regression
//! fails instead of hanging the suite.

#![allow(unused_crate_dependencies, reason = "integration test")]

mod common;

use common::{Cluster, init_tracing};

const PORT: u16 = 19349;

/// Far above a clean drop (milliseconds), far below a suite timeout.
const DROP_BOUND: std::time::Duration = std::time::Duration::from_secs(20);

#[test]
fn dropping_a_runtime_without_cancelling_its_nodes_terminates() {
    init_tracing();

    // The cluster is on its own runtime, as a separate process would be, so
    // its sockets stay open while the CLI's runtime goes away.
    let cluster_runtime = tokio::runtime::Runtime::new().expect("cluster runtime");
    let cluster = cluster_runtime.block_on(Cluster::start(PORT));

    let cli_runtime = tokio::runtime::Runtime::new().expect("cli runtime");
    cli_runtime.block_on(async {
        let cli = cluster.follower("cli").await;
        cli.sync("cli").await;
        cli.write(1, "yasin").await;
        // Everything is dropped here, with nothing cancelled.
    });

    // The assertion is that this returns at all, inside the bound.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        drop(cli_runtime);
        let _ = done_tx.send(());
    });
    assert!(
        done_rx.recv_timeout(DROP_BOUND).is_ok(),
        "dropping the runtime did not return within {DROP_BOUND:?}: a node's writer loop is still parked",
    );

    cluster.shutdown();
    // The cluster's own writer holds replica loops too: let it go before the
    // runtime that would otherwise wait on them.
    drop(cluster);
    drop(cluster_runtime);
}
