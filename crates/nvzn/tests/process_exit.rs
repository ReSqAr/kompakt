//! Dropping a runtime that still holds a node never returns.
//!
//! This is what a CLI does when it returns early — the work is done, the
//! `cancel()` on the way out is never reached, and the runtime is dropped with
//! the node's tasks still live. `Runtime::drop` waits for blocking tasks that
//! have already started, and one of the writer's never finishes:
//!
//! ```text
//!   zslg::channel::Channels::recv                     channel.rs
//!   zslg::quorum::coordinator::run_replica            coordinator.rs
//!   tokio::runtime::blocking::pool::Task::run
//! ```
//!
//! `run_replica` is a `spawn_blocking` loop parked on the replica's response
//! stream. It only re-checks whether the coordinator is still alive *after* a
//! response arrives, so an idle peer keeps it parked for good — and the drop
//! that would end it cannot run, because shutdown is waiting on this very
//! task. Cancelling the node first is what avoids it today.
//!
//! Ignored because it hangs rather than fails: there is nothing to assert
//! against until the wait itself is bounded (a `recv` with a deadline, or a
//! liveness guard like the one [`Channels::into_stream`] already races
//! against). Run it with `--ignored` under a timeout to check a fix.

#![allow(unused_crate_dependencies, reason = "integration test")]

mod common;

use common::{Cluster, init_tracing};

const PORT: u16 = 19349;

#[test]
#[ignore = "reproduces a shutdown wedge: hangs instead of failing"]
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

    // The assertion is that this returns at all.
    drop(cli_runtime);

    cluster.shutdown();
    drop(cluster_runtime);
}
