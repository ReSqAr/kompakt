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

use futures::StreamExt;

use crate::api::Replica;
use crate::hlc::Hlc;
use crate::ids::{HostPort, PeerAddr, RaftGeneration};
use crate::tests::nid;
use crate::tests::{self as faults, ReplicaServiceHarness};

/// `subscribe_status` must yield the current baseline immediately and then push
/// a fresh status on every subsequent `RaftState` change. A long-lived
/// connection that stopped re-observing state would leave the streaming
/// `RaftTracker` silently stale rather than visibly broken.
#[tokio::test]
async fn subscribe_status_streams_baseline_then_updates() {
    let me = nid(1);
    let (clock, _ctl) = faults::skewed_clock_pair(Duration::from_millis(100));
    let harness = ReplicaServiceHarness::new(me, &[me], clock.hlc_clock())
        .await
        .expect("harness");

    let mut stream = harness
        .replica
        .subscribe_status()
        .await
        .expect("subscribe_status");

    // 1. The baseline arrives without any state change having occurred.
    let baseline = stream
        .next()
        .await
        .expect("baseline item")
        .expect("baseline ok");
    assert_eq!(baseline.node_id, me);
    let baseline_gen = baseline.raft_state.generation;

    // 2. Drive a RaftState change with a higher generation.
    let mut next = (*harness.raft_state.snapshot()).clone();
    next.generation = RaftGeneration(baseline_gen.0 + 1);
    harness.raft_state.replace(next.clone());

    // 3. The change is pushed down the same stream — no reconnect required.
    let updated = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("update not timed out")
        .expect("update item")
        .expect("update ok");
    assert_eq!(updated.node_id, me);
    assert_eq!(updated.raft_state.generation, next.generation);
}

/// `subscribe_status` must also push a fresh status when the effective HLC
/// cutoff advances — even with no `RaftState` change (the `status-full` path).
#[tokio::test]
async fn subscribe_status_streams_cutoff_advances() {
    let me = nid(1);
    let (clock, _ctl) = faults::skewed_clock_pair(Duration::from_millis(100));
    let harness = ReplicaServiceHarness::new(me, &[me], clock.hlc_clock())
        .await
        .expect("harness");

    let mut stream = harness
        .replica
        .subscribe_status()
        .await
        .expect("subscribe_status");

    // Baseline: no cutoff has been published yet.
    let baseline = stream
        .next()
        .await
        .expect("baseline item")
        .expect("baseline ok");
    assert_eq!(baseline.effective_cutoff, None);

    // Advance the effective cutoff via the watch publisher (no RaftState change).
    harness
        .cutoff_tx
        .send(Some(Hlc(4242)))
        .expect("publish cutoff");

    // A fresh status reflecting the new cutoff is pushed down the same stream.
    let updated = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("update not timed out")
        .expect("update item")
        .expect("update ok");
    assert_eq!(updated.effective_cutoff, Some(Hlc(4242)));
}

/// `fetch_status` must actually populate `peer_names` from the node's
/// configured peer watcher — not just carry the field on the wire.
///
/// A hand-built `ReplicaStatus` round-tripped through postcard would pass even
/// if `status_snapshot` never read the watcher at all; this drives the real
/// `spawn_replica`-shaped construction path (`ReplicaService::new` with a
/// concrete `PeersWatcher`) and observes the value through `fetch_status`,
/// the same trait method a real RPC handler calls.
#[tokio::test]
async fn fetch_status_reports_configured_peer_names() {
    let me = nid(1);
    let (clock, _ctl) = faults::skewed_clock_pair(Duration::from_millis(100));
    let configured_peers = vec![
        PeerAddr {
            addr: HostPort::new("node-a.internal", 7000),
            server_name: "node-a".into(),
        },
        PeerAddr {
            addr: HostPort::new("node-b.internal", 7000),
            server_name: "node-b".into(),
        },
    ];
    let harness =
        ReplicaServiceHarness::new_with_peers(me, &[me], clock.hlc_clock(), configured_peers)
            .await
            .expect("harness");

    let status = harness.replica.fetch_status().await.expect("fetch_status");

    assert_eq!(
        status.peer_names,
        vec!["node-a".into(), "node-b".into()],
        "peer_names must reflect the node's own configured peers, not arrive empty"
    );
}
