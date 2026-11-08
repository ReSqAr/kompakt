//! Full-cluster outage recovery.
//!
//! Shut down EVERY node, then bring the ORIGINAL nodes back (resuming their
//! persisted Raft state from their preserved db-paths). The cluster must
//! re-elect a leader and serve writes again automatically — no operator action.
//!
//! This is the regression guard for the cold-start crash-loop: startup phases
//! used to run under a hard 10s deadline that, on timeout, made a resuming node
//! exit → the orchestrator restarted it → the restarts desynchronised so a
//! quorum was never simultaneously up → permanent crash-loop. Startup is now
//! intentionally unbounded (it escalates log volume but never aborts), so a
//! node waits for the quorum to reassemble however long that takes.

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
use object_store::memory::InMemory;

use crate::{Cluster, ClusterConfig, assert_cluster_invariants, default_fast_config};

/// Off and on again, ALL AT ONCE: a full outage where every node restarts
/// simultaneously must recover a leader + writable cluster on its own.
#[tokio::test(flavor = "multi_thread")]
async fn full_cluster_restart_simultaneous_recovers() {
    let mut cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        ..Default::default()
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(20)).await;

    let leader_id = cluster.leader().expect("no leader before restart");
    cluster
        .node(leader_id)
        .writer()
        .write(Bytes::from_static(b"before-restart"))
        .await
        .expect("pre-restart write");

    // Stop every node, then bring them ALL back at once (concurrent).
    cluster
        .restart_all()
        .await
        .expect("cluster must recover from a simultaneous full restart");

    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(20)).await;

    let new_leader = cluster.leader().expect("no leader after recovery");
    cluster
        .node(new_leader)
        .writer()
        .write(Bytes::from_static(b"after-restart"))
        .await
        .expect("post-recovery write");

    assert_cluster_invariants(&cluster);

    cluster.shutdown().await.expect("clean shutdown");
}

/// Off and on again, STAGGERED: a full outage where the nodes come back at
/// different times (a quorum unreachable for ~2s for the first node) must still
/// recover on its own. With a tiny startup escalation interval this is also the
/// regression guard for the cold-start crash-loop (the old bounded startup would
/// abort the first node before the quorum reassembled).
#[tokio::test(flavor = "multi_thread")]
async fn full_cluster_restart_staggered_recovers() {
    // A deliberately tiny startup escalation interval: under the OLD *bounded*
    // behaviour the first resuming node would abort at 300ms — long before the
    // staggered quorum returns (~2s later) — and its `start()` would error. With
    // unbounded startup it just logs louder and waits, then recovers.
    let mut base = default_fast_config();
    base.raft_startup_phase_timeout = Duration::from_millis(300);

    let mut cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(base),
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(20)).await;

    // Advance the log so every node has real persisted state to resume.
    let leader_id = cluster.leader().expect("no leader before outage");
    cluster
        .node(leader_id)
        .writer()
        .write(Bytes::from_static(b"before-outage"))
        .await
        .expect("pre-outage write");

    // FULL OUTAGE + STAGGERED RESTART: stop every node, then bring them all back
    // at their original addresses AND db-paths. The first node comes up alone
    // and cannot reach a quorum for ~2s (>> the 300ms escalation interval); it
    // must WAIT, not crash-loop. Under the old bounded startup this call fails.
    cluster
        .restart_all_staggered(Duration::from_secs(2))
        .await
        .expect("cluster must recover from a full outage without crash-looping");

    // Automatic recovery: a leader is re-elected among the original nodes and
    // the replica set re-converges — no operator action.
    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(20)).await;

    // The recovered cluster is functional: a fresh quorum write succeeds.
    let new_leader = cluster.leader().expect("no leader after recovery");
    cluster
        .node(new_leader)
        .writer()
        .write(Bytes::from_static(b"after-outage"))
        .await
        .expect("post-recovery write");

    assert_cluster_invariants(&cluster);

    cluster.shutdown().await.expect("clean shutdown");
}

/// A PARTIAL outage — a minority down while a quorum + leader survive — must not
/// make the leader shrink membership or evict the down node. With no spare
/// available, `compute_target` refuses to change the replica set (it would drop
/// below `replica_node_count`), and raft-voter eviction is config-only, so the
/// down node stays a member and rejoins on its own when it returns.
#[tokio::test(flavor = "multi_thread")]
async fn partial_outage_keeps_membership_and_rejoins() {
    let mut cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        ..Default::default()
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(20)).await;
    cluster.wait_for_convergence(Duration::from_secs(20)).await;

    let all_ids = cluster.node_ids();
    assert_eq!(all_ids.len(), 3, "3-node cluster");

    // Advance the log.
    let leader_id = cluster.leader().expect("no leader");
    cluster
        .node(leader_id)
        .writer()
        .write(Bytes::from_static(b"before-partial-outage"))
        .await
        .expect("pre-outage write");

    // Kill a FOLLOWER (leader + a quorum survive). It stays in config.peers, so
    // it is merely "down", not scaled-down.
    let follower_id = *all_ids
        .iter()
        .find(|&&id| id != leader_id)
        .expect("a follower exists");
    let placement = cluster.remove(follower_id).await.expect("remove follower");

    // Give the leader several liveness + membership ticks to (wrongly) react.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // The replica set must NOT have shrunk: with no spare, compute_target
    // returns None, so the down node stays a member (never evicted).
    let survivor = *all_ids
        .iter()
        .find(|&&id| id != follower_id)
        .expect("a survivor exists");
    let replica_set = cluster
        .node(survivor)
        .state_handle()
        .snapshot()
        .replica
        .current()
        .clone();
    assert_eq!(
        replica_set, all_ids,
        "leader must NOT shrink/evict the down node from the replica set during an outage",
    );

    // The surviving quorum is still writable.
    let leader_id = cluster.leader().expect("no leader during partial outage");
    cluster
        .node(leader_id)
        .writer()
        .write(Bytes::from_static(b"during-partial-outage"))
        .await
        .expect("write during partial outage");

    // The down node returns (resuming its persisted state) and rejoins on its
    // own — no operator action.
    cluster
        .relaunch(placement)
        .await
        .expect("relaunch follower");
    cluster.wait_for_convergence(Duration::from_secs(20)).await;
    assert_eq!(
        cluster.node_ids(),
        all_ids,
        "the returned node must be back in the cluster",
    );

    assert_cluster_invariants(&cluster);

    cluster.shutdown().await.expect("clean shutdown");
}
