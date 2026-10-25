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

use crate::error::Error;
use crate::tests::{Cluster, ClusterConfig, assert_cluster_invariants, default_fast_config};

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

    let leader_id = cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(|n| n.node_id())
        .expect("no leader before restart");
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

    let new_leader = cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(|n| n.node_id())
        .expect("no leader after recovery");
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
    let leader_id = cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(|n| n.node_id())
        .expect("no leader before outage");
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
    let new_leader = cluster
        .nodes()
        .find(|n| n.state_handle().snapshot().leader.is_some())
        .map(|n| n.node_id())
        .expect("no leader after recovery");
    cluster
        .node(new_leader)
        .writer()
        .write(Bytes::from_static(b"after-outage"))
        .await
        .expect("post-recovery write");

    assert_cluster_invariants(&cluster);

    cluster.shutdown().await.expect("clean shutdown");
}

/// The RPC server must be bound and serving BEFORE raft startup blocks —
/// otherwise unbounded startup would be a mutual deadlock (every cold-starting
/// node waiting for peers that aren't listening). A non-bootstrap node whose one
/// configured peer is unreachable takes the `JoinExisting` route and blocks
/// forever in `join_existing`; its server must still accept connections while
/// `start()` is pending. (If `bind_server` were moved after `run_raft_startup`,
/// the connect below would be refused and this test would fail.)
#[tokio::test(flavor = "multi_thread")]
async fn rpc_server_accepts_connections_while_raft_startup_is_blocked() {
    let _dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

    let listen = crate::tests::reserve_port();
    let dead_peer = crate::tests::reserve_port(); // nothing will listen here

    // server_name ("node-a") != bootstrap_server_name ("node-0") ⇒ NOT the
    // bootstrap node; its only reachable peer is itself and the other
    // configured peer (`dead_peer`) never answers, so it joins forever. No
    // object-store fence ⇒ the JoinExisting route. `peers` must list every
    // initially available peer including this node itself (see
    // `validate_rejects_config_whose_peers_omit_self`), so `node-a` is
    // included alongside the unreachable `node-0`.
    let mut cfg = default_fast_config();
    cfg.network_server_name = "node-a".into();
    cfg.peers = vec![
        crate::PeerAddr {
            addr: dead_peer.into(),
            server_name: "node-0".into(),
        },
        crate::PeerAddr {
            addr: listen.into(),
            server_name: "node-a".into(),
        },
    ];
    cfg.network_bootstrap_server_name = "node-0".into();
    cfg.network_listen_addr = listen;
    cfg.db_path = _dir.path().join("node.db");
    cfg.object_store = store;

    // Spawn (do NOT await) — `wait_until_ready` blocks in the join wait and never
    // returns. `start` itself does return (that is the point of the split), so the
    // pending-ness asserted below is the readiness wait, not the bind.
    let handle = tokio::spawn(async move {
        crate::replica_node::ReplicaNode::start(cfg)
            .await?
            .wait_until_ready()
            .await
    });

    // The server must come up (accept a TCP connection) even though start() is
    // still blocked.
    let mut served = false;
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(listen).await.is_ok() {
            served = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        served,
        "RPC server must accept connections while raft startup is blocked",
    );
    // ...and start() must still be pending — the server is up DURING startup, not
    // only after it returned.
    assert!(
        !handle.is_finished(),
        "node must still be blocked in raft startup while its server is already serving",
    );

    handle.abort();
}

/// An operator must be able to stop a node that will never become ready.
///
/// The freshness gate retries forever by design (that is what makes an
/// unattended cold start work), so `shutdown_token` is the *only* way out for a
/// `JoinExisting` node whose peers never answer. Dropping the `wait_until_ready`
/// future is not an answer: that aborts the node's `JoinSet` where it stands.
///
/// The guard is that cancellation is observed **promptly** and reported as
/// `StartupCancelled` — distinguishable from a genuine startup fault — with the
/// node shut down gracefully on the way out.
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_shutdown_token_stops_a_node_that_never_becomes_ready() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

    let listen = crate::tests::reserve_port();
    let dead_peer = crate::tests::reserve_port(); // nothing will ever listen here

    // Same shape as `rpc_server_accepts_connections_while_raft_startup_is_blocked`:
    // not the bootstrap node, no fence, one unreachable peer ⇒ JoinExisting, which
    // blocks forever.
    let mut cfg = default_fast_config();
    cfg.network_server_name = "node-a".into();
    cfg.peers = vec![
        crate::PeerAddr {
            addr: dead_peer.into(),
            server_name: "node-0".into(),
        },
        crate::PeerAddr {
            addr: listen.into(),
            server_name: "node-a".into(),
        },
    ];
    cfg.network_bootstrap_server_name = "node-0".into();
    cfg.network_listen_addr = listen;
    cfg.db_path = dir.path().join("node.strata");
    cfg.object_store = store;

    let initialising = crate::replica_node::ReplicaNode::start(cfg)
        .await
        .expect("start binds without a quorum");

    // Capture the token BEFORE `wait_until_ready` consumes the node.
    let token = initialising.shutdown_token();
    let handle = tokio::spawn(initialising.wait_until_ready());

    // Let it get properly stuck in the join wait, and confirm it is stuck.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !handle.is_finished(),
        "the node must still be waiting — otherwise this test proves nothing",
    );

    token.cancel();

    let err = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("cancellation must stop the gate, not merely be recorded")
        .expect("the startup task must not panic")
        .expect_err("a cancelled startup cannot yield a ready node");
    assert!(
        matches!(err, Error::StartupCancelled),
        "an operator-requested stop must be distinguishable from a startup fault; got {err:?}",
    );
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
    let leader_id = cluster
        .nodes()
        .find_map(|n| n.state_handle().snapshot().leader)
        .expect("no leader");
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
    let leader_id = cluster
        .nodes()
        .find_map(|n| n.state_handle().snapshot().leader)
        .expect("no leader during partial outage");
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
