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

use crate::ids::PeerAddr;
use crate::replica_node::ReplicaNodeConfig;
use crate::tests::{Cluster, ClusterConfig, assert_cluster_invariants};

#[tokio::test(flavor = "multi_thread")]
async fn three_node_cluster_brought_up_via_public_api() {
    crate::tests::init_logging();
    let cluster = Cluster::new(ClusterConfig {
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
        .node_ids()
        .into_iter()
        .next()
        .expect("at least one node");
    tokio::time::timeout(
        Duration::from_secs(10),
        cluster
            .node(leader_id)
            .writer()
            .write(Bytes::from_static(b"multi-node-write")),
    )
    .await
    .expect("write timeout")
    .expect("write");

    assert_cluster_invariants(&cluster);

    cluster.shutdown().await.expect("clean shutdown");
}

// ── whole-cluster cold start ──────────────────────────────────────────

/// Build `count` node configs that all know each other from the start.
///
/// Modelled on `Cluster::build_config`, with two deliberate differences:
///
/// * every port is reserved up front, so each config can carry the *full* peer
///   list — a cold start has no running node to learn addresses from;
/// * the bootstrap node is the **last** one. That is what makes this a cold
///   start rather than a staggered join: nodes 0 and 1 have no fence and are
///   not the bootstrap node, so they sit in `join_existing` until node 2 comes
///   up and initialises the cluster.
///
/// Returns the `TempDir` alongside the configs because it owns the db-paths;
/// dropping it would delete every node's storage out from under it.
fn bringup_configs(
    base: &ReplicaNodeConfig,
    store: &Arc<dyn object_store::ObjectStore>,
    count: usize,
) -> (tempfile::TempDir, Vec<ReplicaNodeConfig>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let names: Vec<crate::ids::ServerName> =
        (0..count).map(|i| format!("node-{i}").into()).collect();
    let addrs: Vec<std::net::SocketAddr> =
        (0..count).map(|_| crate::tests::reserve_port()).collect();
    let peers: Vec<PeerAddr> = names
        .iter()
        .zip(&addrs)
        .map(|(server_name, addr)| PeerAddr {
            addr: (*addr).into(),
            server_name: server_name.clone(),
        })
        .collect();
    let bootstrap = names.last().expect("at least one node").clone();
    // Load-bearing, and the whole reason this test can fail. `bootstrap_decision`
    // sends the bootstrap node down the `Bootstrap` route, which has no quorum
    // gate at all and returns immediately — so if node 0 were the bootstrap node
    // the test would pass whether or not `start` blocks, proving nothing.
    assert_ne!(
        Some(&bootstrap),
        names.first(),
        "the bootstrap node must NOT be started first, or the first node takes the \
         un-gated Bootstrap route and this test loses its teeth",
    );

    let configs = names
        .iter()
        .zip(&addrs)
        .enumerate()
        .map(|(i, (server_name, addr))| {
            let mut cfg = base.clone();
            cfg.network_server_name = server_name.clone();
            cfg.peers = peers.clone();
            cfg.network_bootstrap_server_name = bootstrap.clone();
            cfg.network_listen_addr = *addr;
            cfg.db_path = dir.path().join(format!("node-{i}.strata"));
            cfg.object_store = store.clone();
            cfg
        })
        .collect();
    (dir, configs)
}

/// Every node is started before *any* of them is awaited for readiness — the
/// shape a whole-cluster cold start needs and which today's API cannot express.
///
/// No node can be fresh until a quorum is up simultaneously, so awaiting
/// readiness node-by-node can never complete for the first node. This is not a
/// deadlock in the cluster (each node serves its peers as soon as it is bound);
/// it is the API forcing a caller to wait at the wrong moment.
#[tokio::test(flavor = "multi_thread")]
async fn all_nodes_start_before_any_is_awaited_for_readiness() {
    crate::tests::init_logging();
    let base = crate::tests::default_fast_config();
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());

    let (_dir, configs) = bringup_configs(&base, &store, 3);

    // Phase 1: start all three. Each returns as soon as it is serving.
    //
    // Bounded, because this loop is where the defect lives: if `start` ever goes
    // back to blocking on the freshness gate, node 0 waits for a quorum that
    // cannot form until nodes 1 and 2 are started, and an unbounded loop would
    // hang to the harness deadline instead of failing with a reason.
    let initialising = tokio::time::timeout(Duration::from_secs(30), async {
        let mut nodes = Vec::new();
        for cfg in configs {
            nodes.push(
                crate::replica_node::ReplicaNode::start(cfg)
                    .await
                    .expect("start must not fail"),
            );
        }
        nodes
    })
    .await
    .expect("start must not wait for a quorum");

    // Phase 2: only now await readiness, concurrently.
    let ready = tokio::time::timeout(
        Duration::from_secs(60),
        futures::future::try_join_all(
            initialising
                .into_iter()
                .map(crate::replica_node::InitialisingReplicaNode::wait_until_ready),
        ),
    )
    .await
    .expect("readiness must not hang")
    .expect("all nodes become ready once a quorum is up");

    for node in ready {
        node.shutdown_token().cancel();
        node.run().await.expect("clean shutdown");
    }
}
