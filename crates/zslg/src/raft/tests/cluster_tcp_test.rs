//! A three-node raft cluster over real TCP sockets (`RpcServerConfig`),
//! exercising the openraft RPC layer end to end: leader election and a fenced
//! state write observed by every node.
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
use crate::meta::ReplicaMembership;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::Config;
use openraft::storage::Adaptor;

use crate::api::{ReplicatedRaftState, Router};
use crate::hlc::HlcClock;
use crate::ids::{NodeId, PeerAddr};
use crate::network::{LocalReplicaClient, RemoteRouter, RpcServerConfig, SocketOpts};
use crate::raft::rpc::NetworkFactory;
use crate::raft::{Raft, RaftNode, RaftStateHandle, membership_map};
use crate::shutdown::Shutdown;
use crate::tests::ReadyReplica;
use crate::tests::fast_raft_config as raft_config;

struct ClusterNode {
    node_id: NodeId,
    raft: Raft,
    raft_state: RaftStateHandle,
    shutdown: Shutdown,
    peer_addr: PeerAddr,
    net: Arc<dyn Router>,
    _tempdir: tempfile::TempDir,
}

async fn build_node(node_id: NodeId, raft_config: Arc<Config>) -> ClusterNode {
    let clock = Arc::new(HlcClock::new(Duration::from_secs(60)));
    let net: Arc<dyn Router> = Arc::new(RemoteRouter::for_test(
        clock.clone(),
        Duration::from_secs(10),
    ));
    let factory = NetworkFactory::new(net.clone());

    let tempdir = tempfile::tempdir().expect("tempdir");
    let raft_state = RaftStateHandle::new();
    let storage = crate::raft::raft_storage::RocksRaftStorage::open(
        raft_state.clone(),
        crate::raft::raft_storage::open_test_storage(tempdir.path()).expect("open storage"),
    )
    .await
    .expect("open raft storage");
    let (log_store, state_handle): (Adaptor<_, _>, Adaptor<_, _>) = storage.into_adaptor();
    let raft = Raft::new(node_id, raft_config, factory, log_store, state_handle)
        .await
        .expect("Raft::new");

    // The raft-only cluster never sends replica RPCs, so a stub `Replica`
    // (`ReadyReplica`, whose data methods are `unimplemented!()`) is sufficient
    // padding for the production `bind_server` cut point.
    let mut shutdown = Shutdown::new();
    let self_client: Arc<dyn crate::api::ReplicaClient> = Arc::new(LocalReplicaClient::new(
        Arc::new(ReadyReplica {
            raft_state: raft_state.clone(),
        }),
        Arc::new(RaftNode::new(
            raft.clone(),
            crate::network::permissive_gate(),
            std::time::Duration::from_secs(30),
        )),
        Arc::new(raft_state.clone()),
    ));
    let server_addr = crate::network::bind_server(
        self_client,
        clock.clone(),
        RpcServerConfig {
            listen_addr: "127.0.0.1:0".parse().expect("addr"),
            socket_opts: SocketOpts::for_test(),
            node_id,
        },
        &mut shutdown,
    )
    .expect("server bind");

    ClusterNode {
        node_id,
        raft,
        raft_state,
        shutdown,
        peer_addr: PeerAddr {
            addr: server_addr.into(),
            server_name: "localhost".into(),
        },
        net,
        _tempdir: tempdir,
    }
}

async fn shutdown_node(n: ClusterNode) {
    n.shutdown.token().cancel();
    let _ = n.shutdown.run(Duration::from_secs(1)).await;
    let _ = n.raft.shutdown().await;
}

#[tokio::test]
async fn three_node_tcp_cluster_commits_a_command() {
    let n1 = NodeId::new_v4();
    let n2 = NodeId::new_v4();
    let n3 = NodeId::new_v4();
    let ids = [n1, n2, n3];

    let mut nodes: BTreeMap<NodeId, ClusterNode> = BTreeMap::new();
    for id in ids {
        let cn = build_node(id, raft_config()).await;
        nodes.insert(id, cn);
    }

    for node in nodes.values() {
        for other in nodes.values() {
            if other.node_id != node.node_id {
                node.net
                    .register_peer(other.node_id, other.peer_addr.clone());
            }
        }
    }

    nodes
        .get(&n1)
        .expect("n1")
        .raft
        .initialize(membership_map(
            nodes.values().map(|n| (n.node_id, n.peer_addr.clone())),
        ))
        .await
        .expect("initialize");

    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(10))
        .expect("Instant + 10s does not overflow on supported platforms");
    let leader = loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("no leader elected within deadline");
        }
        let mut leader: Option<NodeId> = None;
        for node in nodes.values() {
            if let Some(id) = node.raft.current_leader().await {
                leader = Some(id);
                break;
            }
        }
        if let Some(id) = leader {
            break id;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let target_set: BTreeSet<NodeId> = ids.iter().copied().collect();
    {
        let leader_node = nodes.get(&leader).expect("leader");
        let expected_generation = leader_node.raft_state.snapshot().generation;
        leader_node
            .raft
            .client_write(crate::raft::FencedStateWrite {
                expected_generation,
                state: ReplicatedRaftState {
                    replica: ReplicaMembership::single(target_set.clone()),
                    manifest: Vec::new(),
                    manifest_watermark: None,
                    hlc_cutoff: None,
                },
            })
            .await
            .expect("client_write");
    }

    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(10))
        .expect("Instant + 10s does not overflow on supported platforms");
    for id in &ids {
        loop {
            let snap = nodes.get(id).expect("node").raft_state.snapshot();
            if *snap.replica.current() == target_set {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("node {id:?} did not observe SetReplicaSet in time");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    for (_, node) in nodes {
        shutdown_node(node).await;
    }
}
