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
/// End-to-end coverage for the leader's `config.peers` admission gate
/// (docs/LIVENESS_REVIEW.md §2). A real `AddMember` arriving by address for a
/// `server_name` that is not in `config.peers` must come back as the
/// `MembershipDenied` error frame — this is the signal the continuous re-join
/// monitor logs (throttled) and survives rather than treats as fatal
/// (`raft::startup::rejoin`; group removal is the group's decision, ending the
/// process is the orchestrator's).
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::api::{AddMemberOutcome, Router};
use crate::hlc::HlcClock;
use crate::network::RemoteRouter;
use crate::replica_node::ReplicaNodeConfig;
use crate::tests::reserve_port;
use crate::tests::wait_until_sync as wait_until;
use crate::{Error, NodeId, PeerAddr, ReplicaNode};

const SERVER_NAME: &str = "localhost";

fn single_node_config(addr: SocketAddr, db_path: PathBuf) -> ReplicaNodeConfig {
    // config.peers carries only "localhost"; "intruder" is NOT in it.
    let peers = vec![PeerAddr {
        addr: addr.into(),
        server_name: SERVER_NAME.into(),
    }];
    let mut cfg = ReplicaNodeConfig::new(
        SERVER_NAME.into(),
        peers,
        SERVER_NAME.into(),
        addr,
        db_path,
        Arc::new(object_store::memory::InMemory::new()),
    )
    .expect("single-node config builds");
    cfg.raft_heartbeat_interval = Duration::from_millis(100);
    cfg.raft_election_timeout_min = Duration::from_millis(500);
    cfg.raft_election_timeout_max = Duration::from_millis(1500);
    cfg.raft_startup_phase_timeout = Duration::from_secs(30);
    cfg.snapshot_interval = Duration::from_secs(3600);
    cfg.snapshot_cutoff_timeout = Duration::from_millis(10);
    cfg.snapshot_compaction_tiers = vec![
        Duration::from_secs(3600).into(),
        Duration::from_secs(86_400).into(),
    ];
    cfg.membership_replica_node_count = 1;
    cfg.drain_timeout = Duration::from_secs(30);
    cfg
}

async fn add_member_by_address(
    remote: &RemoteRouter,
    addr: SocketAddr,
    join_node_id: NodeId,
    join_address: String,
    join_server_name: &str,
) -> Result<AddMemberOutcome, crate::Error> {
    // The connection dials by address; the *request body* carries the join
    // server_name the gate inspects.
    let join_addr: SocketAddr = join_address.parse().expect("valid socket addr");
    remote
        .remote_direct(PeerAddr {
            addr: addr.into(),
            server_name: SERVER_NAME.into(),
        })
        .add_member(
            join_node_id,
            PeerAddr {
                addr: join_addr.into(),
                server_name: join_server_name.into(),
            },
        )
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn add_member_denied_for_server_name_not_in_config_peers() {
    let addr = reserve_port();
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = single_node_config(addr, tempdir.path().join("node.db"));

    let node = ReplicaNode::start(config.clone())
        .await
        .expect("bootstrap node start")
        .wait_until_ready()
        .await
        .expect("bootstrap node ready");
    let node_id = node.node_id();
    wait_until("node elects itself leader", Duration::from_secs(10), || {
        node.state_handle().snapshot().leader == Some(node_id)
    })
    .await;

    let clock = Arc::new(HlcClock::new(config.hlc_max_clock_skew));
    let remote = RemoteRouter::for_test_from_client(config.router_config(), clock);

    // Not in config.peers ⇒ denied (a random joiner with server_name "intruder").
    match add_member_by_address(
        &remote,
        node.server_addr(),
        NodeId::new_v4(),
        "127.0.0.1:9".to_owned(),
        "intruder",
    )
    .await
    {
        Err(Error::MembershipDenied { .. }) => {}
        other => panic!("expected Denied error, got {other:?}"),
    }

    // In config.peers ("localhost") ⇒ NOT denied. Use the node's own id+address
    // so add_member takes the idempotent fast-path and returns promptly.
    match add_member_by_address(
        &remote,
        node.server_addr(),
        node_id,
        node.server_addr().to_string(),
        SERVER_NAME,
    )
    .await
    {
        Ok(AddMemberOutcome::Accepted { .. }) => {}
        other => panic!("a configured server_name must be accepted; got {other:?}"),
    }

    // `fetch_status` is also reachable over the anonymous by-address path.
    remote
        .remote_direct(PeerAddr {
            addr: node.server_addr().into(),
            server_name: SERVER_NAME.into(),
        })
        .fetch_status()
        .await
        .expect("fetch_status over direct transport");

    node.shutdown_token().cancel();
    node.run().await.expect("clean shutdown");
}
