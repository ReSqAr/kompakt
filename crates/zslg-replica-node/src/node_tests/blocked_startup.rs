#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
//! A node that can never finish starting: not the bootstrap node, no fence in
//! the object store, and its one other peer never answers, so it takes the
//! `JoinExisting` route and waits for a quorum that does not exist.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use testing_kit::reserve_port;
use zslg_error::Error;
use zslg_lifecycle_types::LifecycleError;
use zslg_net::PeerAddr;

use crate::{ReplicaNode, ReplicaNodeConfig};

/// `server_name` ("node-a") differs from the bootstrap server name ("node-0"), so
/// this is not the bootstrap node; `dead_peer` ("node-0") never answers. `peers`
/// must list every initially available peer including this node itself, so
/// "node-a" is included alongside the unreachable "node-0".
fn config_with_an_unreachable_peer(
    listen: SocketAddr,
    dead_peer: SocketAddr,
    db_path: std::path::PathBuf,
) -> ReplicaNodeConfig {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let peers = vec![
        PeerAddr {
            addr: dead_peer.into(),
            server_name: "node-0".into(),
        },
        PeerAddr {
            addr: listen.into(),
            server_name: "node-a".into(),
        },
    ];
    let mut cfg = ReplicaNodeConfig::new(
        "node-a".into(),
        peers,
        "node-0".into(),
        listen,
        db_path,
        store,
    )
    .expect("config builds");
    cfg.raft_heartbeat_interval = Duration::from_millis(250);
    cfg.raft_election_timeout_min = Duration::from_millis(1000);
    cfg.raft_election_timeout_max = Duration::from_millis(2000);
    cfg.raft_startup_phase_timeout = Duration::from_secs(30);
    cfg
}

/// The RPC server must be bound and serving BEFORE raft startup blocks —
/// otherwise unbounded startup would be a mutual deadlock (every cold-starting
/// node waiting for peers that aren't listening): the node blocks forever in
/// `join_existing`, and its server must still accept connections while the
/// readiness wait is pending. (If `bind_server` were moved after
/// `run_raft_startup`, the connect below would be refused and this test would
/// fail.)
#[tokio::test(flavor = "multi_thread")]
async fn rpc_server_accepts_connections_while_raft_startup_is_blocked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let listen = reserve_port();
    let dead_peer = reserve_port(); // nothing will listen here
    let cfg = config_with_an_unreachable_peer(listen, dead_peer, dir.path().join("node.db"));

    // Spawn (do NOT await) — `wait_until_ready` blocks in the join wait and never
    // returns. `start` itself does return (that is the point of the split), so the
    // pending-ness asserted below is the readiness wait, not the bind.
    let handle =
        tokio::spawn(async move { ReplicaNode::start(cfg).await?.wait_until_ready().await });

    // The server must come up (accept a TCP connection) even though startup is
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
    // ...and startup must still be pending — the server is up DURING startup,
    // not only after it returned.
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
    let listen = reserve_port();
    let dead_peer = reserve_port(); // nothing will ever listen here
    let cfg = config_with_an_unreachable_peer(listen, dead_peer, dir.path().join("node.strata"));

    let initialising = ReplicaNode::start(cfg)
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
        matches!(err, Error::Lifecycle(LifecycleError::StartupCancelled)),
        "an operator-requested stop must be distinguishable from a startup fault; got {err:?}",
    );
}
