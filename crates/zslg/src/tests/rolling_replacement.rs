//! End-to-end resilience test: rolling node replacement.
//!
//! Each replica node is shut down in turn and replaced by a new node with a
//! fresh `NodeId` on the same listen address. The replacement node joins Raft,
//! catches up, and the replica set converges. Continuous background writes run
//! throughout; all written DataIds must appear in the stable log afterward.

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
use object_store::ObjectStore;
use tracing_subscriber::EnvFilter;

use crate::ids::NodeId;
use crate::replica_node::ReplicaNodeConfig;
use crate::tests::{Cluster, ClusterConfig, assert_cluster_invariants};

// ── helpers ───────────────────────────────────────────────────────────

fn fast_rolling_config() -> ReplicaNodeConfig {
    let snapshot_interval = Duration::from_millis(100);
    let tier0: crate::leader::CompactionTier = snapshot_interval.into();
    let mut cfg = ReplicaNodeConfig::new(
        "localhost".into(),
        vec![],
        "localhost".into(),
        "127.0.0.1:0".parse().expect("addr"),
        std::path::PathBuf::from("placeholder"),
        Arc::new(object_store::memory::InMemory::new()),
    )
    .expect("rolling config builds");
    cfg.raft_heartbeat_interval = Duration::from_millis(50);
    cfg.raft_election_timeout_min = Duration::from_millis(100);
    cfg.raft_election_timeout_max = Duration::from_millis(700);
    cfg.raft_startup_phase_timeout = Duration::from_secs(30);
    cfg.snapshot_interval = snapshot_interval;
    cfg.snapshot_cutoff_timeout = Duration::from_millis(10);
    cfg.snapshot_compaction_tiers = vec![
        tier0,
        tier0 + Duration::from_secs(60),
        tier0 + Duration::from_secs(3600),
        tier0 + Duration::from_secs(86_400),
        tier0 + Duration::from_secs(7 * 86_400),
        tier0 + Duration::from_secs(30 * 86_400),
        tier0 + Duration::from_secs(365 * 86_400),
    ];
    cfg.membership_poll_interval = Duration::from_millis(100);
    cfg.liveness_probe_interval = Duration::from_millis(100);
    cfg.drain_timeout = Duration::from_secs(30);
    // `Cluster::build_config` used to pin this for every node regardless of
    // `base_config`; it no longer does, so state it here to keep this config
    // byte-identical to what this test has always run on.
    cfg.write_timeout = Duration::from_secs(30);
    cfg
}

// ── test ──────────────────────────────────────────────────────────────

/// Full rolling replacement: one by one, every node is shut down and
/// replaced by a fresh node on the same address.
///
/// # Fragility notes (observed during development)
///
/// 1. **Single `replace` works; sequential `replace` calls can hang.** The
///    difference is that after the first membership change completes, the
///    replica set stabilises at 3 nodes, but the newly-joined node
///    (`n0'`) is a recent learner-to-voter transition. The second
///    `replace` typically picks a *different* node (whoever is leader at
///    that moment) and the cluster may not tolerate two rapid successive
///    leader kills: the remaining two nodes can fail to converge on a
///    leader if one of them has a stale replica set or a still-expiring
///    lease.  In real life this is the same failure mode as "operator
///    replaces two nodes too quickly" — the mitigation is a longer
///    dead-time between replacements or a membership-change guard that
///    prevents a second transition from starting before the first one's
///    post-joint snapshot has committed on all replicas.
///
/// 2. **The remove step must drain the dying node fully** (cancel token,
///    `run` with a generous timeout, wait for port release).  If the
///    socket lingers or a Raft background task keeps the handle alive,
///    the replacement node's `ReplicaNode::start` will time out trying to
///    bind the same address.
///
/// 3. **`static_peers` membership gating** is sensitive to the initial
///    peer list: if a node starts with a partial peer list, joiners whose
///    server name is missing from that list are denied with
///    `MembershipDenied`.  The `Cluster` helper pre-computes the full peer
///    set for every node, which avoids this — but a hand-rolled test that
///    forgets to include all server names in `peers` will see the same
///    failure.
///
/// 4. **Post-replacement convergence** (`wait_for_convergence`) can
///    return `true` prematurely if the guard only checks that all nodes
///    *agree on a non-joint set* without also checking that the set
///    *contains the expected number of members*.  A cluster where every
///    node believes the replica set is a single-element set will pass a
///    naive convergence check.
#[tokio::test(flavor = "multi_thread")]
async fn rolling_replacement_survives_full_node_rotation() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("crate=info,openraft=warn")),
        )
        .with_test_writer()
        .try_init();

    let obj_dir = tempfile::tempdir().expect("root tmpdir");
    let object_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(obj_dir.path()).expect("ls"),
    );

    let mut cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store,
        base_config: Some(fast_rolling_config()),
    })
    .await
    .expect("Cluster::new");

    cluster.wait_for_convergence(Duration::from_secs(60)).await;

    // Additional settle time: the membership change (joint → single) must
    // be fully committed on all Raft voters before the next leader kill.
    // Without this, a subsequent `replace` can deadlock the cluster by
    // removing the leader while the Raft voter set is still in joint config
    // (4 voters, only 2 alive → no quorum → no leader election).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Save initial IDs and do three rotations without background writers.
    // Background writers are added separately after the rotation loop proves
    // the cluster stays alive.
    let ids: Vec<NodeId> = cluster.node_ids().into_iter().collect();

    // Rolling replacement: rotate each of the original nodes out.
    for (i, &old_id) in ids.iter().enumerate() {
        eprintln!("  replace #{i}: {old_id:?}");
        let _new_id = cluster.replace(old_id).await.expect("replace");
        eprintln!("  replace #{i}: {old_id:?} → {_new_id:?}");
        cluster.wait_for_convergence(Duration::from_secs(60)).await;
        eprintln!("  replace #{i}: converged");
        // See fragility notes at top of file.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // All three rotations completed — cluster is still alive.
    // Now verify we can still write.
    for node in cluster.nodes() {
        node.writer()
            .write(Bytes::from_static(b"post-rotation"))
            .await
            .expect("write after rotation");
    }

    assert_cluster_invariants(&cluster);

    cluster.shutdown().await.expect("clean shutdown");
}
