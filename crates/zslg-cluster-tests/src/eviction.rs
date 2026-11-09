#![allow(
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
//! A node evicted from the voters (the operator removes it from every other
//! node's `config.peers`) while it keeps running:
//!
//! 1. the replicated `ReplicaMembership` follows the voter set and shrinks through
//!    the normal joint transition (the voter-only spare fills the vacancy);
//! 2. the evicted node's rejoin is affirmatively denied, so it stops serving
//!    client reads (`Error::NotReady`) ...
//! 3. ... but its supervision tree stays up: removal is the group's decision, ending
//!    the process is the orchestrator's.
//!
//! One arrangement, three claims, so one cluster bring-up. Step 3 needs a denial that
//! is *affirmative* (`JoinAttempt::Denied`), which only the real eviction path
//! produces: a contacted peer's own `config.peers` no longer names the victim. The
//! demote-to-learner shortcut in `decommission` changes the voter set without that.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;

use crate::{
    Cluster, ClusterConfig, assert_cluster_invariants, default_fast_config, wait_until_sync,
};
use zslg_consensus::NodeId;
use zslg_error::LifecycleError;

/// Removing one member from every other node's `config.peers` evicts it from
/// the voter set; the leader then shrinks the replicated set `{A,B,C}` →
/// `{B,C,D}` via the joint transition, with the evicted node still running.
#[tokio::test(flavor = "multi_thread")]
async fn evicted_member_leaves_the_set_is_refused_reads_and_stays_up() {
    crate::init_logging();
    // 4 nodes, distinct names: node-0 bootstraps, node-3 is a voter-only spare
    // (replica_node_count = 3). `distinct_names` is required — the eviction
    // job compares server NAMES, and shared "localhost" names can never be
    // absent from config.peers.
    let mut base = default_fast_config();
    base.membership_forced_node_removal_timeout = Duration::from_millis(200);
    base.membership_poll_interval = Duration::from_millis(100);
    base.liveness_probe_interval = Duration::from_millis(100);

    let mut cluster = Cluster::new(ClusterConfig {
        count: 4,
        distinct_names: true,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(base),
    })
    .await
    .expect("Cluster::new");
    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;

    // The replicated set is the first 3 to converge; the 4th is a voter-only
    // spare. Discover the actual member set.
    let replicated: BTreeSet<NodeId> = cluster
        .nodes()
        .next()
        .expect("node")
        .state_handle()
        .snapshot()
        .replica
        .current()
        .clone();
    assert_eq!(replicated.len(), 3, "replica_node_count = 3");
    let spare: NodeId = cluster
        .node_ids()
        .into_iter()
        .find(|id| !replicated.contains(id))
        .expect("a voter-only spare");

    // Pick the victim: a replicated member that is not the current leader (a
    // leader is self-exempt from its own eviction job). It keeps running
    // throughout — only its *name* leaves everyone else's `config.peers`.
    // (`Cluster::leader` is the node reporting *itself* as leader; picking the
    // victim from "some node knows a leader" would land on the real leader
    // often enough to hang this test.)
    let leader_id: NodeId = cluster.leader().expect("leader");
    let victim = replicated
        .iter()
        .copied()
        .find(|id| *id != leader_id)
        .expect("a non-leader replicated member");
    let victim_name = cluster.placement_of(victim).server_name;

    // Build a peer list of every placement EXCEPT the victim's server name.
    // Computed ONCE, before any kill: `kill` removes the node's placement, so
    // `peer_addrs()` shrinks as we go.
    let trimmed_peers: Vec<zslg_net::PeerAddr> = cluster
        .peer_addrs()
        .into_iter()
        .filter(|p| p.server_name != victim_name)
        .collect();

    // ── Why EVERY non-victim node is relaunched, not just the leader ──────
    // `leader:voters` runs on whichever node currently holds leadership, and
    // `config.peers` is static per node (`network::peers::StaticPeers` — there
    // is no runtime setter). Relaunching only the current leader would be a
    // coin flip: the relaunch triggers an election, any of the four can win,
    // and a winner still holding the untrimmed list never evicts anyone — the
    // test would then hang to its deadline for a reason that has nothing to do
    // with the code under test. Trimming every node makes the eviction hold no
    // matter who leads.
    //
    // Sequential, one node at a time: the voter set is 4, so a single node
    // down still leaves a quorum of 3 and the cluster keeps making progress.
    //
    // ── And why the relaunched three get a FASTER election timeout ────────
    // The victim must never hold leadership, or the test deadlocks: the
    // eviction job skips `self`, so a victim that leads can never evict
    // itself. Nothing in the system prevents its election — the membership
    // gate admits *joiners* (`RaftNode::add_member`) and does not filter vote
    // RPCs, so a node absent from everyone's `config.peers` still campaigns
    // and still collects votes.
    //
    // Bouncing the victim to force an election is NOT an option: any downtime
    // makes it non-live, and reconciliation would then drop it for being dead
    // — the test would go green without the voter set ever mattering, which is
    // precisely the thing it exists to prove.
    //
    // So make it lose the race instead. The three relaunched nodes campaign at
    // 150-300 ms while the victim keeps `default_fast_config`'s 1000-2000 ms,
    // so an election is always started and won by a trimmed node long before
    // the victim's timer expires. The victim still *votes*, so quorum is never
    // in question.
    let mut fast_election = default_fast_config();
    fast_election.membership_forced_node_removal_timeout = Duration::from_millis(200);
    fast_election.membership_poll_interval = Duration::from_millis(100);
    fast_election.liveness_probe_interval = Duration::from_millis(100);
    fast_election.raft_election_timeout_min = Duration::from_millis(150);
    fast_election.raft_election_timeout_max = Duration::from_millis(300);
    fast_election.raft_heartbeat_interval = Duration::from_millis(50);
    cluster.set_base_config(fast_election);

    let to_relaunch: Vec<NodeId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != victim)
        .collect();
    for id in to_relaunch {
        let placement = cluster.kill(id).await;
        let relaunched = cluster
            .relaunch_with_peers(placement, &trimmed_peers)
            .await
            .expect("relaunch with trimmed peers");
        assert_eq!(
            relaunched, id,
            "relaunch must resume the same NodeId (db-path reuse ⇒ Rejoin)"
        );
        cluster.wait_for_leader(Duration::from_secs(30)).await;
    }

    // Leadership must land on a node that carries the trimmed peer list.
    // Compare the leader's *id* against the victim — `leader.is_some()` only
    // says a leader exists, which is true even when the victim is it.
    wait_until_sync(
        "leadership on a trimmed node",
        Duration::from_secs(30),
        || {
            cluster.nodes().any(|n| {
                n.state_handle()
                    .snapshot()
                    .leader
                    .is_some_and(|l| l != victim)
            })
        },
    )
    .await;

    // The victim is still in the replicated set at this point: it never went
    // down, so nothing but the voter-set eviction can remove it. Without this
    // the final wait could be satisfied by an unrelated liveness-driven drop.
    assert!(
        cluster
            .node(victim)
            .state_handle()
            .snapshot()
            .replica
            .current()
            .contains(&victim),
        "victim must still be a replicated member before the eviction lands"
    );

    // The reconciliation then shrinks the replicated set: victim leaves, the
    // voter-only spare fills the deficit. The evicted node is STILL RUNNING.
    // Assert on the REMAINING nodes: the victim was evicted from the voter
    // set, so openraft may stop replicating the new membership to it — its own
    // view can legitimately lag behind.
    let expected: BTreeSet<NodeId> = replicated
        .iter()
        .copied()
        .filter(|id| *id != victim)
        .chain([spare])
        .collect();
    wait_until_sync(
        "replicated set follows the voter set",
        Duration::from_secs(60),
        || {
            cluster.nodes().filter(|n| n.node_id() != victim).all(|n| {
                let s = n.state_handle().snapshot();
                !s.replica.is_joint() && *s.replica.current() == expected
            })
        },
    )
    .await;

    // The victim is still a running node — only its membership is gone.
    assert!(
        cluster.node_ids().contains(&victim),
        "evicted node must still be running"
    );

    assert_cluster_invariants(&cluster);

    // THE READ-GATE CLAIM, checked BEFORE extraction (still gated through
    // `Cluster`): `can_serve_clients` must go false and `sync_quorum` must
    // return `NotReady` once — and only once — a reachable peer has
    // AFFIRMATIVELY denied this node's rejoin (`JoinAttempt::Denied`,
    // published as `EvictionStatus`). This is a `wait_until_sync`, not an
    // immediate check, because the victim's own openraft voter-set view can
    // legitimately never converge (see above) — a gate that (incorrectly)
    // read that view would read `can_serve_clients() == true` forever and
    // this wait would time out rather than pass. That is exactly the bug the
    // formal review caught: reads served from a view that would never
    // advance. Bounded by grace (15s) + reconcile (5s) + margin, same as
    // below.
    wait_until_sync(
        "victim's own gate reports it cannot serve clients (Denied observed)",
        Duration::from_secs(30),
        || !cluster.node(victim).can_serve_clients(),
    )
    .await;
    let err = cluster
        .node(victim)
        .reader()
        .sync()
        .await
        .expect_err("a denied node must refuse client reads");
    assert!(
        matches!(err, zslg_log::Error::Lifecycle(LifecycleError::NotReady(_))),
        "expected NotReady, got {err:?}"
    );

    // Extract the victim so its OWN supervision tree can be observed —
    // `Cluster` cannot answer this question (see the doc comment above).
    let victim_node = cluster.take_running(victim);
    let token = victim_node.shutdown_token();
    let handle = tokio::spawn(victim_node.run());

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !handle.is_finished(),
        "the node must still be running right after being taken over — \
         otherwise this test proves nothing",
    );

    // `raft::startup::rejoin::REJOIN_LEADERLESS_GRACE` is 15s and
    // `REJOIN_RECONCILE_INTERVAL` is 5s (both hardcoded, not config-driven).
    // 30s covers grace + at least one more reconcile tick + margin for the
    // denied `AddMember` round-trip.
    tokio::time::sleep(Duration::from_secs(30)).await;

    // THE CLAIM: a denied rejoin must not have torn the supervision tree
    // down. With the old fatal `return Err(Error::MembershipDenied { .. })`
    // restored, `handle` completes here and this assertion fires.
    assert!(
        !handle.is_finished(),
        "a node denied re-join by the group must not terminate itself \
         (removal is the group's decision, not the node's)",
    );

    // Clean up: request shutdown and confirm the node still stops gracefully
    // on request — this is not the claim under test, just hygiene.
    token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("cancellation must stop the node, not merely be recorded")
        .expect("victim's run() task must not panic");
    assert!(
        result.is_ok(),
        "graceful shutdown must still return Ok(()), got {result:?}"
    );

    cluster.shutdown().await.expect("clean shutdown");
}
