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
//! Group removal is not fatal (CLAUDE.md's "membership is the group's
//! decision, ending a process is the orchestrator's"): a node denied
//! re-admission to the group must stay up, refuse client reads with
//! `Error::NotReady`, and let the orchestrator decide its fate.
//!
//! The read gate (`learner::read::can_serve_clients`,
//! `raft::startup::rejoin::EvictionStatus`) is deliberately narrower than "not
//! a voter": it fires only on an AFFIRMATIVE `JoinAttempt::Denied` — a
//! reachable peer answered and refused this node. It must NOT fire on a mere
//! voter-set change with no eviction behind it (this file's first test) and
//! must NOT fire on a failure to reach anyone (unit-tested in
//! `raft::startup::rejoin::tests::unreachable_cluster_never_fatal`, which
//! asserts `EvictionStatus` stays clear through a whole simulated partition).

use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;

use crate::{Cluster, ClusterConfig, default_fast_config, wait_until_sync};

/// A voter-set change alone — with `config.peers` never touched, so the
/// rejoin monitor never has anything to be denied by — must NOT gate reads.
///
/// This guards the exact shape of bug the formal review caught: gating on the
/// openraft voter set *looks* plausible here (`can_serve_clients` used to do
/// exactly that) but freezes solid under a REAL (`retain: false`) eviction,
/// because the leader stops replicating to the removed node entirely, so its
/// own view of `voters` never converges (see
/// `eviction::evicted_member_leaves_the_set_is_refused_reads_and_stays_up`, which drives the real mechanism
/// and is where the affirmative-`NotReady` claim actually lives). Demoting to
/// learner here changes the voter set without an eviction — see
/// `RaftNode::demote_to_learner_for_test` — precisely so this test can prove
/// the gate does NOT trip on that alone.
#[tokio::test(flavor = "multi_thread")]
async fn voter_set_change_alone_does_not_gate_reads() {
    crate::init_logging();
    let mut base = default_fast_config();
    // Short grace so the rejoin monitor would reach its decision quickly, if
    // there were anything for it to decide.
    base.raft_startup_phase_timeout = Duration::from_secs(5);

    let cluster = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: true,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(base),
    })
    .await
    .expect("Cluster::new");
    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;

    let leader = cluster.leader().expect("leader");
    let victim = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("a non-leader node");

    // Remove the victim from the voter set. `Cluster::remove_voter` demotes
    // to learner rather than dropping the node outright, so the victim keeps
    // receiving `AppendEntries` and its own view promptly reflects the
    // demotion — see `RaftNode::demote_to_learner_for_test` for why a fully
    // dropped node's own view cannot be trusted to observe this within a
    // test's lifetime, even though that's what production eviction uses.
    // Crucially: `config.peers` is untouched, so this is NOT an eviction.
    cluster
        .remove_voter(leader, victim)
        .await
        .expect("remove voter");

    wait_until_sync(
        "victim observes it is not a voter",
        Duration::from_secs(30),
        || {
            !cluster
                .node(victim)
                .state_handle()
                .snapshot()
                .voters
                .contains(&victim)
        },
    )
    .await;

    // Well past the rejoin monitor's grace period — plenty of time for it to
    // have (wrongly) probed and (wrongly) been denied, if the mechanism
    // reacted to `config.peers` it was never removed from.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // THE CLAIM: still running, AND still able to serve clients — a voter-set
    // change with no affirmative eviction behind it must be invisible to the
    // read gate.
    assert!(
        cluster.node_ids().contains(&victim),
        "a node with a changed voter-set status must not terminate itself"
    );
    assert!(
        cluster.node(victim).can_serve_clients(),
        "a voter-set change alone (no affirmative Denied) must not gate reads"
    );
    cluster
        .node(victim)
        .reader()
        .sync()
        .await
        .expect("a voter-set change alone (no affirmative Denied) must not refuse reads");

    cluster.shutdown().await.expect("clean shutdown");
}

/// The hazard: a leader absent from every other node's `config.peers` cannot be
/// evicted by anyone else — its own job skips itself, and no other node's job
/// runs. It must remove itself.
///
/// 4 nodes, distinct names: 3 replicated + a voter-only spare (`replica_node_count
/// = 3`), same shape as `eviction::
/// evicted_member_leaves_the_set_is_refused_reads_and_stays_up`. This is not
/// optional headroom: `leader::jobs::membership::change::maybe_initiate` only
/// proposes a new replicated set when enough LIVE voters remain to fill
/// `replica_node_count` (CLAUDE.md §12's "bounded by `replica_node_count` as a
/// floor"). With only 3 nodes total, self-evicting the leader from the voter
/// set leaves 2 live voters — below the floor — so `replica.current()` would
/// never reconcile and part (c) below would hang forever for a reason that has
/// nothing to do with self-eviction. The spare is what makes a live target
/// available to fill the vacancy.
///
/// Election guard: relaunching the two followers is real downtime, and
/// restart churn makes it plausible one of them wins an election before the
/// old leader's next heartbeat lands — if that happens, the PRE-EXISTING
/// peer-eviction pass (`leader::jobs::voters`, exercised by
/// `eviction`) evicts the old leader from the new leader's view
/// in ~200ms, and every assertion below goes green without
/// `tick_self_eviction` ever running. So the relaunched followers get a much
/// LONGER election timeout than the untouched leader (mirroring
/// `evicted_member_leaves_the_set_is_refused_reads_and_stays_up`, which does
/// the inverse — there the victim must lose the race, here the victim must
/// win it): the leader keeps `default_fast_config`'s 1000-2000ms timers and
/// 250ms heartbeat, so its heartbeats reset the followers' election timers
/// long before the followers' own 5-8s timeout could fire.
#[tokio::test(flavor = "multi_thread")]
async fn unwanted_leader_evicts_itself() {
    crate::init_logging();
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

    let replicated: std::collections::BTreeSet<zslg_consensus::NodeId> = cluster
        .nodes()
        .next()
        .expect("node")
        .state_handle()
        .snapshot()
        .replica
        .current()
        .clone();
    assert_eq!(replicated.len(), 3, "replica_node_count = 3");

    // The leader must be a REPLICATED member, not the spare — otherwise
    // part (c) below ("dropped from replica.current()") is vacuous, since the
    // spare was never in that set to begin with.
    let leader = cluster
        .leader()
        .filter(|l| replicated.contains(l))
        .expect("leader must be a replicated member for this test to say anything");
    let leader_name = cluster.placement_of(leader).server_name;

    // Trim the LEADER from every other node's peers, leaving the leader's own
    // list intact — the exact shape that is inescapable today.
    let trimmed: Vec<zslg_net::PeerAddr> = cluster
        .peer_addrs()
        .into_iter()
        .filter(|p| p.server_name != leader_name)
        .collect();

    // Election guard (see doc comment above): the relaunched followers must
    // never win a race against the untouched leader, so they get a much
    // longer election timeout than the leader's own (unchanged)
    // `default_fast_config` timers.
    let mut slow_followers = default_fast_config();
    slow_followers.membership_forced_node_removal_timeout = Duration::from_millis(200);
    slow_followers.membership_poll_interval = Duration::from_millis(100);
    slow_followers.liveness_probe_interval = Duration::from_millis(100);
    slow_followers.raft_election_timeout_min = Duration::from_millis(5000);
    slow_followers.raft_election_timeout_max = Duration::from_millis(8000);
    cluster.set_base_config(slow_followers);

    let to_relaunch: Vec<_> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    for id in to_relaunch {
        let placement = cluster.kill(id).await;
        cluster
            .relaunch_with_peers(placement, &trimmed)
            .await
            .expect("relaunch with trimmed peers");
    }

    // (b) The victim's OWN view: a leader that removes itself applies its own
    // membership change, so its own view converges — a stronger and more
    // specific claim than "some other node noticed" (see docs/TESTING.md's
    // corrected note on this).
    wait_until_sync(
        "unwanted leader's own view no longer lists itself as a voter",
        Duration::from_secs(60),
        || {
            !cluster
                .node(leader)
                .state_handle()
                .snapshot()
                .voters
                .contains(&leader)
        },
    )
    .await;

    // Corroborate with a survivor's view too, so this is not solely load-
    // bearing on the (still novel) self-view claim above. This must be a
    // bounded wait, not an immediate assert: `snapshot().voters` is blended
    // from each node's OWN openraft apply (`raft::state::state_publisher`), so
    // a survivor's applied view lags the ex-leader's by the commit-index
    // propagation. The faster self-eviction (parallel probes) lands the
    // leader's apply sooner after relaunch, widening the window in which the
    // survivors have acked the change but not yet applied it.
    wait_until_sync(
        "at least one survivor no longer lists the ex-leader as a voter",
        Duration::from_secs(30),
        || {
            cluster
                .nodes()
                .filter(|n| n.node_id() != leader)
                .any(|n| !n.state_handle().snapshot().voters.contains(&leader))
        },
    )
    .await;

    // It stays up — Task 3's rule. Only its membership is gone.
    assert!(
        cluster.node_ids().contains(&leader),
        "the self-evicted node must keep running"
    );

    // (c) Leadership moved to a different node — the self-eviction only means
    // anything if the ex-leader stopped being leader in the process.
    wait_until_sync(
        "leadership moved off the self-evicted node",
        Duration::from_secs(60),
        || {
            cluster.nodes().any(|n| {
                n.state_handle()
                    .snapshot()
                    .leader
                    .is_some_and(|l| l != leader)
            })
        },
    )
    .await;

    // (c) The replicated set reconciles: the ex-leader is dropped from
    // `state.replica.current()`, not merely from the raw openraft voter set.
    // This is the branch's central cross-task integration claim (peer
    // eviction's replicated-set machinery reused by self-eviction) and was
    // previously untested.
    //
    // Checked on a SURVIVOR, not the ex-leader's own view: unlike the voter-
    // set removal above (applied by the leader to itself, in the same write,
    // before it steps down — so its own view converges immediately), the
    // replicated-set retarget is a SEPARATE, LATER write proposed by
    // whichever node leads afterward. Once the ex-leader is out of the voter
    // set it receives no further replication at all, so this later entry
    // never reaches it — the ordinary frozen-view case (CLAUDE.md §12 /
    // docs/TESTING.md), which applies here even though the *voter* removal
    // was self-inflicted.
    wait_until_sync(
        "the replicated set reconciles: ex-leader dropped from replica.current()",
        Duration::from_secs(60),
        || {
            cluster.nodes().filter(|n| n.node_id() != leader).all(|n| {
                !n.state_handle()
                    .snapshot()
                    .replica
                    .current()
                    .contains(&leader)
            })
        },
    )
    .await;

    cluster.shutdown().await.expect("clean shutdown");
}
