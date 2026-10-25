//! Partition/heal fault-injection tests over the in-process raft cluster
//! (EXPERT_REVIEW Appendix B.4). The `crate::tests::Partition` handle cuts
//! pairwise raft links; these tests pin the two basic behaviours every
//! partition scenario builds on: a cut minority falls behind and converges
//! after heal, and a cut majority-less leader cannot commit.
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
use std::collections::BTreeSet;
use std::time::Duration;

use crate::api::ReplicatedRaftState;
use crate::hlc::Hlc;
use crate::ids::NodeId;
use crate::tests::fast_raft_config as raft_config;

fn state_with_cutoff(replicas: &BTreeSet<NodeId>, cutoff: u64) -> ReplicatedRaftState {
    ReplicatedRaftState {
        replica: ReplicaMembership::single(replicas.clone()),
        manifest: Vec::new(),
        manifest_watermark: None,
        hlc_cutoff: Some(Hlc(cutoff)),
    }
}

#[tokio::test]
async fn partitioned_follower_converges_after_heal() {
    let ids: [NodeId; 3] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, n2, n3] = ids;
    let (nodes, partition) = crate::tests::start_cluster_with_partition(&ids, raft_config())
        .await
        .expect("cluster");
    nodes
        .get(&n1)
        .expect("n1")
        .raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    crate::tests::wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;

    let replicas: BTreeSet<NodeId> = ids.iter().copied().collect();

    partition.isolate(n3, &ids);

    // n1+n2 are still a quorum; the cluster keeps committing.
    let leader = nodes.get(&n1).expect("n1");
    for cutoff in [100u64, 200, 300] {
        crate::tests::write_state_fenced(
            &leader.raft,
            &leader.raft_state,
            state_with_cutoff(&replicas, cutoff),
        )
        .await
        .expect("write with n3 partitioned");
    }

    let leader_generation = leader.raft_state.snapshot().generation;
    crate::tests::wait_until(
        "n2 converges during the partition",
        Duration::from_secs(5),
        || async {
            nodes.get(&n2).expect("n2").raft_state.snapshot().generation == leader_generation
        },
    )
    .await;

    // The cut follower has seen none of it.
    let stale = nodes.get(&n3).expect("n3").raft_state.snapshot();
    assert!(
        stale.generation < leader_generation,
        "partitioned n3 must be behind: {:?} vs {leader_generation:?}",
        stale.generation,
    );
    assert_ne!(stale.replica.current().clone(), replicas);

    partition.heal_all();

    crate::tests::wait_until(
        "n3 converges after heal",
        Duration::from_secs(5),
        || async {
            let s = nodes.get(&n3).expect("n3").raft_state.snapshot();
            s.generation >= leader_generation && *s.replica.current() == replicas
        },
    )
    .await;
}

#[tokio::test]
async fn isolated_leader_cannot_commit_until_healed() {
    let ids: [NodeId; 3] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, n2, n3] = ids;
    let (nodes, partition) = crate::tests::start_cluster_with_partition(&ids, raft_config())
        .await
        .expect("cluster");
    nodes
        .get(&n1)
        .expect("n1")
        .raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    crate::tests::wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;

    let replicas: BTreeSet<NodeId> = ids.iter().copied().collect();
    let leader = nodes.get(&n1).expect("n1");
    crate::tests::write_state_fenced(
        &leader.raft,
        &leader.raft_state,
        state_with_cutoff(&replicas, 100),
    )
    .await
    .expect("baseline write");
    let baseline = leader.raft_state.snapshot().generation;

    partition.isolate(n1, &ids);

    // A write through the isolated leader must not commit: it either hangs
    // awaiting an unreachable quorum (timeout) or errors once n1 learns it
    // was deposed by the surviving majority.
    let attempt = tokio::time::timeout(
        Duration::from_secs(1),
        crate::tests::write_state_fenced(
            &leader.raft,
            &leader.raft_state,
            state_with_cutoff(&replicas, 200),
        ),
    )
    .await;
    assert!(
        !matches!(attempt, Ok(Ok(()))),
        "write must not commit without a quorum: {attempt:?}",
    );
    for follower in [n2, n3] {
        let s = nodes
            .get(&follower)
            .expect("follower")
            .raft_state
            .snapshot();
        assert!(
            s.generation <= baseline,
            "{follower:?} must not have applied the isolated leader's write",
        );
    }

    partition.heal_all();

    // The cluster is functional again: push a write through whoever leads now
    // (n2/n3 may have elected a new leader during the partition; a node may
    // also briefly report a stale leader right after the heal — re-observe on
    // every attempt rather than pinning the write to one node).
    crate::tests::write_state_fenced_via_leader(&nodes, state_with_cutoff(&replicas, 300))
        .await
        .expect("post-heal write");

    crate::tests::wait_until(
        "all nodes converge after heal",
        Duration::from_secs(5),
        || async {
            let states: Vec<_> = ids
                .iter()
                .map(|id| nodes.get(id).expect("node").raft_state.snapshot())
                .collect();
            states
                .iter()
                .all(|s| s.generation == states[0].generation && s.generation > baseline)
        },
    )
    .await;
}
