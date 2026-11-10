//! Reconciliation of the replicated set against the voter set.

use super::*;

// ── Reconciliation: the replicated set follows the openraft voter set ──────
//
// Liveness probes `all_targets(replica) ∪ voter_ids`, so a node evicted from
// the voters but still in the replica set stays probed and live. Without the
// filter in `maybe_initiate` it is never a drop candidate, and — because
// spares are drawn from the same live set — an unfiltered spare pool would
// re-add the very node being evicted.

/// A replica member evicted from the voters (but still running and probed)
/// must be dropped from the target: `maybe_initiate` proposes a joint config
/// whose `next` excludes it, filling the deficit from a live voter spare.
#[tokio::test]
async fn reconciliation_drops_evicted_member_from_the_replicated_set() {
    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, n2, n3, n4, n5] = ids;
    let initial: BTreeSet<NodeId> = [n1, n2, n3].into_iter().collect();

    let mut nodes = zslg_raft_setup::testing::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(zslg_raft_fake::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;
    zslg_raft_setup::testing::write_state_fenced(
        &leader_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(initial.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed");

    let observer = LocalObserver::from_systems(&nodes);
    // n3 is evicted from the voter set but still running (still probed live).
    let voters: BTreeSet<NodeId> = [n1, n2, n4, n5].into_iter().collect();
    let (inputs, _integrator_cancel) = build_inputs_with_leadership(
        n1,
        nodes.get(&n1).expect("n1"),
        {
            let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
            arc
        },
        ids.iter().copied().collect(), // liveness still sees everyone
        3,
        Arc::new(StaticLeadership::new(n1, voters)),
    );
    let manager = MembershipChangeManager::new(inputs);
    manager.maybe_initiate().await.expect("initiate");

    let state = nodes.get(&n1).expect("n1").raft_state.snapshot();
    let next = state.replica.next().expect("joint config proposed");
    assert!(
        !next.contains(&n3),
        "evicted member must not be in next: {next:?}"
    );
    assert!(
        next.contains(&n1) && next.contains(&n2),
        "surviving voters stay: {next:?}"
    );
    assert_eq!(next.len(), 3, "deficit filled from voter spares: {next:?}");
    assert_eq!(
        state.replica.current().clone(),
        initial,
        "old is the full current set"
    );

    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}

/// The floor: if the voter set has too few members to reach the target size,
/// `maybe_initiate` proposes NOTHING — the cluster stays at its current
/// replicated size and waits for the missing nodes to come back.
#[tokio::test]
async fn reconciliation_floor_no_proposal_when_voters_below_target_size() {
    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, n2, n3, n4, n5] = ids;
    let initial: BTreeSet<NodeId> = [n1, n2, n3].into_iter().collect();

    let mut nodes = zslg_raft_setup::testing::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(zslg_raft_fake::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;
    zslg_raft_setup::testing::write_state_fenced(
        &leader_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(initial.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed");

    let observer = LocalObserver::from_systems(&nodes);
    // Voter set shrunk to one while target size is 3: no viable target exists.
    let voters: BTreeSet<NodeId> = [n1].into_iter().collect();
    // Liveness must NOT see the whole cluster here, or this test has no teeth:
    // with everyone live the unfiltered target is `{n1,n2,n3}` — exactly the
    // current set — so the old code proposes nothing either, for a completely
    // different reason, and the test passes before and after the fix.
    //
    // With n2/n3 down and n4/n5 live-but-not-voters the two paths diverge:
    // unfiltered, `{n1} ∪ spares{n4,n5}` reaches size 3 and a joint config is
    // proposed; filtered, the spare pool is empty, `compute_target` returns
    // `None`, and the floor holds.
    let live: BTreeSet<NodeId> = [n1, n4, n5].into_iter().collect();
    let (inputs, _integrator_cancel) = build_inputs_with_leadership(
        n1,
        nodes.get(&n1).expect("n1"),
        {
            let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
            arc
        },
        live,
        3,
        Arc::new(StaticLeadership::new(n1, voters)),
    );
    let manager = MembershipChangeManager::new(inputs);
    manager.maybe_initiate().await.expect("initiate");

    let state = nodes.get(&n1).expect("n1").raft_state.snapshot();
    assert!(
        !state.replica.is_joint(),
        "no proposal when voters cannot fill the target size"
    );
    assert_eq!(state.replica.current().clone(), initial);

    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}

/// A joint config whose `next` contains an evicted (voter-removed) member must
/// be retargeted even though the member is still live.
#[tokio::test]
async fn reconciliation_retargets_a_next_member_evicted_from_voters() {
    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, n2, n3, n4, n5] = ids;
    let initial: BTreeSet<NodeId> = [n1, n2, n3].into_iter().collect();
    let next_set: BTreeSet<NodeId> = [n2, n3, n4].into_iter().collect();

    let mut nodes = zslg_raft_setup::testing::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(zslg_raft_fake::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;
    // TWO writes, in this order. A joint config may NOT be seeded directly out
    // of genesis: `RaftState::empty()` is `Single { replicas: {} }`, and
    // `validate_transition`'s `(Single, Joint)` arm requires `replicas == old`
    // — the empty-genesis escape hatch exists only on the `(Single, Single)`
    // arm, so seeding `Joint` straight away is rejected as
    // `JointEntryWrongOldSet`. Same two-step shape as
    // `resume_from_joint_after_leader_killed_mid_step`.
    zslg_raft_setup::testing::write_state_fenced(
        &leader_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(initial.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed single");
    zslg_raft_setup::testing::write_state_fenced(
        &leader_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::joint(initial.clone(), next_set.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed joint");

    let observer = LocalObserver::from_systems(&nodes);
    // n4 was evicted from voters mid-change; it is still live but must leave
    // next. Voters exclude n1 as well so the retarget's spare pool is exactly
    // {n5}: with `SpareSelectionStrategy::Random` a pool of {n1, n5} could
    // pick n1, which would make next == old and COLLAPSE the joint config
    // instead of retargeting it (initiate's new == old path) — leaving
    // `next()` None and the assertion below panicking.
    let voters: BTreeSet<NodeId> = [n2, n3, n5].into_iter().collect();
    let (inputs, _integrator_cancel) = build_inputs_with_leadership(
        n1,
        nodes.get(&n1).expect("n1"),
        {
            let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
            arc
        },
        ids.iter().copied().collect(),
        3,
        Arc::new(StaticLeadership::new(n1, voters)),
    );
    let manager = MembershipChangeManager::new(inputs);
    manager.maybe_initiate().await.expect("initiate");

    let state = nodes.get(&n1).expect("n1").raft_state.snapshot();
    let next = state.replica.next().expect("retargeted config");
    assert!(
        !next.contains(&n4),
        "evicted next member must be retargeted out: {next:?}"
    );
    assert_eq!(next.len(), 3, "target size preserved: {next:?}");

    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}
