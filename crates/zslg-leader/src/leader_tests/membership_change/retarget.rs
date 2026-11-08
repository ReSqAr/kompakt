//! Retargeting a joint step whose incoming set is no longer live.

use super::*;

/// A retarget must drop the armed bound (`JointDraining` → `Joint`), because
/// `exit_hlc` attests a generation quorum for the *old* incoming set. The new
/// incoming set has to earn its own.
#[tokio::test]
async fn retarget_disarms_the_exit_condition_and_rearms_monotonically() {
    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, _, _, _, _] = ids;

    let mut nodes = zslg_raft_setup::testing::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(zslg_raft_fake::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;

    let initial: BTreeSet<NodeId> = ids[..3].iter().copied().collect();
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
    observer.set_max_hlc(&initial, Hlc(1000));
    wait_until("converged", Duration::from_secs(5), || async {
        for id in &ids {
            let s = observer.observe(*id).await.expect("observe");
            if s.raft_state.replica.current().clone() != initial {
                return false;
            }
        }
        true
    })
    .await;

    let leader_sys = nodes.get(&n1).expect("n1");
    let (inputs, _integrator_cancel) = build_inputs(
        n1,
        leader_sys,
        {
            let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
            arc
        },
        ids.iter().copied().collect(),
    );
    let manager = MembershipChangeManager::new(inputs);

    let first_target: BTreeSet<NodeId> = ids[..4].iter().copied().collect();
    manager
        .initiate(first_target.clone())
        .await
        .expect("initiate 3→4");
    // Drive to the armed state.
    wait_until("exit condition armed", Duration::from_secs(10), || {
        let manager = &manager;
        let raft_state = leader_sys.raft_state.clone();
        async move {
            let _ = manager.tick().await;
            raft_state.snapshot().replica.exit() == Some(Exit::WatermarkAbove(Hlc(1000)))
        }
    })
    .await;

    // Retarget: the outgoing set is unchanged, the incoming set is not.
    let second_target: BTreeSet<NodeId> = ids.iter().copied().collect();
    observer.set_max_hlc(&initial, Hlc(2000));
    manager
        .initiate(second_target.clone())
        .await
        .expect("retarget 4→5");

    let after = leader_sys.raft_state.snapshot();
    assert_eq!(
        after.replica.exit(),
        None,
        "a retarget must disarm: the bound attested a generation quorum for the previous target"
    );
    assert_eq!(
        after.replica.next(),
        Some(&second_target),
        "the retarget must be committed"
    );
    assert_eq!(
        after.replica.current().clone(),
        initial,
        "a retarget must not change the outgoing set"
    );

    // Re-arming picks up the higher sample.
    wait_until(
        "re-armed at the new sample",
        Duration::from_secs(10),
        || {
            let manager = &manager;
            let raft_state = leader_sys.raft_state.clone();
            async move {
                let _ = manager.tick().await;
                raft_state.snapshot().replica.exit() == Some(Exit::WatermarkAbove(Hlc(2000)))
            }
        },
    )
    .await;

    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}

/// A retarget throws away the step-2 generation quorum and disarms `exit_hlc`,
/// so a cluster with flapping liveness can restart the protocol indefinitely.
/// The counter is the only signal that distinguishes "converging slowly" from
/// "restarting forever", so it must tick exactly when a retarget happens.
#[tokio::test]
async fn a_retarget_increments_the_retarget_counter() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, _, _, _, _] = ids;

    let mut nodes = zslg_raft_setup::testing::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(zslg_raft_fake::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;

    let initial: BTreeSet<NodeId> = ids[..3].iter().copied().collect();
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
    observer.set_max_hlc(&initial, Hlc(1000));
    wait_until("converged", Duration::from_secs(5), || async {
        for id in &ids {
            let s = observer.observe(*id).await.expect("observe");
            if s.raft_state.replica.current().clone() != initial {
                return false;
            }
        }
        true
    })
    .await;

    let leader_sys = nodes.get(&n1).expect("n1");
    let (inputs, _integrator_cancel) = build_inputs(
        n1,
        leader_sys,
        {
            let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
            arc
        },
        ids.iter().copied().collect(),
    );
    let manager = MembershipChangeManager::new(inputs);

    let first_target: BTreeSet<NodeId> = ids[..4].iter().copied().collect();
    manager
        .initiate(first_target.clone())
        .await
        .expect("initiate 3→4");
    // Drive to the armed state.
    wait_until("exit condition armed", Duration::from_secs(10), || {
        let manager = &manager;
        let raft_state = leader_sys.raft_state.clone();
        async move {
            let _ = manager.tick().await;
            raft_state.snapshot().replica.exit() == Some(Exit::WatermarkAbove(Hlc(1000)))
        }
    })
    .await;

    let before = counter_value(&snapshotter, "zslg_membership_retargets_total");

    // Drop ids[3] (a member of `next`) out of the live set and bring in the
    // spare ids[4] instead, so `maybe_initiate` must recompute the incoming
    // set but can still reach the target size (4).
    let flapped_live: BTreeSet<NodeId> = [ids[0], ids[1], ids[2], ids[4]].into_iter().collect();
    let (retarget_inputs, _retarget_integrator_cancel) = build_inputs_with_replica_count(
        n1,
        leader_sys,
        {
            let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
            arc
        },
        flapped_live,
        4,
    );
    let retarget_manager = MembershipChangeManager::new(retarget_inputs);
    retarget_manager.maybe_initiate().await.expect("initiate");

    assert_eq!(
        counter_value(&snapshotter, "zslg_membership_retargets_total"),
        before + 1,
        "a retarget must be visible in zslg_membership_retargets_total",
    );

    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}
