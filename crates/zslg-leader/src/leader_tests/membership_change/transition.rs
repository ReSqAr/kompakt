//! The full joint transition: 3 -> 5, the drained-old-set exit rule, and resuming after the leader dies mid-step.

use super::*;

#[tokio::test]
async fn three_to_five_full_transition_via_state_handle() {
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
    wait_until(
        "initial replica set converged",
        Duration::from_secs(5),
        || async {
            for id in &ids {
                let s = observer.observe(*id).await.expect("observe");
                if s.raft_state.replica.current().clone() != initial {
                    return false;
                }
            }
            true
        },
    )
    .await;

    let leader_sys = nodes.get(&n1).expect("n1");
    let all_live: BTreeSet<NodeId> = ids.iter().copied().collect();
    let (inputs, _integrator_cancel) = build_inputs(
        n1,
        leader_sys,
        {
            let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
            arc
        },
        all_live,
    );
    let manager = MembershipChangeManager::new(inputs);

    let target: BTreeSet<NodeId> = ids.iter().copied().collect();
    manager
        .initiate(target.clone())
        .await
        .expect("initiate 3→5");

    {
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(10))
            .expect("deadline");
        let mut hlc_counter = 0u64;
        loop {
            hlc_counter = hlc_counter.saturating_add(100);
            let cur = leader_sys.raft_state.snapshot();
            leader_sys
                .raft
                .client_write(zslg_raft::FencedStateWrite {
                    expected_generation: cur.generation,
                    state: ReplicatedRaftState {
                        replica: cur.replica.clone(),
                        manifest: cur.manifest_state.manifest.clone(),
                        manifest_watermark: Some(Hlc(hlc_counter)),
                        hlc_cutoff: cur.manifest_state.hlc_cutoff,
                    },
                })
                .await
                .ok();
            if matches!(manager.tick().await.expect("tick"), TickOutcome::Complete) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "membership change did not complete within deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let final_state = nodes.get(&n1).expect("n1").raft_state.snapshot();
    assert_eq!(final_state.replica.current().clone(), target);
    assert!(
        !final_state.replica.is_joint(),
        "joint flag must be cleared"
    );

    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}

/// The membership durability invariant, end to end at the driver level: a joint
/// config must not collapse until the manifest watermark covers the outgoing
/// set's `max_hlc`.
///
/// Before this gate existed the exit test was "the watermark advanced at all",
/// which *any* snapshot satisfied — including one whose cutoff sat below writes
/// the old set held alone. Those writes were lost when the old nodes were
/// dropped.
#[tokio::test]
async fn invariant_joint_exit_waits_for_the_old_set_to_be_drained() {
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
    // The outgoing set holds writes up to HLC 5000 — acknowledged against a
    // quorum of `initial` alone, so only `initial` is known to have them.
    const OLD_SET_MAX_HLC: Hlc = Hlc(5000);
    observer.set_max_hlc(&initial, OLD_SET_MAX_HLC);
    wait_until(
        "initial replica set converged",
        Duration::from_secs(5),
        || async {
            for id in &ids {
                let s = observer.observe(*id).await.expect("observe");
                if s.raft_state.replica.current().clone() != initial {
                    return false;
                }
            }
            true
        },
    )
    .await;

    let leader_sys = nodes.get(&n1).expect("n1");
    let all_live: BTreeSet<NodeId> = ids.iter().copied().collect();
    let (inputs, _integrator_cancel) = build_inputs(
        n1,
        leader_sys,
        {
            let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
            arc
        },
        all_live,
    );
    let manager = MembershipChangeManager::new(inputs);

    let target: BTreeSet<NodeId> = ids.iter().copied().collect();
    manager
        .initiate(target.clone())
        .await
        .expect("initiate 3→5");

    // Phase 1: snapshots keep landing, but all below the bound. The change must
    // reach `JointDraining` and then refuse to finish.
    for watermark in [Hlc(100), Hlc(2500), Hlc(4999)] {
        for _ in 0..3 {
            let cur = leader_sys.raft_state.snapshot();
            leader_sys
                .raft
                .client_write(zslg_raft::FencedStateWrite {
                    expected_generation: cur.generation,
                    state: ReplicatedRaftState {
                        replica: cur.replica.clone(),
                        manifest: cur.manifest_state.manifest.clone(),
                        manifest_watermark: Some(watermark),
                        hlc_cutoff: Some(watermark),
                    },
                })
                .await
                .ok();
            let outcome = manager.tick().await.expect("tick");
            assert_ne!(
                outcome,
                TickOutcome::Complete,
                "joint config collapsed at watermark {watermark:?}, below the drain bound \
                 {OLD_SET_MAX_HLC:?} — writes held only by the outgoing set would be lost"
            );
        }
        let state = leader_sys.raft_state.snapshot();
        assert!(
            state.replica.is_joint(),
            "must still be joint at watermark {watermark:?}"
        );
    }
    // The bound was armed from the sample, not invented.
    assert_eq!(
        leader_sys.raft_state.snapshot().replica.exit(),
        Some(Exit::WatermarkAbove(OLD_SET_MAX_HLC)),
        "step 2b must commit the sampled bound"
    );

    // Phase 2: a snapshot finally covers the bound, and the change completes.
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(10))
        .expect("deadline");
    loop {
        let cur = leader_sys.raft_state.snapshot();
        leader_sys
            .raft
            .client_write(zslg_raft::FencedStateWrite {
                expected_generation: cur.generation,
                state: ReplicatedRaftState {
                    replica: cur.replica.clone(),
                    manifest: cur.manifest_state.manifest.clone(),
                    manifest_watermark: Some(OLD_SET_MAX_HLC),
                    hlc_cutoff: Some(OLD_SET_MAX_HLC),
                },
            })
            .await
            .ok();
        if matches!(manager.tick().await.expect("tick"), TickOutcome::Complete) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "membership change did not complete once the drain bound was covered"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let final_state = leader_sys.raft_state.snapshot();
    assert_eq!(final_state.replica.current().clone(), target);
    assert!(
        !final_state.replica.is_joint(),
        "joint config must be cleared"
    );

    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}

#[tokio::test]
async fn resume_from_joint_after_leader_killed_mid_step() {
    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, n2, _, _, _] = ids;

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
    wait_until(
        "n2 observes initial set",
        Duration::from_secs(5),
        || async {
            observer
                .observe(n2)
                .await
                .map(|s| *s.raft_state.replica.current() == initial)
                .unwrap_or(false)
        },
    )
    .await;

    let target: BTreeSet<NodeId> = ids.iter().copied().collect();

    zslg_raft_setup::testing::write_state_fenced(
        &leader_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::joint(initial.clone(), target.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("set joint");

    wait_until(
        "n2 observes joint config",
        Duration::from_secs(5),
        || async {
            observer
                .observe(n2)
                .await
                .map(|s| s.raft_state.replica.is_joint())
                .unwrap_or(false)
        },
    )
    .await;

    let n1_sys = nodes.remove(&n1).expect("n1");
    let _ = n1_sys.raft.shutdown().await;

    let new_leader = wait_for_any_leader(&nodes, Duration::from_secs(15)).await;
    let new_sys = nodes.get(&new_leader).expect("new leader");

    let remaining_live: BTreeSet<NodeId> = nodes.keys().copied().collect();
    let (inputs, _integrator_cancel) = build_inputs(
        new_leader,
        new_sys,
        {
            let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
            arc
        },
        remaining_live,
    );
    let manager = MembershipChangeManager::new(inputs);

    {
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(10))
            .expect("deadline");
        let mut hlc_counter = 0u64;
        loop {
            hlc_counter = hlc_counter.saturating_add(100);
            let cur = new_sys.raft_state.snapshot();
            new_sys
                .raft
                .client_write(zslg_raft::FencedStateWrite {
                    expected_generation: cur.generation,
                    state: ReplicatedRaftState {
                        replica: cur.replica.clone(),
                        manifest: cur.manifest_state.manifest.clone(),
                        manifest_watermark: Some(Hlc(hlc_counter)),
                        hlc_cutoff: cur.manifest_state.hlc_cutoff,
                    },
                })
                .await
                .ok();
            if matches!(manager.tick().await.expect("tick"), TickOutcome::Complete) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "membership change (resume) did not complete within deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let final_state = new_sys.raft_state.snapshot();
    assert_eq!(final_state.replica.current().clone(), target);
    assert!(!final_state.replica.is_joint());

    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}
