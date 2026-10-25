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
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex as SyncMutex;
use tokio_util::sync::CancellationToken;

use crate::Error;
use crate::api::{RaftLeadership, ReplicatedRaftState};
use crate::hlc::Hlc;
use crate::ids::{NodeId, ServerName};
use crate::leader::jobs::membership::change::{
    LivenessQuery, MembershipChangeInputs, MembershipChangeManager, needs_retarget,
};
use crate::leader::jobs::membership::{MembershipDriver, MembershipObserver, TickOutcome};
use crate::meta::{Exit, ReplicaMembership, ReplicaStatus};
use crate::raft::SpareSelectionStrategy;
use crate::raft::{RaftNode, RaftStateHandle};
use crate::tests::{
    fast_raft_config as raft_config, wait_for_any_leader, wait_for_leader, wait_until,
};

struct LocalObserver {
    states: SyncMutex<BTreeMap<NodeId, RaftStateHandle>>,
    /// Per-node replica-log `max_hlc`, the step-2b drain-bound sample. Absent
    /// (the default) models a replica whose log is empty.
    max_hlc: SyncMutex<BTreeMap<NodeId, Option<Hlc>>>,
}

impl LocalObserver {
    fn from_systems(nodes: &BTreeMap<NodeId, crate::tests::TestNode>) -> Arc<Self> {
        let mut map = BTreeMap::new();
        for (id, sys) in nodes {
            map.insert(*id, sys.raft_state.clone());
        }
        Arc::new(Self {
            states: SyncMutex::new(map),
            max_hlc: SyncMutex::new(BTreeMap::new()),
        })
    }

    /// Give every listed node the same replica-log `max_hlc`, i.e. model a set
    /// that holds writes up to `hlc`.
    fn set_max_hlc(&self, nodes: &BTreeSet<NodeId>, hlc: Hlc) {
        let mut guard = self.max_hlc.lock();
        for id in nodes {
            guard.insert(*id, Some(hlc));
        }
    }
}

#[async_trait]
impl MembershipObserver for LocalObserver {
    async fn observe(&self, peer: NodeId) -> Result<ReplicaStatus, crate::Error> {
        let states = self.states.lock();
        match states.get(&peer) {
            Some(apply) => Ok(ReplicaStatus {
                node_id: peer,
                frontier: None,
                raft_state: (*apply.snapshot()).clone(),
                effective_cutoff: None,
                // Empty log: contributes no drain bound, but still counts
                // toward the step-2b quorum. Tests that need a bound set it
                // through `max_hlc`.
                max_hlc: self.max_hlc.lock().get(&peer).copied().flatten(),
                peer_names: Vec::new(),
            }),
            None => Err(Error::Raft(format!("peer {peer:?} not registered"))),
        }
    }
}

struct StaticLiveness(BTreeSet<NodeId>);

impl LivenessQuery for StaticLiveness {
    fn candidates(&self) -> std::collections::BTreeMap<NodeId, crate::api::CandidateInfo> {
        self.0
            .iter()
            .map(|&node_id| (node_id, crate::api::CandidateInfo { node_id, rtt: None }))
            .collect()
    }
}

/// A [`RaftLeadership`] whose voter set is whatever the test says it is.
///
/// The real `RaftNode` reports openraft's voters, which in these tests always
/// equals the live set — so it cannot express the case reconciliation exists
/// for: a node still running and still probed, but no longer a voter.
struct StaticLeadership {
    leader: SyncMutex<Option<NodeId>>,
    voters: SyncMutex<BTreeSet<NodeId>>,
}

impl StaticLeadership {
    fn new(leader: NodeId, voters: BTreeSet<NodeId>) -> Self {
        Self {
            leader: SyncMutex::new(Some(leader)),
            voters: SyncMutex::new(voters),
        }
    }
}

#[async_trait]
impl RaftLeadership for StaticLeadership {
    fn current_leader(&self) -> Option<NodeId> {
        *self.leader.lock()
    }

    fn voter_ids(&self) -> BTreeSet<NodeId> {
        self.voters.lock().clone()
    }

    fn voter_server_names(&self) -> Vec<(NodeId, ServerName)> {
        Vec::new()
    }

    async fn remove_member(&self, _node_id: NodeId) -> Result<(), crate::Error> {
        Ok(())
    }
}

fn build_inputs(
    self_id: NodeId,
    sys: &crate::tests::TestNode,
    observer: Arc<dyn MembershipObserver>,
    live: BTreeSet<NodeId>,
) -> (MembershipChangeInputs, CancellationToken) {
    build_inputs_with_replica_count(self_id, sys, observer, live, 5)
}

fn build_inputs_with_replica_count(
    self_id: NodeId,
    sys: &crate::tests::TestNode,
    observer: Arc<dyn MembershipObserver>,
    live: BTreeSet<NodeId>,
    replica_node_count: u32,
) -> (MembershipChangeInputs, CancellationToken) {
    // A real `RaftNode` leadership: its voter set is openraft's, which in these
    // tests is every node, so the reconciliation filter is a no-op here and
    // every pre-existing test keeps its original meaning.
    let leadership = Arc::new(RaftNode::new(
        sys.raft.clone(),
        crate::network::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));
    build_inputs_with_leadership(self_id, sys, observer, live, replica_node_count, leadership)
}

fn build_inputs_with_leadership(
    self_id: NodeId,
    sys: &crate::tests::TestNode,
    observer: Arc<dyn MembershipObserver>,
    live: BTreeSet<NodeId>,
    replica_node_count: u32,
    leadership: Arc<dyn RaftLeadership>,
) -> (MembershipChangeInputs, CancellationToken) {
    // The real publish path: every membership step goes through the integrator,
    // so these tests exercise its transition validation too.
    let (intents, integrator_cancel) = crate::leader::Integrator::spawn_for_test(
        Arc::new(RaftNode::new(
            sys.raft.clone(),
            crate::network::permissive_gate(),
            std::time::Duration::from_secs(30),
        )),
        Arc::new(sys.raft_state.clone()),
    );
    let inputs = MembershipChangeInputs {
        self_node_id: self_id,
        intents,
        leadership,
        raft_state: Arc::new(sys.raft_state.clone()),
        observer,
        replica_node_count,
        spare_selection: SpareSelectionStrategy::Random,
        live_nodes: Arc::new(StaticLiveness(live)),
    };
    (inputs, integrator_cancel)
}

#[tokio::test]
async fn three_to_five_full_transition_via_state_handle() {
    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, _, _, _, _] = ids;

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;

    let initial: BTreeSet<NodeId> = ids[..3].iter().copied().collect();
    crate::tests::write_state_fenced(
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
                .client_write(crate::raft::FencedStateWrite {
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

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;

    let initial: BTreeSet<NodeId> = ids[..3].iter().copied().collect();
    crate::tests::write_state_fenced(
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
                .client_write(crate::raft::FencedStateWrite {
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
            .client_write(crate::raft::FencedStateWrite {
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

/// A retarget must drop the armed bound (`JointDraining` → `Joint`), because
/// `exit_hlc` attests a generation quorum for the *old* incoming set. The new
/// incoming set has to earn its own.
#[tokio::test]
async fn retarget_disarms_the_exit_condition_and_rearms_monotonically() {
    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, _, _, _, _] = ids;

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;

    let initial: BTreeSet<NodeId> = ids[..3].iter().copied().collect();
    crate::tests::write_state_fenced(
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

#[tokio::test]
async fn resume_from_joint_after_leader_killed_mid_step() {
    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, n2, _, _, _] = ids;

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;

    let initial: BTreeSet<NodeId> = ids[..3].iter().copied().collect();
    crate::tests::write_state_fenced(
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

    crate::tests::write_state_fenced(
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
                .client_write(crate::raft::FencedStateWrite {
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

/// Whether the incoming set of a joint config must be recomputed because it is
/// no longer fully live. Split out from `maybe_initiate` so the retarget
/// decision — the one that discards step 2 — is assertable without a metrics
/// recorder.
#[test]
fn retarget_is_needed_only_when_an_incoming_member_is_not_live() {
    let next: BTreeSet<NodeId> = [crate::tests::nid(3), crate::tests::nid(4)]
        .into_iter()
        .collect();
    let all_live: BTreeSet<NodeId> = [
        crate::tests::nid(1),
        crate::tests::nid(3),
        crate::tests::nid(4),
    ]
    .into_iter()
    .collect();
    let one_dead: BTreeSet<NodeId> = [crate::tests::nid(1), crate::tests::nid(3)]
        .into_iter()
        .collect();
    assert!(!needs_retarget(&next, &all_live));
    assert!(needs_retarget(&next, &one_dead));
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

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;

    let initial: BTreeSet<NodeId> = ids[..3].iter().copied().collect();
    crate::tests::write_state_fenced(
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

fn counter_value(snapshotter: &metrics_util::debugging::Snapshotter, name: &str) -> u64 {
    let snap = snapshotter.snapshot();
    let map = snap.into_hashmap();
    map.into_iter()
        .find(|(ck, _)| ck.key().name() == name)
        .map(|(_, (_, _, value))| match value {
            metrics_util::debugging::DebugValue::Counter(v) => v,
            other => panic!("expected counter, got {other:?}"),
        })
        .unwrap_or(0)
}

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

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;
    crate::tests::write_state_fenced(
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

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;
    crate::tests::write_state_fenced(
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

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(crate::tests::test_membership_map(&ids))
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
    crate::tests::write_state_fenced(
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
    crate::tests::write_state_fenced(
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
