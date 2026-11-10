//! The membership change manager driven over a real multi-node Raft: joint
//! transitions replicated to every node, retargets, reconciliation.
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

mod reconciliation;
mod retarget;
mod transition;

use std::collections::{BTreeMap, BTreeSet};

use std::sync::Arc;

use std::time::Duration;

use zslg_consensus::ConsensusError;

use zslg_consensus::RaftLeadership;

use async_trait::async_trait;

use parking_lot::Mutex as SyncMutex;

use tokio_util::sync::CancellationToken;

use testing_kit::wait_until;

use zslg_consensus::NodeId;

use zslg_consensus::RaftStateHandle;

use zslg_consensus::ReplicatedRaftState;

use zslg_consensus::SpareSelectionStrategy;

use zslg_consensus::{Exit, ReplicaMembership, ReplicaStatus};

use zslg_leader_membership::{LivenessQuery, MembershipChangeInputs, MembershipChangeManager};

use zslg_leader_membership::{MembershipDriver, MembershipObserver, TickOutcome};

use zslg_net::ServerName;

use zslg_raft_setup::RaftNode;

use zslg_raft_setup::testing::{
    fast_raft_config as raft_config, wait_for_any_leader, wait_for_leader,
};

use zslg_time::Hlc;

pub(super) struct LocalObserver {
    states: SyncMutex<BTreeMap<NodeId, RaftStateHandle>>,
    /// Per-node replica-log `max_hlc`, the step-2b drain-bound sample. Absent
    /// (the default) models a replica whose log is empty.
    max_hlc: SyncMutex<BTreeMap<NodeId, Option<Hlc>>>,
}

impl LocalObserver {
    fn from_systems(nodes: &BTreeMap<NodeId, zslg_raft_setup::testing::TestNode>) -> Arc<Self> {
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
    async fn observe(&self, peer: NodeId) -> Result<ReplicaStatus, zslg_leader_membership::Error> {
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
            None => Err(zslg_leader_membership::Error::Consensus(
                ConsensusError::Raft(format!("peer {peer:?} not registered")),
            )),
        }
    }
}

pub(super) struct StaticLiveness(BTreeSet<NodeId>);

impl LivenessQuery for StaticLiveness {
    fn candidates(&self) -> std::collections::BTreeMap<NodeId, zslg_consensus::CandidateInfo> {
        self.0
            .iter()
            .map(|&node_id| {
                (
                    node_id,
                    zslg_consensus::CandidateInfo { node_id, rtt: None },
                )
            })
            .collect()
    }
}

/// A [`RaftLeadership`] whose voter set is whatever the test says it is.
///
/// The real `RaftNode` reports openraft's voters, which in these tests always
/// equals the live set — so it cannot express the case reconciliation exists
/// for: a node still running and still probed, but no longer a voter.
pub(super) struct StaticLeadership {
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

    async fn remove_member(
        &self,
        _node_id: zslg_consensus::NodeId,
    ) -> Result<(), zslg_consensus::Error> {
        Ok(())
    }
}

pub(super) fn build_inputs(
    self_id: NodeId,
    sys: &zslg_raft_setup::testing::TestNode,
    observer: Arc<dyn MembershipObserver>,
    live: BTreeSet<NodeId>,
) -> (MembershipChangeInputs, CancellationToken) {
    build_inputs_with_replica_count(self_id, sys, observer, live, 5)
}

pub(super) fn build_inputs_with_replica_count(
    self_id: NodeId,
    sys: &zslg_raft_setup::testing::TestNode,
    observer: Arc<dyn MembershipObserver>,
    live: BTreeSet<NodeId>,
    replica_node_count: u32,
) -> (MembershipChangeInputs, CancellationToken) {
    // A real `RaftNode` leadership: its voter set is openraft's, which in these
    // tests is every node, so the reconciliation filter is a no-op here and
    // every pre-existing test keeps its original meaning.
    let leadership = Arc::new(RaftNode::new(
        sys.raft.clone(),
        zslg_consensus_fake::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));
    build_inputs_with_leadership(self_id, sys, observer, live, replica_node_count, leadership)
}

pub(super) fn build_inputs_with_leadership(
    self_id: NodeId,
    sys: &zslg_raft_setup::testing::TestNode,
    observer: Arc<dyn MembershipObserver>,
    live: BTreeSet<NodeId>,
    replica_node_count: u32,
    leadership: Arc<dyn RaftLeadership>,
) -> (MembershipChangeInputs, CancellationToken) {
    // The real publish path: every membership step goes through the integrator,
    // so these tests exercise its transition validation too.
    let (intents, integrator_cancel) = crate::Integrator::spawn_for_test(
        Arc::new(RaftNode::new(
            sys.raft.clone(),
            zslg_consensus_fake::permissive_gate(),
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

pub(super) fn counter_value(snapshotter: &metrics_util::debugging::Snapshotter, name: &str) -> u64 {
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
