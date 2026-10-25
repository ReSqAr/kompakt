//! Leader-only background work: six jobs, one tokio task each, and one writer.
//!
//! This module is the composition root — it builds the handle bundle and hands
//! each job its config. The jobs and the properties that follow from "one task
//! per job" are in [`jobs`]; the write path is in [`integrator`].
//!
//! The jobs are concurrent, not serialized, because the dependency between them
//! is cyclic: the membership drain waits on snapshot progress (§18), so running
//! one job at a time can deadlock. Safety therefore sits at the write point —
//! see [`integrator`].

mod integrator;
pub(crate) mod jobs;

#[cfg(test)]
mod jobs_tests;

#[cfg(test)]
mod leadership_churn_tests;

use crate::leader::jobs::LeaderHandles;
pub(super) use crate::leader::jobs::membership::liveness::LivenessConfig;
pub(super) use crate::leader::jobs::snapshot::SnapshotConfig;
pub(in crate::leader) use integrator::{Integrator, IntentOutcome, IntentSubmitter, RaftIntent};
pub use jobs::compaction::merge::CompactionTier;

#[cfg(test)]
pub(crate) use jobs::snapshot::create::SnapshotResult;
#[cfg(test)]
pub(in crate::leader) use jobs::snapshot::create::{SnapshotInputs, build_snapshot};

use std::sync::Arc;
use std::time::Duration;

use crate::api::PeersWatcher;
use crate::api::{
    Clock, LocalStoreView, RaftLeadership, RaftStateView, Router, SnapshotStore, StableLogSnapshot,
    StateReplicator,
};
use crate::raft::MembershipConfig;
use crate::shutdown::Shutdown;

/// The leader-loop config bundle. Produced by
/// [`ReplicaNodeConfig::leader_loop_config`](crate::replica_node::ReplicaNodeConfig::leader_loop_config).
pub(super) struct LeaderLoopConfig {
    pub(super) snapshot: SnapshotConfig,
    pub(super) membership: MembershipConfig,
    pub(super) liveness: LivenessConfig,
    /// Poll interval for periodic snapshot and membership steps.
    pub(super) poll_interval: Duration,
    /// Timeout for waiting on quorum responses during a snapshot build.
    pub(super) quorum_wait_timeout: Duration,
    /// Watcher providing the expected Raft member list.
    pub(super) peers_watcher: Arc<dyn PeersWatcher>,
    /// The local node's server name, included in the allowed-voter set alongside
    /// the peer list.
    pub(super) self_server_name: crate::ids::ServerName,
    /// The durable node id of the local node — filled by the producer.
    pub(super) self_node_id: crate::ids::NodeId,
}

/// Spawn every leader task on the shutdown tree.
///
/// All of them are spawned unconditionally and each gates on `is_local_leader`
/// internally, so leadership can change without any task being started or
/// stopped.
#[allow(
    clippy::too_many_arguments,
    reason = "the leader wires every handle at spawn; the single config arg already bundles tunings"
)]
pub(super) fn spawn_leader_loop(
    shutdown: &mut Shutdown,
    clock: Arc<dyn Clock>,
    local_store: Arc<dyn LocalStoreView>,
    stable_log: Arc<dyn StableLogSnapshot>,
    snapshot_store: Arc<dyn SnapshotStore>,
    leadership: Arc<dyn RaftLeadership>,
    state_replicator: Arc<dyn StateReplicator>,
    raft_state: Arc<dyn RaftStateView>,
    router: Arc<dyn Router>,
    config: LeaderLoopConfig,
) {
    // The integrator first: every other job holds a submitter for it.
    let (integrator, intents) = Integrator::new(state_replicator, raft_state.clone());
    shutdown.spawn("leader:integrator", integrator.run(shutdown.token()));

    let handles = LeaderHandles {
        clock,
        local_store,
        stable_log,
        snapshot_store,
        leadership,
        raft_state,
        router,
        self_node_id: config.self_node_id,
        intents,
    };

    jobs::cutoff::spawn(shutdown, &handles, &config.snapshot);
    jobs::snapshot::spawn(
        shutdown,
        &handles,
        &config.snapshot,
        config.poll_interval,
        config.quorum_wait_timeout,
    );
    jobs::compaction::spawn(shutdown, &handles, &config.snapshot);
    jobs::cleanup::spawn(shutdown, &handles, &config.snapshot);
    jobs::membership::spawn(shutdown, &handles, &config.membership, &config.liveness);
    jobs::voters::spawn(
        shutdown,
        &handles,
        &config.membership,
        config.self_server_name,
        config.peers_watcher,
        Arc::new(jobs::membership::NetworkMembershipObserver::new(
            handles.router.clone(),
        )),
    );
}
