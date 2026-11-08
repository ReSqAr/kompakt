//! Leader-only background work: six jobs, one tokio task each, and one writer.
//!
//! This crate is the composition root — it hands each job crate its slice of
//! the node's config and the handles it reads and writes through. The jobs live
//! in `zslg-leader-{snapshot,membership,compaction}` and the write path in
//! `zslg-leader-integrator`.
//!
//! The jobs are concurrent, not serialized, because the dependency between them
//! is cyclic: the membership drain waits on snapshot progress (§18), so running
//! one job at a time can deadlock. Safety therefore sits at the write point —
//! see the integrator.
//!
//! ```text
//!   cutoff       timer                    ─┐
//!   snapshot     cutoff > watermark        │
//!   compaction   timer                     ├─ intents ──> integrator ──> raft
//!   cleanup      timer                     │              (single writer)
//!   membership   raft state + liveness     │
//!   voters       timer + peer config      ─┘
//! ```
//!
//! Three properties are why there is no planner and no lock:
//!
//! 1. **A job never overlaps itself** — one task, one serial loop, no
//!    per-iteration spawn. While working it is not watching its trigger.
//! 2. **Triggers are predicates over current state, not deltas** — every wake-up
//!    re-reads current state. The trigger channels are `watch`es, so that is
//!    enforced rather than merely intended: a wake-up carries the level, never a
//!    queue of edges to drain, and one missed while busy is not lost.
//! 3. **Jobs never write Raft state** — they submit intents to the integrator,
//!    where the invariants are enforced.
//!
//! Every job gates on `RaftLeadership::is_leader` and stops acting on demotion,
//! so leadership can change without a task being spawned or stopped.
//!
//! Pacing: the timer-driven jobs rate-limit themselves via `MissedTickBehavior::
//! Skip`. `snapshot` and `membership` do not — they re-evaluate immediately, and
//! carry an explicit `pace_kit::Pace` floor. Each states its own reason at its
//! constant; the reasons differ.

mod config;

#[cfg(test)]
mod pacing_tests;

use std::sync::Arc;
use std::time::Duration;

use shutdown::Shutdown;
use zslg_consensus::{NodeId, RaftLeadership, RaftStateView, StateReplicator};
use zslg_log::{LocalStoreView, StableLogSnapshot};
use zslg_net::{PeersWatcher, Router, ServerName};
use zslg_snapshot::SnapshotStore;
use zslg_time::Clock;

pub use config::SnapshotConfig;
pub use zslg_leader_integrator::Integrator;
pub use zslg_leader_membership::{LivenessConfig, MembershipConfig};

/// The leader-loop config bundle.
pub struct LeaderLoopConfig {
    pub snapshot: SnapshotConfig,
    pub membership: MembershipConfig,
    pub liveness: LivenessConfig,
    /// Poll interval for periodic snapshot and membership steps.
    pub poll_interval: Duration,
    /// Timeout for waiting on quorum responses during a snapshot build.
    pub quorum_wait_timeout: Duration,
    /// Watcher providing the expected Raft member list.
    pub peers_watcher: Arc<dyn PeersWatcher>,
    /// The local node's server name, included in the allowed-voter set alongside
    /// the peer list.
    pub self_server_name: ServerName,
    /// The durable node id of the local node — filled by the producer.
    pub self_node_id: NodeId,
}

/// Spawn every leader task on the shutdown tree.
///
/// All of them are spawned unconditionally and each gates on leadership
/// internally, so leadership can change without any task being started or
/// stopped.
#[allow(
    clippy::too_many_arguments,
    reason = "the leader wires every handle at spawn; the single config arg already bundles tunings"
)]
pub fn spawn_leader_loop<E>(
    shutdown: &mut Shutdown<E>,
    clock: Arc<dyn Clock>,
    local_store: Arc<dyn LocalStoreView>,
    stable_log: Arc<dyn StableLogSnapshot>,
    snapshot_store: Arc<dyn SnapshotStore>,
    leadership: Arc<dyn RaftLeadership>,
    state_replicator: Arc<dyn StateReplicator>,
    raft_state: Arc<dyn RaftStateView>,
    router: Arc<dyn Router>,
    config: LeaderLoopConfig,
) where
    E: From<zslg_leader_integrator::Error>
        + From<zslg_leader_snapshot::Error>
        + From<zslg_leader_membership::Error>
        + From<zslg_leader_compaction::Error>
        + From<shutdown::Stopped>
        + std::fmt::Display
        + Send
        + 'static,
{
    // The integrator first: every other job holds a submitter for it.
    let (integrator, intents) = Integrator::new(state_replicator, raft_state.clone());
    shutdown.spawn("leader:integrator", integrator.run(shutdown.token()));

    let self_node_id = config.self_node_id;
    zslg_leader_snapshot::spawn_cutoff(
        shutdown,
        zslg_leader_snapshot::CutoffHandles {
            self_node_id,
            clock,
            leadership: leadership.clone(),
            raft_state: raft_state.clone(),
            intents: intents.clone(),
        },
        zslg_leader_snapshot::CutoffConfig {
            cutoff_lag: config.snapshot.cutoff_timeout,
            interval: config.snapshot.interval,
        },
    );
    zslg_leader_snapshot::spawn_snapshot(
        shutdown,
        zslg_leader_snapshot::SnapshotHandles {
            self_node_id,
            local_store,
            stable_log,
            snapshot_store: snapshot_store.clone(),
            raft_state: raft_state.clone(),
            router: router.clone(),
            leadership: leadership.clone(),
            intents: intents.clone(),
        },
        zslg_leader_snapshot::SnapshotJobConfig {
            quorum_poll_interval: config.poll_interval,
            quorum_wait_timeout: config.quorum_wait_timeout,
            compression: config.snapshot.compression,
            max_snapshot_entries: config.snapshot.max_snapshot_entries,
        },
    );
    zslg_leader_compaction::spawn(
        shutdown,
        zslg_leader_compaction::CompactionHandles {
            self_node_id,
            snapshot_store: snapshot_store.clone(),
            raft_state: raft_state.clone(),
            leadership: leadership.clone(),
            intents: intents.clone(),
        },
        zslg_leader_compaction::CompactionConfig {
            tiers: config.snapshot.compaction_tiers.clone(),
            compression: config.snapshot.compression,
            interval: config.snapshot.compaction_interval,
        },
    );
    zslg_leader_snapshot::spawn_cleanup(
        shutdown,
        zslg_leader_snapshot::CleanupHandles {
            self_node_id,
            snapshot_store,
            leadership: leadership.clone(),
            raft_state: raft_state.clone(),
        },
        zslg_leader_snapshot::CleanupConfig {
            retention: config.snapshot.cleanup_retention,
            interval: config.snapshot.cleanup_interval,
        },
    );
    zslg_leader_membership::spawn_membership(
        shutdown,
        zslg_leader_membership::MembershipHandles {
            self_node_id,
            raft_state: raft_state.clone(),
            leadership: leadership.clone(),
            router: router.clone(),
            intents,
        },
        &config.membership,
        &config.liveness,
    );
    zslg_leader_membership::spawn_voters(
        shutdown,
        zslg_leader_membership::VoterHandles {
            self_node_id,
            leadership,
            router,
        },
        &config.membership,
        config.self_server_name,
        config.peers_watcher,
    );
}

error_union::error_union! {
    /// What the leader assembly can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(zslg_lifecycle_types::LifecycleError),
        Config(zslg_lifecycle_types::ConfigError),
        Clock(zslg_time::ClockError),
        Log(zslg_log::LogError),
        Snapshot(zslg_snapshot::SnapshotError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    into zslg_log::Error
    into zslg_snapshot::Error
    into zslg_consensus::Error
    into zslg_error::Error
}
