//! The startup decision procedure: one gate on durable facts, then one phase
//! per outcome.
//!
//! ```text
//!   has_local_state  in_membership   fence  is_bootstrap_node   decision
//!   ───────────────  ─────────────   ─────  ─────────────────   ─────────────
//!        yes             yes          any         any           Rejoin
//!        yes             no           any         any           HardCrashRestart
//!        no              —            yes         any           JoinExisting
//!        no              —            no          yes           Bootstrap
//!        no              —            no          no            JoinExisting
//! ```
//!
//! Local durable state dominates: it means this node was part of the cluster,
//! and membership decides whether it resumes (`Rejoin`) or was wiped and must
//! rejoin under a fresh `NodeId` (`HardCrashRestart`, CLAUDE.md §8). Without
//! state, the object-store bootstrap fence is the "is the cluster initialized?"
//! check; only the designated bootstrap node may start it.
//!
//! Startup is **intentionally unbounded**: a full-cluster cold start must wait
//! for a quorum of the original nodes to return, however long that takes —
//! bounding it made nodes exit and crash-loop, which *prevented* the quorum
//! from assembling (`docs/LIVENESS_REVIEW.md` §2). Every phase
//! ([`run_phase`]) escalates its logging the longer it is blocked but never
//! aborts; only the inner future's own `Err` propagates.
//!
//! `Rejoin` and `JoinExisting` both end at the same gate ([`wait_for_raft_recent`]):
//! the node is not "started" until it has reached a quorum and caught up to
//! that quorum's applied generation.
use futures::FutureExt as _;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use zslg_consensus::ConsensusError;
use zslg_consensus::RaftStateView;
use zslg_lifecycle_types::LifecycleError;
use zslg_net::{PeersWatcher, Router};
use zslg_snapshot::SnapshotStore;

use crate::rejoin::{
    NodeView, REJOIN_LEADERLESS_GRACE, REJOIN_RECONCILE_INTERVAL, RejoinMonitor,
    RejoinMonitorInputs, RejoinProbe, RpcRejoinProbe,
};

use crate::Error;
use shutdown::Shutdown;
use zslg_consensus::EvictionStatus;
use zslg_consensus::NodeId;
use zslg_consensus::ReplicaMembership;
use zslg_consensus::ReplicatedRaftState;
use zslg_net::PeerAddr;
use zslg_raft::FencedStateWrite;
use zslg_raft::{ClusterBootstrap, membership_map};

mod decision;
mod join;
mod wait;

pub(crate) use self::decision::{
    BootstrapDecision, BootstrapInputs, bootstrap_decision, resolve_bootstrap_fence,
};
pub(crate) use self::join::{
    JOIN_INITIAL_BACKOFF, JOIN_MAX_BACKOFF, JoinAttempt, attempt_join_once, join_existing,
};
pub(crate) use self::wait::{
    STARTUP_WARN_INTERVAL, run_phase, startup_warn_ticker, wait_for_raft_recent,
};

/// Config-only fields for `run_raft_startup`. Active handles are passed as
/// separate parameters — this struct carries only tunables and identity.
/// Produced by `ReplicaNodeConfig::raft_startup_config`.
pub struct RaftStartupConfig {
    pub node_id: NodeId,
    pub peers_watcher: Arc<dyn PeersWatcher>,
    pub is_bootstrap_node: bool,
    pub has_local_state: bool,
    pub self_peer_addr: PeerAddr,
    pub startup_phase_timeout: Duration,
}

pub async fn run_raft_startup<E>(
    raft: Arc<dyn ClusterBootstrap>,
    raft_state: Arc<dyn RaftStateView>,
    snapshot_store: Arc<dyn SnapshotStore>,
    router: Arc<dyn Router>,
    RaftStartupConfig {
        node_id,
        peers_watcher,
        is_bootstrap_node,
        has_local_state,
        self_peer_addr,
        startup_phase_timeout,
    }: RaftStartupConfig,
    shutdown: &mut Shutdown<E>,
) -> Result<EvictionStatus, Error>
where
    E: From<Error> + From<shutdown::Stopped> + std::fmt::Display + Send + 'static,
{
    let fence_present = run_phase(
        "fence_check",
        startup_phase_timeout,
        resolve_bootstrap_fence(&snapshot_store).boxed(),
    )
    .await?;
    let in_membership = if has_local_state {
        run_phase(
            "membership_load",
            startup_phase_timeout,
            async {
                let metrics = {
                    let mut rx = raft.metrics();
                    let started = Instant::now();
                    let mut warn_tick = startup_warn_ticker();
                    loop {
                        {
                            let m = rx.borrow();
                            if m.membership_config
                                .membership()
                                .voter_ids()
                                .next()
                                .is_some()
                            {
                                break m.clone();
                            }
                        }
                        tokio::select! {
                            changed = rx.changed() => {
                                if changed.is_err() {
                                    break rx.borrow().clone();
                                }
                            }
                            _ = warn_tick.tick() => warn!(
                                elapsed_s = started.elapsed().as_secs(),
                                "raft startup: waiting for local raft membership to load \
                                 (openraft reports no voters yet)"
                            ),
                        }
                    }
                };
                let voters: BTreeSet<NodeId> =
                    metrics.membership_config.membership().voter_ids().collect();
                Ok::<bool, Error>(voters.contains(&node_id))
            }
            .boxed(),
        )
        .await?
    } else {
        false
    };

    // Published by the monitor below, consulted by
    // `learner::read::can_serve_clients`; the caller of `run_raft_startup`
    // keeps this clone to hand to the reader gate.
    let eviction = EvictionStatus::new();
    let probe: Arc<dyn RejoinProbe> = Arc::new(RpcRejoinProbe::new(
        router.clone(),
        peers_watcher.clone(),
        node_id,
        self_peer_addr.clone(),
    ));
    let monitor = RejoinMonitor::new(RejoinMonitorInputs {
        self_node_id: node_id,
        view: Arc::new(NodeView(raft.clone())),
        probe,
        reconcile_interval: REJOIN_RECONCILE_INTERVAL,
        leaderless_grace: REJOIN_LEADERLESS_GRACE,
        eviction: eviction.clone(),
    });
    let cancel = shutdown.token();
    shutdown.spawn("raft:bootstrap_monitor", async move {
        monitor.run(cancel).await.map_err(E::from)
    });
    info!("rejoin monitor spawned");

    let decision = bootstrap_decision(BootstrapInputs {
        has_local_state,
        fence_present,
        is_bootstrap_node,
        in_membership,
    });
    // warn (not info): a rare, pivotal startup event that must be visible at the
    // harness default RUST_LOG=warn. `has_local_state=false` + `fence_present`
    // across all nodes is the unrecoverable case (empty state behind a stale
    // bootstrap fence — e.g. a db-path rename) that otherwise hangs silently.
    warn!(
        ?node_id,
        has_local_state,
        fence_present,
        is_bootstrap_node,
        in_membership,
        ?decision,
        "raft bootstrap decision",
    );

    let outcome: Result<(), Error> = match decision {
        BootstrapDecision::Bootstrap => {
            // Sole, self-elected leader — trivially current, no catch-up gate.
            info!("bootstrap: initializing cluster");
            run_phase(
                "bootstrap_init",
                startup_phase_timeout,
                bootstrap_single_node_cluster(
                    node_id,
                    raft.clone(),
                    snapshot_store,
                    self_peer_addr,
                )
                .boxed(),
            )
            .await
        }
        BootstrapDecision::Rejoin => {
            info!(
                ?node_id,
                "bootstrap: rejoin — durable state resumes openraft"
            );
            // Durable state may be arbitrarily stale — reach out to a quorum to
            // learn how recent the cluster is and catch up before reporting ready.
            wait_for_raft_recent(raft, router, raft_state, node_id, startup_phase_timeout)
                .boxed()
                .await
        }
        BootstrapDecision::JoinExisting => {
            if !has_local_state {
                crate::metrics::node_rejoin();
            }
            info!("bootstrap: join existing — contacting peers");
            run_phase(
                "join_existing",
                startup_phase_timeout,
                join_existing(&*router, node_id, &peers_watcher, &self_peer_addr).boxed(),
            )
            .await?;
            // Same quorum gate as rejoin: don't report ready until the node can
            // actually reach a quorum (and has caught up to it) — see
            // `wait_for_raft_recent` for why a fresh joiner converges unaided.
            wait_for_raft_recent(raft, router, raft_state, node_id, startup_phase_timeout)
                .boxed()
                .await
        }
        BootstrapDecision::HardCrashRestart => {
            // Deliberately no counter here. This path returns an error and the
            // node refuses to start, so a metric emitted now is only exported if
            // a scrape happens to land in the window before the process exits —
            // unreliable by construction. The log line below plus the node's
            // absence are the signals an operator actually gets.
            error!(
                ?node_id,
                "bootstrap: hard crash data loss detected — this node holds durable \
                 state but is not in the cluster membership. Wipe the data directory \
                 and restart with a new NodeId (CLAUDE.md §8); reusing the old id is \
                 a safety violation",
            );
            Err(Error::Lifecycle(LifecycleError::HardCrashDataLoss))
        }
    };
    outcome.map(|()| eviction)
}

async fn bootstrap_single_node_cluster(
    node_id: NodeId,
    raft: Arc<dyn ClusterBootstrap>,
    snapshot_store: Arc<dyn SnapshotStore>,
    self_peer_addr: PeerAddr,
) -> Result<(), Error> {
    // Uncancellable by construction: once the fence is durably written, this
    // node MUST also reach `initialize` + self-election + the first
    // `client_write`, or a caller that drops this future mid-sequence (the
    // startup `tokio::select!` in `InitialisingReplicaNode::into_ready` does
    // exactly that on cancellation) leaves the fence object durable while no
    // node holds durable membership. Every future restart then reads
    // `has_local_state=false, fence_present=true` and decides `JoinExisting`
    // — including the bootstrap node itself — so the cluster can never
    // initialise (CLAUDE.md §11: the fence is the sole "is the cluster
    // initialized?" check). `tokio::spawn` decouples this sequence's lifetime
    // from the awaiting future: dropping the `.await` below does not abort
    // the spawned task, so the sequence always runs to completion.
    tokio::spawn(async move {
        snapshot_store.write_bootstrap_fence().boxed().await?;
        raft.initialize(membership_map([(node_id, self_peer_addr)]))
            .boxed()
            .await?;

        raft.wait_leader(node_id, "bootstrap: wait for self-election")
            .boxed()
            .await?;

        let initial: BTreeSet<NodeId> = [node_id].into_iter().collect();
        raft.write_fenced(FencedStateWrite {
            expected_generation: zslg_consensus::RaftGeneration::ZERO,
            state: ReplicatedRaftState {
                replica: ReplicaMembership::single(initial),
                manifest: Vec::new(),
                manifest_watermark: None,
                hlc_cutoff: None,
            },
        })
        .boxed()
        .await?;
        Ok::<(), Error>(())
    })
    .await
    .map_err(|e| {
        Error::Consensus(ConsensusError::Raft(format!(
            "bootstrap: single-node init task panicked: {e}"
        )))
    })?
}

#[cfg(test)]
mod tests;
