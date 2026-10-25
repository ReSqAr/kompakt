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
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use super::rejoin::{
    EvictionStatus, REJOIN_LEADERLESS_GRACE, REJOIN_RECONCILE_INTERVAL, RejoinMonitor,
    RejoinMonitorInputs, RejoinProbe, RpcRejoinProbe,
};
use crate::api::{
    AddMemberOutcome, PeersWatcher, RaftStateView, ReplicatedRaftState, Router, SnapshotStore,
};
use crate::error::{Error, ToRaft};
use crate::ids::{NodeId, PeerAddr, RaftGeneration};
use crate::meta::{RaftState, ReplicaMembership};
use crate::quorum::{FanOutResponse, fan_out_quorum};
use crate::raft::{FencedStateWrite, RaftNode, membership_map};
use crate::shutdown::Shutdown;

const JOIN_INITIAL_BACKOFF: Duration = Duration::from_millis(50);
const JOIN_MAX_BACKOFF: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Cadence for `warn!`ing that a raft startup convergence wait is still blocked,
/// so a stuck resume (e.g. all nodes empty behind a stale bootstrap fence) is
/// visible at the harness default `RUST_LOG=warn` instead of hanging silently.
const STARTUP_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// A periodic ticker whose first tick fires after `STARTUP_WARN_INTERVAL` (not
/// immediately), for warning inside a blocking startup wait.
#[expect(
    clippy::expect_used,
    reason = "now + 5s (STARTUP_WARN_INTERVAL) cannot overflow Instant"
)]
fn startup_warn_ticker() -> tokio::time::Interval {
    let start = tokio::time::Instant::now()
        .checked_add(STARTUP_WARN_INTERVAL)
        .expect("now + STARTUP_WARN_INTERVAL cannot overflow");
    tokio::time::interval_at(start, STARTUP_WARN_INTERVAL)
}

/// Run one startup phase, escalating log volume the longer it is blocked but
/// **never** aborting it — startup is intentionally unbounded (header). The
/// inner future's own `Err` still propagates (a real fault, e.g. shutdown or
/// `MembershipDenied`); only the time-based abort is gone. The caller may
/// bound total startup itself if it wants a hard cap. See
/// `RaftConfig::startup_phase_timeout`.
async fn run_phase<F, T>(phase: &'static str, escalate_after: Duration, fut: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    let started = Instant::now();
    tokio::pin!(fut);
    let mut escalate = tokio::time::interval_at(
        tokio::time::Instant::now()
            .checked_add(escalate_after)
            .unwrap_or_else(tokio::time::Instant::now),
        escalate_after,
    );
    escalate.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            res = &mut fut => return res,
            _ = escalate.tick() => error!(
                phase,
                elapsed_s = started.elapsed().as_secs(),
                "raft startup: phase still blocked (waiting for the cluster/quorum to become \
                 reachable — this is expected during a full-cluster cold start until a quorum \
                 of the original nodes returns; startup is intentionally unbounded)"
            ),
        }
    }
}

/// Read the object-store bootstrap fence, retrying forever on a transient
/// object-store error: during a full cold start the object store may itself
/// still be coming up, and a single failed read must not abort the node. A
/// persistent misconfiguration loops loudly rather than crash-looping —
/// consistent with unbounded startup (header).
async fn resolve_bootstrap_fence(snapshot_store: &Arc<dyn SnapshotStore>) -> Result<bool, Error> {
    let mut backoff = JOIN_INITIAL_BACKOFF;
    loop {
        match snapshot_store.has_bootstrap_fence().await {
            Ok(present) => return Ok(present),
            Err(e) => {
                warn!(
                    error = %e,
                    ?backoff,
                    "raft startup: bootstrap-fence check failed (object store not ready?); retrying"
                );
                let jittered = crate::jitter(backoff);
                tokio::time::sleep(jittered).await;
                backoff = backoff.saturating_mul(2).min(JOIN_MAX_BACKOFF);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::raft) struct BootstrapInputs {
    pub(in crate::raft) has_local_state: bool,
    pub(in crate::raft) fence_present: bool,
    pub(in crate::raft) is_bootstrap_node: bool,
    pub(in crate::raft) in_membership: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::raft) enum BootstrapDecision {
    Rejoin,
    JoinExisting,
    Bootstrap,
    HardCrashRestart,
}

#[must_use]
pub(in crate::raft) fn bootstrap_decision(
    BootstrapInputs {
        has_local_state,
        fence_present,
        is_bootstrap_node,
        in_membership,
    }: BootstrapInputs,
) -> BootstrapDecision {
    if has_local_state {
        if in_membership {
            BootstrapDecision::Rejoin
        } else {
            BootstrapDecision::HardCrashRestart
        }
    } else if fence_present {
        BootstrapDecision::JoinExisting
    } else if is_bootstrap_node {
        BootstrapDecision::Bootstrap
    } else {
        BootstrapDecision::JoinExisting
    }
}

/// Config-only fields for `run_raft_startup`. Active handles are passed as
/// separate parameters — this struct carries only tunables and identity.
/// Produced by [`ReplicaNodeConfig::raft_startup_config`].
pub(crate) struct RaftStartupConfig {
    pub(crate) node_id: NodeId,
    pub(crate) peers_watcher: Arc<dyn PeersWatcher>,
    pub(crate) is_bootstrap_node: bool,
    pub(crate) has_local_state: bool,
    pub(crate) self_peer_addr: PeerAddr,
    pub(crate) startup_phase_timeout: Duration,
}

pub(crate) async fn run_raft_startup(
    raft: Arc<RaftNode>,
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
    shutdown: &mut Shutdown,
) -> Result<EvictionStatus, Error> {
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
                    let mut rx = raft.inner().metrics();
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
        view: raft.clone(),
        probe,
        reconcile_interval: REJOIN_RECONCILE_INTERVAL,
        leaderless_grace: REJOIN_LEADERLESS_GRACE,
        eviction: eviction.clone(),
    });
    shutdown.spawn("raft:bootstrap_monitor", monitor.run(shutdown.token()));
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
            Err(Error::HardCrashDataLoss)
        }
    };
    outcome.map(|()| eviction)
}

/// Blocks until this node can reach a quorum and its state machine has caught
/// up to that quorum's applied generation — i.e. until it can actually serve
/// reads/writes — before `wait_until_ready` returns. Used by both Rejoin and
/// JoinExisting.
///
/// A fresh joiner starts with no outbound routes and a possibly-stale initial
/// snapshot, but converges with no action of its own: openraft's RaftCore
/// applies the leader's pushes in the background (updating the replica set) and
/// `PeerRegistrationWatcher` registers live peers' routes, so a later fan-out
/// attempt reaches a live quorum. [`quorum_applied_generation`] re-reads local
/// state each attempt and backs off, tolerating that transient.
///
/// # Guarantee
///
/// After return, the local Raft state is at least as fresh as the most-advanced
/// node in a responding quorum at call time — weaker than strict linearizable (a
/// quorum member may hold a newer committed entry in its *log* without having
/// *applied* it), but bounded by apply-lag, never arbitrarily stale. If a quorum
/// is unreachable, this blocks (backoff) rather than reporting ready.
async fn wait_for_raft_recent(
    raft: Arc<RaftNode>,
    router: Arc<dyn Router>,
    raft_state: Arc<dyn RaftStateView>,
    node_id: NodeId,
    startup_phase_timeout: Duration,
) -> Result<(), Error> {
    // Wait for the initial replicated state to arrive (the leader's snapshot for
    // a joiner; immediate for a rejoin, replayed in `setup_raft`).
    run_phase(
        "wait_replica_set",
        startup_phase_timeout,
        wait_for_replica_set(raft_state.as_ref(), node_id).boxed(),
    )
    .await?;

    // ZERO means no state-machine write has been applied anywhere (only reachable
    // pre-bootstrap, which does not use this gate).
    let generation = run_phase(
        "quorum_applied_generation",
        startup_phase_timeout,
        quorum_applied_generation(&router, &raft_state).boxed(),
    )
    .await?;
    let target_index = (generation > RaftGeneration::ZERO).then(|| generation.raw());

    // Wait until the local state machine reaches the quorum's generation. The
    // target is committed (a quorum has it), so this always terminates. Race it
    // against a warn ticker so a slow catch-up is visible rather than silent.
    if let Some(index) = target_index {
        run_phase(
            "startup_catch_up",
            startup_phase_timeout,
            async {
                let started = Instant::now();
                let waiter = raft.inner().wait(None);
                let wait_fut =
                    waiter.applied_index_at_least(Some(index), "raft startup: catch up to quorum");
                tokio::pin!(wait_fut);
                let mut warn_tick = startup_warn_ticker();
                loop {
                    tokio::select! {
                        res = &mut wait_fut => {
                            res.map_err(|e| Error::Raft(format!("startup catch-up: {e}")))?;
                            break;
                        }
                        _ = warn_tick.tick() => warn!(
                            elapsed_s = started.elapsed().as_secs(),
                            generation = index,
                            "raft startup: still catching up to the quorum's applied generation"
                        ),
                    }
                }
                Ok::<(), Error>(())
            }
            .boxed(),
        )
        .await?;
    }
    Ok(())
}

/// Highest applied `generation` (openraft log index of the last applied
/// state-machine write) across a responding quorum of the current replica set.
/// A peer only reports a generation it has *applied*, so the result is
/// committed. Re-reads local state each attempt — so a membership update
/// streamed in by openraft supersedes a stale durable set — and retries with
/// exponential backoff on [`Error::QuorumUnreachable`] (matching [`join_existing`]).
async fn quorum_applied_generation(
    router: &Arc<dyn Router>,
    raft_state: &Arc<dyn RaftStateView>,
) -> Result<RaftGeneration, Error> {
    let mut backoff = JOIN_INITIAL_BACKOFF;
    let started = Instant::now();
    let mut last_warn = Instant::now();
    loop {
        let local_state = (*raft_state.snapshot()).clone();
        let router = router.clone();
        let result = fan_out_quorum(local_state, move |peer| {
            let transport = router.remote(peer);
            async move {
                Ok(match transport.fetch_status().await {
                    Ok(status) => FanOutResponse::Ready(status.raft_state),
                    Err(_) => FanOutResponse::Failed,
                })
            }
        })
        .await;
        match result {
            Ok((state, _)) => return Ok(state.generation),
            Err(Error::QuorumUnreachable) => {
                debug!(?backoff, "raft catchup: quorum unreachable, retrying");
                if last_warn.elapsed() >= STARTUP_WARN_INTERVAL {
                    last_warn = Instant::now();
                    warn!(
                        elapsed_s = started.elapsed().as_secs(),
                        replica_set = ?raft_state.snapshot().replica.current(),
                        "raft startup: quorum of the replica set unreachable, still retrying"
                    );
                }
                let jittered = crate::jitter(backoff);
                tokio::time::sleep(jittered).await;
                backoff = backoff.saturating_mul(2).min(JOIN_MAX_BACKOFF);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Waits until the local [`RaftStateView`] reports a non-empty replica set and
/// returns that state. Subscribes before reading so a populate between the
/// subscribe and the first read is not missed; re-reads the authoritative
/// snapshot on every broadcast rather than trusting event payloads.
async fn wait_for_replica_set(
    raft_state: &dyn RaftStateView,
    node_id: NodeId,
) -> Result<RaftState, Error> {
    let mut rx = raft_state.subscribe();
    let started = Instant::now();
    let mut warn_tick = startup_warn_ticker();
    loop {
        let snap = raft_state.snapshot();
        if !snap.replica.current().is_empty() {
            // Detection only (do NOT gate on this): a restored replica set that
            // does not list *this* node is the top suspect for "replicates but
            // cannot write" — the replica rejects every local write with
            // `NotAReplica`. Surface it loudly; startup still proceeds (openraft
            // may add us back as the leader pushes membership).
            let in_current = snap.replica.current().contains(&node_id);
            let in_next = snap.replica.next().is_some_and(|s| s.contains(&node_id));
            if !in_current && !in_next {
                warn!(
                    ?node_id,
                    replica_node_ids = ?snap.replica.current(),
                    generation = ?snap.generation,
                    "raft startup: restored replica set does not list this node \
                     (writes will be rejected as NotAReplica until membership is pushed)"
                );
            }
            return Ok((*snap).clone());
        }
        tokio::select! {
            changed = rx.changed() => if changed.is_err() {
                return Err(Error::Shutdown);
            },
            _ = warn_tick.tick() => warn!(
                elapsed_s = started.elapsed().as_secs(),
                "raft startup: waiting for a non-empty replica set (needs a leader \
                 to push membership, or local state is empty)"
            ),
        }
    }
}

async fn bootstrap_single_node_cluster(
    node_id: NodeId,
    raft: Arc<RaftNode>,
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
        raft.inner()
            .initialize(membership_map([(node_id, self_peer_addr)]))
            .boxed()
            .await
            .to_raft("Raft::initialize")?;

        raft.inner()
            .wait(None)
            .current_leader(node_id, "bootstrap: wait for self-election")
            .boxed()
            .await
            .map_err(|e| Error::Raft(format!("bootstrap: wait for self-election: {e}")))?;

        let initial: BTreeSet<NodeId> = [node_id].into_iter().collect();
        raft.inner()
            .client_write(FencedStateWrite {
                expected_generation: crate::ids::RaftGeneration::ZERO,
                state: ReplicatedRaftState {
                    replica: ReplicaMembership::single(initial),
                    manifest: Vec::new(),
                    manifest_watermark: None,
                    hlc_cutoff: None,
                },
            })
            .boxed()
            .await
            .to_raft("client_write SetReplicaSet")?;
        Ok::<(), Error>(())
    })
    .await
    .map_err(|e| Error::Raft(format!("bootstrap: single-node init task panicked: {e}")))?
}

async fn join_existing(
    router: &dyn Router,
    node_id: NodeId,
    peers_watcher: &Arc<dyn PeersWatcher>,
    self_peer_addr: &PeerAddr,
) -> Result<(), Error> {
    let start = tokio::time::Instant::now();
    let mut backoff = JOIN_INITIAL_BACKOFF;
    let mut attempt_num: u32 = 0;
    let mut last_warn = tokio::time::Instant::now();
    loop {
        attempt_num = attempt_num.saturating_add(1);
        let t_attempt = tokio::time::Instant::now();
        let peers_snapshot = peers_watcher.current();
        match attempt_join_once(router, &peers_snapshot, node_id, self_peer_addr.clone()).await {
            JoinAttempt::Accepted => {
                info!(
                    node_id = ?node_id,
                    attempt_num,
                    attempt_ms = ?t_attempt.elapsed().as_millis(),
                    total_ms = ?start.elapsed().as_millis(),
                    "join accepted by peer",
                );
                return Ok(());
            }
            JoinAttempt::Denied => {
                return Err(Error::MembershipDenied { node_id });
            }
            JoinAttempt::Retry => {
                if last_warn.elapsed() >= STARTUP_WARN_INTERVAL {
                    last_warn = tokio::time::Instant::now();
                    warn!(
                        node_id = ?node_id,
                        attempt_num,
                        elapsed_s = start.elapsed().as_secs(),
                        peers = peers_snapshot.len(),
                        "raft startup: still trying to join an existing cluster \
                         (no reachable leader accepted the add-member yet); see \
                         debug logs for per-peer outcomes"
                    );
                }
                debug!(
                    ?backoff,
                    attempt_num,
                    attempt_ms = ?t_attempt.elapsed().as_millis(),
                    "retrying RaftAddMember after backoff",
                );
                let jittered = crate::jitter(backoff);
                tokio::time::sleep(jittered).await;
                backoff = backoff.saturating_mul(2).min(JOIN_MAX_BACKOFF);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::raft) enum JoinAttempt {
    Accepted,
    Denied,
    Retry,
}

pub(in crate::raft) async fn attempt_join_once(
    net: &dyn Router,
    peers: &[PeerAddr],
    node_id: NodeId,
    self_peer_addr: PeerAddr,
) -> JoinAttempt {
    for peer_addr in peers {
        let transport = net.remote_direct(peer_addr.clone());
        match tokio::time::timeout(
            CONNECT_TIMEOUT,
            transport.add_member(node_id, self_peer_addr.clone()),
        )
        .await
        {
            Err(_) => {
                debug!(server_name = %peer_addr.server_name, addr = %peer_addr.addr, "RaftAddMember timed out; trying next peer");
            }
            Ok(Ok(AddMemberOutcome::Accepted { .. })) => {
                info!(server_name = %peer_addr.server_name, ?node_id, "RaftAddMember accepted");
                return JoinAttempt::Accepted;
            }
            Ok(Ok(AddMemberOutcome::Rejected { state })) => {
                debug!(server_name = %peer_addr.server_name, ?node_id, leader_node_id = ?state.leader, "RaftAddMember rejected; will retry");
            }
            Ok(Err(Error::MembershipDenied { .. })) => {
                // `debug`, not `warn`: this fires on every call once denied, and
                // `RejoinMonitor::run` is a continuous retry loop (never fatal —
                // see its module doc) that calls this every
                // `REJOIN_RECONCILE_INTERVAL` for as long as the node stays
                // decommissioned. The monitor already emits its own `warn` at
                // the point the outcome CHANGES (`EvictionStatus`), which is the
                // signal worth alerting on; this line would otherwise repeat
                // forever at `warn` for a state that no longer causes an exit.
                debug!(
                    server_name = %peer_addr.server_name,
                    ?node_id,
                    "RaftAddMember denied: this node is not in config.peers",
                );
                return JoinAttempt::Denied;
            }
            Ok(Err(e)) => {
                debug!(server_name = %peer_addr.server_name, error = %e, "RaftAddMember failed; trying next peer");
            }
        }
    }
    JoinAttempt::Retry
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(
        has_local_state: bool,
        fence_present: bool,
        is_bootstrap_node: bool,
        in_membership: bool,
    ) -> BootstrapInputs {
        BootstrapInputs {
            has_local_state,
            fence_present,
            is_bootstrap_node,
            in_membership,
        }
    }

    #[test]
    fn rejoin_when_local_state_and_in_membership() {
        assert_eq!(
            bootstrap_decision(input(true, false, false, true)),
            BootstrapDecision::Rejoin,
        );
        assert_eq!(
            bootstrap_decision(input(true, true, true, true)),
            BootstrapDecision::Rejoin,
        );
    }

    #[test]
    fn hard_crash_when_local_state_but_not_in_membership() {
        assert_eq!(
            bootstrap_decision(input(true, true, true, false)),
            BootstrapDecision::HardCrashRestart,
        );
    }

    #[test]
    fn join_existing_when_fence_present_and_no_local_state() {
        assert_eq!(
            bootstrap_decision(input(false, true, true, false)),
            BootstrapDecision::JoinExisting,
        );
        assert_eq!(
            bootstrap_decision(input(false, true, false, false)),
            BootstrapDecision::JoinExisting,
        );
    }

    #[test]
    fn bootstrap_when_first_member_and_no_state_no_fence() {
        assert_eq!(
            bootstrap_decision(input(false, false, true, false)),
            BootstrapDecision::Bootstrap,
        );
    }

    #[test]
    fn join_when_not_first_member_and_no_state_no_fence() {
        assert_eq!(
            bootstrap_decision(input(false, false, false, false)),
            BootstrapDecision::JoinExisting,
        );
    }

    #[test]
    fn local_state_dominates_fence_and_bootstrap_flags() {
        // With local state present, the fence / bootstrap flags are irrelevant:
        // in-membership decides Rejoin vs HardCrashRestart.
        for fence in [false, true] {
            for boot in [false, true] {
                assert_eq!(
                    bootstrap_decision(input(true, fence, boot, true)),
                    BootstrapDecision::Rejoin,
                );
                assert_eq!(
                    bootstrap_decision(input(true, fence, boot, false)),
                    BootstrapDecision::HardCrashRestart,
                );
            }
        }
    }

    #[test]
    fn join_attempt_is_copy_eq() {
        let a = JoinAttempt::Accepted;
        let b = a; // Copy
        assert_eq!(a, b);
        assert_ne!(JoinAttempt::Accepted, JoinAttempt::Denied);
        assert_ne!(JoinAttempt::Denied, JoinAttempt::Retry);
    }

    #[tokio::test(start_paused = true)]
    async fn run_phase_passes_through_inner_result() {
        // A completed phase returns its own result, Ok or Err, untouched.
        let ok: Result<u32, Error> =
            run_phase("p", Duration::from_secs(30), async { Ok(7u32) }).await;
        assert!(matches!(ok, Ok(7)));

        let err: Result<u32, Error> = run_phase("p", Duration::from_secs(30), async {
            Err(Error::QuorumUnreachable)
        })
        .await;
        assert!(matches!(err, Err(Error::QuorumUnreachable)));
    }

    #[tokio::test(start_paused = true)]
    async fn run_phase_waits_past_escalation_without_aborting() {
        // A phase that only completes AFTER several escalation intervals must
        // still return its inner `Ok` — `run_phase` escalates logging but never
        // aborts; startup is intentionally unbounded (header).
        let escalate = Duration::from_millis(10);
        let r: Result<u32, Error> = run_phase("slow_phase", escalate, async move {
            // Five escalation intervals of blocked-but-progressing work.
            tokio::time::sleep(escalate * 5).await;
            Ok(42u32)
        })
        .await;
        assert!(
            matches!(r, Ok(42)),
            "run_phase must wait for a slow phase, not abort it: {r:?}"
        );
    }
}
