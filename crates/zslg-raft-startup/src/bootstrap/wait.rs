//! Waiting for the cluster: bounded phases with escalating warnings, and the quorum reads they poll.

use super::{JOIN_INITIAL_BACKOFF, JOIN_MAX_BACKOFF};
use crate::Error;
use futures::FutureExt as _;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};
use zslg_consensus::ConsensusError;
use zslg_consensus::NodeId;
use zslg_consensus::RaftGeneration;
use zslg_consensus::RaftState;
use zslg_consensus::RaftStateView;
use zslg_lifecycle_types::LifecycleError;
use zslg_net::Router;
use zslg_quorum_read::{FanOutResponse, fan_out_quorum};
use zslg_raft::ClusterBootstrap;

/// Cadence for `warn!`ing that a raft startup convergence wait is still blocked,
/// so a stuck resume (e.g. all nodes empty behind a stale bootstrap fence) is
/// visible at the harness default `RUST_LOG=warn` instead of hanging silently.
pub(crate) const STARTUP_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// A periodic ticker whose first tick fires after `STARTUP_WARN_INTERVAL` (not
/// immediately), for warning inside a blocking startup wait.
#[expect(
    clippy::expect_used,
    reason = "now + 5s (STARTUP_WARN_INTERVAL) cannot overflow Instant"
)]
pub(crate) fn startup_warn_ticker() -> tokio::time::Interval {
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
pub(crate) async fn run_phase<F, T>(
    phase: &'static str,
    escalate_after: Duration,
    fut: F,
) -> Result<T, Error>
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
pub(crate) async fn wait_for_raft_recent(
    raft: Arc<dyn ClusterBootstrap>,
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
                let wait_fut = raft.wait_applied_at_least(index, "raft startup: catch up to quorum");
                tokio::pin!(wait_fut);
                let mut warn_tick = startup_warn_ticker();
                loop {
                    tokio::select! {
                        res = &mut wait_fut => {
                            res.map_err(|e| Error::Consensus(ConsensusError::Raft(format!("startup catch-up: {e}"))))?;
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
pub(crate) async fn quorum_applied_generation(
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
            Err(Error::Consensus(ConsensusError::QuorumUnreachable)) => {
                debug!(?backoff, "raft catchup: quorum unreachable, retrying");
                if last_warn.elapsed() >= STARTUP_WARN_INTERVAL {
                    last_warn = Instant::now();
                    warn!(
                        elapsed_s = started.elapsed().as_secs(),
                        replica_set = ?raft_state.snapshot().replica.current(),
                        "raft startup: quorum of the replica set unreachable, still retrying"
                    );
                }
                let jittered = backoff_kit::jitter(backoff);
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
pub(crate) async fn wait_for_replica_set(
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
                return Err(Error::Lifecycle(LifecycleError::Shutdown));
            },
            _ = warn_tick.tick() => warn!(
                elapsed_s = started.elapsed().as_secs(),
                "raft startup: waiting for a non-empty replica set (needs a leader \
                 to push membership, or local state is empty)"
            ),
        }
    }
}
