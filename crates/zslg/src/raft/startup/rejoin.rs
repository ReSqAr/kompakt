//! Continuous re-join monitor. Never fatal: removal from the group is the
//! group's decision, ending the process is the orchestrator's.
//!
//! `config.peers` is the single authority for Raft membership. The leader
//! enforces it both ways: it evicts voters absent from `config.peers`
//! (`leader::evict_stale_raft_voters`) and **denies** admission to joiners
//! absent from it (`network::handlers`, returning [`Error::MembershipDenied`]).
//!
//! This monitor is the node's side of that contract. While the node is settled
//! (has a leader *and* is in its own voter set) it does nothing. When it has
//! been unsettled for a grace period it re-runs the join handshake **by
//! address** (identity-independent, so it works even after every peer `NodeId`
//! has churned). The outcome:
//!
//! - `Accepted` / already-a-member ⇒ keep running; openraft catches the node up.
//! - `Denied` ⇒ the cluster has evicted us on purpose (we are no longer in
//!   `config.peers`) ⇒ log a throttled warning and keep running. The node
//!   stays up refusing client I/O (`ReplicaNode::can_serve_clients`,
//!   `learner::GatedReader`) — under a StatefulSet, exiting here would just
//!   restart the pod into the identical decommissioned state. Data deletion
//!   and process termination both stay operator/orchestrator decisions
//!   (CLAUDE.md §"Easy-to-miss gotchas").
//! - unreachable / no leader ⇒ retry forever (never fatal on an outage).
//!
//! See `docs/LIVENESS_REVIEW.md` §2.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::bootstrap::{JoinAttempt, attempt_join_once};
use crate::api::PeersWatcher;
use crate::api::Router;
use crate::error::Error;
use crate::ids::{NodeId, PeerAddr};
use crate::raft::RaftNode;

/// How often the monitor re-evaluates whether it needs to probe.
pub(in crate::raft) const REJOIN_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

/// How long the node must be continuously unsettled (no leader, or not in its
/// own voter set) before the monitor starts re-joining by address. Comfortably
/// larger than an election round so normal churn does not trigger it.
pub(in crate::raft) const REJOIN_LEADERLESS_GRACE: Duration = Duration::from_secs(15);

/// Whether a reachable peer has **affirmatively refused** this node's rejoin
/// attempt (`JoinAttempt::Denied`) — i.e. this node has been removed from the
/// group's `config.peers`.
///
/// Published by [`RejoinMonitor`], consulted by
/// `learner::read::can_serve_clients`. Deliberately narrower than "not
/// settled": a partition yields `JoinAttempt::Retry` (nothing answered), never
/// `Denied`, and must NOT set this — affirmative evidence only, silence is not
/// evidence (the same principle the design doc applies to leader
/// self-eviction). A node that cannot reach anyone keeps serving reads; a node
/// a reachable peer has explicitly turned away does not.
///
/// Cheap to clone (one `Arc`); reads/writes are `Relaxed` — this gates a
/// coarse "can this node serve clients" check, not a linearizable fact, and
/// every writer/reader is on the same field with no other invariant to
/// preserve ordering against.
#[derive(Clone)]
pub(crate) struct EvictionStatus(Arc<AtomicBool>);

impl EvictionStatus {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// True once a reachable peer has denied this node's rejoin attempt.
    #[must_use]
    pub(crate) fn is_denied(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// A status a peer has already denied — the evicted node, for read-gate
    /// tests outside this module.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn denied_for_test() -> Self {
        let status = Self::new();
        status.set(true);
        status
    }

    fn set(&self, denied: bool) {
        self.0.store(denied, Ordering::Relaxed);
    }
}

/// "Is the node currently settled into a working cluster?" — has a leader and is
/// a voter in its own membership view. Abstracted for testability.
pub(in crate::raft) trait MembershipView: Send + Sync + 'static {
    #[must_use]
    fn is_settled(&self, self_node_id: NodeId) -> bool;
}

impl MembershipView for RaftNode {
    fn is_settled(&self, self_node_id: NodeId) -> bool {
        let metrics = self.inner().metrics();
        let m = metrics.borrow();
        m.current_leader.is_some()
            && m.membership_config
                .membership()
                .voter_ids()
                .any(|id| id == self_node_id)
    }
}

/// One re-join sweep over the configured peers. Abstracted so the monitor's
/// control flow can be tested without a live network.
#[async_trait]
pub(in crate::raft) trait RejoinProbe: Send + Sync + 'static {
    async fn attempt(&self) -> JoinAttempt;
}

/// Production probe: re-runs the real `AddMember` handshake by address.
pub(in crate::raft) struct RpcRejoinProbe {
    net: Arc<dyn Router>,
    peers_watcher: Arc<dyn PeersWatcher>,
    self_node_id: NodeId,
    self_peer_addr: PeerAddr,
}

impl RpcRejoinProbe {
    #[must_use]
    pub(in crate::raft) fn new(
        net: Arc<dyn Router>,
        peers_watcher: Arc<dyn PeersWatcher>,
        self_node_id: NodeId,
        self_peer_addr: PeerAddr,
    ) -> Self {
        Self {
            net,
            peers_watcher,
            self_node_id,
            self_peer_addr,
        }
    }
}

#[async_trait]
impl RejoinProbe for RpcRejoinProbe {
    async fn attempt(&self) -> JoinAttempt {
        let peers_snapshot = self.peers_watcher.current();
        attempt_join_once(
            self.net.as_ref(),
            &peers_snapshot,
            self.self_node_id,
            self.self_peer_addr.clone(),
        )
        .await
    }
}

/// Wiring for [`RejoinMonitor`].
pub(in crate::raft) struct RejoinMonitorInputs {
    pub(in crate::raft) self_node_id: NodeId,
    pub(in crate::raft) view: Arc<dyn MembershipView>,
    pub(in crate::raft) probe: Arc<dyn RejoinProbe>,
    pub(in crate::raft) reconcile_interval: Duration,
    pub(in crate::raft) leaderless_grace: Duration,
    /// Where this monitor publishes "has a reachable peer denied us". Cloned
    /// into `RejoinMonitor`; the caller keeps its own clone to hand to
    /// `learner::read::can_serve_clients`.
    pub(in crate::raft) eviction: EvictionStatus,
}

/// Background task implementing the continuous re-join / eviction-detection loop.
///
/// On eviction ([`JoinAttempt::Denied`]) this publishes [`EvictionStatus`] and
/// keeps running — it never returns early from group removal. `run()` only
/// returns on cancellation (`Ok(())`); ending the process is the
/// orchestrator's call, not this monitor's.
pub(in crate::raft) struct RejoinMonitor {
    inputs: RejoinMonitorInputs,
}

impl RejoinMonitor {
    #[must_use]
    pub(in crate::raft) fn new(inputs: RejoinMonitorInputs) -> Self {
        Self { inputs }
    }

    pub(in crate::raft) async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        info!(node_id = ?self.inputs.self_node_id, "rejoin monitor: starting");
        let mut ticker = tokio::time::interval(self.inputs.reconcile_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut unsettled_since: Option<Instant> = None;
        // Edge-triggered logging: once the state stops changing tick to tick
        // (a durable eviction, or a durable outage), the loop keeps probing
        // forever (never fatal — see the module header) but stops repeating
        // the same log line every `reconcile_interval`. Without this, a
        // node left decommissioned would warn ~17k times/day.
        let mut last_probe_result: Option<JoinAttempt> = None;

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    info!("rejoin monitor: shutdown");
                    return Ok(());
                }
                _ = ticker.tick() => {}
            }

            if self.inputs.view.is_settled(self.inputs.self_node_id) {
                unsettled_since = None;
                last_probe_result = None;
                // Re-settling (has a leader, is in our own voter set again) is
                // itself affirmative evidence we can serve again.
                self.inputs.eviction.set(false);
                continue;
            }

            let now = Instant::now();
            let since = *unsettled_since.get_or_insert(now);
            if now.saturating_duration_since(since) < self.inputs.leaderless_grace {
                continue;
            }

            let attempt = self.inputs.probe.attempt().await;
            let changed = last_probe_result != Some(attempt);
            last_probe_result = Some(attempt);

            match attempt {
                JoinAttempt::Accepted => {
                    debug!("rejoin monitor: re-join accepted (or already a member)");
                    unsettled_since = None;
                    self.inputs.eviction.set(false);
                }
                JoinAttempt::Denied => {
                    // Being removed from the voter set is the GROUP's decision.
                    // Ending this process is the orchestrator's. Previously this
                    // returned a fatal error, which under a StatefulSet restarts
                    // the pod into the identical condition.
                    //
                    // A transient removal still recovers: the join handshake keeps
                    // retrying above. What changed is only that failing it is no
                    // longer fatal.
                    //
                    // Affirmative evidence: a reachable peer answered and refused
                    // us. This is the ONLY thing that sets `eviction` — never a
                    // failure to reach anyone (see `EvictionStatus`).
                    self.inputs.eviction.set(true);
                    if changed {
                        warn!(
                            node_id = ?self.inputs.self_node_id,
                            "rejoin monitor: denied re-join (not in own config.peers); \
                             staying up and refusing client reads (removal is the \
                             group's decision, process lifetime is the \
                             orchestrator's)",
                        );
                    }
                }
                JoinAttempt::Retry => {
                    if changed {
                        warn!(
                            node_id = ?self.inputs.self_node_id,
                            unsettled_s = now.saturating_duration_since(since).as_secs(),
                            "rejoin monitor: unsettled past grace (no leader, or not in \
                             own voter set); re-joining by address",
                        );
                    } else {
                        debug!("rejoin monitor: no reachable leader yet; will retry");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::tests::nid;

    struct FixedView(bool);
    impl MembershipView for FixedView {
        fn is_settled(&self, _self_node_id: NodeId) -> bool {
            self.0
        }
    }

    struct FixedProbe(JoinAttempt);
    #[async_trait]
    impl RejoinProbe for FixedProbe {
        async fn attempt(&self) -> JoinAttempt {
            self.0
        }
    }

    fn monitor(settled: bool, attempt: JoinAttempt) -> (RejoinMonitor, EvictionStatus) {
        let eviction = EvictionStatus::new();
        let mon = RejoinMonitor::new(RejoinMonitorInputs {
            self_node_id: nid(1),
            view: Arc::new(FixedView(settled)),
            probe: Arc::new(FixedProbe(attempt)),
            reconcile_interval: Duration::from_secs(1),
            leaderless_grace: Duration::from_secs(5),
            eviction: eviction.clone(),
        });
        (mon, eviction)
    }

    async fn drive(total: Duration, step: Duration) {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let mut elapsed = Duration::ZERO;
        while elapsed < total {
            tokio::time::advance(step).await;
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            elapsed = elapsed.checked_add(step).expect("elapsed");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn denied_rejoin_never_terminates_the_node() {
        // Group removal is the group's decision, not grounds for this monitor
        // to end the process: a denied re-join must be logged and survived,
        // never returned as a fatal error.
        let cancel = CancellationToken::new();
        let (mon, _eviction) = monitor(false, JoinAttempt::Denied);
        let handle = tokio::spawn(mon.run(cancel.clone()));
        drive(Duration::from_secs(10), Duration::from_secs(1)).await;
        assert!(
            !handle.is_finished(),
            "a denied re-join must never return from run() — the process is not \
             the one who decides to stop"
        );
        cancel.cancel();
        let result = handle.await.expect("task should not panic");
        assert!(
            result.is_ok(),
            "cancellation must result in Ok(()), got {result:?}"
        );
    }

    /// `EvictionStatus` is the ONLY thing `learner::read::can_serve_clients`
    /// consults post-fix: it must be set on an affirmative `Denied`, exactly
    /// the case this test drives.
    #[tokio::test(start_paused = true)]
    async fn denied_rejoin_publishes_eviction() {
        let cancel = CancellationToken::new();
        let (mon, eviction) = monitor(false, JoinAttempt::Denied);
        assert!(!eviction.is_denied(), "must start not-evicted");
        let handle = tokio::spawn(mon.run(cancel.clone()));
        drive(Duration::from_secs(10), Duration::from_secs(1)).await;
        assert!(
            eviction.is_denied(),
            "a denied re-join must publish EvictionStatus, so reads get gated"
        );
        cancel.cancel();
        let _ = handle.await.expect("task should not panic");
    }

    /// Affirmative evidence only: a partition (nothing answers ⇒ `Retry`)
    /// must NEVER set `EvictionStatus`, even after the grace period and many
    /// probe attempts — this is the property the gate exists to preserve
    /// (a partitioned node keeps serving reads).
    #[tokio::test(start_paused = true)]
    async fn unreachable_cluster_never_fatal() {
        let cancel = CancellationToken::new();
        let (mon, eviction) = monitor(false, JoinAttempt::Retry);
        let handle = tokio::spawn(mon.run(cancel.clone()));
        drive(Duration::from_secs(10), Duration::from_secs(1)).await;
        // After driving past the grace period, the monitor will have probed
        // (got Retry) and looped — it should still be running.
        assert!(
            !handle.is_finished(),
            "an outage must never return from run()"
        );
        assert!(
            !eviction.is_denied(),
            "a failure to reach anyone is not affirmative evidence of eviction \
             — a partitioned node must keep serving reads"
        );
        cancel.cancel();
        let result = handle.await.expect("task should not panic");
        assert!(
            result.is_ok(),
            "cancellation must result in Ok(()), got {result:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn settled_node_never_probes_or_errors() {
        let cancel = CancellationToken::new();
        // Settled = has leader and is a voter; even a Denied probe must never run.
        let (mon, eviction) = monitor(true, JoinAttempt::Denied);
        let handle = tokio::spawn(mon.run(cancel.clone()));
        drive(Duration::from_secs(10), Duration::from_secs(1)).await;
        assert!(
            !handle.is_finished(),
            "a settled node must never probe or exit"
        );
        assert!(
            !eviction.is_denied(),
            "a settled node must never be marked evicted"
        );
        cancel.cancel();
        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok(), "cancellation must give Ok(())");
    }

    /// A node that WAS denied and later re-joins (operator re-added it to
    /// `config.peers`) must clear `EvictionStatus` — the gate is not a
    /// one-way latch.
    #[tokio::test(start_paused = true)]
    async fn recovering_after_denial_clears_eviction() {
        let cancel = CancellationToken::new();
        let eviction = EvictionStatus::new();
        let attempt = Arc::new(std::sync::Mutex::new(JoinAttempt::Denied));
        struct SwitchableProbe(Arc<std::sync::Mutex<JoinAttempt>>);
        #[async_trait]
        impl RejoinProbe for SwitchableProbe {
            async fn attempt(&self) -> JoinAttempt {
                *self.0.lock().expect("lock")
            }
        }
        let mon = RejoinMonitor::new(RejoinMonitorInputs {
            self_node_id: nid(1),
            view: Arc::new(FixedView(false)),
            probe: Arc::new(SwitchableProbe(attempt.clone())),
            reconcile_interval: Duration::from_secs(1),
            leaderless_grace: Duration::from_secs(5),
            eviction: eviction.clone(),
        });
        let handle = tokio::spawn(mon.run(cancel.clone()));
        drive(Duration::from_secs(10), Duration::from_secs(1)).await;
        assert!(eviction.is_denied(), "must be evicted after Denied");

        *attempt.lock().expect("lock") = JoinAttempt::Accepted;
        drive(Duration::from_secs(10), Duration::from_secs(1)).await;
        assert!(
            !eviction.is_denied(),
            "an accepted re-join must clear EvictionStatus"
        );

        cancel.cancel();
        let _ = handle.await.expect("task should not panic");
    }
}
