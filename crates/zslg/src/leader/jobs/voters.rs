//! `leader:voters` — evict Raft voters dropped from `config.peers`.
//!
//! ```text
//!   every membership.poll_interval:
//!       absent = voter_server_names - config.peers - self
//!       evict v where absent continuously for
//!                     membership.forced_node_removal_timeout   (0 disables)
//! ```
//!
//! openraft's voter list, not the replica set — so no replicated state, no
//! intent submitter, and no interaction with the rest of the leader.
//!
//! Liveness is deliberately **not** consulted: `config.peers` is the sole
//! authority, so a configured node that is merely unreachable is never evicted
//! and openraft heals it when contact returns. "Continuously" is literal — a
//! voter that reappears drops its timer.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::api::{PeersWatcher, RaftLeadership};
use crate::error::Error;
use crate::ids::{NodeId, ServerName};
use crate::leader::jobs::is_local_leader;
use crate::leader::jobs::membership::MembershipObserver;
use crate::metrics::{self, LeaderJob};

/// Spawn `leader:voters` on the shutdown tree. Touches openraft's own member
/// list, writes no replicated state, and deliberately ignores liveness.
pub(in crate::leader) fn spawn(
    shutdown: &mut crate::shutdown::Shutdown,
    handles: &crate::leader::jobs::LeaderHandles,
    config: &crate::raft::MembershipConfig,
    self_server_name: ServerName,
    peers_watcher: Arc<dyn PeersWatcher>,
    observer: Arc<dyn MembershipObserver>,
) {
    let job = VoterEvictionJob {
        self_node_id: handles.self_node_id,
        self_server_name,
        leadership: handles.leadership.clone(),
        peers_watcher,
        observer,
        eviction: Eviction::from_timeout(config.forced_node_removal_timeout),
        interval: config.poll_interval,
    };
    shutdown.spawn("leader:voters", job.run(shutdown.token()));
}

pub(in crate::leader) struct VoterEvictionJob {
    pub(in crate::leader) self_node_id: NodeId,
    pub(in crate::leader) self_server_name: ServerName,
    pub(in crate::leader) leadership: Arc<dyn RaftLeadership>,
    pub(in crate::leader) peers_watcher: Arc<dyn PeersWatcher>,
    pub(in crate::leader) observer: Arc<dyn MembershipObserver>,
    pub(in crate::leader) eviction: Eviction,
    pub(in crate::leader) interval: Duration,
}

impl VoterEvictionJob {
    pub(in crate::leader) async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        info!("leader:voters: starting");
        let mut ticker = interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Owned by the loop rather than the job, so `tick(&self)` needs no lock.
        let mut absent_since = AbsentFromConfigTracker::default();
        // Separate tracker: peer-absence and self-rejection are different
        // predicates over different evidence, and conflating their debounce
        // timers would let one reset the other's clock.
        let mut self_absent_since = AbsentFromConfigTracker::default();
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let started = tokio::time::Instant::now();
                    self.tick(&mut absent_since, &mut self_absent_since).await;
                    metrics::leader_job_ran(LeaderJob::Voters, started.elapsed());
                }
            }
        }
        info!("leader:voters: shutdown");
        Ok(())
    }

    async fn tick(
        &self,
        absent_since: &mut AbsentFromConfigTracker,
        self_absent_since: &mut AbsentFromConfigTracker,
    ) {
        if !is_local_leader(self.leadership.as_ref(), self.self_node_id) {
            absent_since.clear();
            self_absent_since.clear();
            return;
        }
        let allowed: BTreeSet<ServerName> = self
            .peers_watcher
            .current()
            .iter()
            .map(|p| p.server_name.clone())
            .chain(std::iter::once(self.self_server_name.clone()))
            .collect();
        let absent: BTreeSet<NodeId> = self
            .leadership
            .voter_server_names()
            .into_iter()
            .filter_map(|(id, server_name)| {
                (id != self.self_node_id && !allowed.contains(&server_name)).then_some(id)
            })
            .collect();

        for node_id in absent_since.observe(&absent, Instant::now(), self.eviction) {
            info!(
                ?node_id,
                "evicting Raft voter: absent from config.peers past forced_node_removal_timeout",
            );
            if let Err(e) = self.leadership.remove_member(node_id).await {
                warn!(
                    ?node_id,
                    error = %e,
                    "could not evict stale Raft voter; will retry next tick",
                );
            }
        }

        self.tick_self_eviction(self_absent_since).await;
    }

    /// The hazard this job exists to close: a leader dropped from every OTHER
    /// node's `config.peers` cannot be evicted by anyone — its own pass above
    /// skips itself by construction, and no other node is leader to run this
    /// job at all. Nothing escapes that state except the leader noticing on
    /// its own.
    ///
    /// Self-removal (rather than waiting to be evicted by someone else) is the
    /// only mechanism available: a node removed by another leader is never
    /// told — the leader simply stops replicating to it — so per openraft's
    /// node-lifecycle docs it "enters the Candidate state and continues to
    /// increase their term and elect themselves" forever. A node that applies
    /// its own removal knows it is a non-voter and stops campaigning by
    /// construction. openraft 0.9.24 also has no `transfer_leader` (`Trigger`
    /// offers only `elect`/`heartbeat`/`snapshot`/`purge_log`), so self-removal
    /// is the only lever that moves leadership away at all; `ChangeMembershipError`
    /// has no self-removal guard, so it is permitted.
    async fn tick_self_eviction(&self, self_absent_since: &mut AbsentFromConfigTracker) {
        // Fan out one probe task per voter and stop the instant the outcome is
        // fixed. Probing is parallel — one wedged peer costs only its own
        // transport deadline, never a serialised queue — and once a quorum has
        // either affirmatively dropped us or become unreachable, the remaining
        // in-flight probes cannot change the answer, so we do not wait on them.
        let voters = self.leadership.voter_ids();
        let probed: Vec<NodeId> = self
            .leadership
            .voter_server_names()
            .into_iter()
            .filter_map(|(id, _)| (id != self.self_node_id).then_some(id))
            .collect();
        let mut probe = SelfEvictionProbe::new(
            self.self_node_id,
            self.self_server_name.clone(),
            voters,
            probed.clone(),
        );
        let mut tasks: JoinSet<(NodeId, Option<Vec<ServerName>>)> = JoinSet::new();
        for id in probed {
            let observer = self.observer.clone();
            tasks.spawn(async move {
                let peers = observer.observe(id).await.ok().map(|s| s.peer_names);
                (id, peers)
            });
        }
        // `observe` failures are silence, and silence is not evidence — they
        // contribute nothing to the rejection count. A panicked probe (no id
        // recoverable from `JoinError`) leaves its slot in `pending`, which can
        // only delay the "keep self" early-out, never force a false removal.
        let unwanted = loop {
            match tasks.join_next().await {
                Some(Ok((id, peers))) => probe.observe(id, peers.as_deref()),
                Some(Err(_)) => {}
                None => break false,
            }
            match probe.decided() {
                Some(true) => break true,
                Some(false) => break false,
                None => {}
            }
        };
        // Same debounce discipline as the peer rule: continuous absence only,
        // timer dropped the instant a quorum lists us again.
        let due = self_absent_since.observe(
            &if unwanted {
                [self.self_node_id].into_iter().collect()
            } else {
                BTreeSet::new()
            },
            Instant::now(),
            self.eviction,
        );
        if !due.is_empty() {
            warn!(
                "leader:voters: a quorum of voters no longer lists this node in \
                 config.peers; removing self from the voter set. Self-removal is \
                 what makes this node LEARN of its own removal — a node removed \
                 by someone else never finds out and campaigns forever \
                 (openraft node-lifecycle)."
            );
            if let Err(e) = self.leadership.remove_member(self.self_node_id).await {
                warn!(error = %e, "leader:voters: self-removal failed; will retry");
            }
        }
    }
}

/// Whether a quorum of the voter set has affirmatively dropped this node from
/// its `config.peers`, decided incrementally as probe results arrive.
///
/// Counts only peers that RESPONDED and whose list omits `self_name`. A silent
/// peer is *unknown*, never a vote against: a leader partitioned from everyone
/// would otherwise evict itself on no evidence at all.
///
/// [`Self::decided`] reports the outcome the instant it is fixed — either a
/// quorum of voters has already rejected us (`Some(true)`), or the voters still
/// being probed cannot possibly reach a quorum no matter how they answer
/// (`Some(false)`) — so the probe fan-out can stop early instead of waiting on
/// the tail.
struct SelfEvictionProbe {
    self_id: NodeId,
    self_name: ServerName,
    voters: BTreeSet<NodeId>,
    rejecting: BTreeSet<NodeId>,
    pending: BTreeSet<NodeId>,
}

impl SelfEvictionProbe {
    fn new(
        self_id: NodeId,
        self_name: ServerName,
        voters: BTreeSet<NodeId>,
        probed: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        Self {
            self_id,
            self_name,
            voters,
            rejecting: BTreeSet::new(),
            pending: probed.into_iter().collect(),
        }
    }

    /// Record one probe outcome. `peers` is `None` on failure — a silent peer
    /// is *unknown*, never a vote against.
    fn observe(&mut self, id: NodeId, peers: Option<&[ServerName]>) {
        self.pending.remove(&id);
        if id == self.self_id {
            return;
        }
        if let Some(peers) = peers
            && !peers.contains(&self.self_name)
        {
            self.rejecting.insert(id);
        }
    }

    /// The decision, once fixed; `None` while neither side can be concluded.
    #[must_use]
    fn decided(&self) -> Option<bool> {
        // `is_quorum` takes a `ReplicaMembership`, not a bare set, so wrap the
        // voter set in a `Single`. Reusing it rather than hand-rolling a
        // majority keeps one definition of "quorum" in the crate.
        let membership = crate::meta::ReplicaMembership::single(self.voters.clone());
        if crate::quorum::is_quorum(&membership, &self.rejecting) {
            return Some(true);
        }
        let max_possible: BTreeSet<NodeId> = self.rejecting.union(&self.pending).copied().collect();
        if !crate::quorum::is_quorum(&membership, &max_possible) {
            return Some(false);
        }
        None
    }
}

/// Whether stale voters are evicted at all. An operator sets the timeout to
/// zero to turn eviction off, which is a different thing from a zero-length
/// wait and so is a different variant.
#[derive(Debug, Clone, Copy)]
pub(in crate::leader) enum Eviction {
    Disabled,
    After(Duration),
}

impl Eviction {
    #[must_use]
    pub(in crate::leader) fn from_timeout(timeout: Duration) -> Self {
        if timeout.is_zero() {
            Self::Disabled
        } else {
            Self::After(timeout)
        }
    }
}

/// How long each voter has been continuously absent from `config.peers`.
#[derive(Debug, Default)]
pub(in crate::leader) struct AbsentFromConfigTracker {
    since: HashMap<NodeId, Instant>,
}

impl AbsentFromConfigTracker {
    /// Drop every timer. Called on demotion: a node that is re-promoted must
    /// not evict on the strength of observations from a previous epoch.
    pub(in crate::leader) fn clear(&mut self) {
        self.since.clear();
    }

    /// Record the current absent set and return those due for eviction.
    ///
    /// A voter that reappears loses its timer, so eviction requires
    /// *uninterrupted* absence for the whole period.
    pub(in crate::leader) fn observe(
        &mut self,
        absent_voters: &BTreeSet<NodeId>,
        now: Instant,
        eviction: Eviction,
    ) -> BTreeSet<NodeId> {
        self.since.retain(|id, _| absent_voters.contains(id));

        let Eviction::After(timeout) = eviction else {
            return BTreeSet::new();
        };

        let mut due = BTreeSet::new();
        for &voter in absent_voters {
            let first = *self.since.entry(voter).or_insert(now);
            if now.saturating_duration_since(first) >= timeout {
                due.insert(voter);
            }
        }
        due
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;
    use crate::tests::nid;

    /// Stable `NodeId` for a short name, for readable quorum-predicate tests.
    fn nid_for(name: &str) -> NodeId {
        nid(name.as_bytes().first().copied().unwrap_or(0))
    }

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    fn resp(name: &str, peers: &[&str]) -> (NodeId, Vec<ServerName>) {
        (nid_for(name), peers.iter().map(|p| (*p).into()).collect())
    }

    /// `decided` reports `Some(true)` the instant a quorum of voters has
    /// affirmatively dropped us — the self-eviction trigger.
    #[test]
    fn quorum_omitting_self_decides_reject() {
        let voters = set(&[nid_for("a"), nid_for("b"), nid_for("c")]);
        let mut probe = SelfEvictionProbe::new(
            nid_for("a"),
            "a".into(),
            voters,
            [nid_for("b"), nid_for("c")],
        );
        let (b, b_peers) = resp("b", &["b", "c"]);
        let (c, c_peers) = resp("c", &["b", "c"]);
        probe.observe(b, Some(&b_peers));
        probe.observe(c, Some(&c_peers));
        assert_eq!(probe.decided(), Some(true));
    }

    /// A MINORITY omitting me is not a rejection; once the round is exhausted
    /// the outcome is fixed as "keep self". Lists disagree transiently during a
    /// rolling config change, and one disagreeing peer must not unseat a
    /// healthy leader.
    #[test]
    fn minority_omitting_self_decides_keep() {
        let voters = set(&[nid_for("a"), nid_for("b"), nid_for("c")]);
        let mut probe = SelfEvictionProbe::new(
            nid_for("a"),
            "a".into(),
            voters,
            [nid_for("b"), nid_for("c")],
        );
        let (b, b_peers) = resp("b", &["b", "c"]);
        let (c, c_peers) = resp("c", &["a", "b", "c"]);
        probe.observe(b, Some(&b_peers));
        probe.observe(c, Some(&c_peers));
        assert_eq!(probe.decided(), Some(false));
    }

    /// SILENCE IS NOT EVIDENCE. A failed probe never counts as a rejection.
    /// Here `b` fails and `c` is the only remaining voter; one voter is no
    /// quorum, so the round is already decided as "keep self".
    #[test]
    fn failed_probe_is_not_evidence() {
        let voters = set(&[nid_for("a"), nid_for("b"), nid_for("c")]);
        let mut probe = SelfEvictionProbe::new(
            nid_for("a"),
            "a".into(),
            voters,
            [nid_for("b"), nid_for("c")],
        );
        probe.observe(nid_for("b"), None);
        assert_eq!(probe.decided(), Some(false));
    }

    /// While a quorum is still reachable, `decided` stays `None` — the
    /// early-out must not fire before the outcome is fixed.
    #[test]
    fn undecided_while_quorum_reachable() {
        let voters = set(&[nid_for("a"), nid_for("b"), nid_for("c")]);
        let mut probe = SelfEvictionProbe::new(
            nid_for("a"),
            "a".into(),
            voters,
            [nid_for("b"), nid_for("c")],
        );
        let (b, b_peers) = resp("b", &["b", "c"]);
        probe.observe(b, Some(&b_peers));
        assert_eq!(
            probe.decided(),
            None,
            "c could still reject; nothing is fixed yet"
        );
        let (c, c_peers) = resp("c", &["b", "c"]);
        probe.observe(c, Some(&c_peers));
        assert_eq!(probe.decided(), Some(true));
    }

    /// The other early-out: once the in-flight voters cannot reach a quorum no
    /// matter how they answer, the outcome is fixed as "keep self".
    #[test]
    fn decides_keep_when_remaining_cannot_reach_quorum() {
        let voters = set(&[nid_for("a"), nid_for("b"), nid_for("c")]);
        let mut probe = SelfEvictionProbe::new(
            nid_for("a"),
            "a".into(),
            voters,
            [nid_for("b"), nid_for("c")],
        );
        let (b, b_peers) = resp("b", &["a", "b", "c"]);
        probe.observe(b, Some(&b_peers));
        assert_eq!(probe.decided(), Some(false));
    }

    /// Defensive: a self response (the fan-out skips self, but never trust the
    /// caller) can never be a rejection — our own list always includes us.
    #[test]
    fn self_response_is_never_a_rejection() {
        let voters = set(&[nid_for("a"), nid_for("b"), nid_for("c")]);
        let mut probe = SelfEvictionProbe::new(
            nid_for("a"),
            "a".into(),
            voters,
            [nid_for("b"), nid_for("c")],
        );
        let (a, a_peers) = resp("a", &["b", "c"]);
        probe.observe(a, Some(&a_peers));
        assert_eq!(probe.decided(), None);
    }

    #[test]
    fn evicts_only_after_debounce() {
        let a = nid(1);
        let absent: BTreeSet<NodeId> = [a].into_iter().collect();
        let mut t = AbsentFromConfigTracker::default();
        let t0 = std::time::Instant::now();
        let evict = Eviction::After(Duration::from_secs(10));

        assert!(t.observe(&absent, t0, evict).is_empty(), "starts the clock");
        let before = t0.checked_add(Duration::from_secs(9)).expect("instant");
        assert!(t.observe(&absent, before, evict).is_empty());
        let at = t0.checked_add(Duration::from_secs(10)).expect("instant");
        assert_eq!(t.observe(&absent, at, evict), absent, "due at timeout");
    }

    #[test]
    fn timer_resets_when_voter_returns_to_config() {
        let a = nid(1);
        let absent: BTreeSet<NodeId> = [a].into_iter().collect();
        let present: BTreeSet<NodeId> = BTreeSet::new();
        let mut t = AbsentFromConfigTracker::default();
        let t0 = std::time::Instant::now();
        let evict = Eviction::After(Duration::from_secs(10));

        assert!(t.observe(&absent, t0, evict).is_empty());
        // Voter reappears in config at t0+5s: timer cleared.
        let t5 = t0.checked_add(Duration::from_secs(5)).expect("instant");
        assert!(t.observe(&present, t5, evict).is_empty());
        // Absent again; must wait a fresh full timeout from t5.
        let t6 = t0.checked_add(Duration::from_secs(6)).expect("instant");
        assert!(t.observe(&absent, t6, evict).is_empty());
        let t15 = t0.checked_add(Duration::from_secs(15)).expect("instant");
        assert!(t.observe(&absent, t15, evict).is_empty());
        let t16 = t0.checked_add(Duration::from_secs(16)).expect("instant");
        assert_eq!(t.observe(&absent, t16, evict), absent);
    }

    #[test]
    fn disabled_eviction_never_evicts() {
        let a = nid(1);
        let absent: BTreeSet<NodeId> = [a].into_iter().collect();
        let mut t = AbsentFromConfigTracker::default();
        assert!(
            t.observe(&absent, std::time::Instant::now(), Eviction::Disabled)
                .is_empty()
        );
    }
}
