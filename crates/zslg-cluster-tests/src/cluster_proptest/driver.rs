//! Running one generated case over a real cluster and collecting what the battery needs.

use super::{Op, config_for};
use crate::{
    Cluster, ClusterConfig, CutoffObserver, NodePlacement, assert_cluster_invariants_with_acks,
};
use bytes::Bytes;
use object_store::memory::InMemory;
use std::sync::Arc;
use std::time::Duration;
use zslg_consensus::NodeId;
use zslg_log::DataId;

/// Recovery budget for a *generated* case.
///
/// Shorter than the 60 s [`stressed_regime_admits_writes`] gives its floor
/// corner, and deliberately: that test sits at the most aggressive dials the
/// generator can produce and is the one expected to be near the edge, while a
/// generated case is somewhere in the range and should not be able to spend a
/// minute proving it is wedged — the proptest runs dozens of them. Only a case
/// that never recovers pays this in full; a healthy one returns on the first
/// attempt in milliseconds.
pub(crate) const GENERATED_RECOVERY_BUDGET_MS: u64 = 30_000;

// ── driver ────────────────────────────────────────────────────────────

/// How many writes a case attempted and how many were acked. Printed per case
/// so a stratum that quietly stops admitting writes is visible rather than
/// silently vacuous.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Tally {
    pub(crate) attempted: usize,
    pub(crate) acked: usize,
    /// The same two counts, restricted to bursts issued while a node was down.
    /// Reported, never asserted: two of three nodes are a quorum, so a
    /// degraded-but-quorate cluster is *expected* to keep acking — but a burst
    /// issued into the re-election that a leader's `Down` triggers may still
    /// fail, and a failed write licenses no claim (CLAUDE.md §20). The
    /// assertion that carries the degraded-liveness claim is the recovery
    /// probe every script ends with ([`Op::WriteUntilAcked`]); these two make
    /// it visible how much of the ack count was earned while degraded.
    pub(crate) attempted_while_down: usize,
    pub(crate) acked_while_down: usize,
    /// How many `Op::Replace`s actually executed. Printed for the same reason
    /// as the write counts: a `Replace` that never fires is silent vacuity,
    /// and the drain invariant this module exists to cover is only under test
    /// when this is non-zero.
    pub(crate) replaced: usize,
    /// How many of the executed down-windows were opened by [`Op::Kill`] rather
    /// than [`Op::Down`]. Printed for the same vacuity reason: the crash-recovery
    /// composition this op exists for is only under test when this is non-zero,
    /// and it shares every other counter with the graceful stop, so nothing else
    /// would reveal a run in which no kill was drawn.
    pub(crate) killed: usize,
}

pub(crate) fn spawn_observer(cluster: &Cluster) -> CutoffObserver {
    CutoffObserver::spawn_for(cluster)
}

/// Wait for any in-flight membership change to collapse before taking a node
/// down. **A precondition of the fault, not a constraint on the script**, which
/// is why it is here and not in [`script_strategy`]: no op is skipped, no
/// generated value is discarded, and the tally is untouched.
///
/// The reason is a real deadlock, found by this proptest on its first run and
/// reproduced deterministically before being fixed here. While a config is
/// joint, `old` is immutable by design (CLAUDE.md §8 — it is what makes the
/// drain bound sound), and the leader's step 2 needs a generation quorum in
/// *both* `old` and `next`. Taking a second member of `old` down leaves it at
/// 1-of-3: the change can never progress, the joint config can never collapse,
/// and a node started into it blocks forever in `quorum_applied_generation`
/// (raft startup is unbounded by design — `run_phase` escalates but never
/// aborts). The observed script was `Replace(1)` then `Replace(0)` two writes
/// later, ending in `old = {A, B, leader}` with both `A` and `B` gone.
///
/// So it is an operator-contract violation, not a system bug: the system's
/// response — block, lose nothing — is correct, and "a cluster with no quorum
/// in `old` is unavailable" is not an invariant question. That is the same
/// judgement [`script_strategy`]'s quorum floor makes. It applies to
/// [`Op::Down`] as well as [`Op::Replace`]: taking down the one remaining live
/// `old` member is equally fatal while joint, so both fence on this.
///
/// # `downed` is why this takes an argument
///
/// The predicate is "the agreed non-joint set is exactly the *membership*",
/// and `Cluster::node_ids` is the set of *running* nodes, which is one short
/// while a node is down. A node being down changes nothing about the replica
/// set — with three members, one dead and no spare, `compute_target` cannot
/// reach the target size and declines to initiate — so the membership to
/// compare against is the live nodes plus the downed one. Passing
/// `node_ids()` alone would spin for the full deadline and then panic.
///
/// What it deliberately does *not* do is wait after the change is *started*.
/// Writes still race the joint config — the concurrency this module exists to
/// exercise — because only the next *fault* waits, never the next write.
///
/// # Why not `Cluster::wait_for_convergence`
///
/// Because it does not answer this question. It asks only "does every node
/// agree on some non-joint set", which is *already true* in the window between
/// `Cluster::replace` returning and the leader's membership job even starting:
/// the committed set is still the old one, nobody is joint, everyone agrees —
/// and the second `Replace` fires straight into it. That is fragility note 4 on
/// `rolling_replacement.rs`, and it is why that test pairs the same call with a
/// blind two-second sleep. The predicate below is the precise one instead: the
/// agreed non-joint set must be *exactly the set of live nodes*, which is false
/// until the outgoing node is out and the incoming one is in. No sleep needed.
pub(crate) async fn settle_membership(cluster: &Cluster, downed: Option<NodeId>) {
    let mut want = cluster.node_ids();
    want.extend(downed);
    crate::wait_until_sync("membership settled", Duration::from_secs(30), || {
        cluster.nodes().all(|n| {
            let s = n.state_handle().snapshot();
            !s.replica.is_joint() && *s.replica.current() == want
        })
    })
    .await;
}

/// Outcome of running one generated script. Returned rather than asserted
/// inside so the proptest body can use `prop_assert!` (which shrinks) instead
/// of a bare panic.
pub(crate) struct CaseResult {
    pub(crate) tally: Tally,
    /// Whether any node reached a state where `assert_unstable_above_watermark`
    /// had something to check: a promoted watermark *and* a non-empty
    /// `unstable_tail`. Reported so a permanently vacuous boundary check is
    /// visible.
    pub(crate) boundary_exercised: bool,
    /// Whether the script actually executed a `Down` or a `Replace`. Scopes the
    /// liveness claim — see the note on it in the proptest body.
    pub(crate) faulted: bool,
    /// How many `Down` … `Up` windows had at least one op inside them.
    ///
    /// Reported as the vacuity guard on the [`Op::Down`]/[`Op::Up`] split: a
    /// window with nothing inside it degrades the cluster only for as long as a
    /// relaunch takes, which is the immediate bounce the split exists to
    /// improve on. Zero across a whole run means it bought nothing.
    pub(crate) degraded_windows: usize,
    /// How many observer polls across the case saw a non-`Single` replica
    /// membership.
    ///
    /// Reported rather than asserted, and it is a vacuity guard on the *script*,
    /// not evidence about the invariant battery. These are samples taken
    /// *during* a script; the battery runs only after `wait_for_convergence`,
    /// whose predicate (`Cluster::all_converged`) is false unless every node is
    /// non-joint. So a non-zero count here says nothing about whether
    /// `assert_membership_well_formed` or `assert_no_satisfied_drain_left_armed`
    /// — both no-ops on a `Single` membership — ever had anything to check, and
    /// a zero count would say nothing either. What it does say is whether a
    /// membership change was ever in flight while writes and faults were still
    /// running: zero means the concurrency this module exists for never
    /// happened.
    pub(crate) joint_polls: usize,
    /// Outcome of the script's [`Op::WriteUntilAcked`], if it ran one.
    pub(crate) recovery: Option<Recovery>,
}

/// What an [`Op::WriteUntilAcked`] observed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Recovery {
    /// Whether a write was ever acked before the deadline. This is the whole
    /// point: `false` means the cluster could not admit a single write in the
    /// entire window, which is "broken" rather than "stressed".
    pub(crate) admitted: bool,
    /// How many single writes it took. `1` means the cluster was already
    /// healthy; a large number means it recovered, and says how hard.
    pub(crate) attempts: usize,
    /// How long the probe ran before it succeeded or gave up.
    pub(crate) elapsed: Duration,
}

/// Runs one generated script and returns what happened. Takes the dials rather
/// than the `Regime` they came from: the regime selects the ranges and scopes
/// the *assertions*, both of which live in the caller, so the driver itself is
/// regime-agnostic.
pub(crate) async fn run_case(
    snapshot_ms: u64,
    election_min_ms: u64,
    max_flush_batch: u64,
    write_capacity: u64,
    ops: Vec<Op>,
) -> CaseResult {
    let cluster_res = Cluster::new(ClusterConfig {
        count: 3,
        distinct_names: false,
        object_store: Arc::new(InMemory::new()),
        base_config: Some(config_for(
            snapshot_ms,
            election_min_ms,
            max_flush_batch,
            write_capacity,
        )),
    })
    .await;
    let mut cluster = cluster_res.expect("Cluster::new");

    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;

    // One observer for the whole case. A `RaftStateHandle` belongs to one
    // `ReplicaNode` instance, so every fault that mints a new instance calls
    // `refresh` to swap the handle set; the recorded history is keyed by
    // `NodeId` and carries across, which is what lets a regression *straddling*
    // a fault be seen at all — a node resuming from persisted state is where a
    // backwards cutoff would matter most.
    let observer = spawn_observer(&cluster);

    let mut acked: Vec<(DataId, Bytes)> = Vec::new();
    let mut tally = Tally::default();
    let mut faulted = false;
    // The node the script has taken down, if any, with the placement needed to
    // put it back on the same address and db-path. `script_strategy` guarantees
    // at most one at a time, which is why this is an `Option` and not a set —
    // and the `expect`s below turn a generator that broke that guarantee into a
    // clear failure rather than a cluster that wedges below quorum.
    let mut downed: Option<(NodeId, NodePlacement)> = None;
    // How many ops the current `Down` window has carried, and how many windows
    // carried at least one. See `CaseResult::degraded_windows`.
    let mut window_ops = 0usize;
    let mut degraded_windows = 0usize;
    let mut recovery: Option<Recovery> = None;

    for op in ops {
        if downed.is_some() && !matches!(op, Op::Up) {
            window_ops += 1;
        }
        match op {
            Op::WriteBurst { n, len, target } => {
                let ids: Vec<NodeId> = cluster.node_ids().into_iter().collect();
                // Incremented before anything is issued, so nothing below this
                // line may `continue` or return early — see the note on
                // `script_strategy`. The burst is issued unconditionally.
                tally.attempted += n;
                if downed.is_some() {
                    tally.attempted_while_down += n;
                }
                let target = ids[target % ids.len()];
                let writer = cluster.node(target).writer();
                // `Arc<dyn Writer>` is `Send + Sync`, so every future in the
                // burst can hold its own handle and they can all be in flight
                // at once. `join_all` polls all of them from this task, so all
                // `n` reach their submit point before the first one completes —
                // which is the whole point: that is what puts more than one
                // record in the per-node queues.
                let burst = futures::future::join_all((0..n).map(|i| {
                    let writer = Arc::clone(&writer);
                    // Distinct payloads. `DataId` is minted from 64 random bits
                    // plus the creation HLC, so uniqueness is already
                    // guaranteed by construction (CLAUDE.md §8) — varying the
                    // first byte makes a dumped blob traceable to its position
                    // in the burst, and gives the durability chain's payload
                    // comparison something that actually differs per write.
                    // Nothing here reuses a `DataId`.
                    let mut bytes = vec![0xABu8; len];
                    bytes[0] = (i % 256) as u8;
                    // Two handles to the same bytes: `write` consumes one, the
                    // other is recorded as the ground truth the chain verifies
                    // against. The `async move` must not reach back into an
                    // outer collection — that would move it into the first
                    // future and fail to compile on the second.
                    let payload = Bytes::from(bytes);
                    let recorded = payload.clone();
                    async move { writer.write(payload).await.map(|id| (id, recorded)) }
                }));
                // Bounded so a wedged cluster cannot hang the case. The bound
                // covers the *whole joined burst*, not each write: they are
                // submitted together, so their individual deadlines run
                // concurrently and the burst costs about one `write_timeout`.
                // 30 s sits above the largest `write_timeout` `config_for`
                // derives (18 s), so this is a backstop against a wedge, not
                // the thing that normally ends a stalled write — that is the
                // writer's own deadline, which returns a per-write `Err`
                // instead of discarding the burst's other results.
                let res = tokio::time::timeout(Duration::from_secs(30), burst).await;
                // An outer timeout drops the whole burst; an inner `Err` is one
                // failed write. Both mean "quorum not observed", and neither
                // licenses any claim about absence — so only `Ok`s are counted
                // and nothing is asserted about the rest.
                if let Ok(results) = res {
                    for (data_id, payload) in results.into_iter().flatten() {
                        tally.acked += 1;
                        if downed.is_some() {
                            tally.acked_while_down += 1;
                        }
                        acked.push((data_id, payload));
                    }
                }
            }
            Op::Down(idx) | Op::Kill(idx) => {
                // The two openers differ only in *how* the node stops; every
                // piece of bookkeeping below is deliberately shared, so the
                // quorum floor, the observation window and the `Up` that closes
                // the window cannot drift between them.
                let hard = matches!(op, Op::Kill(_));
                assert!(
                    downed.is_none(),
                    "script_strategy must never emit a second Down/Kill while one is open — \
                     two of three down is a cluster without a quorum, which asserts nothing"
                );
                // A fault must not start while a membership change is in
                // flight; see `settle_membership`. Nothing is down yet, so the
                // membership is exactly the live set.
                settle_membership(&cluster, None).await;
                let ids: Vec<NodeId> = cluster.node_ids().into_iter().collect();
                let victim = ids[idx % ids.len()];
                faulted = true;
                if hard {
                    tally.killed += 1;
                }
                let placement = if hard {
                    cluster.kill(victim).await
                } else {
                    cluster.remove(victim).await.expect("remove node")
                };
                downed = Some((victim, placement));
                window_ops = 0;
                observer.refresh(&cluster);
            }
            Op::Up => {
                let (victim, placement) = downed
                    .take()
                    .expect("script_strategy must never emit an Up with no Down open");
                if window_ops > 0 {
                    degraded_windows += 1;
                }
                // The downed node is still a replica-set member — with three
                // members, one dead and no spare, `compute_target` cannot reach
                // the target size, so nothing was initiated on its account — so
                // it belongs in the expected membership.
                settle_membership(&cluster, Some(victim)).await;
                let relaunched = cluster.relaunch(placement).await.expect("relaunch node");
                assert_eq!(
                    relaunched, victim,
                    "relaunch on the same address and db-path must resume the same NodeId — \
                     otherwise this is a Replace and the persisted raft state was not reused"
                );
                observer.refresh(&cluster);
            }
            Op::Replace(idx) => {
                assert!(
                    downed.is_none(),
                    "script_strategy must close a Down window before a Replace — a Replace \
                     while a node is down leaves one live node of three and the successor's \
                     raft join blocks forever"
                );
                settle_membership(&cluster, None).await;
                let ids: Vec<NodeId> = cluster.node_ids().into_iter().collect();
                let victim = ids[idx % ids.len()];
                faulted = true;
                tally.replaced += 1;
                let fresh = cluster.replace(victim).await.expect("replace node");
                assert_ne!(
                    fresh, victim,
                    "replace must mint a new NodeId — otherwise no membership change occurs \
                     and this op silently degenerates into a restart"
                );
                observer.refresh(&cluster);
            }
            Op::WriteUntilAcked { within_ms } => {
                let started = tokio::time::Instant::now();
                let deadline = started
                    .checked_add(Duration::from_millis(within_ms))
                    .expect("Instant + within_ms does not overflow");
                let ids: Vec<NodeId> = cluster.node_ids().into_iter().collect();
                let mut attempts = 0usize;
                let mut admitted = false;
                loop {
                    attempts = attempts.saturating_add(1);
                    // Re-resolved per attempt, and through the leader when
                    // there is one: `Writer::write` fans out over the router
                    // and is never gated on the coordinating node's own
                    // membership (CLAUDE.md §12), so any node can serve it —
                    // but a fixed node picked once can be the one the script
                    // just replaced, and re-reading who leads keeps the retries
                    // from all going to the same unlucky place.
                    let target = cluster
                        .leader()
                        .filter(|id| ids.contains(id))
                        .unwrap_or(ids[0]);
                    let writer = cluster.node(target).writer();
                    // A distinct payload per attempt, so a retry never looks
                    // like a resend of the one before it.
                    let mut bytes = vec![0xCDu8; 64];
                    bytes[0] = (attempts % 256) as u8;
                    let payload = Bytes::from(bytes);
                    let recorded = payload.clone();
                    if let Ok(data_id) = writer.write(payload).await {
                        acked.push((data_id, recorded));
                        admitted = true;
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    // The write's own `write_timeout` already paced the
                    // attempt; this is just enough of a gap for an election in
                    // flight to finish rather than to be interrupted again.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                recovery = Some(Recovery {
                    admitted,
                    attempts,
                    elapsed: started.elapsed(),
                });
            }
            Op::Settle(ms) => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
        }
    }

    // ── quiesce ───────────────────────────────────────────────────────
    // Every fault is dropped *before* anything is waited on, so none survives
    // into the assertions: nothing below this line runs against a two-node
    // cluster. `script_strategy` closes any open window with an appended `Up`,
    // so for a generated script this is already a no-op — it is reachable from
    // the hand-written scripts (`stressed_regime_admits_writes`), which is
    // exactly the case where forgetting it would silently assert the battery
    // against a degraded cluster.
    if let Some((victim, placement)) = downed.take() {
        if window_ops > 0 {
            degraded_windows += 1;
        }
        let relaunched = cluster.relaunch(placement).await.expect("relaunch node");
        assert_eq!(
            relaunched, victim,
            "relaunch on the same address and db-path must resume the same NodeId"
        );
        observer.refresh(&cluster);
    }
    cluster.wait_for_leader(Duration::from_secs(30)).await;
    cluster.wait_for_convergence(Duration::from_secs(30)).await;
    // `wait_for_convergence` alone is not quiescence: it asks only whether
    // everyone agrees on some non-joint set, which is already true in the window
    // between a `Replace` returning and the leader's membership job starting
    // (see `settle_membership`). A script ending in `Replace` could clear the
    // fence before its change began, and the battery would then assert over a
    // cluster whose last op contributed nothing. The stronger predicate — the
    // agreed non-joint set is *exactly* the live nodes — is the one that waits
    // for the change to finish. Nothing is down here, so the live set is the
    // membership.
    settle_membership(&cluster, None).await;

    // ── assert ────────────────────────────────────────────────────────
    let boundary_exercised = assert_cluster_invariants_with_acks(&cluster, &acked).await;
    observer.assert_monotonic();
    let joint_polls = observer.joint_polls();
    observer.stop().await;

    cluster.shutdown().await.expect("clean shutdown");

    CaseResult {
        tally,
        boundary_exercised,
        faulted,
        degraded_windows,
        joint_polls,
        recovery,
    }
}
