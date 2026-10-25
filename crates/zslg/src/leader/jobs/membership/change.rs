//! The membership state machine — one `tick()` advances one step.
//!
//! Which step is *derived* from the committed [`ReplicaMembership`], not
//! tracked: `Single` is idle, `Joint` needs the generation quorum then the
//! drain bound, `JointDraining` carries its own bound and needs the drain wait.
//! The one leader-local fact is the generation this leader saw its own joint
//! config commit at.
//!
//! The bound (§18) is `max(max_hlc)` over a quorum of `old`, sampled only after
//! the step-2 generation quorum. Both halves carry weight: quorums of `old`
//! intersect, so the max covers any write acked without `next`; and after the
//! generation quorum no further write *can* be acked without `next`. So nothing
//! above the bound was ever acked against the outgoing set alone.
//!
//! Every step is re-validated at the Raft write point, so a bug here stalls a
//! change rather than dropping data.

#![allow(
    clippy::expect_used,
    reason = "expects on Mutex::lock() (no poison) and bounded conversions"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::FutureExt as _;
use tracing::{debug, info, warn};

use crate::api::{CandidateInfo, RaftLeadership, RaftStateView, SpareSelector};
use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::{NodeId, RaftGeneration};
use crate::leader::jobs::membership::{MembershipDriver, MembershipObserver, TickOutcome};
use crate::meta::{Exit, ReplicaMembership};
use crate::quorum::{FanOutResponse, fan_out_quorum};
use crate::raft::SpareSelectionStrategy;

pub(in crate::leader) trait LivenessQuery: Send + Sync + 'static {
    #[must_use]
    fn candidates(&self) -> BTreeMap<NodeId, CandidateInfo>;
}

pub(in crate::leader) struct MembershipChangeInputs {
    pub(in crate::leader) self_node_id: NodeId,
    /// Membership steps are published through the leader's single Raft
    /// writer, which validates each transition before it can reach the log.
    pub(in crate::leader) intents: crate::leader::IntentSubmitter,
    pub(in crate::leader) leadership: Arc<dyn RaftLeadership>,
    pub(in crate::leader) raft_state: Arc<dyn RaftStateView>,
    pub(in crate::leader) observer: Arc<dyn MembershipObserver>,
    pub(in crate::leader) replica_node_count: u32,
    pub(in crate::leader) spare_selection: SpareSelectionStrategy,
    pub(in crate::leader) live_nodes: Arc<dyn LivenessQuery>,
}

pub(in crate::leader) struct MembershipChangeManager {
    inputs: MembershipChangeInputs,
    /// The generation this leader saw *its own* joint config commit at.
    ///
    /// Cleared whenever this node is not the leader: a config retargeted by
    /// someone else during a leadership gap must not be measured against a
    /// generation from before it.
    episode: Mutex<Option<RaftGeneration>>,
}

impl MembershipChangeManager {
    #[must_use]
    pub(in crate::leader) fn new(inputs: MembershipChangeInputs) -> Self {
        Self {
            inputs,
            episode: Mutex::new(None),
        }
    }

    /// Step 1, and the abort path: if `new` has converged back on the set in
    /// force this collapses instead, which drops nobody and needs no bound.
    pub(in crate::leader) async fn initiate(&self, new: BTreeSet<NodeId>) -> Result<(), Error> {
        if new.is_empty() {
            return Err(Error::InvalidConfiguration(
                "membership change: new_replica_set must not be empty".into(),
            ));
        }

        let state = self.inputs.raft_state.snapshot();
        let old = state.replica.current().clone();

        if state.replica.next() == Some(&new) {
            return Ok(());
        }

        let membership = if new == old {
            if !state.replica.is_joint() {
                return Ok(());
            }

            info!(
                ?old,
                "membership change: clearing joint config, back to single set"
            );
            ReplicaMembership::single(old)
        } else {
            info!(?old, ?new, "membership change: step 1 SetJointReplicaSet");
            // Never `JointDraining`: a retarget must re-earn step 2 first.
            ReplicaMembership::joint(old, new)
        };
        let is_joint = membership.is_joint();

        self.submit(membership).await?;

        // Set, not merged: a retarget measures against the config it wrote.
        let post_write = self.inputs.raft_state.snapshot();
        *self.episode.lock().expect("episode lock") = is_joint.then_some(post_write.generation);
        Ok(())
    }

    /// Step 2: has a quorum of *both* sets applied the joint config? Once true,
    /// no further write can be acked without `next` — step 2b's precondition.
    async fn generation_quorum(
        &self,
        state: &crate::meta::RaftState,
        old: &BTreeSet<NodeId>,
        next: &BTreeSet<NodeId>,
        minimum_generation: RaftGeneration,
    ) -> bool {
        let mut initial = state.clone();
        initial.generation = minimum_generation;
        initial.replica = ReplicaMembership::joint(old.clone(), next.clone());

        fan_out_quorum(initial, {
            let observer = self.inputs.observer.clone();
            move |peer| {
                let observer = observer.clone();
                async move {
                    Ok(match observer.observe(peer).await {
                        Ok(status) if status.raft_state.generation >= minimum_generation => {
                            FanOutResponse::Ready(status.raft_state)
                        }
                        Ok(_) => FanOutResponse::Failed,
                        Err(e) => {
                            debug!(?peer, error = %e, "observer probe failed");
                            FanOutResponse::Failed
                        }
                    })
                }
            }
        })
        .await
        .is_ok()
    }

    /// Step 2b: sample the drain bound and commit it as `JointDraining`.
    /// See the module header for why the bound is exact.
    async fn arm_exit_condition(&self) -> Result<TickOutcome, Error> {
        let state = self.inputs.raft_state.snapshot();
        if !state.replica.is_joint() {
            *self.episode.lock().expect("episode lock") = None;
            return Ok(TickOutcome::Idle);
        }
        let old = state.replica.current().clone();
        let next = state.replica.next().cloned().unwrap_or_default();

        // `old` only — the incoming set must not be able to satisfy it.
        let mut sample_initial = (*state).clone();
        sample_initial.replica = ReplicaMembership::single(old.clone());

        let sampled: Arc<Mutex<Option<Hlc>>> = Arc::new(Mutex::new(None));
        let outcome = fan_out_quorum(sample_initial, {
            let observer = self.inputs.observer.clone();
            let sampled = sampled.clone();
            move |peer| {
                let observer = observer.clone();
                let sampled = sampled.clone();
                async move {
                    Ok(match observer.observe(peer).await {
                        Ok(status) => {
                            // A replica with an empty log contributes no bound;
                            // it still counts toward the quorum.
                            if let Some(max_hlc) = status.max_hlc {
                                let mut slot = sampled.lock().expect("sample lock");
                                *slot = Some(slot.map_or(max_hlc, |cur: Hlc| cur.max(max_hlc)));
                            }
                            FanOutResponse::Ready(status.raft_state)
                        }
                        Err(e) => {
                            debug!(?peer, error = %e, "step 2b: max_hlc probe failed");
                            FanOutResponse::Failed
                        }
                    })
                }
            }
        })
        .await;

        if outcome.is_err() {
            debug!(?old, "step 2b: no quorum of the outgoing set to sample");
            return Ok(TickOutcome::Pending);
        }

        if !self.is_still_leader() {
            debug!("membership change: lost leadership during step 2b; deferring");
            return Ok(TickOutcome::Pending);
        }

        let sampled = *sampled.lock().expect("sample lock");
        let latest = self.inputs.raft_state.snapshot();
        // `sampled` is None when every responder had an empty log — nothing can
        // be stranded on the outgoing set, so the collapse needs no snapshot.
        // `Exit`'s ordering puts `AnyTime` below every real bound, so one `max`
        // handles that case and the monotonicity below at once.
        //
        // Maxing against the *applied* membership's bound keeps an armed
        // episode monotone. Note it only reaches back one step: a retarget
        // rewrites the membership to `Joint`, which carries no exit, so the
        // arming after a retarget is a bare fresh sample and may be lower than
        // the bound the retarget dropped. That is deliberate and safe — a
        // sample taken after the generation quorum bounds anything acked
        // against `old` alone, because quorums of `old` intersect. Keeping the
        // higher value where one is visible is belt-and-braces, not the
        // property the drain rests on; the argument lives on
        // `raft_storage::validate_transition`'s retarget arm and
        // `meta::drain_satisfied`.
        let sampled = sampled.map(Exit::WatermarkAbove);
        let exit = [sampled, latest.replica.exit()]
            .into_iter()
            .flatten()
            .max()
            .unwrap_or(Exit::AnyTime);

        self.commit_draining(old, next, exit).await?;
        Ok(TickOutcome::Pending)
    }

    async fn commit_draining(
        &self,
        old: BTreeSet<NodeId>,
        next: BTreeSet<NodeId>,
        exit: Exit,
    ) -> Result<(), Error> {
        info!(?old, ?next, ?exit, "step 2b complete: exit condition armed");
        self.submit(ReplicaMembership::JointDraining { old, next, exit })
            .await?;
        Ok(())
    }

    /// Publish a membership transition through the leader's single Raft writer.
    async fn submit(
        &self,
        replica: ReplicaMembership,
    ) -> Result<crate::leader::IntentOutcome, Error> {
        self.inputs
            .intents
            .submit(crate::leader::RaftIntent::SetMembership { replica })
            .await
    }

    fn is_still_leader(&self) -> bool {
        self.inputs.leadership.current_leader() == Some(self.inputs.self_node_id)
    }
}

/// Whether the incoming set of a joint config must be recomputed because it is
/// no longer fully live. Split out from `maybe_initiate` so the retarget
/// decision — the one that discards step 2 — is assertable without a metrics
/// recorder.
#[must_use]
pub(in crate::leader) fn needs_retarget(
    current_next: &BTreeSet<NodeId>,
    live: &BTreeSet<NodeId>,
) -> bool {
    !current_next.is_subset(live)
}

#[async_trait]
impl MembershipDriver for MembershipChangeManager {
    async fn maybe_initiate(&self) -> Result<(), Error> {
        let state = self.inputs.raft_state.snapshot();
        let candidates = self.inputs.live_nodes.candidates();
        // Reconciliation (designdoc §"Membership Change"): the replica set
        // follows the openraft voter set. Liveness probes
        // `all_targets(replica) ∪ voter_ids`, so a node evicted from the voters
        // but still in the replica set stays probed and live; filtering here
        // makes it a drop candidate, and the same filter keeps the *spare* pool
        // from re-adding it — spares are drawn from `live`, so filtering only
        // the base set would hand the evicted node straight back.
        //
        // The floor is `compute_target`'s own `target.len() < want ⇒ None`: if
        // the voter set cannot fill the target size nothing is proposed and the
        // cluster waits for nodes to come back.
        let voters = self.inputs.leadership.voter_ids();
        let live: BTreeSet<NodeId> = candidates
            .keys()
            .copied()
            .filter(|id| voters.contains(id))
            .collect();

        if live.len() < candidates.len() {
            debug!(
                probed = candidates.len(),
                voters = voters.len(),
                eligible = live.len(),
                "membership change: live set filtered to the voter set",
            );
        }

        if live.is_empty() {
            return Ok(());
        }

        let next = if state.replica.is_joint() {
            let current_next = state
                .replica
                .next()
                .expect("joint ⇒ next_replica_node_ids is Some");
            if !needs_retarget(current_next, &live) {
                return Ok(());
            }
            let Some(next) = compute_target(
                current_next,
                &live,
                self.inputs.replica_node_count,
                &self.inputs.spare_selection,
                &candidates,
            ) else {
                warn!("membership change: all target nodes no longer live during joint transition");
                return Ok(());
            };
            crate::metrics::membership_retarget();
            warn!(
                old_target = ?current_next,
                ?next,
                "membership change restart: target node(s) no longer live during joint transition",
            );
            next
        } else {
            let Some(next) = compute_target(
                state.replica.current(),
                &live,
                self.inputs.replica_node_count,
                &self.inputs.spare_selection,
                &candidates,
            ) else {
                warn!("membership change: no viable replica set (no live nodes available)");
                return Ok(());
            };
            next
        };

        self.initiate(next).await
    }

    async fn tick(&self) -> Result<TickOutcome, Error> {
        // Any gap in leadership invalidates the episode: a config retargeted by
        // whoever held it meanwhile must not be measured against our generation.
        if !self.is_still_leader() {
            *self.episode.lock().expect("episode lock") = None;
            return Ok(TickOutcome::Pending);
        }

        let state = self.inputs.raft_state.snapshot();
        match &state.replica {
            ReplicaMembership::Single { .. } => {
                *self.episode.lock().expect("episode lock") = None;
                Ok(TickOutcome::Idle)
            }

            ReplicaMembership::Joint { old, next } => {
                // Adopting a joint config we did not start (new leader, or a
                // restart) resumes at step 2 against the generation as of now,
                // per the design doc. A bound already armed in the committed
                // state is not evidence of *our* generation quorum — step 2b
                // keeps it only as a monotone floor.
                let minimum_generation = *self
                    .episode
                    .lock()
                    .expect("episode lock")
                    .get_or_insert(state.generation);

                if !self
                    .generation_quorum(&state, old, next, minimum_generation)
                    .await
                {
                    debug!(?minimum_generation, "step 2: generation quorum not yet met");
                    return Ok(TickOutcome::Pending);
                }
                if !self.is_still_leader() {
                    debug!("membership change: lost leadership after step 2; deferring");
                    return Ok(TickOutcome::Pending);
                }
                info!(
                    ?minimum_generation,
                    "step 2 complete: no further write can be acked without the incoming set"
                );
                // Boxed: the two arms would otherwise sum into one oversized
                // poll frame (CLAUDE.md §2, `large_stack_frames`).
                self.arm_exit_condition().boxed().await
            }

            ReplicaMembership::JointDraining { next, exit, .. } => {
                // The exit condition (CLAUDE.md §8, designdoc §"Membership
                // Change"): every write the outgoing set may hold without a
                // quorum of `next` is bounded by `exit`, so the stable prefix
                // must cover it before those nodes are dropped. Re-checked
                // independently at the Raft write point.
                if !crate::meta::drain_satisfied(state.manifest_state.manifest_watermark, *exit) {
                    debug!(
                        current_watermark = ?state.manifest_state.manifest_watermark,
                        ?exit,
                        "step 3: waiting for the outgoing set to be drained into a snapshot"
                    );
                    return Ok(TickOutcome::Pending);
                }
                info!(?exit, "step 3: outgoing set drained");

                info!(?next, "step 4: collapsing to the single replica set");
                // The integrator re-checks the drain condition against the state
                // as of the write, and the Raft apply fence checks it again — so
                // losing leadership or racing a watermark change between the
                // check above and the write cannot drop un-snapshotted data.
                match self
                    .submit(ReplicaMembership::single(next.clone()))
                    .boxed()
                    .await?
                {
                    crate::leader::IntentOutcome::Applied { .. }
                    | crate::leader::IntentOutcome::AlreadySatisfied => {
                        *self.episode.lock().expect("episode lock") = None;
                        Ok(TickOutcome::Complete)
                    }
                    crate::leader::IntentOutcome::Superseded { reason } => {
                        debug!(%reason, "step 4 superseded; restarting at step 2");
                        *self.episode.lock().expect("episode lock") = None;
                        Ok(TickOutcome::Pending)
                    }
                }
            }
        }
    }
}

/// Fills a replica-set deficit by promoting spare live nodes. Which spares are
/// chosen is decided by the configured [`SpareSelector`] (see
/// [`SpareSelectionStrategy`]).
fn compute_target(
    current: &BTreeSet<NodeId>,
    live: &BTreeSet<NodeId>,
    target_size: u32,
    selector: &dyn SpareSelector,
    candidate_info: &BTreeMap<NodeId, CandidateInfo>,
) -> Option<BTreeSet<NodeId>> {
    let mut target: BTreeSet<NodeId> = current.intersection(live).copied().collect();
    let want = usize::try_from(target_size).unwrap_or(usize::MAX);
    let deficit = want.saturating_sub(target.len());
    let spares: Vec<CandidateInfo> = live
        .difference(&target)
        .map(|id| {
            candidate_info.get(id).cloned().unwrap_or(CandidateInfo {
                node_id: *id,
                rtt: None,
            })
        })
        .collect();
    target.extend(selector.rank(&spares).into_iter().take(deficit));
    if target.len() < want {
        warn!(
            target_len = target.len(),
            target_size = want,
            available_live = live.len(),
            "compute_target: insufficient live nodes to reach target replica set size",
        );
        return None;
    }
    Some(target)
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
    use std::collections::BTreeSet;
    use std::time::Duration;

    use super::*;
    use crate::tests::nid;

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    #[test]
    fn compute_target_all_current_live_met() {
        let cur = set(&[nid(1), nid(2), nid(3)]);
        let live = set(&[nid(1), nid(2), nid(3)]);
        assert_eq!(
            compute_target(
                &cur,
                &live,
                3,
                &SpareSelectionStrategy::Random,
                &BTreeMap::new()
            )
            .unwrap(),
            cur
        );
    }

    #[test]
    fn compute_target_adds_spares() {
        let cur = set(&[nid(1)]);
        let live = set(&[nid(1), nid(2), nid(3)]);
        let r = compute_target(
            &cur,
            &live,
            3,
            &SpareSelectionStrategy::Random,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(r.len(), 3);
        assert!(r.is_superset(&cur));
    }

    #[test]
    fn compute_target_drops_dead() {
        // nid(2) is dead; target_size 2 is satisfiable by the two live nodes.
        let cur = set(&[nid(1), nid(2), nid(3)]);
        let live = set(&[nid(1), nid(3)]);
        let r = compute_target(
            &cur,
            &live,
            2,
            &SpareSelectionStrategy::Random,
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(!r.contains(&nid(2)));
        assert_eq!(r, set(&[nid(1), nid(3)]));
    }

    #[test]
    fn compute_target_drops_dead_and_adds_spare() {
        // nid(2) dead, but a live spare nid(4) lets us still reach size 3.
        let cur = set(&[nid(1), nid(2), nid(3)]);
        let live = set(&[nid(1), nid(3), nid(4)]);
        let r = compute_target(
            &cur,
            &live,
            3,
            &SpareSelectionStrategy::Random,
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(!r.contains(&nid(2)));
        assert!(r.contains(&nid(4)));
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn compute_target_insufficient_live() {
        assert!(
            compute_target(
                &set(&[nid(1), nid(2)]),
                &set(&[nid(1)]),
                3,
                &SpareSelectionStrategy::Random,
                &BTreeMap::new(),
            )
            .is_none()
        );
    }

    #[test]
    fn compute_target_no_live_nodes() {
        assert!(
            compute_target(
                &set(&[nid(1)]),
                &BTreeSet::new(),
                3,
                &SpareSelectionStrategy::Random,
                &BTreeMap::new(),
            )
            .is_none()
        );
    }

    #[test]
    fn compute_target_all_current_dead_replaced() {
        let cur = set(&[nid(1), nid(2)]);
        let live = set(&[nid(3), nid(4), nid(5)]);
        let r = compute_target(
            &cur,
            &live,
            2,
            &SpareSelectionStrategy::Random,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(r.len(), 2);
        assert!(!r.contains(&nid(1)));
        assert!(!r.contains(&nid(2)));
    }

    #[test]
    fn compute_target_exact_sized_no_change() {
        let cur = set(&[nid(1), nid(2)]);
        let live = set(&[nid(1), nid(2), nid(3)]);
        let r = compute_target(
            &cur,
            &live,
            2,
            &SpareSelectionStrategy::Random,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn compute_target_random_fills_with_valid_spares() {
        // One live member, surplus of spares: the deficit slot is filled by
        // exactly one of the live spares (which one is random).
        let cur = set(&[nid(1)]);
        let live = set(&[nid(1), nid(2), nid(3), nid(4)]);
        let r = compute_target(
            &cur,
            &live,
            2,
            &SpareSelectionStrategy::Random,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(r.len(), 2);
        assert!(r.contains(&nid(1)));
        assert!(r.is_subset(&live));
        let fillers: Vec<NodeId> = r.difference(&cur).copied().collect();
        assert_eq!(fillers.len(), 1);
        assert!([nid(2), nid(3), nid(4)].contains(&fillers[0]));
    }

    #[test]
    fn random_rank_is_a_permutation() {
        use proptest::prelude::*;

        // `rank` must reorder `spares` without adding or dropping any element —
        // the invariant `compute_target` relies on when it `take`s a prefix.
        proptest!(
            ProptestConfig::with_cases(64),
            |(seeds in proptest::collection::btree_set(any::<u8>(), 0..20))| {
                let ids: BTreeSet<NodeId> = seeds.iter().map(|&s| nid(s)).collect();
                let candidates: Vec<CandidateInfo> = ids
                    .iter()
                    .map(|&node_id| CandidateInfo { node_id, rtt: None })
                    .collect();
                let ranked = SpareSelectionStrategy::Random.rank(&candidates);
                prop_assert_eq!(ranked.len(), ids.len());
                let as_set: BTreeSet<NodeId> = ranked.into_iter().collect();
                prop_assert_eq!(as_set, ids);
            }
        );
    }

    fn cand(node: NodeId, rtt_ms: Option<u64>) -> CandidateInfo {
        CandidateInfo {
            node_id: node,
            rtt: rtt_ms.map(Duration::from_millis),
        }
    }

    #[test]
    fn lowest_latency_orders_by_rtt() {
        let candidates = vec![
            cand(nid(1), Some(30)),
            cand(nid(2), Some(10)),
            cand(nid(3), Some(20)),
        ];
        assert_eq!(
            SpareSelectionStrategy::LowestLatency.rank(&candidates),
            vec![nid(2), nid(3), nid(1)]
        );
    }

    #[test]
    fn lowest_latency_none_sorts_last() {
        let candidates = vec![cand(nid(1), None), cand(nid(2), Some(50))];
        assert_eq!(
            SpareSelectionStrategy::LowestLatency.rank(&candidates),
            vec![nid(2), nid(1)]
        );
    }

    #[test]
    fn lowest_latency_ties_break_on_node_id() {
        let candidates = vec![
            cand(nid(3), Some(10)),
            cand(nid(1), Some(10)),
            cand(nid(2), Some(10)),
        ];
        assert_eq!(
            SpareSelectionStrategy::LowestLatency.rank(&candidates),
            vec![nid(1), nid(2), nid(3)]
        );
    }

    #[test]
    fn compute_target_lowest_latency_prefers_low_rtt() {
        // One current member + three spares with distinct RTTs; want 2 ⇒ the
        // lowest-RTT spare (nid(3)) fills the single deficit slot.
        let cur = set(&[nid(1)]);
        let live = set(&[nid(1), nid(2), nid(3), nid(4)]);
        let mut info = BTreeMap::new();
        info.insert(nid(2), cand(nid(2), Some(30)));
        info.insert(nid(3), cand(nid(3), Some(10)));
        info.insert(nid(4), cand(nid(4), Some(20)));
        let r = compute_target(
            &cur,
            &live,
            2,
            &SpareSelectionStrategy::LowestLatency,
            &info,
        )
        .unwrap();
        assert_eq!(r, set(&[nid(1), nid(3)]));
    }
}
