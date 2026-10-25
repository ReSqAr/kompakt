use crate::hlc::Hlc;
use crate::meta::ReplicaMembership;
use crate::raft::{RaftLogIndex, RaftStateError};

/// The structural-validity check for a proposed raft state, judged **on its
/// own** — no reference to the state it replaces. Runs pre-flight on the
/// leader's propose path (`replicate_state`, mapped to an immediate `Err`) and
/// again at the fence ([`evaluate_fence`], mapped to a value rejection). The
/// membership *transition* rules need the previous state and live in
/// [`validate_transition`], which only the fence can run — so a state can pass
/// pre-flight and still be value-rejected at the fence.
///
/// Two invariants, both assumed by callers this file cannot see (the leader's
/// snapshot-index assignment and the learner's seamless-append precondition):
///
/// * **Manifest shape** — sorted, contiguous, covering the index space from 0
///   (`entry[0].start == 0`, `entry[k+1].start == entry[k].end + 1`),
///   `start <= end` per entry.
/// * **`manifest_watermark <= hlc_cutoff`** — the snapshot window is
///   `(watermark, cutoff]` and every future window starts strictly above the
///   watermark, so a committed cutoff below the watermark would admit writes no
///   snapshot can ever cover.
pub(crate) fn validate_replicated_state(
    state: &crate::api::ReplicatedRaftState,
) -> Result<(), RaftStateError> {
    let mut expected_start = RaftLogIndex::ZERO;
    for meta in &state.manifest {
        if RaftLogIndex::from(meta.start_index.raw()) != expected_start {
            return Err(RaftStateError::ManifestNonContiguous {
                snapshot_id: meta.snapshot_id,
                start: meta.start_index.raw(),
                expected: expected_start.raw(),
            });
        }
        if meta.end_index < meta.start_index {
            return Err(RaftStateError::ManifestEndBeforeStart {
                snapshot_id: meta.snapshot_id,
                start: meta.start_index.raw(),
                end: meta.end_index.raw(),
            });
        }
        // `end + 1`: overflow means a malformed (absurd) manifest, so reject
        // rather than wrap.
        match meta.end_index.raw().checked_add(1) {
            Some(next) => expected_start = RaftLogIndex::from(next),
            None => {
                return Err(RaftStateError::ManifestIndexOverflow {
                    snapshot_id: meta.snapshot_id,
                });
            }
        }
    }
    if let (Some(watermark), Some(cutoff)) = (state.manifest_watermark, state.hlc_cutoff)
        && watermark > cutoff
    {
        return Err(RaftStateError::WatermarkAboveCutoff { watermark, cutoff });
    }
    Ok(())
}

/// Is the proposed HLC cutoff a legal successor of the committed one?
///
/// Needs the prior *cutoff*, which [`validate_transition`] does not receive
/// (it takes only the prior membership), so it is its own validator rather than
/// a widened signature through four call sites.
///
/// CLAUDE.md §8 — the cutoff may never go backwards. That rule is already kept
/// by the leader's integrator, which turns a non-advancing `AdvanceCutoff` into
/// a no-op; checking it here too is the same argument
/// [`RaftStateError::JointExitTooEarly`] makes for its own rule, that one
/// writer's discipline is not the place to enforce a durability invariant.
///
/// A proposed cutoff *below the previous leader's* is a different thing and is
/// expected (CLAUDE.md §12): it is skipped before proposal when it would open an
/// empty window, and absorbed as a no-op by each replica's `HlcGate` otherwise.
/// This rule is about the committed value, which only ever advances.
pub(crate) fn validate_cutoff_advance(
    previous: Option<Hlc>,
    proposed: &crate::api::ReplicatedRaftState,
) -> Result<(), RaftStateError> {
    if let (Some(previous), Some(proposed)) = (previous, proposed.hlc_cutoff)
        && proposed < previous
    {
        return Err(RaftStateError::CutoffRegressed { previous, proposed });
    }
    Ok(())
}

/// Is `proposed` a legal successor of `previous`? Needs the prior state, so
/// only the fence runs it — neither `StateReplicator` impl holds the applied
/// state. The load-bearing rule is [`RaftStateError::JointExitTooEarly`]: a
/// joint config may only collapse once the stable prefix covers its `exit`,
/// and checking here (not only in the leader's tick) means no leader bug, no
/// lost leadership mid-change, and no stale in-flight proposal can drop
/// replicas holding the only copy of acknowledged writes (CLAUDE.md §8). The
/// remaining rules keep `exit_hlc` meaningful: it attests a generation quorum
/// for a specific `(old, next)` pair, so neither set may change while armed,
/// and the bound may never regress.
#[tracing::instrument(level = "debug", skip_all, name = "raft_validate_transition")]
pub(crate) fn validate_transition(
    previous: &ReplicaMembership,
    proposed: &crate::api::ReplicatedRaftState,
) -> Result<(), RaftStateError> {
    use crate::meta::ReplicaMembership as M;

    // `old` is fixed for the lifetime of a joint episode.
    if previous.is_joint()
        && proposed.replica.is_joint()
        && previous.current() != proposed.replica.current()
    {
        return Err(RaftStateError::JointOldSetChanged {
            previous: previous.current().clone(),
            proposed: proposed.replica.current().clone(),
        });
    }

    match (previous, &proposed.replica) {
        // Steady state: a re-proposal of the *same* single set. Every
        // snapshot/compaction/cutoff write carries the membership along
        // unchanged, so this arm is hot — but it must stay an identity check.
        // A direct set swap here would bypass the joint protocol entirely and
        // drop the outgoing replicas without a drain bound.
        // The empty set is genesis: it holds no data, so seeding the first real
        // replica set out of it drains nothing (`bootstrap_single_node_cluster`).
        (M::Single { replicas: previous }, M::Single { replicas: proposed }) => {
            if previous == proposed || previous.is_empty() {
                Ok(())
            } else {
                Err(RaftStateError::SingleSetChangedWithoutJoint {
                    previous: previous.clone(),
                    proposed: proposed.clone(),
                })
            }
        }

        // Step 1: entering a joint config. The outgoing set must be the set that
        // was actually in force.
        (M::Single { replicas }, M::Joint { old, .. }) => {
            if replicas == old {
                Ok(())
            } else {
                Err(RaftStateError::JointEntryWrongOldSet {
                    proposed: old.clone(),
                    previous: replicas.clone(),
                })
            }
        }
        // A single config may not arm a bound it has no joint config for.
        (M::Single { replicas }, M::JointDraining { old, next, .. }) => {
            Err(RaftStateError::JointExitWrongSet {
                proposed: replicas.clone(),
                old: old.clone(),
                next: next.clone(),
            })
        }

        // Retarget, or a pass-through re-proposal of the same joint config.
        //
        // Unconditional, including from `JointDraining` — which *disarms*, and
        // so lets the next arming commit a numerically lower bound than the one
        // dropped here. That is sound, not a hole in the regression check
        // below: a bound is the max over a quorum of `old`, every write acked
        // against `old` alone reached some quorum of `old`, and quorums of
        // `old` intersect — so any post-generation-quorum sample still covers
        // it. A lower fresh sample means the earlier bound was over-conservative
        // (driven by a write that never achieved quorum), not that the new one
        // is unsafe. `old` is pinned above for the whole episode, which is what
        // makes that argument hold, and a retarget must re-earn the generation
        // quorum before it can arm again (`leader::jobs::membership::change`).
        // Monotonicity within an unbroken armed chain is therefore
        // defence-in-depth, not a safety requirement.
        (M::Joint { .. } | M::JointDraining { .. }, M::Joint { .. }) => Ok(()),

        // Step 2b: arming. The sets carry over unchanged (`old` already checked).
        (
            M::Joint {
                next: prev_next, ..
            },
            M::JointDraining { next, .. },
        ) => {
            if prev_next == next {
                Ok(())
            } else {
                Err(RaftStateError::JointArmingChangedNextSet {
                    previous: prev_next.clone(),
                    proposed: next.clone(),
                })
            }
        }

        // Re-arming an already-armed config: the bound may only rise, and the
        // incoming set must not change under it.
        (
            M::JointDraining {
                next: prev_next,
                exit: prev_exit,
                ..
            },
            M::JointDraining { next, exit, .. },
        ) => {
            if prev_next != next {
                return Err(RaftStateError::JointArmingChangedNextSet {
                    previous: prev_next.clone(),
                    proposed: next.clone(),
                });
            }
            // `Exit`'s ordering puts `AnyTime` below every real bound, so
            // downgrading a committed bound to "nothing to drain" is a
            // regression like any other.
            if exit < prev_exit {
                return Err(RaftStateError::ExitHlcRegressed {
                    previous: *prev_exit,
                    proposed: *exit,
                });
            }
            Ok(())
        }

        // Step 4 / abort: collapsing a joint config.
        (M::Joint { old, next }, M::Single { replicas }) => {
            // Aborting back to `old` drops nobody, so it needs no bound. Landing
            // on `next` does, and an unarmed `Joint` has none.
            if replicas == old {
                Ok(())
            } else if replicas == next {
                Err(RaftStateError::JointExitNotArmed)
            } else {
                Err(RaftStateError::JointExitWrongSet {
                    proposed: replicas.clone(),
                    old: old.clone(),
                    next: next.clone(),
                })
            }
        }
        (M::JointDraining { old, next, exit }, M::Single { replicas }) => {
            if replicas == old {
                return Ok(());
            }
            if replicas != next {
                return Err(RaftStateError::JointExitWrongSet {
                    proposed: replicas.clone(),
                    old: old.clone(),
                    next: next.clone(),
                });
            }
            // **The invariant.** Everything the outgoing set may hold alone is
            // bounded by `exit`; the stable prefix must cover it before those
            // nodes stop counting toward quorum.
            if !crate::meta::drain_satisfied(proposed.manifest_watermark, *exit) {
                return Err(RaftStateError::JointExitTooEarly {
                    exit: *exit,
                    watermark: proposed.manifest_watermark,
                });
            }
            Ok(())
        }
    }
}
