//! The apply fence: a committed entry is judged, never rejected with an `Err`.

use crate::Error;
use zslg_consensus::RaftGeneration;
use zslg_consensus::RaftLogIndex;
use zslg_consensus::RaftStateHandle;
use zslg_consensus::ReplicaMembership;
use zslg_consensus::{validate_cutoff_advance, validate_replicated_state, validate_transition};
use zslg_raft::RaftResponse;
use zslg_time::Hlc;

// SUPER

pub(super) fn evaluate_fence(
    current: RaftGeneration,
    previous_replica: &ReplicaMembership,
    previous_cutoff: Option<Hlc>,
    write: &zslg_raft::FencedStateWrite,
    log_index: RaftLogIndex,
) -> RaftResponse {
    if write.expected_generation != current {
        tracing::warn!(
            generation = ?current,
            expected_generation = ?write.expected_generation,
            log_index = %log_index,
            "fenced state write rejected: stale generation",
        );
        return RaftResponse::RejectedStaleGeneration {
            expected: write.expected_generation,
            current,
        };
    }
    // A structural-invariant violation is a *value* rejection, never an `Err`
    // — the header explains why `Err` on a committed entry is fatal. The leader
    // also refuses to *propose* such a state (the same check runs pre-flight in
    // `replicate_state`), so in correct operation this never fires.
    match validate_replicated_state(&write.state)
        .and_then(|()| validate_transition(previous_replica, &write.state))
        .and_then(|()| validate_cutoff_advance(previous_cutoff, &write.state))
    {
        Ok(()) => RaftResponse::Applied,
        Err(reason) => {
            tracing::error!(
                log_index = %log_index,
                %reason,
                "fenced state write rejected: invalid raft state",
            );
            RaftResponse::RejectedInvalidState { reason }
        }
    }
}

pub(super) fn apply_normal_fenced(
    raft_state: &RaftStateHandle,
    write: &zslg_raft::FencedStateWrite,
    log_index: RaftLogIndex,
) -> Result<RaftResponse, Error> {
    let applied = raft_state.snapshot();
    let response = evaluate_fence(
        applied.generation,
        &applied.replica,
        applied.manifest_state.hlc_cutoff,
        write,
        log_index,
    );
    if matches!(response, RaftResponse::Applied) {
        raft_state.apply_replicated(write.state.clone(), RaftGeneration(log_index.raw()))?;
    }
    Ok(response)
}
