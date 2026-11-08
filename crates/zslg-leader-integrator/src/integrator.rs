//! The leader's single Raft writer.
//!
//! Jobs submit an *intent* — the delta they want, plus the premise it was
//! computed under. The integrator drains a batch, re-checks each premise against
//! the state as of now, folds the survivors into one proposal, and answers each
//! submitter's oneshot.
//!
//! ```text
//!   jobs ──intents──> [drain] ──> fold_checked ──> replicate_state ──> raft
//!                                  per-intent          fenced on
//!                                  premise             base.generation
//! ```
//!
//! Safety rests on three things:
//!
//! 1. **One writer** — a single task owns the read-modify-write, so no two
//!    proposals are built from the same base.
//! 2. **Semantic premises, not a generation fence** — an intent states what it
//!    assumed (the watermark it extends, the snapshots it replaces), so it
//!    survives a concurrent change that touched something else. A generation CAS
//!    would fail on *any* change.
//! 3. **The Raft apply fence validates independently**
//!    (`raft_storage::{validate_replicated_state, validate_transition}`), so a
//!    bug here cannot commit an unsafe state.
//!
//! Premises are per intent, not per batch: one rejected intent does not sink the
//! others drained with it.

use std::sync::Arc;
use zslg_consensus::ConsensusError;
use zslg_consensus::{IntentOutcome, IntentSubmitter, RaftIntent, RaftStateView, StateReplicator};
use zslg_lifecycle_types::LifecycleError;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::Error;
use crate::metrics::IntentState;
use zslg_consensus::RaftState;
use zslg_consensus::ReplicatedRaftState;

/// Queue depth. Every submitter awaits its own reply, so this only has to
/// absorb one drain's worth of concurrent jobs.
const INTENT_QUEUE_CAPACITY: usize = 64;

type Reply = oneshot::Sender<Result<IntentOutcome, Error>>;

struct IntentRequest {
    intent: RaftIntent,
    reply: Reply,
}

impl IntentRequest {
    /// Answer the submitter. A dropped receiver means it stopped waiting —
    /// nothing here can act on that, and the intent already had its effect.
    fn answer(self, outcome: Result<IntentOutcome, Error>) {
        let _ = self.reply.send(outcome);
    }
}

/// Submission handle held by every leader job.
#[derive(Clone)]
pub struct IntentQueue {
    tx: mpsc::Sender<IntentRequest>,
}

#[async_trait::async_trait]
impl IntentSubmitter for IntentQueue {
    async fn submit(&self, intent: RaftIntent) -> Result<IntentOutcome, zslg_consensus::Error> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(IntentRequest { intent, reply })
            .await
            .map_err(|_| LifecycleError::Shutdown)?;
        rx.await
            .map_err(|_| LifecycleError::Shutdown)?
            .map_err(Into::into)
    }
}

/// Owns the receiving half of the intent queue.
pub struct Integrator {
    rx: mpsc::Receiver<IntentRequest>,
    state_replicator: Arc<dyn StateReplicator>,
    raft_state: Arc<dyn RaftStateView>,
}

impl Integrator {
    #[must_use]
    pub fn new(
        state_replicator: Arc<dyn StateReplicator>,
        raft_state: Arc<dyn RaftStateView>,
    ) -> (Self, Arc<dyn IntentSubmitter>) {
        let (tx, rx) = mpsc::channel(INTENT_QUEUE_CAPACITY);
        (
            Self {
                rx,
                state_replicator,
                raft_state,
            },
            Arc::new(IntentQueue { tx }),
        )
    }

    /// The real publish path — premise checks, transition validation, the
    /// generation fence — without standing up the whole leader.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn spawn_for_test(
        state_replicator: Arc<dyn StateReplicator>,
        raft_state: Arc<dyn RaftStateView>,
    ) -> (Arc<dyn IntentSubmitter>, CancellationToken) {
        let (integrator, submitter) = Self::new(state_replicator, raft_state);
        let cancel = CancellationToken::new();
        tokio::spawn(integrator.run::<Error>(cancel.clone()));
        (submitter, cancel)
    }

    pub async fn run<E>(mut self, cancel: CancellationToken) -> Result<(), E> {
        info!("leader integrator: starting");
        loop {
            let first = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                req = self.rx.recv() => match req {
                    Some(req) => req,
                    None => break,
                },
            };
            // The drain-batch shape of CLAUDE.md §8.
            let mut batch = vec![first];
            while let Ok(req) = self.rx.try_recv() {
                batch.push(req);
            }
            let started = tokio::time::Instant::now();
            self.integrate_batch(batch).await;
            zslg_consensus::metrics::leader_job_ran(
                zslg_consensus::metrics::LeaderJob::Integrator,
                started.elapsed(),
            );
        }
        info!("leader integrator: shutdown");
        Ok(())
    }

    async fn integrate_batch(&self, batch: Vec<IntentRequest>) {
        let base = self.raft_state.snapshot();
        let mut proposed = replicated_from(&base);
        let accepted = fold_batch(batch, &mut proposed);
        if accepted.is_empty() {
            return;
        }
        let contexts: Vec<&'static str> = accepted.iter().map(|r| r.intent.describe()).collect();
        debug!(
            intents = ?contexts,
            generation = ?base.generation,
            "integrating leader intents"
        );
        match self
            .state_replicator
            .replicate_state(proposed, base.generation, "client_write leader intents")
            .await
            .map_err(Error::from)
        {
            Ok(()) => {
                // Not re-read after the commit: the submitter asked which
                // generation it was fenced on, not what is current now.
                let generation = base.generation;
                crate::metrics::leader_intent_batch();
                for req in accepted {
                    crate::metrics::leader_intent(IntentState::Applied);
                    req.answer(Ok(IntentOutcome::Applied { generation }));
                }
            }
            Err(e) => {
                // Deliberately not retried. The generation only advances on
                // `EntryPayload::Normal`, written only here and serially, so a
                // lost fence means the epoch changed — and a deposed leader
                // re-proposing is the wrong response. The job re-derives.
                let state = if matches!(e, Error::Consensus(ConsensusError::StaleStateWrite { .. }))
                {
                    IntentState::StaleFence
                } else {
                    IntentState::Failed
                };
                warn!(error = %e, intents = ?contexts, "leader intents failed to commit");
                for req in accepted {
                    crate::metrics::leader_intent(state);
                    req.answer(Err(e.clone()));
                }
            }
        }
    }
}

/// Whether folding an intent into the proposal actually changed anything.
#[derive(Debug)]
enum Applied {
    Changed,
    NoOp,
}

fn replicated_from(state: &RaftState) -> ReplicatedRaftState {
    ReplicatedRaftState {
        replica: state.replica.clone(),
        manifest: state.manifest_state.manifest.clone(),
        manifest_watermark: state.manifest_state.manifest_watermark,
        hlc_cutoff: state.manifest_state.hlc_cutoff,
    }
}

/// Fold every intent into `proposed`, answering the ones that do not survive.
/// Returns those still awaiting the proposal's outcome.
fn fold_batch(batch: Vec<IntentRequest>, proposed: &mut ReplicatedRaftState) -> Vec<IntentRequest> {
    let mut accepted: Vec<IntentRequest> = Vec::with_capacity(batch.len());
    for req in batch {
        match fold_checked(proposed, &req.intent) {
            Ok(Applied::Changed) => accepted.push(req),
            Ok(Applied::NoOp) => {
                crate::metrics::leader_intent(IntentState::AlreadySatisfied);
                req.answer(Ok(IntentOutcome::AlreadySatisfied));
            }
            Err(reason) => {
                debug!(
                    intent = req.intent.describe(),
                    %reason,
                    "intent superseded by the current state"
                );
                crate::metrics::leader_intent(IntentState::Superseded);
                req.answer(Ok(IntentOutcome::Superseded { reason }));
            }
        }
    }
    accepted
}

/// Fold one intent, then re-validate the *whole* folded state with the same
/// checks the Raft apply fence applies; roll the fold back if it fails.
///
/// [`apply_intent`] judges the intent's own premise. This second layer exists
/// because individually reasonable intents can compose into an invalid state —
/// a splice that leaves a manifest gap, a membership collapse made premature by
/// a watermark change folded alongside it, a batch walking `Single -> Joint ->
/// Single` and dropping the outgoing set with no drain bound met (§18).
///
/// Validating after *each* fold, against the state as folded so far, is what
/// makes the offending intent the one superseded rather than the whole batch
/// failing at the propose site.
fn fold_checked(
    proposed: &mut ReplicatedRaftState,
    intent: &RaftIntent,
) -> Result<Applied, String> {
    let before_fold = proposed.clone();
    match apply_intent(proposed, intent) {
        Ok(Applied::NoOp) => Ok(Applied::NoOp),
        Ok(Applied::Changed) => {
            let verdict = zslg_consensus::validate_replicated_state(proposed)
                .map_err(|e| e.to_string())
                .and_then(|()| {
                    zslg_consensus::validate_transition(&before_fold.replica, proposed)
                        .map_err(|e| e.to_string())
                })
                .and_then(|()| {
                    zslg_consensus::validate_cutoff_advance(before_fold.hlc_cutoff, proposed)
                        .map_err(|e| e.to_string())
                });
            match verdict {
                Ok(()) => Ok(Applied::Changed),
                Err(reason) => {
                    *proposed = before_fold;
                    Err(reason)
                }
            }
        }
        Err(reason) => {
            *proposed = before_fold;
            Err(reason)
        }
    }
}

/// Fold one intent into `proposed`, judging its premise against the folding so
/// far. `Err(reason)` means superseded.
///
/// No access to the pre-batch state, deliberately: a premise must be judged
/// against the state the batch will actually leave, not the one it started
/// from. Call through [`fold_checked`], which also validates the result.
fn apply_intent(
    proposed: &mut ReplicatedRaftState,
    intent: &RaftIntent,
) -> Result<Applied, String> {
    match intent {
        RaftIntent::AdvanceCutoff { cutoff } => {
            if proposed.hlc_cutoff.is_some_and(|cur| *cutoff <= cur) {
                return Ok(Applied::NoOp);
            }
            proposed.hlc_cutoff = Some(*cutoff);
            Ok(Applied::Changed)
        }

        RaftIntent::CommitSnapshot {
            entry,
            new_watermark,
            prev_watermark,
            prev_end_index,
        } => {
            // Indices continue from `prev_end_index`; if either premise moved
            // the entry cannot be spliced in and the window must be recomputed.
            if proposed.manifest_watermark != *prev_watermark {
                return Err(format!(
                    "watermark moved from {prev_watermark:?} to {:?} while the snapshot was built",
                    proposed.manifest_watermark
                ));
            }
            let current_end = proposed.manifest.last().map(|m| m.end_index);
            if current_end != *prev_end_index {
                return Err(format!(
                    "manifest end index moved from {prev_end_index:?} to {current_end:?} while \
                     the snapshot was built"
                ));
            }
            if proposed
                .manifest_watermark
                .is_some_and(|prev| *new_watermark < prev)
            {
                return Err(format!(
                    "refusing to regress the manifest watermark to {new_watermark:?} from {:?}",
                    proposed.manifest_watermark
                ));
            }
            if let Some(meta) = entry {
                proposed.manifest.push(meta.clone());
            }
            proposed.manifest_watermark = Some(*new_watermark);
            // A committed `manifest_watermark > hlc_cutoff` is forbidden, so
            // carry the cutoff along when it would otherwise lag.
            if proposed.hlc_cutoff.is_none_or(|cur| cur < *new_watermark) {
                proposed.hlc_cutoff = Some(*new_watermark);
            }
            Ok(Applied::Changed)
        }

        RaftIntent::CompactManifest {
            replaced,
            replacement,
        } => {
            if replaced.is_empty() {
                return Ok(Applied::NoOp);
            }
            if let Some(missing) = replaced
                .iter()
                .find(|id| !proposed.manifest.iter().any(|m| m.snapshot_id == **id))
            {
                return Err(format!(
                    "snapshot {missing:?} is no longer in the manifest; the compaction was \
                     computed against a superseded manifest"
                ));
            }
            // Entries appended while the merge ran keep their place, so a long
            // compaction survives a concurrent snapshot commit.
            let at = proposed
                .manifest
                .iter()
                .position(|m| replaced.contains(&m.snapshot_id))
                .ok_or_else(|| "replaced set vanished from the manifest".to_owned())?;
            proposed
                .manifest
                .retain(|m| !replaced.contains(&m.snapshot_id));
            for (offset, meta) in replacement.iter().enumerate() {
                proposed.manifest.insert(
                    at.checked_add(offset)
                        .ok_or_else(|| "manifest splice overflow".to_owned())?,
                    meta.clone(),
                );
            }
            Ok(Applied::Changed)
        }

        RaftIntent::SetMembership { replica } => {
            if proposed.replica == *replica {
                return Ok(Applied::NoOp);
            }
            proposed.replica = replica.clone();
            // Validated by `fold_checked`: a collapse's legality depends on
            // the watermark, which a `CommitSnapshot` here may have moved.
            Ok(Applied::Changed)
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests;
