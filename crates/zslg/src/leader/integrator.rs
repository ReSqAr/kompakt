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

use std::collections::BTreeSet;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::api::{RaftStateView, ReplicatedRaftState, StateReplicator};
use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::{LogIndex, RaftGeneration, SnapshotId};
use crate::meta::{RaftState, ReplicaMembership, SnapshotMetadata};
use crate::metrics::IntentState;

/// Queue depth. Every submitter awaits its own reply, so this only has to
/// absorb one drain's worth of concurrent jobs.
const INTENT_QUEUE_CAPACITY: usize = 64;

/// A requested change to the replicated Raft state.
///
/// Each variant carries the premise it was computed under, so the integrator can
/// tell "this is still applicable" from "the world moved on".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::leader) enum RaftIntent {
    /// Publish a new HLC cutoff (snapshot protocol step 1). Idempotent: a cutoff
    /// at or below the committed one is already satisfied.
    AdvanceCutoff { cutoff: Hlc },

    /// Append `entry` to the manifest (absent for an empty window) and advance
    /// the watermark. Premise: the manifest it was built against — its index
    /// assignment continues from `prev_end_index`, so it cannot be rebased onto
    /// a manifest that has grown since.
    CommitSnapshot {
        entry: Option<SnapshotMetadata>,
        new_watermark: Hlc,
        prev_watermark: Option<Hlc>,
        prev_end_index: Option<LogIndex>,
    },

    /// Splice `replacement` over `replaced`, leaving the rest of the manifest
    /// alone. A splice rather than a whole manifest so a long compaction
    /// survives snapshot commits that land while it runs.
    CompactManifest {
        replaced: BTreeSet<SnapshotId>,
        replacement: Vec<SnapshotMetadata>,
    },

    /// A replica-membership transition, validated by the same function the
    /// Raft apply fence uses.
    SetMembership { replica: ReplicaMembership },
}

impl RaftIntent {
    fn describe(&self) -> &'static str {
        match self {
            Self::AdvanceCutoff { .. } => "AdvanceCutoff",
            Self::CommitSnapshot { .. } => "CommitSnapshot",
            Self::CompactManifest { .. } => "CompactManifest",
            Self::SetMembership { .. } => "SetMembership",
        }
    }
}

/// Distinct from `Err`: the integrator worked, the intent no longer applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::leader) enum IntentOutcome {
    /// Folded into a proposal that committed.
    Applied { generation: RaftGeneration },
    /// Already true in the committed state; nothing to propose.
    AlreadySatisfied,
    /// The premise no longer holds; recompute from current state.
    Superseded { reason: String },
}

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
pub(in crate::leader) struct IntentSubmitter {
    tx: mpsc::Sender<IntentRequest>,
}

impl IntentSubmitter {
    /// Submit an intent and wait for its outcome. `Error::Shutdown` if the
    /// integrator is gone, which can only mean shutdown — same cancellation
    /// tree as its callers.
    #[must_use = "an intent's outcome says whether the state actually changed"]
    pub(in crate::leader) async fn submit(
        &self,
        intent: RaftIntent,
    ) -> Result<IntentOutcome, Error> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(IntentRequest { intent, reply })
            .await
            .map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)?
    }
}

/// Owns the receiving half of the intent queue.
pub(in crate::leader) struct Integrator {
    rx: mpsc::Receiver<IntentRequest>,
    state_replicator: Arc<dyn StateReplicator>,
    raft_state: Arc<dyn RaftStateView>,
}

impl Integrator {
    #[must_use]
    pub(in crate::leader) fn new(
        state_replicator: Arc<dyn StateReplicator>,
        raft_state: Arc<dyn RaftStateView>,
    ) -> (Self, IntentSubmitter) {
        let (tx, rx) = mpsc::channel(INTENT_QUEUE_CAPACITY);
        (
            Self {
                rx,
                state_replicator,
                raft_state,
            },
            IntentSubmitter { tx },
        )
    }

    /// The real publish path — premise checks, transition validation, the
    /// generation fence — without standing up the whole leader.
    #[cfg(test)]
    #[must_use]
    pub(in crate::leader) fn spawn_for_test(
        state_replicator: Arc<dyn StateReplicator>,
        raft_state: Arc<dyn RaftStateView>,
    ) -> (IntentSubmitter, CancellationToken) {
        let (integrator, submitter) = Self::new(state_replicator, raft_state);
        let cancel = CancellationToken::new();
        tokio::spawn(integrator.run(cancel.clone()));
        (submitter, cancel)
    }

    pub(in crate::leader) async fn run(mut self, cancel: CancellationToken) -> Result<(), Error> {
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
            crate::metrics::leader_job_ran(
                crate::metrics::LeaderJob::Integrator,
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
                let state = if matches!(e, Error::StaleStateWrite { .. }) {
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
            let verdict = crate::raft::validate_replicated_state(proposed)
                .map_err(|e| e.to_string())
                .and_then(|()| {
                    crate::raft::validate_transition(&before_fold.replica, proposed)
                        .map_err(|e| e.to_string())
                })
                .and_then(|()| {
                    crate::raft::validate_cutoff_advance(before_fold.hlc_cutoff, proposed)
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
mod tests {
    use crate::meta::Exit;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::ids::NodeId;
    use crate::raft::RaftStateHandle;
    use crate::tests::nid;

    // ── harness ──────────────────────────────────────────────────────────
    //
    // A `StateReplicator` that applies straight into a `RaftStateHandle` after
    // running the same fence checks Raft would. That gives the real integrator
    // loop — the queue, the `try_recv` drain, the retry, the oneshot replies —
    // without an openraft cluster, so these tests are deterministic and fast.

    struct FakeRaft {
        state: RaftStateHandle,
        /// Proposals accepted. The coalescing measurement: N intents landing in
        /// fewer than N proposals is the property under test.
        proposals: AtomicUsize,
        /// Reject the next N proposals with `StaleStateWrite`, as a competing
        /// writer would, to drive the retry path.
        stale_budget: AtomicUsize,
    }

    impl FakeRaft {
        fn new(initial: RaftState) -> Arc<Self> {
            let state = RaftStateHandle::new();
            state.replace(initial);
            Arc::new(Self {
                state,
                proposals: AtomicUsize::new(0),
                stale_budget: AtomicUsize::new(0),
            })
        }

        fn proposals(&self) -> usize {
            self.proposals.load(Ordering::Relaxed)
        }

        fn snapshot(&self) -> Arc<RaftState> {
            self.state.snapshot()
        }

        /// Simulate another writer winning the race `n` times.
        fn reject_next(&self, n: usize) {
            self.stale_budget.store(n, Ordering::Relaxed);
        }

        /// Rejections still owed. Counts down once per *attempt*, so it is how
        /// a test distinguishes one attempt from a retry ladder — `proposals`
        /// only counts the ones that were accepted.
        fn rejections_left(&self) -> usize {
            self.stale_budget.load(Ordering::Relaxed)
        }

        /// Commit something behind the integrator's back, bumping the generation
        /// exactly as a competing leader epoch would.
        fn commit_foreign(&self, mutate: impl FnOnce(&mut ReplicatedRaftState)) {
            let current = self.state.snapshot();
            let mut proposed = replicated_from(&current);
            mutate(&mut proposed);
            let next = RaftGeneration(current.generation.raw().saturating_add(1));
            self.state.apply_replicated(proposed, next).expect("apply");
        }
    }

    #[async_trait::async_trait]
    impl StateReplicator for FakeRaft {
        async fn replicate_state(
            &self,
            state: ReplicatedRaftState,
            expected_generation: RaftGeneration,
            context: &'static str,
        ) -> Result<(), Error> {
            let current = self.state.snapshot();
            if self.stale_budget.load(Ordering::Relaxed) > 0 {
                self.stale_budget.fetch_sub(1, Ordering::Relaxed);
                return Err(Error::stale_state_write(
                    context,
                    expected_generation,
                    current.generation,
                ));
            }
            if expected_generation != current.generation {
                return Err(Error::stale_state_write(
                    context,
                    expected_generation,
                    current.generation,
                ));
            }
            // The integrator must never propose a state the fence would reject;
            // asserting that here turns any such bug into a test failure rather
            // than a silent value-rejection in production.
            crate::raft::validate_replicated_state(&state)
                .expect("integrator proposed a structurally invalid state");
            crate::raft::validate_transition(&current.replica, &state)
                .expect("integrator proposed an illegal membership transition");
            crate::raft::validate_cutoff_advance(current.manifest_state.hlc_cutoff, &state)
                .expect("integrator proposed a regressing hlc cutoff");
            self.proposals.fetch_add(1, Ordering::Relaxed);
            let next = RaftGeneration(current.generation.raw().saturating_add(1));
            self.state.apply_replicated(state, next)
        }
    }

    fn meta(start: u64, end: u64, seed: u64) -> SnapshotMetadata {
        SnapshotMetadata {
            start_hlc: Hlc(start),
            end_hlc: Hlc(end),
            start_index: LogIndex(start),
            end_index: LogIndex(end),
            snapshot_id: SnapshotId::from_parts(LogIndex(start), Some(LogIndex(end)), seed),
        }
    }

    fn state_with(manifest: Vec<SnapshotMetadata>, watermark: Option<Hlc>) -> RaftState {
        let mut s = RaftState::empty();
        s.replica = ReplicaMembership::single([nid(1), nid(2), nid(3)].into_iter().collect());
        s.manifest_state.manifest = manifest;
        s.manifest_state.manifest_watermark = watermark;
        s.manifest_state.hlc_cutoff = watermark;
        s
    }

    /// Start a real integrator over a `FakeRaft`.
    fn start(initial: RaftState) -> (Arc<FakeRaft>, IntentSubmitter, CancellationToken) {
        let raft = FakeRaft::new(initial);
        let view: Arc<dyn RaftStateView> = Arc::new(raft.state.clone());
        let (integrator, submitter) = Integrator::new(raft.clone(), view);
        let cancel = CancellationToken::new();
        tokio::spawn(integrator.run(cancel.clone()));
        (raft, submitter, cancel)
    }

    fn snapshot_intent(
        prev_wm: Option<Hlc>,
        prev_end: Option<LogIndex>,
        entry: SnapshotMetadata,
        new_wm: u64,
    ) -> RaftIntent {
        RaftIntent::CommitSnapshot {
            entry: Some(entry),
            new_watermark: Hlc(new_wm),
            prev_watermark: prev_wm,
            prev_end_index: prev_end,
        }
    }

    // ── outcomes ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn applies_a_cutoff_and_reports_idempotence() {
        let (raft, intents, _c) = start(state_with(vec![], Some(Hlc(100))));

        let out = intents
            .submit(RaftIntent::AdvanceCutoff { cutoff: Hlc(200) })
            .await
            .expect("submit");
        assert!(matches!(out, IntentOutcome::Applied { .. }));
        assert_eq!(raft.snapshot().manifest_state.hlc_cutoff, Some(Hlc(200)));

        // Re-submitting the same (or a lower) cutoff proposes nothing at all.
        for cutoff in [Hlc(200), Hlc(150)] {
            assert_eq!(
                intents
                    .submit(RaftIntent::AdvanceCutoff { cutoff })
                    .await
                    .expect("submit"),
                IntentOutcome::AlreadySatisfied,
            );
        }
        assert_eq!(raft.proposals(), 1, "only the advance was proposed");
    }

    #[tokio::test]
    async fn snapshot_whose_premise_moved_is_superseded_not_rebased() {
        let (raft, intents, _c) = start(state_with(vec![meta(0, 9, 1)], Some(Hlc(100))));

        // Something else appends to the manifest, so our global indices no longer
        // continue from what we measured.
        raft.commit_foreign(|s| s.manifest.push(meta(10, 19, 2)));

        let out = intents
            .submit(snapshot_intent(
                Some(Hlc(100)),
                Some(LogIndex(9)),
                meta(10, 19, 3),
                200,
            ))
            .await
            .expect("submit");
        assert!(
            matches!(out, IntentOutcome::Superseded { .. }),
            "a build must not be spliced onto a manifest it no longer extends: {out:?}"
        );
        assert_eq!(
            raft.snapshot().manifest_state.manifest.len(),
            2,
            "and the foreign entry must survive untouched"
        );
    }

    #[tokio::test]
    async fn empty_window_advances_the_watermark_without_a_manifest_entry() {
        let (raft, intents, _c) = start(state_with(vec![meta(0, 9, 1)], Some(Hlc(100))));

        let out = intents
            .submit(RaftIntent::CommitSnapshot {
                entry: None,
                new_watermark: Hlc(200),
                prev_watermark: Some(Hlc(100)),
                prev_end_index: Some(LogIndex(9)),
            })
            .await
            .expect("submit");
        assert!(matches!(out, IntentOutcome::Applied { .. }));
        let state = raft.snapshot();
        assert_eq!(state.manifest_state.manifest.len(), 1);
        assert_eq!(state.manifest_state.manifest_watermark, Some(Hlc(200)));
        assert_eq!(
            state.manifest_state.hlc_cutoff,
            Some(Hlc(200)),
            "the cutoff must be carried up with the watermark, or the fence \
             would reject the state (WatermarkAboveCutoff)"
        );
    }

    // ── coalescing ───────────────────────────────────────────────────────

    /// The `try_recv` drain: intents queued while the integrator is mid-proposal
    /// are folded into one entry rather than one entry each.
    #[tokio::test]
    async fn concurrent_intents_coalesce_into_one_proposal() {
        let (raft, intents, _c) = start(state_with(vec![meta(0, 9, 1)], Some(Hlc(100))));

        // Fire a batch of independent intents at once. They must all succeed, and
        // the number of Raft proposals must be strictly fewer than the number of
        // intents — otherwise nothing coalesced.
        let submits = (0..8u64).map(|i| {
            let intents = intents.clone();
            tokio::spawn(async move {
                intents
                    .submit(RaftIntent::AdvanceCutoff {
                        cutoff: Hlc(1_000 + i),
                    })
                    .await
            })
        });
        let mut applied = 0usize;
        let mut satisfied = 0usize;
        for handle in submits.collect::<Vec<_>>() {
            match handle.await.expect("join").expect("submit") {
                IntentOutcome::Applied { .. } => applied += 1,
                IntentOutcome::AlreadySatisfied => satisfied += 1,
                IntentOutcome::Superseded { reason } => panic!("superseded: {reason}"),
            }
        }
        assert_eq!(applied + satisfied, 8, "every intent must be answered");
        assert!(
            raft.proposals() < 8,
            "8 concurrent intents took {} proposals — no coalescing happened",
            raft.proposals()
        );
        assert_eq!(
            raft.snapshot().manifest_state.hlc_cutoff,
            Some(Hlc(1_007)),
            "the highest cutoff must win regardless of fold order"
        );
    }

    /// A snapshot commit and a compaction of *different* manifest entries are
    /// independent, so batching them must not make either fail.
    #[tokio::test]
    async fn a_compaction_and_a_snapshot_commit_can_share_one_proposal() {
        let initial = state_with(
            vec![meta(0, 9, 1), meta(10, 19, 2), meta(20, 29, 3)],
            Some(Hlc(300)),
        );
        let (raft, intents, _c) = start(initial);

        let compaction = RaftIntent::CompactManifest {
            replaced: [meta(0, 9, 1).snapshot_id, meta(10, 19, 2).snapshot_id]
                .into_iter()
                .collect(),
            replacement: vec![meta(0, 19, 9)],
        };
        let snapshot = snapshot_intent(Some(Hlc(300)), Some(LogIndex(29)), meta(30, 39, 4), 400);

        let (a, b) = tokio::join!(intents.submit(compaction), intents.submit(snapshot),);
        assert!(matches!(a.expect("submit"), IntentOutcome::Applied { .. }));
        assert!(matches!(b.expect("submit"), IntentOutcome::Applied { .. }));

        let state = raft.snapshot();
        let ids: Vec<SnapshotId> = state
            .manifest_state
            .manifest
            .iter()
            .map(|m| m.snapshot_id)
            .collect();
        assert_eq!(
            ids,
            vec![
                meta(0, 19, 9).snapshot_id,
                meta(20, 29, 3).snapshot_id,
                meta(30, 39, 4).snapshot_id,
            ],
            "the merged entry replaces its inputs in place and the new snapshot \
             keeps its place at the end"
        );
        assert_eq!(state.manifest_state.manifest_watermark, Some(Hlc(400)));
    }

    #[tokio::test]
    async fn one_superseded_intent_does_not_sink_its_batch() {
        let (raft, intents, _c) = start(state_with(vec![meta(0, 9, 1)], Some(Hlc(100))));

        // A doomed snapshot commit (its premise was never true) alongside a
        // perfectly good cutoff advance.
        let doomed = snapshot_intent(Some(Hlc(1)), Some(LogIndex(9)), meta(10, 19, 2), 200);
        let good = RaftIntent::AdvanceCutoff { cutoff: Hlc(500) };
        let (a, b) = tokio::join!(intents.submit(doomed), intents.submit(good));

        assert!(matches!(
            a.expect("submit"),
            IntentOutcome::Superseded { .. }
        ));
        assert!(matches!(b.expect("submit"), IntentOutcome::Applied { .. }));
        assert_eq!(raft.snapshot().manifest_state.hlc_cutoff, Some(Hlc(500)));
    }

    // ── comprehensive state validation ───────────────────────────────────

    /// A splice that would leave a hole in the manifest index space must be
    /// rejected *and attributed to itself*, leaving the accumulated fold intact
    /// for its innocent neighbours.
    #[tokio::test]
    async fn a_splice_that_breaks_manifest_contiguity_is_rejected_alone() {
        let (raft, intents, _c) = start(state_with(
            vec![meta(0, 9, 1), meta(10, 19, 2)],
            Some(Hlc(200)),
        ));

        let bad = RaftIntent::CompactManifest {
            replaced: [meta(0, 9, 1).snapshot_id].into_iter().collect(),
            // Covers 0..=4 where 0..=9 was: leaves a gap at index 5.
            replacement: vec![meta(0, 4, 7)],
        };
        let good = RaftIntent::AdvanceCutoff { cutoff: Hlc(900) };
        let (a, b) = tokio::join!(intents.submit(bad), intents.submit(good));

        assert!(
            matches!(a.expect("submit"), IntentOutcome::Superseded { .. }),
            "a non-contiguous manifest must never be proposed"
        );
        assert!(matches!(b.expect("submit"), IntentOutcome::Applied { .. }));
        let state = raft.snapshot();
        assert_eq!(
            state.manifest_state.manifest.len(),
            2,
            "the rolled-back splice must leave the manifest untouched"
        );
        assert_eq!(state.manifest_state.hlc_cutoff, Some(Hlc(900)));
    }

    /// The joint-exit drain invariant (CLAUDE.md §8; the reasoning is on `meta::drain_satisfied`) holds at the
    /// integrator, and it is judged against the *folded* state — so a collapse
    /// batched with the very snapshot that satisfies its bound is legal, while the
    /// same collapse alone is not.
    #[tokio::test]
    async fn a_joint_collapse_is_judged_against_the_folded_watermark() {
        let old: BTreeSet<NodeId> = [nid(1), nid(2), nid(3)].into_iter().collect();
        let next: BTreeSet<NodeId> = [nid(3), nid(4), nid(5)].into_iter().collect();
        let mut initial = state_with(vec![meta(0, 9, 1)], Some(Hlc(100)));
        initial.replica = ReplicaMembership::JointDraining {
            old,
            next: next.clone(),
            exit: Exit::WatermarkAbove(Hlc(500)),
        };
        let (raft, intents, _c) = start(initial);

        // Alone: the watermark is 100, the bound is 500 — refused.
        let out = intents
            .submit(RaftIntent::SetMembership {
                replica: ReplicaMembership::single(next.clone()),
            })
            .await
            .expect("submit");
        assert!(
            matches!(out, IntentOutcome::Superseded { .. }),
            "collapsing before the outgoing set is drained must be refused: {out:?}"
        );
        assert!(raft.snapshot().replica.is_joint(), "still joint");

        // Batched with the snapshot that carries the watermark to the bound: the
        // collapse becomes legal, and both land in one entry.
        let before = raft.proposals();
        let (a, b) = tokio::join!(
            intents.submit(snapshot_intent(
                Some(Hlc(100)),
                Some(LogIndex(9)),
                meta(10, 19, 2),
                500,
            )),
            intents.submit(RaftIntent::SetMembership {
                replica: ReplicaMembership::single(next.clone()),
            }),
        );
        assert!(matches!(a.expect("submit"), IntentOutcome::Applied { .. }));
        assert!(
            matches!(b.expect("submit"), IntentOutcome::Applied { .. }),
            "the collapse must be judged against the watermark as folded, not as \
             it was when the batch started"
        );
        let state = raft.snapshot();
        assert!(!state.replica.is_joint());
        assert_eq!(*state.replica.current(), next);
        assert_eq!(state.manifest_state.manifest_watermark, Some(Hlc(500)));
        assert_eq!(
            raft.proposals().saturating_sub(before),
            1,
            "the drain-satisfying snapshot and the collapse shared one entry"
        );
    }

    /// Two membership steps in one batch must compose: the second is judged
    /// against the membership *as the first left it*, not against the state the
    /// batch started from. Validating against the base instead would let a batch
    /// walk `Single -> Joint -> Single` in one entry, dropping the outgoing
    /// replicas without ever satisfying the drain bound (CLAUDE.md §8; the reasoning is on `meta::drain_satisfied`).
    #[tokio::test]
    async fn a_second_membership_step_is_judged_against_the_first() {
        let old: BTreeSet<NodeId> = [nid(1), nid(2), nid(3)].into_iter().collect();
        let next: BTreeSet<NodeId> = [nid(3), nid(4), nid(5)].into_iter().collect();
        let (raft, intents, _c) = start(state_with(vec![], Some(Hlc(100))));

        let (a, b) = tokio::join!(
            intents.submit(RaftIntent::SetMembership {
                replica: ReplicaMembership::joint(old.clone(), next.clone()),
            }),
            intents.submit(RaftIntent::SetMembership {
                replica: ReplicaMembership::single(next.clone()),
            }),
        );

        // Whichever order they fold in, exactly one legal outcome exists: the
        // entry into the joint config. The collapse is illegal from `Joint`
        // (unarmed) and illegal from `Single` (it is not the set in force).
        let outcomes = [a.expect("submit"), b.expect("submit")];
        let applied = outcomes
            .iter()
            .filter(|o| matches!(o, IntentOutcome::Applied { .. }))
            .count();
        assert_eq!(applied, 1, "exactly one step may land: {outcomes:?}");

        let state = raft.snapshot();
        assert!(
            state.replica.is_joint(),
            "the collapse must never land in the same batch as the entry: {:?}",
            state.replica,
        );
        assert_eq!(
            *state.replica.current(),
            old,
            "the outgoing set must survive"
        );
    }

    #[tokio::test]
    async fn an_illegal_membership_step_never_reaches_raft() {
        let old: BTreeSet<NodeId> = [nid(1), nid(2), nid(3)].into_iter().collect();
        let (raft, intents, _c) = start(state_with(vec![], Some(Hlc(100))));

        // The outgoing set of a joint config must be the set actually in force.
        let out = intents
            .submit(RaftIntent::SetMembership {
                replica: ReplicaMembership::joint(
                    [nid(7)].into_iter().collect(),
                    [nid(8)].into_iter().collect(),
                ),
            })
            .await
            .expect("submit");
        assert!(matches!(out, IntentOutcome::Superseded { .. }), "{out:?}");
        assert_eq!(raft.proposals(), 0, "nothing was proposed");
        assert_eq!(*raft.snapshot().replica.current(), old);
    }

    // ── retry ────────────────────────────────────────────────────────────

    /// Losing the generation fence re-evaluates each intent against the new base
    /// `Applied { generation }` names the generation the proposal was fenced
    /// on. Re-reading it after the write reports whatever is current, which is
    /// a different fact and is wrong the moment anything trusts it.
    #[tokio::test]
    async fn applied_reports_the_generation_the_proposal_was_fenced_on() {
        let (raft, intents, _c) = start(state_with(vec![], Some(Hlc(100))));
        let fenced_on = raft.snapshot().generation;

        let out = intents
            .submit(RaftIntent::AdvanceCutoff { cutoff: Hlc(200) })
            .await
            .expect("submit");

        assert_eq!(
            out,
            IntentOutcome::Applied {
                generation: fenced_on
            },
            "the reported generation must be the one the write was fenced on, \
             not the one the commit produced",
        );
        assert_ne!(
            raft.snapshot().generation,
            fenced_on,
            "sanity: the commit did bump the generation, so the two differ",
        );
    }

    /// A lost fence fails the batch on the spot — one attempt, no ladder.
    ///
    /// The generation only advances on `EntryPayload::Normal`, and this
    /// integrator is that payload's only writer and writes serially, so a lost
    /// fence means the leader epoch changed. Re-proposing as a probably-deposed
    /// leader is the wrong response; the submitting job re-derives instead. The
    /// submitter must see the replicator's real `StaleStateWrite` (not a
    /// fabricated one, and not a dropped reply surfacing as `Shutdown`), and
    /// the batch must be visible under `state="stale_fence"` — a rate that is
    /// non-zero outside an election is the operator's signal that something
    /// else is writing replicated state.
    #[tokio::test]
    async fn a_lost_fence_fails_immediately_and_records_stale_fence() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);

        let (raft, intents, _c) = start(state_with(vec![], Some(Hlc(100))));
        // Far more rejections than any retry ladder would have consumed, so the
        // count left over measures how many attempts were actually made.
        raft.reject_next(5);

        let before = counter_value(&snapshotter, "zslg_leader_intents_total", "stale_fence");

        let err = intents
            .submit(RaftIntent::AdvanceCutoff { cutoff: Hlc(300) })
            .await
            .expect_err("a lost fence must fail the batch");

        assert!(
            matches!(err, Error::StaleStateWrite { .. }),
            "the submitter must see the replicator's real error: {err:?}"
        );
        assert_eq!(
            raft.rejections_left(),
            4,
            "exactly one attempt: a lost fence must not be retried",
        );
        assert_eq!(
            counter_value(&snapshotter, "zslg_leader_intents_total", "stale_fence"),
            before + 1,
            "a lost fence must be visible in zslg_leader_intents_total{{state=\"stale_fence\"}}",
        );
        assert_eq!(
            counter_value(&snapshotter, "zslg_leader_intents_total", "failed"),
            0,
            "a lost fence is not an ordinary failure; it has its own label",
        );
    }

    fn counter_value(
        snapshotter: &metrics_util::debugging::Snapshotter,
        name: &str,
        state: &str,
    ) -> u64 {
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        map.into_iter()
            .filter(|(ck, _)| {
                ck.key().name() == name
                    && ck
                        .key()
                        .labels()
                        .any(|l| l.key() == "state" && l.value() == state)
            })
            .map(|(_, (_, _, value))| match value {
                metrics_util::debugging::DebugValue::Counter(v) => v,
                other => panic!("expected counter, got {other:?}"),
            })
            .sum()
    }

    #[tokio::test]
    async fn submitting_after_shutdown_reports_shutdown() {
        let (_raft, intents, cancel) = start(state_with(vec![], Some(Hlc(100))));
        cancel.cancel();
        // The integrator task must drop its receiver, so the submitter learns the
        // writer is gone instead of hanging on its oneshot forever.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match intents
                .submit(RaftIntent::AdvanceCutoff { cutoff: Hlc(200) })
                .await
            {
                Err(Error::Shutdown) => break,
                other => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "expected Shutdown once cancelled, got {other:?}"
                    );
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}
