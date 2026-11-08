//! The node's live view of replicated raft state, published to every consumer
//! as a single `watch<Arc<RaftState>>`: the current value and the change
//! notification are one thing, so apply and metric-blending cannot interleave
//! and no second lock is needed to keep them in step.
//!
//! Writes follow the **fence split-of-labor**: structural validation
//! (manifest shape, `manifest_watermark <= hlc_cutoff`, membership
//! transitions) happens *before* commit at `raft_storage::evaluate_fence`
//! (a value rejection) and again pre-flight at the leader's propose site
//! (`raft_storage::validate_replicated_state`). [`apply_replicated`] only
//! guards the generation, which must never regress.
use crate::ConsensusError;
use std::collections::BTreeSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::debug;

use crate::RaftGeneration;
use crate::ReplicatedRaftState;
use crate::meta::{RaftManifestState, RaftState};
use zslg_node_types::NodeId;

/// The published Raft state: one value, one change notification.
///
/// A [`watch`] rather than a snapshot cell plus a broadcast, which is what this
/// was. Every consumer of the old broadcast ignored the payload (or fell back
/// to re-reading the cell on `Lagged`) and then read current state — they want
/// the *level*, not the edges — so the two halves were always one thing, kept in
/// step by hand under a second lock and paying a full `RaftState` clone per
/// publish. The watch is also unmissable: a consumer that was busy sees the
/// latest state on its next poll rather than a lag it has to remember to treat
/// as a wake-up.
///
/// `Arc<RaftState>` and not `RaftState`: `snapshot()` is called far more often
/// than the state changes, and the manifest inside makes a clone unpleasant.
#[derive(Debug, Clone)]
pub struct RaftStateHandle {
    tx: Arc<watch::Sender<Arc<RaftState>>>,
}

impl Default for RaftStateHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftStateHandle {
    #[must_use]
    pub fn new() -> Self {
        let (tx, _) = watch::channel(Arc::new(RaftState::empty()));
        Self { tx: Arc::new(tx) }
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<RaftState> {
        Arc::clone(&self.tx.borrow())
    }

    /// Every committed state transition, from the moment of subscription.
    ///
    /// Public, like the trait method it shadows. An inherent method wins over
    /// a trait method in call position, so leaving this narrower than
    /// `crate::api::RaftStateView::subscribe` would make `handle.subscribe()`
    /// a privacy error outside `crate::raft` even where the trait is in scope.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Arc<RaftState>> {
        self.tx.subscribe()
    }

    /// Replace the replicated portion of the state at `generation`.
    ///
    /// This runs on openraft's *post-commit* apply path (and again on the
    /// recovery replay), so it must not `Err` on the *content* of an
    /// already-committed entry — that would become a fatal `StorageError` and
    /// halt the state machine. Structural validation is fenced pre-commit
    /// (header); only the generation guard remains here: a backwards
    /// generation is an invariant violation and is rejected with
    /// [`ConsensusError::StateRegression`] instead of panicking the host.
    pub fn apply_replicated(
        &self,
        state: ReplicatedRaftState,
        generation: RaftGeneration,
    ) -> Result<(), ConsensusError> {
        // One critical section, the watch's own: the read-modify-write and the
        // publish are the same operation, so the separate write lock this used
        // to need is gone.
        let mut outcome = Ok(());
        self.tx.send_if_modified(|slot| {
            let prev = Arc::clone(slot);
            if generation < prev.generation {
                tracing::error!(
                    generation = ?prev.generation,
                    attempted_generation = ?generation,
                    "raft state generation went backwards; rejecting apply",
                );
                outcome = Err(ConsensusError::StateRegression {
                    current: prev.generation,
                    attempted: generation,
                });
                return false;
            }
            *slot = Arc::new(RaftState {
                leader: prev.leader,
                raft_term: prev.raft_term,
                generation,
                replica: state.replica.clone(),
                manifest_state: RaftManifestState {
                    manifest: state.manifest.clone(),
                    manifest_watermark: state.manifest_watermark,
                    hlc_cutoff: state.hlc_cutoff,
                },
                // Carried over, not recomputed: `voters` is blended from
                // openraft's metrics and has nothing to do with the replicated
                // payload being applied. Dropping it here would blank the voter
                // set on every apply and refill it only on the next metrics
                // change.
                voters: prev.voters.clone(),
            });
            log_update(slot);
            true
        });
        outcome
    }

    /// Fold this node's openraft-derived facts into the published state.
    ///
    /// The early return compares `voters` too. Leaving it out would skip a
    /// voter-set change whenever the leader and term happened to be unchanged
    /// — which is the common case, since membership changes do not move the
    /// leader.
    pub fn blend_raft_metrics(
        &self,
        leader: Option<NodeId>,
        raft_term: u64,
        voters: BTreeSet<NodeId>,
    ) {
        self.tx.send_if_modified(|slot| {
            if slot.leader == leader && slot.raft_term == raft_term && slot.voters == voters {
                return false;
            }
            *slot = Arc::new(RaftState {
                leader,
                raft_term,
                voters,
                ..(**slot).clone()
            });
            log_update(slot);
            true
        });
    }

    pub fn replace(&self, new_state: RaftState) {
        self.tx.send_if_modified(|slot| {
            *slot = Arc::new(new_state);
            log_update(slot);
            true
        });
    }
}

fn log_update(new_state: &RaftState) {
    debug!(
        generation = ?new_state.generation,
        node_id = ?new_state.leader,
        raft_term = new_state.raft_term,
        replica = ?new_state.replica,
        manifest_len = new_state.manifest_state.manifest.len(),
        manifest_watermark = ?new_state.manifest_state.manifest_watermark,
        hlc_cutoff = ?new_state.manifest_state.hlc_cutoff,
        "RaftState updated"
    );
}

impl Serialize for RaftStateHandle {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let snap = self.snapshot();
        let replicated = ReplicatedRaftState {
            replica: snap.replica.clone(),
            manifest: snap.manifest_state.manifest.clone(),
            manifest_watermark: snap.manifest_state.manifest_watermark,
            hlc_cutoff: snap.manifest_state.hlc_cutoff,
        };
        replicated.serialize(ser)
    }
}

impl<'de> Deserialize<'de> for RaftStateHandle {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let state = ReplicatedRaftState::deserialize(de)?;
        let s = Self::new();
        // A fresh handle is at generation ZERO; applying at ZERO cannot regress.
        s.apply_replicated(state, RaftGeneration::ZERO)
            .map_err(serde::de::Error::custom)?;
        Ok(s)
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
mod tests;
