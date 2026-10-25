//! Drives [`HlcGate`] from committed Raft state.
//!
//! ```text
//!   subscribe, then read the initial snapshot   (that order, see below)
//!   for each state carrying an hlc_cutoff:
//!       gate.set_cutoff(c).await
//!           advanced ─> publish c on drained_tx
//!           ignored  ─> count rejected_backwards
//! ```
//!
//! A replica enforces the committed `hlc_cutoff` verbatim — there is no
//! read-time floor against the manifest watermark, because
//! `manifest_watermark <= hlc_cutoff` is already enforced at the Raft write
//! point (`raft_storage::{validate_replicated_state, evaluate_fence}`). A
//! committed cutoff can therefore never sit below snapshot coverage, leaving
//! nothing to floor.
//!
//! A new leader may legitimately commit a *lower* cutoff than its predecessor
//! (CLAUDE.md §12). That is not this loop's problem to detect: it calls
//! `set_cutoff` unconditionally and lets the gate arbitrate, which is why
//! `drained_tx` publishes only on an advance and stays in lockstep with what is
//! actually being enforced.

use std::sync::Arc;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::api::RaftStateView;
use crate::error::Error;
use crate::hlc::{Hlc, HlcGate};
use crate::metrics::{self, CutoffSetState};

pub(crate) struct CutoffApplier {
    raft_state: Arc<dyn RaftStateView>,
    hlc_gate: Arc<HlcGate>,
    drained_tx: watch::Sender<Option<Hlc>>,
    cancel: CancellationToken,
}

impl CutoffApplier {
    #[must_use]
    pub(crate) fn new(
        raft_state: Arc<dyn RaftStateView>,
        hlc_gate: Arc<HlcGate>,
        drained_tx: watch::Sender<Option<Hlc>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            raft_state,
            hlc_gate,
            drained_tx,
            cancel,
        }
    }

    pub(crate) async fn run(self) -> Result<(), Error> {
        tracing::info!("cutoff applier: starting");
        // Subscribe before snapshotting: a cutoff committed between the two
        // then arrives as an event, rather than waiting for the next unrelated
        // state change to reveal it.
        let mut rx = self.raft_state.subscribe();

        let initial_state = self.raft_state.snapshot();
        if let Some(initial_cutoff) = initial_state.manifest_state.hlc_cutoff
            && self.hlc_gate.set_cutoff(initial_cutoff).await
        {
            let _ = self.drained_tx.send(Some(initial_cutoff));
            debug!(cutoff = ?initial_cutoff, "initial hlc cutoff applied");
        }

        loop {
            let state = tokio::select! {
                () = self.cancel.cancelled() => {
                    tracing::info!("cutoff applier: shutdown");
                    return Ok(());
                }
                changed = rx.changed() => match changed {
                    // Level-triggered: the value in hand is the current state,
                    // so there is no lag to fall back from.
                    Ok(()) => rx.borrow_and_update().clone(),
                    Err(_) => {
                        tracing::info!("cutoff applier: raft state channel closed");
                        return Ok(());
                    }
                },
            };

            if let Some(new_cutoff) = state.manifest_state.hlc_cutoff {
                if self.hlc_gate.set_cutoff(new_cutoff).await {
                    metrics::hlc_cutoff_set(CutoffSetState::Accepted);
                    let _ = self.drained_tx.send(Some(new_cutoff));
                    debug!(cutoff = ?new_cutoff, "hlc cutoff drained");
                } else {
                    metrics::hlc_cutoff_set(CutoffSetState::RejectedBackwards);
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
    reason = "test code"
)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::watch;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::meta::{RaftManifestState, RaftState};
    use crate::raft::RaftStateHandle;

    fn state_with(cutoff: Option<Hlc>, watermark: Option<Hlc>) -> RaftState {
        RaftState {
            manifest_state: RaftManifestState {
                manifest: Vec::new(),
                manifest_watermark: watermark,
                hlc_cutoff: cutoff,
            },
            ..RaftState::empty()
        }
    }

    /// Pins that a restart enforces the committed `hlc_cutoff` verbatim, and
    /// that this is enough: since the cutoff can never be committed below the
    /// watermark, enforcing it also excludes everything a committed snapshot
    /// already covers.
    #[tokio::test]
    async fn invariant_restart_enforces_committed_cutoff() {
        let raft_state = RaftStateHandle::new();
        // Valid committed state: watermark == cutoff, the post-snapshot steady state.
        raft_state.replace(state_with(Some(Hlc(100)), Some(Hlc(100))));

        let hlc_cutoff = Arc::new(HlcGate::new());
        let (drained_tx, mut drained_rx) = watch::channel(None);
        let cancel = CancellationToken::new();
        let applier = CutoffApplier::new(
            Arc::new(raft_state.clone()),
            hlc_cutoff.clone(),
            drained_tx,
            cancel.clone(),
        );
        let handle = tokio::spawn(applier.run());

        tokio::time::timeout(Duration::from_secs(5), async {
            while *drained_rx.borrow() != Some(Hlc(100)) {
                drained_rx.changed().await.expect("watch alive");
            }
        })
        .await
        .expect("initial cutoff must drain to the committed hlc_cutoff");

        // At or below the cutoff (== watermark): rejected. Strictly above: admitted.
        assert!(hlc_cutoff.check_hlc(Hlc(100)).is_err());
        assert!(hlc_cutoff.check_hlc(Hlc(10)).is_err());
        assert!(hlc_cutoff.check_hlc(Hlc(101)).is_ok());

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    /// The regression guard lives in `HlcGate`: once a cutoff has drained, a
    /// later raft state whose (invariant-respecting) cutoff regressed below it —
    /// a new leader lowering the cutoff but staying at or above the watermark —
    /// must leave both the published cutoff and the enforced cutoff untouched.
    #[tokio::test]
    async fn applier_ignores_regressed_cutoff() {
        let raft_state = RaftStateHandle::new();
        raft_state.replace(state_with(Some(Hlc(200)), Some(Hlc(100))));

        let hlc_cutoff = Arc::new(HlcGate::new());
        let (drained_tx, mut drained_rx) = watch::channel(None);
        let cancel = CancellationToken::new();
        let applier = CutoffApplier::new(
            Arc::new(raft_state.clone()),
            hlc_cutoff.clone(),
            drained_tx,
            cancel.clone(),
        );
        let handle = tokio::spawn(applier.run());

        // Initial cutoff: 200.
        tokio::time::timeout(Duration::from_secs(5), async {
            while *drained_rx.borrow() != Some(Hlc(200)) {
                drained_rx.changed().await.expect("watch alive");
            }
        })
        .await
        .expect("initial cutoff must drain to 200");

        // New leader commits a regressed cutoff (150) that still respects the
        // invariant (>= watermark 100). 150 < 200, so HlcGate ignores it.
        raft_state.replace(state_with(Some(Hlc(150)), Some(Hlc(100))));

        // A no-op produces no drained_tx send, so there is nothing to await:
        // yield enough for the single-threaded runtime to run the applier
        // through the (ignored) change, then assert nothing moved.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            *drained_rx.borrow(),
            Some(Hlc(200)),
            "published cutoff must not regress"
        );
        // Still enforcing 200: 150 (the regressed target) is rejected — it would
        // be admitted if the enforced cutoff had regressed to 150.
        assert!(
            hlc_cutoff.check_hlc(Hlc(150)).is_err(),
            "still enforces 200"
        );
        assert!(hlc_cutoff.check_hlc(Hlc(201)).is_ok());

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }
}
