//! `leader:cutoff` — step 1 of the snapshot protocol.
//!
//! ```text
//!   every snapshot.interval:
//!       cutoff = hlc_now() - snapshot.cutoff_timeout,  logical := 0
//!       skip if cutoff <= manifest_watermark           (empty window)
//!       submit AdvanceCutoff{cutoff}
//! ```
//!
//! The lag is what makes the cutoff safe to snapshot against: writes below it
//! have already been accepted or rejected. Zeroing the logical component keeps
//! it a pure instant, so it admits the same writes on every leader.
//!
//! One Raft write, nothing to wait for — which is why it is a separate task
//! from the build. A cutoff keeps advancing while a build sits in its quorum
//! wait, and the build takes whatever is committed when it next runs.

use std::sync::Arc;
use std::time::Duration;
use zslg_consensus::RaftStateView;
use zslg_lifecycle_types::LifecycleError;

use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::Error;
use crate::metrics::{self, SnapshotState, SnapshotStep};
use zslg_consensus::NodeId;
use zslg_consensus::metrics::{LeaderJob, leader_job_ran};
use zslg_consensus::{IntentOutcome, IntentSubmitter, RaftIntent, RaftLeadership};
use zslg_time::Clock;
use zslg_time::Hlc;

/// What the cutoff job reads and writes through.
#[derive(Clone)]
pub struct CutoffHandles {
    pub self_node_id: NodeId,
    pub clock: Arc<dyn Clock>,
    pub leadership: Arc<dyn RaftLeadership>,
    pub raft_state: Arc<dyn RaftStateView>,
    pub intents: Arc<dyn IntentSubmitter>,
}

/// Cutoff tunings.
#[derive(Debug, Clone)]
pub struct CutoffConfig {
    /// How far behind "now" a new cutoff is placed, so in-flight writes below it
    /// have already been accepted or rejected.
    pub cutoff_lag: Duration,
    pub interval: Duration,
}

/// Spawn `leader:cutoff` on the shutdown tree.
pub fn spawn_cutoff<E>(
    shutdown: &mut shutdown::Shutdown<E>,
    handles: CutoffHandles,
    config: CutoffConfig,
) where
    E: From<Error> + From<shutdown::Stopped> + std::fmt::Display + Send + 'static,
{
    let job = CutoffJob { handles, config };
    let cancel = shutdown.token();
    shutdown.spawn("leader:cutoff", async move {
        job.run(cancel).await.map_err(E::from)
    });
}

struct CutoffJob {
    handles: CutoffHandles,
    config: CutoffConfig,
}

impl CutoffJob {
    async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        info!("leader:cutoff: starting");
        let mut ticker = interval(self.config.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let started = Instant::now();
                    self.tick().await;
                    leader_job_ran(LeaderJob::Cutoff, started.elapsed());
                }
            }
        }
        info!("leader:cutoff: shutdown");
        Ok(())
    }

    async fn tick(&self) {
        if !self.handles.leadership.is_leader(self.handles.self_node_id) {
            return;
        }
        let state = self.handles.raft_state.snapshot();
        let watermark = state.manifest_state.manifest_watermark;
        let Some(cutoff) = next_cutoff(
            self.handles.clock.as_ref(),
            self.config.cutoff_lag,
            watermark,
        ) else {
            debug!(?watermark, "cutoff would open an empty window; skipping");
            metrics::snapshot_creation(SnapshotState::NoopEmptyWindow);
            return;
        };

        let started = Instant::now();
        match self
            .handles
            .intents
            .submit(RaftIntent::AdvanceCutoff { cutoff })
            .await
        {
            Ok(IntentOutcome::Applied { .. }) => {
                metrics::snapshot_creation_step(SnapshotStep::SetCutoff, started.elapsed());
                debug!(?cutoff, "hlc cutoff published");
            }
            Ok(IntentOutcome::AlreadySatisfied) => {
                debug!(?cutoff, "hlc cutoff already at or above this value");
            }
            Ok(IntentOutcome::Superseded { reason }) => {
                debug!(?cutoff, %reason, "hlc cutoff superseded; retrying next tick");
            }
            Err(zslg_consensus::Error::Lifecycle(LifecycleError::Shutdown)) => {}
            Err(e) => {
                let e = Error::from(e);
                warn!(error = %e, ?cutoff, "failed to publish hlc cutoff; retrying next tick");
                metrics::snapshot_creation(e.snapshot_state());
            }
        }
    }
}

/// The cutoff to propose, or `None` when it would sit at or below committed
/// snapshot coverage.
///
/// Such a cutoff opens an empty window *and* would violate
/// `manifest_watermark <= hlc_cutoff` at the Raft write point. It is routine on
/// a fresh leader whose clock has not yet passed the previous leader's
/// watermark — expected, not an error (CLAUDE.md §12).
#[must_use]
fn next_cutoff(clock: &dyn Clock, lag: Duration, watermark: Option<Hlc>) -> Option<Hlc> {
    let cutoff = compute_cutoff(clock, lag);
    match watermark {
        Some(wm) if cutoff <= wm => None,
        _ => Some(cutoff),
    }
}

/// `HLC now - lag`, logical component zeroed.
///
/// Zeroing matters: the cutoff is compared against write HLCs, and a cutoff
/// carrying a logical counter would reject or admit writes at the same physical
/// millisecond depending on this leader's local event count.
#[must_use]
fn compute_cutoff(clock: &dyn Clock, lag: Duration) -> Hlc {
    let parts = clock.now().parts();
    let lag_ms = zslg_time::duration_as_ms_clamped(lag);
    Hlc::from_parts(zslg_time::HlcParts {
        physical_ms: parts.physical_ms.saturating_sub(lag_ms),
        logical: 0,
    })
}
