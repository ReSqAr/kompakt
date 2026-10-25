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

use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::api::{Clock, RaftStateView};
use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::NodeId;
use crate::leader::integrator::{IntentOutcome, IntentSubmitter, RaftIntent};
use crate::leader::jobs::is_local_leader;
use crate::metrics::{self, LeaderJob, SnapshotState, SnapshotStep};

/// Spawn `leader:cutoff` on the shutdown tree.
pub(in crate::leader) fn spawn(
    shutdown: &mut crate::shutdown::Shutdown,
    handles: &crate::leader::jobs::LeaderHandles,
    config: &crate::leader::jobs::snapshot::SnapshotConfig,
) {
    let job = CutoffJob {
        self_node_id: handles.self_node_id,
        clock: handles.clock.clone(),
        leadership: handles.leadership.clone(),
        raft_state: handles.raft_state.clone(),
        intents: handles.intents.clone(),
        cutoff_lag: config.cutoff_timeout,
        interval: config.interval,
    };
    shutdown.spawn("leader:cutoff", job.run(shutdown.token()));
}

pub(in crate::leader) struct CutoffJob {
    pub(in crate::leader) self_node_id: NodeId,
    pub(in crate::leader) clock: Arc<dyn Clock>,
    pub(in crate::leader) leadership: Arc<dyn crate::api::RaftLeadership>,
    pub(in crate::leader) raft_state: Arc<dyn RaftStateView>,
    pub(in crate::leader) intents: IntentSubmitter,
    /// How far behind "now" a new cutoff is placed, so in-flight writes below it
    /// have already been accepted or rejected.
    pub(in crate::leader) cutoff_lag: Duration,
    pub(in crate::leader) interval: Duration,
}

impl CutoffJob {
    pub(in crate::leader) async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        info!("leader:cutoff: starting");
        let mut ticker = interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let started = Instant::now();
                    self.tick().await;
                    metrics::leader_job_ran(LeaderJob::Cutoff, started.elapsed());
                }
            }
        }
        info!("leader:cutoff: shutdown");
        Ok(())
    }

    async fn tick(&self) {
        if !is_local_leader(self.leadership.as_ref(), self.self_node_id) {
            return;
        }
        let state = self.raft_state.snapshot();
        let watermark = state.manifest_state.manifest_watermark;
        let Some(cutoff) = next_cutoff(self.clock.as_ref(), self.cutoff_lag, watermark) else {
            debug!(?watermark, "cutoff would open an empty window; skipping");
            metrics::snapshot_creation(SnapshotState::NoopEmptyWindow);
            return;
        };

        let started = Instant::now();
        match self
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
            Err(Error::Shutdown) => {}
            Err(e) => {
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
pub(in crate::leader) fn next_cutoff(
    clock: &dyn Clock,
    lag: Duration,
    watermark: Option<Hlc>,
) -> Option<Hlc> {
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
pub(in crate::leader) fn compute_cutoff(clock: &dyn Clock, lag: Duration) -> Hlc {
    let parts = clock.now().parts();
    let lag_ms = crate::hlc::duration_as_ms_clamped(lag);
    Hlc::from_parts(crate::hlc::HlcParts {
        physical_ms: parts.physical_ms.saturating_sub(lag_ms),
        logical: 0,
    })
}
