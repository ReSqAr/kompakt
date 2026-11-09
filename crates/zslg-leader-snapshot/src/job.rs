//! `leader:snapshot` — steps 2-5 of the snapshot protocol.
//!
//! `hlc_cutoff > manifest_watermark` *is* an open window `(watermark, cutoff]`,
//! opened by `jobs::cutoff`. The job builds a snapshot for it and submits the
//! manifest entry.
//!
//! ```text
//!   pending_window() ──> build_snapshot ──> CommitSnapshot intent
//!         ^                                         │
//!         └──── window still open (chunked) ────────┘
//! ```
//!
//! A build capped by `max_snapshot_entries` advances the watermark only partway,
//! so the window stays open and the loop-back drives the next chunk. That
//! loop-back is load-bearing — see [`SNAPSHOT_LOOP_FLOOR`].

use std::sync::Arc;
use std::time::Duration;
use zslg_lifecycle_types::LifecycleError;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::Error;
use crate::create::{SnapshotInputs, build_snapshot};
use crate::metrics::{self, SnapshotStep};
use pace_kit::Pace;
use warn_throttle::ThrottledWarn;
use zslg_consensus::NodeId;
use zslg_consensus::metrics::{LeaderJob, leader_job_ran};
use zslg_consensus::{IntentOutcome, IntentSubmitter, RaftLeadership, RaftStateView};
use zslg_log::{LocalStoreView, StableLogSnapshot};
use zslg_net::Router;
use zslg_snapshot::CompressionLevel;
use zslg_snapshot::SnapshotStore;
use zslg_time::Hlc;

/// What the snapshot job reads and writes through.
#[derive(Clone)]
pub struct SnapshotHandles {
    pub self_node_id: NodeId,
    pub local_store: Arc<dyn LocalStoreView>,
    pub stable_log: Arc<dyn StableLogSnapshot>,
    pub snapshot_store: Arc<dyn SnapshotStore>,
    pub raft_state: Arc<dyn RaftStateView>,
    pub router: Arc<dyn Router>,
    pub leadership: Arc<dyn RaftLeadership>,
    pub intents: Arc<dyn IntentSubmitter>,
}

/// Snapshot-job tunings.
#[derive(Debug, Clone)]
pub struct SnapshotJobConfig {
    pub quorum_poll_interval: Duration,
    pub quorum_wait_timeout: Duration,
    pub compression: CompressionLevel,
    /// Soft cap on entries per snapshot object. Bounds build time, object size,
    /// and a catching-up node's load granularity: without it, a stalled cluster
    /// produces one unbounded object that must be loaded monolithically.
    ///
    /// Soft because a chunk always runs to the end of the HLC it lands in
    /// (`stream_window`), so the real count can exceed this slightly.
    pub max_snapshot_entries: std::num::NonZeroUsize,
}

/// Spawn `leader:snapshot` on the shutdown tree.
pub fn spawn_snapshot<E>(
    shutdown: &mut shutdown::Shutdown<E>,
    handles: SnapshotHandles,
    config: SnapshotJobConfig,
) where
    E: From<Error> + From<shutdown::Stopped> + std::fmt::Display + Send + 'static,
{
    let job = SnapshotJob {
        handles,
        config,
        build_warn: ThrottledWarn::new(SNAPSHOT_WARN_INTERVAL),
    };
    let cancel = shutdown.token();
    shutdown.spawn("leader:snapshot", async move {
        job.run(cancel).await.map_err(E::from)
    });
}

/// Paces the chunk loop; **must not gate it**. Turning the immediate loop-back
/// into "wait for the next broadcast event" would throttle a backlog to one
/// chunk per event.
///
/// The floor exists for the iterations that return with the window still open
/// and the watermark unmoved — a failed build, a superseded commit — so the
/// loop-back is safe without depending on those staying transient. Being
/// start-to-start, it delays a chunk that outran it not at all; the cost is a
/// drain capped near ten chunks/s when a chunk finishes inside the floor.
pub const SNAPSHOT_LOOP_FLOOR: Duration = Duration::from_millis(100);

/// Bound the `snapshot build failed; will retry` warn under a sustained
/// `StableLogBehindManifest` stall — without the gate that path logs one warn
/// per retry (observed: ~1,100 lines during a 10k-writes/s burst).
const SNAPSHOT_WARN_INTERVAL: Duration = Duration::from_secs(5);

pub struct SnapshotJob {
    pub handles: SnapshotHandles,
    pub config: SnapshotJobConfig,
    pub build_warn: ThrottledWarn,
}

impl SnapshotJob {
    pub async fn run(mut self, cancel: CancellationToken) -> Result<(), Error> {
        info!("leader:snapshot: starting");
        let mut events = self.handles.raft_state.subscribe();
        let mut pace = Pace::new(SNAPSHOT_LOOP_FLOOR);
        loop {
            if let Some(cutoff) = self.pending_window() {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    () = async {
                        pace.wait().await;
                        let started = Instant::now();
                        // This `continue` drives the next chunk; see
                        // `SNAPSHOT_LOOP_FLOOR` before changing it.
                        self.build_and_publish(cutoff).await;
                        leader_job_ran(LeaderJob::Snapshot, started.elapsed());
                    } => continue,
                }
            }
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                // Level-triggered: however many transitions landed while this
                // job was working, one wake-up covers them — the next iteration
                // re-reads current state. No queue to drain.
                changed = events.changed() => if changed.is_err() {
                    break;
                },
            }
        }
        info!("leader:snapshot: shutdown");
        Ok(())
    }

    /// The cutoff of an open window, if this node leads and one is open.
    fn pending_window(&self) -> Option<Hlc> {
        if !self.handles.leadership.is_leader(self.handles.self_node_id) {
            return None;
        }
        let state = self.handles.raft_state.snapshot();
        let cutoff = state.manifest_state.hlc_cutoff?;
        match state.manifest_state.manifest_watermark {
            Some(wm) if cutoff <= wm => None,
            _ => Some(cutoff),
        }
    }

    async fn build_and_publish(&mut self, cutoff: Hlc) {
        let inputs = SnapshotInputs {
            local_store: self.handles.local_store.clone(),
            stable_log: self.handles.stable_log.as_ref(),
            snapshot_store: self.handles.snapshot_store.as_ref(),
            raft_state: self.handles.raft_state.as_ref(),
            router: self.handles.router.clone(),
            leadership: self.handles.leadership.clone(),
            self_node_id: self.handles.self_node_id,
            quorum_poll_interval: self.config.quorum_poll_interval,
            quorum_wait_timeout: self.config.quorum_wait_timeout,
            compression: self.config.compression,
            max_snapshot_entries: self.config.max_snapshot_entries,
        };
        let build = match build_snapshot(&inputs, cutoff).await {
            Ok(build) => build,
            Err(e) => {
                if let Some(suppressed) = self.build_warn.gate() {
                    if suppressed > 0 {
                        warn!(
                            suppressed,
                            ?cutoff,
                            "snapshot build failures: {} suppressed in the last {}s (see zslg_snapshot_creation_total for the exact count)",
                            suppressed,
                            SNAPSHOT_WARN_INTERVAL.as_secs(),
                        );
                    }
                    warn!(error = %e, ?cutoff, "snapshot build failed; will retry");
                }
                metrics::snapshot_creation(e.snapshot_state());
                // No back-off: the open window re-enters the loop, which is
                // already paced. Pacing lives in exactly one place.
                return;
            }
        };

        let Some(intent) = build.commit_intent() else {
            debug!(?cutoff, "snapshot skipped: window already covered");
            return;
        };
        let started = Instant::now();
        match self.handles.intents.submit(intent).await {
            Ok(IntentOutcome::Applied { .. }) => {
                metrics::snapshot_creation_step(SnapshotStep::Commit, started.elapsed());
                debug!(?cutoff, "snapshot committed");
            }
            Ok(IntentOutcome::AlreadySatisfied) => {
                debug!(?cutoff, "snapshot watermark already published");
            }
            Ok(IntentOutcome::Superseded { reason }) => {
                // The uploaded object is left for `jobs::cleanup`.
                info!(?cutoff, %reason, "snapshot superseded while building; rebuilding");
            }
            Err(zslg_consensus::Error::Lifecycle(LifecycleError::Shutdown)) => {}
            Err(e) => {
                let e = Error::from(e);
                warn!(error = %e, ?cutoff, "failed to publish snapshot; will retry");
                metrics::snapshot_creation(e.snapshot_state());
            }
        }
    }
}

#[cfg(test)]
mod tests;
