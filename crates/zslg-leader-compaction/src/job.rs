//! `leader:compaction` — merge old snapshot objects into fewer, larger ones.
//!
//! ```text
//!   every snapshot.compaction_interval:
//!       for each closed tier bucket with >= 2 entries:
//!           read + merge + upload
//!           submit CompactManifest{replaced, replacement}   (a splice)
//! ```
//!
//! The longest-running leader job — it rewrites whole objects. Publishing per
//! group as a splice rather than a whole manifest is what lets a snapshot
//! committed mid-merge keep its place. Bucketing is in [`merge`].

use std::sync::Arc;
use std::time::Duration;
use zslg_lifecycle_types::LifecycleError;

use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::Error;
use crate::merge::compact;
use zslg_consensus::NodeId;
use zslg_consensus::metrics::{LeaderJob, leader_job_ran};
use zslg_consensus::{IntentSubmitter, RaftLeadership, RaftStateView};
use zslg_snapshot::CompactionTier;
use zslg_snapshot::CompressionLevel;
use zslg_snapshot::SnapshotStore;

/// What the compaction job reads and writes through.
#[derive(Clone)]
pub struct CompactionHandles {
    pub self_node_id: NodeId,
    pub snapshot_store: Arc<dyn SnapshotStore>,
    pub raft_state: Arc<dyn RaftStateView>,
    pub leadership: Arc<dyn RaftLeadership>,
    pub intents: Arc<dyn IntentSubmitter>,
}

/// Compaction tunings.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub tiers: Vec<CompactionTier>,
    pub compression: CompressionLevel,
    pub interval: Duration,
}

/// Spawn `leader:compaction` on the shutdown tree.
pub fn spawn<E>(
    shutdown: &mut shutdown::Shutdown<E>,
    handles: CompactionHandles,
    config: CompactionConfig,
) where
    E: From<Error> + From<shutdown::Stopped> + std::fmt::Display + Send + 'static,
{
    let job = CompactionJob { handles, config };
    let cancel = shutdown.token();
    shutdown.spawn("leader:compaction", async move {
        job.run(cancel).await.map_err(E::from)
    });
}

struct CompactionJob {
    handles: CompactionHandles,
    config: CompactionConfig,
}

impl CompactionJob {
    async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        info!("leader:compaction: starting");
        let mut ticker = interval(self.config.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let started = tokio::time::Instant::now();
                    self.tick().await;
                    leader_job_ran(LeaderJob::Compaction, started.elapsed());
                }
            }
        }
        info!("leader:compaction: shutdown");
        Ok(())
    }

    async fn tick(&self) {
        if !self.handles.leadership.is_leader(self.handles.self_node_id) {
            return;
        }
        match compact(
            &self.handles.snapshot_store,
            self.handles.raft_state.as_ref(),
            &self.config.tiers,
            self.config.compression,
            self.handles.intents.as_ref(),
        )
        .await
        {
            Ok(outcome) if outcome.merges_performed > 0 => info!(
                merges = outcome.merges_performed,
                manifest_len = outcome.final_manifest_len,
                "compaction merged snapshots"
            ),
            Ok(_) => debug!("compaction: nothing to merge"),
            Err(Error::Lifecycle(LifecycleError::Shutdown)) => {}
            Err(e) => warn!(error = %e, "compaction failed; will retry next tick"),
        }
    }
}
