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

use std::time::Duration;

use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::ids::NodeId;
use crate::leader::integrator::IntentSubmitter;
use crate::leader::jobs::compaction::merge::{CompactionTier, compact};
use crate::leader::jobs::{LeaderHandles, is_local_leader};
use crate::metrics::{self, LeaderJob};
use crate::object_store::CompressionLevel;

pub(in crate::leader) mod merge;

/// Spawn `leader:compaction` on the shutdown tree.
pub(in crate::leader) fn spawn(
    shutdown: &mut crate::shutdown::Shutdown,
    handles: &crate::leader::jobs::LeaderHandles,
    config: &crate::leader::jobs::snapshot::SnapshotConfig,
) {
    let job = CompactionJob {
        self_node_id: handles.self_node_id,
        handles: handles.clone(),
        intents: handles.intents.clone(),
        tiers: config.compaction_tiers.clone(),
        compression: config.compression,
        interval: config.compaction_interval,
    };
    shutdown.spawn("leader:compaction", job.run(shutdown.token()));
}

pub(in crate::leader) struct CompactionJob {
    pub(in crate::leader) self_node_id: NodeId,
    pub(in crate::leader) handles: LeaderHandles,
    pub(in crate::leader) intents: IntentSubmitter,
    pub(in crate::leader) tiers: Vec<CompactionTier>,
    pub(in crate::leader) compression: CompressionLevel,
    pub(in crate::leader) interval: Duration,
}

impl CompactionJob {
    pub(in crate::leader) async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        info!("leader:compaction: starting");
        let mut ticker = interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let started = tokio::time::Instant::now();
                    self.tick().await;
                    metrics::leader_job_ran(LeaderJob::Compaction, started.elapsed());
                }
            }
        }
        info!("leader:compaction: shutdown");
        Ok(())
    }

    async fn tick(&self) {
        if !is_local_leader(self.handles.leadership.as_ref(), self.self_node_id) {
            return;
        }
        match compact(
            &self.handles.snapshot_store,
            self.handles.raft_state.as_ref(),
            &self.tiers,
            self.compression,
            &self.intents,
        )
        .await
        {
            Ok(outcome) if outcome.merges_performed > 0 => info!(
                merges = outcome.merges_performed,
                manifest_len = outcome.final_manifest_len,
                "compaction merged snapshots"
            ),
            Ok(_) => debug!("compaction: nothing to merge"),
            Err(Error::Shutdown) => {}
            Err(e) => warn!(error = %e, "compaction failed; will retry next tick"),
        }
    }
}
