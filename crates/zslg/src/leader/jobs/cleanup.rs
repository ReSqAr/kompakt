//! `leader:cleanup` — delete snapshot objects no manifest references.
//!
//! ```text
//!   every snapshot.cleanup_interval:
//!       delete objects where  id not in manifest
//!                       and   age > snapshot.cleanup_retention
//! ```
//!
//! The age term is what makes this safe beside a concurrent build or merge: an
//! object written but not yet published — a build that lost its premise, a
//! splice that was superseded — is younger than the retention window.
//!
//! Writes no Raft state, so it holds no intent submitter.

use std::collections::BTreeSet;
use std::time::Duration;

use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::ids::{NodeId, SnapshotId};
use crate::leader::jobs::{LeaderHandles, is_local_leader};
use crate::metrics::{self, LeaderJob};

/// Spawn `leader:cleanup` on the shutdown tree. Writes no Raft state, so it
/// never touches `handles.intents`.
pub(in crate::leader) fn spawn(
    shutdown: &mut crate::shutdown::Shutdown,
    handles: &crate::leader::jobs::LeaderHandles,
    config: &crate::leader::jobs::snapshot::SnapshotConfig,
) {
    let job = CleanupJob {
        self_node_id: handles.self_node_id,
        handles: handles.clone(),
        retention: config.cleanup_retention,
        interval: config.cleanup_interval,
    };
    shutdown.spawn("leader:cleanup", job.run(shutdown.token()));
}

pub(in crate::leader) struct CleanupJob {
    pub(in crate::leader) self_node_id: NodeId,
    pub(in crate::leader) handles: LeaderHandles,
    pub(in crate::leader) retention: Duration,
    pub(in crate::leader) interval: Duration,
}

impl CleanupJob {
    pub(in crate::leader) async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        info!("leader:cleanup: starting");
        let mut ticker = interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let started = tokio::time::Instant::now();
                    self.tick().await;
                    metrics::leader_job_ran(LeaderJob::Cleanup, started.elapsed());
                }
            }
        }
        info!("leader:cleanup: shutdown");
        Ok(())
    }

    async fn tick(&self) {
        if !is_local_leader(self.handles.leadership.as_ref(), self.self_node_id) {
            return;
        }
        let live: BTreeSet<SnapshotId> = self
            .handles
            .raft_state
            .snapshot()
            .manifest_state
            .manifest
            .iter()
            .map(|m| m.snapshot_id)
            .collect();
        match self
            .handles
            .snapshot_store
            .cleanup_snapshots(live, self.retention)
            .await
        {
            Ok(report) => debug!(?report, "cleanup ok"),
            Err(e) => warn!(error = %e, "cleanup tick failed; will retry"),
        }
    }
}
