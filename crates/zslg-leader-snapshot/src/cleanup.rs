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
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::Error;
use zslg_consensus::{RaftLeadership, RaftStateView};
use zslg_snapshot::SnapshotStore;

use zslg_consensus::NodeId;
use zslg_consensus::metrics::{LeaderJob, leader_job_ran};
use zslg_snapshot::SnapshotId;

/// What the cleanup job reads. Writes no Raft state, so it holds no intent
/// submitter.
#[derive(Clone)]
pub struct CleanupHandles {
    pub self_node_id: NodeId,
    pub snapshot_store: Arc<dyn SnapshotStore>,
    pub leadership: Arc<dyn RaftLeadership>,
    pub raft_state: Arc<dyn RaftStateView>,
}

/// Cleanup tunings.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    pub retention: Duration,
    pub interval: Duration,
}

/// Spawn `leader:cleanup` on the shutdown tree.
pub fn spawn_cleanup<E>(
    shutdown: &mut shutdown::Shutdown<E>,
    handles: CleanupHandles,
    config: CleanupConfig,
) where
    E: From<Error> + From<shutdown::Stopped> + std::fmt::Display + Send + 'static,
{
    let job = CleanupJob { handles, config };
    let cancel = shutdown.token();
    shutdown.spawn("leader:cleanup", async move {
        job.run(cancel).await.map_err(E::from)
    });
}

struct CleanupJob {
    handles: CleanupHandles,
    config: CleanupConfig,
}

impl CleanupJob {
    async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        info!("leader:cleanup: starting");
        let mut ticker = interval(self.config.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let started = tokio::time::Instant::now();
                    self.tick().await;
                    leader_job_ran(LeaderJob::Cleanup, started.elapsed());
                }
            }
        }
        info!("leader:cleanup: shutdown");
        Ok(())
    }

    async fn tick(&self) {
        if !self.handles.leadership.is_leader(self.handles.self_node_id) {
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
            .cleanup_snapshots(live, self.config.retention)
            .await
        {
            Ok(report) => debug!(?report, "cleanup ok"),
            Err(e) => warn!(error = %e, "cleanup tick failed; will retry"),
        }
    }
}
