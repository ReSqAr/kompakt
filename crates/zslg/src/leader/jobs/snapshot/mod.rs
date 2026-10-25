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

use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::NodeId;
use crate::leader::integrator::{IntentOutcome, IntentSubmitter};
use crate::leader::jobs::Pace;
use crate::leader::jobs::compaction::merge::CompactionTier;
use crate::leader::jobs::snapshot::create::{SnapshotInputs, build_snapshot};
use crate::leader::jobs::{LeaderHandles, is_local_leader};
use crate::metrics::{self, LeaderJob, SnapshotStep};
use crate::object_store::CompressionLevel;
use crate::warn_throttle::ThrottledWarn;

pub(in crate::leader) mod create;

#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod member_lifecycle_tests;

/// Feeds `SnapshotStore::write_snapshot`. A full channel blocks the forwarder —
/// the backpressure pacing the producer to the off-reactor encoder.
pub(in crate::leader) const SNAPSHOT_WRITE_FORWARDER_CAP: usize = 256;

/// Spawn `leader:snapshot` on the shutdown tree.
pub(in crate::leader) fn spawn(
    shutdown: &mut crate::shutdown::Shutdown,
    handles: &crate::leader::jobs::LeaderHandles,
    config: &SnapshotConfig,
    quorum_poll_interval: Duration,
    quorum_wait_timeout: Duration,
) {
    let job = SnapshotJob {
        self_node_id: handles.self_node_id,
        handles: handles.clone(),
        intents: handles.intents.clone(),
        quorum_poll_interval,
        quorum_wait_timeout,
        compression: config.compression,
        max_snapshot_entries: config.max_snapshot_entries,
        build_warn: ThrottledWarn::new(SNAPSHOT_WARN_INTERVAL),
    };
    shutdown.spawn("leader:snapshot", job.run(shutdown.token()));
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
pub(in crate::leader) const SNAPSHOT_LOOP_FLOOR: Duration = Duration::from_millis(100);

/// Bound the `snapshot build failed; will retry` warn under a sustained
/// `StableLogBehindManifest` stall — without the gate that path logs one warn
/// per retry (observed: ~1,100 lines during a 10k-writes/s burst).
const SNAPSHOT_WARN_INTERVAL: Duration = Duration::from_secs(5);

pub(in crate::leader) struct SnapshotJob {
    pub(in crate::leader) self_node_id: NodeId,
    pub(in crate::leader) handles: LeaderHandles,
    pub(in crate::leader) intents: IntentSubmitter,
    pub(in crate::leader) quorum_poll_interval: Duration,
    pub(in crate::leader) quorum_wait_timeout: Duration,
    pub(in crate::leader) compression: CompressionLevel,
    pub(in crate::leader) max_snapshot_entries: std::num::NonZeroUsize,
    pub(in crate::leader) build_warn: ThrottledWarn,
}

impl SnapshotJob {
    pub(in crate::leader) async fn run(mut self, cancel: CancellationToken) -> Result<(), Error> {
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
                        metrics::leader_job_ran(LeaderJob::Snapshot, started.elapsed());
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
        if !is_local_leader(self.handles.leadership.as_ref(), self.self_node_id) {
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
            self_node_id: self.self_node_id,
            quorum_poll_interval: self.quorum_poll_interval,
            quorum_wait_timeout: self.quorum_wait_timeout,
            compression: self.compression,
            max_snapshot_entries: self.max_snapshot_entries,
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
        match self.intents.submit(intent).await {
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
            Err(Error::Shutdown) => {}
            Err(e) => {
                warn!(error = %e, ?cutoff, "failed to publish snapshot; will retry");
                metrics::snapshot_creation(e.snapshot_state());
            }
        }
    }
}

/// Snapshot creation, compaction, and cleanup configuration.
#[derive(Debug, Clone)]
pub(crate) struct SnapshotConfig {
    pub(crate) interval: Duration,
    pub(crate) cutoff_timeout: Duration,
    pub(crate) compaction_tiers: Vec<CompactionTier>,
    pub(crate) compaction_interval: Duration,
    pub(crate) cleanup_retention: Duration,
    pub(crate) cleanup_interval: Duration,
    pub(crate) compression: CompressionLevel,
    /// Soft cap on entries per snapshot object. Bounds build time, object size,
    /// and a catching-up node's load granularity: without it, a stalled cluster
    /// produces one unbounded object that must be loaded monolithically.
    ///
    /// Soft because a chunk always runs to the end of the HLC it lands in
    /// (`stream_window_ordered`), so the real count can exceed this slightly.
    pub(crate) max_snapshot_entries: std::num::NonZeroUsize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(1),
            cutoff_timeout: std::time::Duration::from_millis(200),
            compaction_tiers: vec![
                Duration::from_secs(60).into(),
                Duration::from_secs(60 * 15).into(),
                Duration::from_secs(60 * 60).into(),
                Duration::from_secs(60 * 60 * 24).into(),
                Duration::from_secs(60 * 60 * 24 * 7).into(),
                Duration::from_secs(60 * 60 * 24 * 30).into(),
                Duration::from_secs(60 * 60 * 24 * 365).into(),
            ],
            compaction_interval: Duration::from_secs(60),
            cleanup_retention: Duration::from_secs(60 * 60),
            cleanup_interval: Duration::from_secs(5 * 60),
            compression: CompressionLevel::standard(),
            max_snapshot_entries: std::num::NonZeroUsize::new(100_000)
                .unwrap_or(std::num::NonZeroUsize::MIN),
        }
    }
}

impl SnapshotConfig {
    pub(crate) fn validate(&self) -> Result<(), crate::error::Error> {
        match self.compaction_tiers.first() {
            None => {
                return Err(crate::error::Error::InvalidConfiguration(
                    "compaction_tiers must not be empty".to_owned(),
                ));
            }
            Some(first) if first.as_duration() < self.interval => {
                return Err(crate::error::Error::InvalidConfiguration(format!(
                    "compaction_tiers[0] ({:?}) must be >= snapshot_interval ({:?})",
                    first, self.interval
                )));
            }
            Some(_) => {}
        }
        for window in self.compaction_tiers.windows(2) {
            let (a, b) = (window[0], window[1]);
            if b < a {
                return Err(crate::error::Error::InvalidConfiguration(format!(
                    "compaction_tiers must be non-decreasing; saw {a:?} before {b:?}"
                )));
            }
        }
        if self.compaction_interval.is_zero() {
            return Err(crate::error::Error::InvalidConfiguration(
                "compaction_interval must be > 0".to_owned(),
            ));
        }
        // `compression` needs no check here: `CompressionLevel` cannot be
        // constructed out of zstd's range.
        Ok(())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod config_tests {
    use super::*;
    use crate::error::Error;
    use std::time::Duration;

    // ── config validation ────────────────────────────────────────────────

    fn ok_snapshot_config() -> SnapshotConfig {
        SnapshotConfig::default()
    }

    // Note: the `max_clock_skew >= cutoff_timeout` cross-field constraint lives on
    // `ReplicaNodeConfig::validate` (both knobs meet there) and is tested in
    // `replica_node`.

    #[test]
    fn rejects_first_tier_below_snapshot_interval() {
        let mut c = ok_snapshot_config();
        c.interval = Duration::from_secs(2);
        c.compaction_tiers[0] = Duration::from_secs(1).into();
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }

    #[test]
    fn rejects_empty_compaction_tiers() {
        let mut c = ok_snapshot_config();
        c.compaction_tiers.clear();
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }

    #[test]
    fn rejects_non_monotonic_compaction_tiers() {
        let mut c = ok_snapshot_config();
        c.compaction_tiers = vec![
            Duration::from_secs(60).into(),
            Duration::from_secs(10).into(),
            Duration::from_secs(3600).into(),
        ];
        assert!(matches!(c.validate(), Err(Error::InvalidConfiguration(_))));
    }

    #[test]
    fn accepts_first_tier_equal_to_snapshot_interval() {
        let mut c = ok_snapshot_config();
        c.interval = Duration::from_secs(60);
        c.compaction_tiers[0] = Duration::from_secs(60).into();
        c.validate().expect("equality boundary admitted");
    }

    #[test]
    fn accepts_the_default_snapshot_entry_cap() {
        let c = ok_snapshot_config();
        assert_eq!(c.max_snapshot_entries.get(), 100_000);
        c.validate().expect("the default config is valid");
    }
}
