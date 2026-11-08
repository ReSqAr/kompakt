//! The leader's snapshot protocol jobs: `leader:cutoff` (step 1),
//! `leader:snapshot` (steps 2-5) and `leader:cleanup`.

pub mod create;
pub mod metrics;

mod cleanup;
mod cutoff;
mod job;

pub use cleanup::{CleanupConfig, CleanupHandles, spawn_cleanup};
pub use cutoff::{CutoffConfig, CutoffHandles, spawn_cutoff};
pub use job::{
    SNAPSHOT_LOOP_FLOOR, SnapshotHandles, SnapshotJob, SnapshotJobConfig, spawn_snapshot,
};

error_union::error_union! {
    /// What this crate can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(zslg_lifecycle_types::LifecycleError),
        Config(zslg_lifecycle_types::ConfigError),
        Clock(zslg_time::ClockError),
        Log(zslg_log::LogError),
        Snapshot(zslg_snapshot::SnapshotError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_quorum_read::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

error_union::error_bridge!(Error => zslg_log::Error, zslg_snapshot::Error, zslg_consensus::Error, zslg_raft::Error, zslg_quorum_read::Error; Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus);
error_union::error_home!(Error);

impl From<shutdown::Stopped> for Error {
    fn from(_: shutdown::Stopped) -> Self {
        Self::Lifecycle(zslg_lifecycle_types::LifecycleError::Shutdown)
    }
}

impl Error {
    /// The `zslg_snapshot_creation_total` failure bucket for this error. Lives on
    /// the error type so new variants get classified here, next to their
    /// definition, instead of in a caller-side catch-all.
    #[must_use]
    pub(crate) fn snapshot_state(&self) -> crate::metrics::SnapshotState {
        use crate::metrics::SnapshotState;
        match self {
            Error::Consensus(zslg_consensus::ConsensusError::StaleStateWrite { .. }) => {
                SnapshotState::FailureStaleGeneration
            }
            Error::Consensus(zslg_consensus::ConsensusError::QuorumUnreachable)
            | Error::Log(zslg_log::LogError::QuorumReadTimeout { .. }) => {
                SnapshotState::FailureQuorumTimeout
            }
            Error::Snapshot(zslg_snapshot::SnapshotError::ObjectStore(_)) => {
                SnapshotState::FailureObjectStore
            }
            Error::Log(zslg_log::LogError::StableLogBehindManifest { .. }) => {
                SnapshotState::WaitingForStableLog
            }
            // HLC-cutoff rejection comes from a replica RPC error; the
            // leader sees it as a fan-out failure. Everything else counts
            // as a storage-side failure.
            _ => SnapshotState::FailureStorage,
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
    use super::*;
    use crate::metrics::{SNAPSHOT_CREATION_DURATION_SECONDS, SnapshotState, SnapshotStep};
    use zslg_log::LogError;
    use zslg_log::LogIndex;

    /// A stable log trailing the manifest is backpressure, not failure: the
    /// leader simply has not re-ingested its own last snapshot yet. It must not
    /// land in a `failure_*` bucket, or `zslg_snapshot_creation_total{state=~"failure.*"}`
    /// alerts on normal operation.
    #[test]
    fn stable_log_behind_manifest_is_not_a_failure_state() {
        let e = Error::Log(LogError::StableLogBehindManifest {
            end_index: Some(LogIndex(41)),
            manifest_end_index: LogIndex(99),
        });
        assert_eq!(e.snapshot_state(), SnapshotState::WaitingForStableLog);
    }

    #[test]
    fn labeled_histogram_emission_is_observable() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        ::metrics::with_local_recorder(&recorder, || {
            crate::metrics::snapshot_creation_step(
                SnapshotStep::Total,
                std::time::Duration::from_millis(42),
            );
        });
        let found = snapshotter
            .snapshot()
            .into_hashmap()
            .keys()
            .any(|ck| ck.key().name() == SNAPSHOT_CREATION_DURATION_SECONDS);
        assert!(
            found,
            "snapshot_creation_duration_seconds histogram should be present"
        );
    }
}
