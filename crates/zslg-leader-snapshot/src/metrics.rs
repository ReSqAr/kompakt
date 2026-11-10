//! Metrics of the leader snapshot, cutoff and cleanup jobs.

use std::time::Duration;

use metrics_kit::{LABEL_STATE, MetricDef, label_enum, metrics};

const LABEL_STEP: &str = "step";

metrics!(pub "zslg_", snapshot; {
    labeled_counter snapshot_creation SNAPSHOT_CREATION_TOTAL "snapshot_creation_total"
        "Snapshot creation attempts by outcome (see SnapshotState). \
         waiting_for_stable_log is backpressure, not failure: the leader's \
         local stable log has not re-ingested its own last snapshot yet."
        (state: SnapshotState) [LABEL_STATE => state.as_str()];
    custom  snapshot_creation_step SNAPSHOT_CREATION_DURATION_SECONDS
        "snapshot_creation_duration_seconds" Histogram Seconds
        "Snapshot creation latency per step (set_cutoff, await_quorum, \
         write_object, commit) plus total.";
    gauge_usize snapshot_entries SNAPSHOT_ENTRIES_COUNT "snapshot_entries_count" Count
        "Entries in the last successful snapshot." (n: usize);
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &snapshot!();

pub fn snapshot_creation_step(step: SnapshotStep, elapsed: Duration) {
    metrics_kit::metrics_crate::histogram!(
        SNAPSHOT_CREATION_DURATION_SECONDS,
        LABEL_STEP => step.as_str(),
    )
    .record(elapsed);
}

label_enum!(#[doc = "`state` label of [`SNAPSHOT_CREATION_TOTAL`]."] pub SnapshotState {
    Success => "success",
    /// The (watermark, cutoff] window was empty — nothing to snapshot.
    NoopEmptyWindow => "noop_empty_window",
    /// The replica quorum did not confirm the cutoff in time.
    FailureQuorumTimeout => "failure_quorum_timeout",
    /// The final fenced commit lost the generation CAS (leadership churn).
    FailureStaleGeneration => "failure_stale_generation",
    FailureObjectStore => "failure_object_store",
    FailureStorage => "failure_storage",
    /// The leader's local stable log has not caught up with the manifest yet,
    /// so index assignment cannot proceed. Backpressure, not failure — it
    /// clears once the leader re-ingests its own last snapshot.
    WaitingForStableLog => "waiting_for_stable_log",
});

label_enum!(#[doc = "`step` label of [`SNAPSHOT_CREATION_DURATION_SECONDS`]."] pub SnapshotStep {
    SetCutoff => "set_cutoff",
    AwaitQuorum => "await_quorum",
    WriteObject => "write_object",
    Commit => "commit",
    Total => "total",
});
