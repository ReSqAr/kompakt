//! Metrics of the leader compaction job.

use metrics_kit::{LABEL_STATE, MetricDef, OpState, metrics};

metrics!(pub "zslg_", compaction; {
    labeled_counter compaction_run COMPACTION_RUNS_TOTAL "compaction_runs_total"
        "Manifest compaction runs by outcome."
        (state: OpState) [LABEL_STATE => state.as_str()];
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &compaction!();
