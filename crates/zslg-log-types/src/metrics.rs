//! Metrics every log implementation emits the same way: writes, index frontier and purged entries, by log type.

use metrics_kit::{MetricDef, label_enum, lossy_f64, metrics};

const LABEL_LOG_TYPE: &str = "log_type";

metrics!(pub "zslg_", log_shared; {
    custom  log_writes  LOG_WRITES_TOTAL  "log_writes_total"  Counter None
        "Entries appended/integrated, by log (replica = single-writer \
         replica log, local = learner local store).";
    custom  log_index_frontier  LOG_INDEX_FRONTIER  "log_index_frontier"
        Gauge None
        "Highest log index per log type. replica-vs-local spread on a \
         learner = replication lag in entries.";
    custom  purged_entries  PURGED_ENTRIES_TOTAL  "purged_entries_total"  Counter None
        "Entries removed by purge workers, by log type.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &log_shared!();

pub fn log_writes(log_type: LogType, count: usize) {
    if count == 0 {
        return;
    }
    metrics_kit::metrics_crate::counter!(
        LOG_WRITES_TOTAL,
        LABEL_LOG_TYPE => log_type.as_str(),
    )
    .increment(u64::try_from(count).expect("log write count fits u64"));
}

pub fn log_index_frontier(log_type: LogType, index: u64) {
    metrics_kit::metrics_crate::gauge!(
        LOG_INDEX_FRONTIER,
        LABEL_LOG_TYPE => log_type.as_str(),
    )
    .set(lossy_f64(index));
}

pub fn purged_entries(log_type: LogType, purged: usize) {
    if purged == 0 {
        return;
    }
    metrics_kit::metrics_crate::counter!(
        PURGED_ENTRIES_TOTAL,
        LABEL_LOG_TYPE => log_type.as_str(),
    )
    .increment(u64::try_from(purged).expect("purge count fits u64"));
}

label_enum!(#[doc = "`log_type` label ([`LOG_WRITES_TOTAL`], [`LOG_INDEX_FRONTIER`], \
[`PURGED_ENTRIES_TOTAL`])."] pub LogType {
    Local => "local",
    Replica => "replica",
    Stable => "stable",
    Raft => "raft",
});
