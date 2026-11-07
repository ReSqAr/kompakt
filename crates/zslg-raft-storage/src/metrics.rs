//! What the raft storage emits: append/apply/snapshot latency, the applied index, and
//! reads that found a purged range.

use metrics_kit::{MetricDef, metrics};

metrics!(pub "zslg_", raft_storage; {
    gauge   raft_applied_log_index RAFT_APPLIED_LOG_INDEX "raft_applied_log_index" None
        "Last applied raft log index. Spread across nodes = apply lag." (index: u64);
    probe_unlabeled raft_apply_probe RAFT_APPLY_DURATION_SECONDS "raft_apply_duration_seconds"
        "Raft apply_to_state_machine latency (membership writes + commit + \
         fsync). The hottest I/O path after the replica log.";
    probe_unlabeled raft_build_snapshot_probe RAFT_BUILD_SNAPSHOT_DURATION_SECONDS
        "raft_build_snapshot_duration_seconds"
        "Raft build_snapshot latency (read all meta + encode payload + \
         commit + fsync). The most expensive single storage operation.";
    probe_unlabeled raft_install_snapshot_probe RAFT_INSTALL_SNAPSHOT_DURATION_SECONDS
        "raft_install_snapshot_duration_seconds"
        "Raft install_snapshot latency (decode + batch write + commit + \
         fsync). Snapshot transfer cost on the receiving side.";
    probe_unlabeled raft_log_append_probe RAFT_LOG_APPEND_DURATION_SECONDS
        "raft_log_append_duration_seconds"
        "Raft append_to_log latency (encode + batch + commit + fsync). \
         Slow values delay replication to followers.";
    counter raft_log_read_empty_range RAFT_LOG_READ_EMPTY_RANGE_TOTAL
        "raft_log_read_empty_range_total"
        "Raft-log reads for a non-empty index range that returned no \
         entries, i.e. the whole range was already purged. openraft's \
         DEFAULT limited_get_log_entries would unwrap that empty result \
         and abort the process (our release profile sets panic=abort); \
         we override it to return a StorageError instead, so openraft \
         falls back to snapshot replication. Non-zero therefore means \
         that fallback is being exercised — a follower fell behind the \
         purge floor — not that anything crashed.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &raft_storage!();
