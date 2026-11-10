//! What the stable log emits: watermark position, purge threshold and append/commit latency.

use metrics_kit::{MetricDef, metrics, set_gauge, unix_now_secs};
use zslg_time::Hlc;

metrics!(pub "zslg_", stable_log; {
    gauge_via     stable_log_end_hlc_physical_ms STABLE_LOG_END_HLC_PHYSICAL_MS
        "stable_log_end_hlc_physical_ms" Milliseconds
        "Physical-millisecond part of the stable log's end HLC (highest \
         stabilized HLC). Distance to hlc_now_physical_ms = stabilization lag."
        (end_hlc: Hlc => Hlc::physical_ms);
    gauge_via     stable_log_purge_threshold_physical_ms STABLE_LOG_PURGE_THRESHOLD_PHYSICAL_MS
        "stable_log_purge_threshold_physical_ms" Milliseconds
        "Physical-millisecond part of the HLC the stable log is being purged \
         up to (manifest_watermark − purge_ttl, before the frontier clamp). \
         Only moves when TTL purge is enabled; 0/absent means purge off or \
         nothing aged out yet. Gap to stable_log_end_hlc_physical_ms = the \
         live retention window still held locally (design doc §794)."
        (threshold: Hlc => Hlc::physical_ms);
    custom  stable_log_end_advanced STABLE_LOG_END_HLC_ADVANCED_TIMESTAMP_SECONDS
        "stable_log_end_hlc_advanced_timestamp_seconds" Gauge Seconds
        "Unix-epoch seconds of the last stable-log upper-watermark (HLC end) \
         advance on this node (new data going quorum-stable). Paired with the \
         manifest stamp it separates 'idle' (both flat) from 'snapshot stalled' \
         (this advancing, manifest flat).";
    probe_unlabeled stable_log_commit_probe STABLE_LOG_COMMIT_SECONDS
        "stable_log_commit_seconds"
        "Segment-WAL append + fsync latency inside StableLog::append \
         batches (the stable log's own durability fence).  High values = \
         slow WAL fsyncs during snapshot load.";
    probe   stable_log_append_probe STABLE_LOG_APPEND_DURATION_SECONDS
        "stable_log_append_duration_seconds"
        "StableLog::append latency by outcome. _count is the append \
         throughput; _bucket the latency distribution.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &stable_log!();

/// Wall-clock stamp (Unix epoch seconds) of the last time this node's stable-log
/// upper watermark (HLC end) advanced — new data going quorum-stable, via append
/// or empty-window promotion. Grafana: `time() - stable_log_end_hlc_advanced_timestamp_seconds`.
pub fn stable_log_end_advanced() {
    set_gauge(
        STABLE_LOG_END_HLC_ADVANCED_TIMESTAMP_SECONDS,
        unix_now_secs(),
    );
}
