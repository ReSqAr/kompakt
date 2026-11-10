//! The clock's metrics: its own reading, its drift against the wall clock, and
//! what it rejects from peers.

use metrics_kit::{MetricDef, label_enum, lossy_f64_signed, metrics};
use zslg_time::Hlc;

const LABEL_DIRECTION: &str = "direction";

metrics!(pub "zslg_", hlc_clock; {
    gauge_via     hlc_now_physical_ms HLC_NOW_PHYSICAL_MS "hlc_now_physical_ms" Milliseconds
        "Physical-millisecond part of the node's current HLC." (hlc: Hlc => Hlc::physical_ms);
    gauge   hlc_logical HLC_LOGICAL "hlc_logical" Count
        "Logical component of the node's current HLC. Sustained high \
         values = wall clock stalled or heavy same-millisecond traffic."
        (logical: u64);
    custom  hlc_wall_clock_drift_ms HLC_WALL_CLOCK_DRIFT_MS "hlc_wall_clock_drift_ms"
        Gauge Milliseconds
        "Signed difference between the HLC physical part and the wall \
         clock. Approaching max_clock_skew = imminent write rejections.";
    hist_duration hlc_drift HLC_DRIFT_SECONDS "hlc_drift_seconds"
        "Absolute drift between a peer HLC physical-ms and local wall clock \
         observed during integrate(). Values beyond the configured skew limit \
         are rejected; the histogram reveals the normal operating band. Under \
         load this is dominated by receive-buffer latency, not clock skew."
        (drift: Duration);
    labeled_counter hlc_drift_rejection HLC_DRIFT_REJECTIONS_TOTAL
        "hlc_drift_rejections_total"
        "Peer HLC integrations rejected as hlc_drift_too_large, by direction. \
         `behind` (older peer stamp) is usually receive-buffer latency under \
         load; `ahead` points at a genuinely fast peer clock."
        (direction: DriftDirection) [LABEL_DIRECTION => direction.as_str()];
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &hlc_clock!();

pub fn hlc_wall_clock_drift_ms(drift_ms: i64) {
    metrics_kit::metrics_crate::gauge!(HLC_WALL_CLOCK_DRIFT_MS).set(lossy_f64_signed(drift_ms));
}

label_enum!(#[doc = "`direction` label of [`HLC_DRIFT_REJECTIONS_TOTAL`]."] pub DriftDirection {
    /// Peer HLC physical-ms is behind local (older) — usually buffer latency.
    Behind => "behind",
    /// Peer HLC physical-ms is ahead of local (newer) — peer clock ahead.
    Ahead => "ahead",
});
