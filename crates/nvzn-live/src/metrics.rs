//! Metrics of the live view and of snapshot reads.

use std::time::Duration;

use metrics_kit::{MetricDef, lossy_f64, metrics};

use nvzn_types::Tenant;

const LABEL_TENANT: &str = "tenant";
const LABEL_KIND: &str = "kind";
const LABEL_STATE: &str = "state";

metrics!(pub "nvzn_", live; {
    custom live_held_events LIVE_HELD_EVENTS "live_held_events" Gauge Count
        "Writes a live view holds above the fold's watermark, by tenant: the \
               `S` events that reached this node and are not folded yet. Small \
               unless the fold stalls or `S` events flood.";
    custom live_held_bytes LIVE_HELD_BYTES "live_held_bytes" Gauge Bytes
        "Payload bytes of the writes a live view holds, by tenant; with \
               nvzn_live_held_events, what the overflow cap is checked against.";
    custom live_coverage LIVE_COVERAGE "live_coverage" Gauge None
        "A live view's state, by tenant and state: 1 for the current one of \
               `scanning` (cold start, snapshots wait), `complete`, `overflowed` \
               (over the cap: snapshots serve the fold's state alone until it \
               passes what was dropped).";
    custom live_materialise_seconds LIVE_MATERIALISE_SECONDS "live_materialise_seconds" Histogram Seconds
        "Time to build one live snapshot: open the store, fold the held \
               events through the live projections.";
    custom live_materialise_entries LIVE_MATERIALISE_ENTRIES "live_materialise_entries" Histogram Count
        "Held writes folded into one live snapshot. Repeated per snapshot, \
               so this is the per-read cost the held set adds.";
    custom live_scan_seconds LIVE_SCAN_SECONDS "live_scan_seconds" Histogram Seconds
        "Duration of a live view's cold-start scan of the stable log, \
               which the first snapshots wait for.";
    custom live_scan_entries LIVE_SCAN_ENTRIES "live_scan_entries" Histogram Count
        "Stable-log entries a live view's cold-start scan read: the gap \
               between the store's watermark and the stable watermark at start.";
    custom live_undecodable_total LIVE_UNDECODABLE_TOTAL "live_undecodable_total" Counter None
        "Payloads a live view could not decode, by tenant: logged and \
               skipped (the fold stops on the same entry once it is stable).";
    custom reads_total READS_TOTAL "reads_total" Counter None
        "Snapshots opened, by tenant and freshness. `stable` sees only what has \
               gone stable in the log, so a caller expecting read-your-own-writes \
               and finding `stable` here is reading the wrong one.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &live!();

pub fn live_held(tenant: Tenant, events: usize, bytes: usize) {
    metrics_kit::metrics_crate::gauge!(LIVE_HELD_EVENTS, LABEL_TENANT => tenant.name())
        .set(lossy_f64(u64::try_from(events).unwrap_or(u64::MAX)));
    metrics_kit::metrics_crate::gauge!(LIVE_HELD_BYTES, LABEL_TENANT => tenant.name())
        .set(lossy_f64(u64::try_from(bytes).unwrap_or(u64::MAX)));
}

pub fn live_coverage(tenant: Tenant, current: &'static str) {
    for state in ["scanning", "complete", "overflowed"] {
        metrics_kit::metrics_crate::gauge!(LIVE_COVERAGE, LABEL_TENANT => tenant.name(), LABEL_STATE => state)
            .set(if state == current { 1.0 } else { 0.0 });
    }
}

pub fn live_materialise(entries: usize, elapsed: Duration) {
    metrics_kit::metrics_crate::histogram!(LIVE_MATERIALISE_SECONDS).record(elapsed);
    metrics_kit::metrics_crate::histogram!(LIVE_MATERIALISE_ENTRIES)
        .record(lossy_f64(u64::try_from(entries).unwrap_or(u64::MAX)));
}

pub fn live_scan(entries: u64, elapsed: Duration) {
    metrics_kit::metrics_crate::histogram!(LIVE_SCAN_SECONDS).record(elapsed);
    metrics_kit::metrics_crate::histogram!(LIVE_SCAN_ENTRIES).record(lossy_f64(entries));
}

pub fn live_undecodable(tenant: Tenant) {
    metrics_kit::metrics_crate::counter!(LIVE_UNDECODABLE_TOTAL, LABEL_TENANT => tenant.name())
        .increment(1);
}

pub fn read(tenant: Tenant, kind: ReadKind) {
    metrics_kit::metrics_crate::counter!(READS_TOTAL, LABEL_TENANT => tenant.name(), LABEL_KIND => kind.as_str())
        .increment(1);
}

/// Which snapshot a read was served from. `stable` sees only what has gone
/// stable in the log; `live` adds the live events this node has received.
#[derive(Debug, Clone, Copy)]
pub enum ReadKind {
    Stable,
    Live,
}

impl ReadKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Live => "live",
        }
    }
}
