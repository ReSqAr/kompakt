//! Metrics of the fold loop and its store fence.

use std::time::Duration;

use metrics_kit::{MetricDef, lossy_f64, metrics, unix_now_secs};

use nvzn_types::{StopReason, Tenant};

const LABEL_OUTCOME: &str = "outcome";
const LABEL_REASON: &str = "reason";
const LABEL_TENANT: &str = "tenant";
const LABEL_KIND: &str = "kind";
const LABEL_STATE: &str = "state";

metrics!(pub "nvzn_", dispatcher; {
    custom fold_watermark_advanced_timestamp_seconds FOLD_WATERMARK_ADVANCED_TIMESTAMP_SECONDS "fold_watermark_advanced_timestamp_seconds" Gauge Seconds
        "Unix-epoch seconds of the last fold-watermark advance. THE staleness \
               alert, evaluated as `time() - nvzn_fold_watermark_advanced_timestamp_seconds`: \
               a lag gauge goes stale exactly when the dispatcher dies, this keeps climbing.";
    custom fold_watermark_log_index FOLD_WATERMARK_LOG_INDEX "fold_watermark_log_index" Gauge Count
        "Stable-log index the read model has folded through. Where the \
               projections are, in log terms.";
    custom fold_watermark_hlc_physical_ms FOLD_WATERMARK_HLC_PHYSICAL_MS "fold_watermark_hlc_physical_ms" Gauge Milliseconds
        "Physical-millisecond part of the folded watermark's HLC. Distance to \
               zslg's zslg_manifest_end_hlc_physical_ms is projection lag in \
               wall-clock terms.";
    custom fold_batches_total FOLD_BATCHES_TOTAL "fold_batches_total" Counter None
        "Batches committed by the fold loop. One batch is one store transaction \
               covering whole log entries until it holds `fold_commit` bytes of writes.";
    custom fold_batch_seconds FOLD_BATCH_SECONDS "fold_batch_seconds" Histogram Seconds
        "Time to fold and commit one batch: decode, project, and commit.";
    custom fold_batch_entries FOLD_BATCH_ENTRIES "fold_batch_entries" Histogram Count
        "Log entries per committed batch.";
    custom fold_batch_bytes FOLD_BATCH_BYTES "fold_batch_bytes" Histogram Bytes
        "Bytes a committed batch staged (rows and index entries, estimated). \
               At `fold_commit` (4 MiB unless configured) a burst folds in several commits.";
    custom fold_entries_total FOLD_ENTRIES_TOTAL "fold_entries_total" Counter None
        "Log entries seen by the fold loop, by what became of them. Deliberately \
               unlabelled by tenant: `foreign_tenant` counts entries for tenants this \
               dispatcher does not host, whose ids are unbounded on a shared log.";
    custom fold_stopped_total FOLD_STOPPED_TOTAL "fold_stopped_total" Counter None
        "Fold-loop exits by reason. `poison_entry` and `non_monotone_data_id` \
               are permanent: the entry stays in the log, so a restart fails \
               identically and the read model is frozen until an operator acts.";
    custom store_fence_total STORE_FENCE_TOTAL "store_fence_total" Counter None
        "Store fsyncs by outcome. `failed` is terminal — nothing this dispatcher \
               commits can be made durable any more, so the fold loop stops.";
    custom store_fence_seconds STORE_FENCE_SECONDS "store_fence_seconds" Histogram Seconds
        "Time for one store fsync (flush_wal).";
    custom store_resets_total STORE_RESETS_TOTAL "store_resets_total" Counter None
        "Config-hash mismatches that wiped the store and refolded the log from \
               index zero. Expected on a deploy that changes the registered \
               projections; a steady rate is not.";
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
pub const METRICS: &[MetricDef] = &dispatcher!();

pub fn fold_watermark(index: Option<zslg::LogIndex>, hlc: Option<zslg::Hlc>) {
    metrics_kit::metrics_crate::gauge!(FOLD_WATERMARK_ADVANCED_TIMESTAMP_SECONDS)
        .set(lossy_f64(unix_now_secs()));
    if let Some(index) = index {
        metrics_kit::metrics_crate::gauge!(FOLD_WATERMARK_LOG_INDEX).set(lossy_f64(index.raw()));
    }
    if let Some(hlc) = hlc {
        metrics_kit::metrics_crate::gauge!(FOLD_WATERMARK_HLC_PHYSICAL_MS)
            .set(lossy_f64(hlc.physical_ms()));
    }
}

pub fn fold_batch(entries: usize, bytes: usize, elapsed: Duration) {
    metrics_kit::metrics_crate::counter!(FOLD_BATCHES_TOTAL).increment(1);
    metrics_kit::metrics_crate::histogram!(FOLD_BATCH_SECONDS).record(elapsed);
    metrics_kit::metrics_crate::histogram!(FOLD_BATCH_ENTRIES)
        .record(lossy_f64(u64::try_from(entries).unwrap_or(0)));
    metrics_kit::metrics_crate::histogram!(FOLD_BATCH_BYTES)
        .record(lossy_f64(u64::try_from(bytes).unwrap_or(0)));
}

pub fn fold_entry(outcome: EntryOutcome) {
    metrics_kit::metrics_crate::counter!(FOLD_ENTRIES_TOTAL, LABEL_OUTCOME => outcome.as_str())
        .increment(1);
}

pub fn fold_stopped(reason: StopReason) {
    metrics_kit::metrics_crate::counter!(FOLD_STOPPED_TOTAL, LABEL_REASON => reason.as_str())
        .increment(1);
}

pub fn store_fence(outcome: FenceOutcome, elapsed: Duration) {
    metrics_kit::metrics_crate::counter!(STORE_FENCE_TOTAL, LABEL_OUTCOME => outcome.as_str())
        .increment(1);
    metrics_kit::metrics_crate::histogram!(STORE_FENCE_SECONDS).record(elapsed);
}

pub fn store_reset() {
    metrics_kit::metrics_crate::counter!(STORE_RESETS_TOTAL).increment(1);
}

/// What the fold loop did with a log entry.
#[derive(Debug, Clone, Copy)]
pub enum EntryOutcome {
    /// Decoded and handed to the projectors.
    Applied,
    /// At or below the stored watermark: already folded.
    Replayed,
    /// A tenant this dispatcher hosts no handler for.
    ForeignTenant,
    /// A watermark advance, carrying no payload.
    Watermark,
}

impl EntryOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Replayed => "replayed",
            Self::ForeignTenant => "foreign_tenant",
            Self::Watermark => "watermark",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FenceOutcome {
    Ok,
    Failed,
}

impl FenceOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

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
