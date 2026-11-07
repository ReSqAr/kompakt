//! What the local store emits: integration, flushing and stored volume.

use metrics_kit::{MetricDef, label_enum, lossy_f64, metrics, set_gauge, unix_now_secs};

const LABEL_KIND: &str = "kind";

metrics!(pub "zslg_", local_store; {
    hist    local_store_entry_bytes LOCAL_STORE_ENTRY_BYTES
        "local_store_entry_bytes" Bytes
        "Encoded size of full-data entries written to the local store. \
         _bucket is the payload-size distribution."
        (bytes: usize);
    hist    local_store_flush_bytes LOCAL_STORE_FLUSH_BYTES
        "local_store_flush_bytes" Bytes
        "Bytes written per local-store flush (one WriteBatch). _sum \
         is byte throughput, _count the number of flushes."
        (bytes: usize);
    hist    local_store_flush_entries LOCAL_STORE_FLUSH_ENTRIES
        "local_store_flush_entries" Count
        "Operations folded into one local-store flush. Near 1 under load = \
         batching starvation; near the cap = healthy batching."
        (count: usize);
    custom  local_store_flushed LOCAL_STORE_LAST_FLUSH_TIMESTAMP_SECONDS
        "local_store_last_flush_timestamp_seconds" Gauge Seconds
        "Unix timestamp of the last local-store flush (Grafana: time() minus \
         this for 'time since last flush').";
    probe   local_store_integrate_probe LOCAL_STORE_INTEGRATE_DURATION_SECONDS
        "local_store_integrate_duration_seconds"
        "LocalStore::integrate_batch latency by outcome. _count is the \
         integrate throughput.";
    custom  local_store_integrate_queue_depth LOCAL_STORE_INTEGRATE_QUEUE_DEPTH
        "local_store_integrate_queue_depth" Gauge None
         "Depth of the node's shared local-store integration channel, which \
         every subscription forwards into. Non-zero means the single \
         integration worker is falling behind the incoming streams. Not \
         labeled by peer: there is one queue per node, not one per \
         subscription (it replaced the per-peer subscription_buffer_depth \
         when the per-subscription queue was removed).";
    custom  local_store_integrate_skipped_duplicate LOCAL_STORE_INTEGRATE_SKIPPED_DUPLICATE_TOTAL
        "local_store_integrate_skipped_duplicate_total" Counter None
        "Duplicate copies of an already-written data_id dropped before the LSM \
         write. Every learner subscribes to every replica, so each write \
         arrives once per replica; this counts copies 2..n. Expect roughly \
         (replicas - 1)/replicas of arrivals. Distinct from \
         skipped_stale, which is about the stable prefix, not duplication.";
    custom  local_store_integrate_skipped_stale LOCAL_STORE_INTEGRATE_SKIPPED_STALE_TOTAL
        "local_store_integrate_skipped_stale_total" Counter None
        "Subscription records skipped at integrate because the stable prefix \
         already covers them (HLC <= end_hlc). Catch-up redundancy avoided; \
         near-zero in steady state.";
    custom  local_store_ops  LOCAL_STORE_OPS_TOTAL  "local_store_ops_total"  Counter None
        "Local-store write operations by kind (integrate).";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &local_store!();

pub fn local_store_flushed() {
    set_gauge(LOCAL_STORE_LAST_FLUSH_TIMESTAMP_SECONDS, unix_now_secs());
}

pub fn local_store_integrate_queue_depth(depth: usize) {
    let v: u64 = u64::try_from(depth).unwrap_or(0);
    metrics_kit::metrics_crate::gauge!(LOCAL_STORE_INTEGRATE_QUEUE_DEPTH).set(lossy_f64(v));
}

pub fn local_store_integrate_skipped_duplicate(count: u64) {
    if count == 0 {
        return;
    }
    metrics_kit::metrics_crate::counter!(LOCAL_STORE_INTEGRATE_SKIPPED_DUPLICATE_TOTAL)
        .increment(count);
}

pub fn local_store_integrate_skipped_stale(count: u64) {
    if count == 0 {
        return;
    }
    metrics_kit::metrics_crate::counter!(LOCAL_STORE_INTEGRATE_SKIPPED_STALE_TOTAL)
        .increment(count);
}

pub fn local_store_ops(kind: LocalWriteKind, count: u64) {
    if count == 0 {
        return;
    }
    metrics_kit::metrics_crate::counter!(LOCAL_STORE_OPS_TOTAL, LABEL_KIND => kind.as_str())
        .increment(count);
}

label_enum!(#[doc = "`kind` label of [`LOCAL_STORE_OPS_TOTAL`] — the local-store write payload type."] pub LocalWriteKind {
    Integrate => "integrate",
});
