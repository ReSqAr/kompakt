//! What the object store emits: request latency and transfer volume by
//! operation, and the size and compression of what was written.

use metrics_kit::{LABEL_STATE, MetricDef, OpState, Probe, label_enum, metrics};
use zslg_snapshot::ObjectStoreReason;

const LABEL_OP: &str = "op";
const LABEL_REASON: &str = "reason";

metrics!(pub "zslg_", object_store; {
    gauge   snapshot_size SNAPSHOT_SIZE_BYTES "snapshot_size_bytes" Bytes
        "Size of the last successful snapshot object." (bytes: u64);
    custom  snapshot_compression_ratio SNAPSHOT_COMPRESSION_RATIO
        "snapshot_compression_ratio" Gauge None
        "Compression ratio of the last written snapshot \
         (uncompressed_bytes / compressed_bytes). >1 means compression \
         saved space; 1 means incompressible data.";
    custom  object_store_transfer_bytes OBJECT_STORE_TRANSFER_BYTES_TOTAL
        "object_store_transfer_bytes_total" Counter Bytes
        "Bytes moved to/from the object store, by operation and by *why* the \
         transfer happened. The `reason` split is the one that matters: reads \
         run at a multiple of writes (26.7 GB GET against 12.3 GB PUT on one \
         harness run), and `op` alone cannot say whether that is compaction \
         re-reading what it just wrote or learners re-fetching a purged prefix \
         — which have opposite fixes.";
    custom  object_store_request_probe OBJECT_STORE_REQUEST_DURATION_SECONDS
        "object_store_request_duration_seconds" Histogram Seconds
        "Object-store request latency by operation and outcome. Count \
         per (op, state) doubles as the request counter.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &object_store!();

label_enum!(#[doc = "`op` label of the object-store metrics."] pub ObjectStoreOp {
    Get => "get",
    Put => "put",
    Head => "head",
});

pub fn snapshot_compression_ratio(uncompressed: u64, compressed: u64) {
    if compressed == 0 || uncompressed == 0 {
        return;
    }
    let pct = uncompressed
        .saturating_mul(100)
        .checked_div(compressed)
        .unwrap_or(u64::MAX);
    metrics_kit::metrics_crate::gauge!(SNAPSHOT_COMPRESSION_RATIO).set(metrics_kit::lossy_f64(pct));
}

pub fn object_store_transfer_bytes(op: ObjectStoreOp, reason: ObjectStoreReason, bytes: u64) {
    metrics_kit::metrics_crate::counter!(
        OBJECT_STORE_TRANSFER_BYTES_TOTAL,
        LABEL_OP => op.as_str(),
        LABEL_REASON => reason.as_str(),
    )
    .increment(bytes);
}

/// Probe for [`OBJECT_STORE_REQUEST_DURATION_SECONDS`] with the given
/// operation baked into an extra `op` label.
pub fn object_store_request_probe(op: ObjectStoreOp) -> Probe<OpState> {
    Probe::start_with_extra(
        OBJECT_STORE_REQUEST_DURATION_SECONDS,
        LABEL_STATE,
        (LABEL_OP, op.as_str()),
        OpState::Failure,
    )
}
