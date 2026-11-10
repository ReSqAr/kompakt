//! What the replica service emits: write requests by outcome and the dispatch wait.

use metrics_kit::{LABEL_STATE, MetricDef, label_enum, metrics};

metrics!(pub "zslg_", replica_service; {
    labeled_counter replica_write_request REPLICA_WRITE_REQUESTS_TOTAL
        "replica_write_requests_total"
        "Replica-side write requests by outcome. fail_hlc_cutoff spikes are \
         expected briefly after snapshot cutoffs. (Wall-clock skew is \
         enforced at ingress as hlc_drift_too_large on the fresh per-RPC \
         HLC, not here.)"
        (state: ReplicaWriteState) [LABEL_STATE => state.as_str()];
    hist_duration server_dispatch_wait SERVER_DISPATCH_WAIT_SECONDS
        "server_dispatch_wait_seconds"
        "Time a replicated write sat in the follower's server -> raft-append \
         dispatch channel (frame received -> the raft submit picks it up). \
         Directly timestamped on the queued object; grows under follower \
         backpressure."
        (elapsed: Duration);
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &replica_service!();

label_enum!(#[doc = "`state` label of [`REPLICA_WRITE_REQUESTS_TOTAL`]."] pub ReplicaWriteState {
    Success => "success",
    FailHlcCutoff => "fail_hlc_cutoff",
    FailNotReplica => "fail_not_replica",
    FailStorage => "fail_storage",
});
