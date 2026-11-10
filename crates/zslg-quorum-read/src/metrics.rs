//! The one metric the write path and the read path share: how each replica
//! answered a fan-out.

use metrics_kit::{LABEL_STATE, MetricDef, label_enum, metrics};

metrics!(pub "zslg_", quorum_fanout; {
    custom  write_fanout_response  WRITE_FANOUT_RESPONSES_TOTAL
        "write_fanout_responses_total" Counter None
        "Per-replica fan-out responses (ack / rejected / unreachable). \
         Persistent unreachable share = degraded replica set.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &quorum_fanout!();

pub fn write_fanout_response(state: FanoutResponseState) {
    metrics_kit::metrics_crate::counter!(
        WRITE_FANOUT_RESPONSES_TOTAL,
        LABEL_STATE => state.as_str(),
    )
    .increment(1);
}

label_enum!(#[doc = "`state` label of [`WRITE_FANOUT_RESPONSES_TOTAL`]."] pub FanoutResponseState {
    Ack => "ack",
    Rejected => "rejected",
    Unreachable => "unreachable",
});
