//! The gate's metrics: writes admitted but not yet durable, cutoff changes and
//! the time a cutoff waits for them.

use metrics_kit::{LABEL_STATE, MetricDef, label_enum, metrics};

metrics!(pub "zslg_", hlc_gate; {
    custom  hlc_guards_inc  HLC_GUARDS_IN_FLIGHT  "hlc_guards_in_flight"
        Gauge Count "Outstanding HlcGuards (writes admitted but not yet \
         durable + broadcast). Cutoff drain blocks on these.";
    labeled_counter hlc_cutoff_set HLC_CUTOFF_SETS_TOTAL "hlc_cutoff_sets_total"
        "Cutoff set attempts on this replica. rejected_backwards is expected \
         once per leader change; alert on sustained rate."
        (state: CutoffSetState) [LABEL_STATE => state.as_str()];
    probe_unlabeled hlc_cutoff_drain_probe HLC_CUTOFF_DRAIN_DURATION_SECONDS
        "hlc_cutoff_drain_duration_seconds"
        "Time set_cutoff waited for in-flight HlcGuards to drain before \
         ACKing. Tail growth = slow write paths holding guards.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &hlc_gate!();

pub fn hlc_guards_inc() {
    metrics_kit::metrics_crate::gauge!(HLC_GUARDS_IN_FLIGHT).increment(1.0);
}

pub fn hlc_guards_dec() {
    metrics_kit::metrics_crate::gauge!(HLC_GUARDS_IN_FLIGHT).decrement(1.0);
}

label_enum!(#[doc = "`state` label of [`HLC_CUTOFF_SETS_TOTAL`]. `rejected_backwards` is \
*expected* after a leader change (design doc gotcha: a new leader's first proposed cutoff \
may precede its predecessor's); alert on sustained rate, not occurrence."] pub CutoffSetState {
    Accepted => "accepted",
    RejectedBackwards => "rejected_backwards",
});
