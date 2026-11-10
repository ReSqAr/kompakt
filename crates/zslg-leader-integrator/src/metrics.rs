//! Metrics of the leader integrator.

use metrics_kit::{LABEL_STATE, MetricDef, label_enum, metrics};

metrics!(pub "zslg_", integrator; {
    custom  leader_intent   LEADER_INTENTS_TOTAL "leader_intents_total" Counter None
        "Raft-state intents resolved by the leader integrator, by outcome \
         (applied/already_satisfied/superseded/failed).";
    custom  leader_intent_batch LEADER_INTENT_BATCHES_TOTAL "leader_intent_batches_total"
        Counter None "Raft proposals issued by the leader integrator. Divide \
        leader_intents_total{state=\"applied\"} by this for the coalescing factor.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &integrator!();

/// One intent resolved by the integrator, by outcome.
pub fn leader_intent(outcome: IntentState) {
    metrics_kit::metrics_crate::counter!(LEADER_INTENTS_TOTAL, LABEL_STATE => outcome.as_str())
        .increment(1);
}

/// One Raft proposal issued by the integrator, carrying one or more intents.
///
/// The coalescing signal is the ratio:
/// `rate(leader_intents_total{state="applied"}) / rate(leader_intent_batches_total)`
/// — above 1 means several jobs' writes rode one Raft entry instead of
/// contending for the generation fence. Sustained 1 is fine at low load and
/// worth a look under it. Counters rather than a histogram because the registry
/// requires histograms to carry a real unit, and "intents" is a count.
pub fn leader_intent_batch() {
    metrics_kit::metrics_crate::counter!(LEADER_INTENT_BATCHES_TOTAL).increment(1);
}

label_enum!(#[doc = "`state` label of [`LEADER_INTENTS_TOTAL`]."] pub IntentState {
    /// Folded into a proposal that committed.
    Applied => "applied",
    /// Already true in the committed state; nothing proposed.
    AlreadySatisfied => "already_satisfied",
    /// The premise no longer held, so the submitter must recompute.
    Superseded => "superseded",
    /// The proposal itself failed (Raft error).
    Failed => "failed",
    /// The proposal lost the generation fence. The integrator is the only
    /// writer of replicated state and it writes serially, so within a leader
    /// epoch this cannot happen: a non-zero rate outside an election means
    /// either a deposed leader is still submitting or something else is
    /// writing `EntryPayload::Normal`.
    StaleFence => "stale_fence",
});
