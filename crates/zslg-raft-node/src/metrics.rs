//! What the raft node emits: fenced writes and client-write timeouts.

use metrics_kit::{MetricDef, metrics};

metrics!(pub "zslg_", raft_node; {
    counter raft_fenced_write  RAFT_FENCED_WRITES_TOTAL "raft_fenced_writes_total"
        "Fenced state writes rejected by the apply-time generation CAS \
         (StaleStateWrite). Expected around leadership churn; sustained rate \
         without churn = bug.";
    counter raft_client_write_timeout RAFT_CLIENT_WRITE_TIMEOUTS_TOTAL
        "raft_client_write_timeouts_total" "Fenced state writes abandoned because \
         openraft's client_write did not resolve within raft_client_write_timeout. \
         openraft has no deadline of its own, so this is the backstop against a \
         partitioned leader stalling the integrator — and with it every leader \
         job. Any non-zero rate warrants investigation.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &raft_node!();
