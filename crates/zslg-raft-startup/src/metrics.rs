//! What startup emits: rejoin attempts.

use metrics_kit::{MetricDef, metrics};

metrics!(pub "zslg_", startup; {
    custom  node_rejoin   NODE_REJOINS_TOTAL  "node_rejoins_total"  Counter None
        "Nodes with no local state that joined an existing cluster. Covers \
         initial cluster formation, scale-out, and an operator wiping a node \
         and rejoining it under a new NodeId after data loss — \
         indistinguishable from inside the process. A LIFECYCLE counter, not an \
         alarm: expect n-1 on a healthy n-node startup. Alert on unexpected \
         CHANGES, not on a non-zero value.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &startup!();

/// A node with **no local state** joined an existing cluster. Three causes,
/// indistinguishable from inside the process (all are "empty state, cluster
/// already exists"): initial cluster formation, scale-out, and an operator
/// wiping a node and rejoining it under a new `NodeId` after data loss.
///
/// So this is a **lifecycle counter, not an alarm** — it reads non-zero on
/// every healthy multi-node startup (n-1 on formation of an n-node cluster).
/// Alert on it *changing when you did not deploy anything*, never on it being
/// non-zero.
///
/// The `HardCrashRestart` decision is deliberately not counted here: that path
/// refuses to start, so any metric it emits is only exported if a scrape lands
/// before the process exits. Its `error!` log is the reliable signal.
pub fn node_rejoin() {
    metrics_kit::metrics_crate::counter!(NODE_REJOINS_TOTAL).increment(1);
}
