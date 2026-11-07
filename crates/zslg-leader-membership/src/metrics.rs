//! Metrics of the leader membership, liveness and voter jobs.

use metrics_kit::{LABEL_STATE, MetricDef, OpState, lossy_f64, metrics, unix_now_secs};

const LABEL_NODE_ID: &str = "node_id";

metrics!(pub "zslg_", membership; {
    labeled_counter membership_change MEMBERSHIP_CHANGES_TOTAL "membership_changes_total"
        "Completed membership-change attempts by outcome."
        (state: OpState) [LABEL_STATE => state.as_str()];
    counter membership_retarget MEMBERSHIP_RETARGETS_TOTAL "membership_retargets_total"
        "Joint membership changes that recomputed their incoming set because a \
         member of it stopped being live. Each retarget disarms the drain bound \
         and discards the step-2 generation quorum, so a sustained rate means \
         the change is restarting, not progressing.";
    custom  node_last_seen NODE_LAST_SEEN_TIMESTAMP_SECONDS "node_last_seen_timestamp_seconds"
        Gauge Seconds "Unix-epoch seconds of the last successful liveness probe \
        reply from each peer (label node_id), per observing node. Cluster \
        liveness is `time() - node_last_seen_timestamp_seconds`.";
    custom  node_live     NODE_LIVE  "node_live"  Gauge None
        "1 while a peer (label node_id) is in the observing node's live set, 0 \
         once probe failures drop it or it is evicted.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &membership!();

/// Unix-epoch seconds when this node last received a successful liveness probe
/// reply from `node_id`. Emitted from the liveness worker; Grafana derives
/// "time since last seen" as `time() - node_last_seen_timestamp_seconds`.
pub fn node_last_seen(node_id: zslg_consensus::NodeId) {
    metrics_kit::metrics_crate::gauge!(NODE_LAST_SEEN_TIMESTAMP_SECONDS, LABEL_NODE_ID => node_id.to_string())
        .set(lossy_f64(unix_now_secs()));
}

/// 1 while this node considers `node_id` live (probes succeeding), 0 once it has
/// been dropped from the live set (probe failures past threshold, or evicted).
pub fn node_live(node_id: zslg_consensus::NodeId, live: bool) {
    metrics_kit::metrics_crate::gauge!(NODE_LIVE, LABEL_NODE_ID => node_id.to_string())
        .set(if live { 1.0 } else { 0.0 });
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;

    #[test]
    fn node_liveness_gauges_carry_node_id_label() {
        let node = zslg_consensus::NodeId::new_v4();
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        ::metrics::with_local_recorder(&recorder, || {
            node_last_seen(node);
            node_live(node, true);
        });
        let map = snapshotter.snapshot().into_hashmap();
        let labeled = |ck: &metrics_util::CompositeKey, name: &str| {
            ck.key().name() == name
                && ck
                    .key()
                    .labels()
                    .any(|l| l.key() == LABEL_NODE_ID && l.value() == node.to_string())
        };
        assert!(
            map.iter().any(
                |(ck, (_, _, v))| labeled(ck, NODE_LAST_SEEN_TIMESTAMP_SECONDS)
                    && matches!(
                        v,
                        metrics_util::debugging::DebugValue::Gauge(g) if g.0 > 1_700_000_000.0
                    )
            ),
            "node_last_seen should emit an epoch stamp labeled by node_id"
        );
        assert!(
            map.iter().any(|(ck, (_, _, v))| labeled(ck, NODE_LIVE)
                && matches!(v, metrics_util::debugging::DebugValue::Gauge(g) if g.0 > 0.5)),
            "node_live should be 1 for a healthy peer"
        );
    }

    #[test]
    fn node_live_reports_zero_when_dropped() {
        let node = zslg_consensus::NodeId::new_v4();
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        ::metrics::with_local_recorder(&recorder, || {
            node_live(node, false);
        });
        let map = snapshotter.snapshot().into_hashmap();
        assert!(
            map.iter().any(|(ck, (_, _, v))| {
                ck.key().name() == NODE_LIVE
                    && matches!(v, metrics_util::debugging::DebugValue::Gauge(g) if g.0 < 0.5)
            }),
            "node_live should be 0 once the peer is dropped from the live set"
        );
    }
}
