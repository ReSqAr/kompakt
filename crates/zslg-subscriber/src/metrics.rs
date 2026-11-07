//! What the replication subscriber emits: connections, reconnects, messages, retired records and peer clock skew.

use metrics_kit::{MetricDef, label_enum, lossy_f64, metrics};

const LABEL_REASON: &str = "reason";
const LABEL_PEER_ID: &str = "peer_id";

metrics!(pub "zslg_", subscriber; {
    labeled_counter subscription_ended LOG_SUBSCRIPTION_ENDED_TOTAL
        "log_subscription_ended_total"
        "Subscription attempts that ended, by reason (learner side). \
         \"ended\": the stream stopped — a socket drop, a replica \
         restart, or a shed on either side, which are indistinguishable \
         on the wire; correlate log_subscription_shed_total on this node \
         and on the serving replica to tell them apart. \"peer_gone\": \
         the peer is unroutable or replaced (membership converging). \
         \"failed\": stream or integrate error. A high \"ended\" rate on \
         a healthy network means the buffers, not the peers, are ending \
         connections."
        (reason: SubscriptionEndReason) [LABEL_REASON => reason.as_str()];
    hist_duration subscription_connection_seconds LOG_SUBSCRIPTION_CONNECTION_SECONDS
        "log_subscription_connection_seconds"
        "Lifetime of one subscription attempt (learner side), from \
         connect to stream end. Healthy = long-lived (minutes to hours). \
         A tight cluster of sub-second lifetimes means connections are \
         being ended by buffer pressure and every reconnect re-pays the \
         replica's catch-up scan."
        (elapsed: Duration);
    custom  subscription_records_retired LOG_SUBSCRIPTION_RECORDS_RETIRED
        "log_subscription_records_retired_entries" Histogram Count
        "Durable progress made by one subscription attempt (learner \
         side): the advance of this peer's persisted subscription cursor \
         between connect and stream end. The progress-per-connection \
         measure. A distribution pinned near the combined \
         receive-buffer capacity means each connection dies exactly when \
         its buffers fill, so a reconnect never retires more than a \
         bufferful and the replica's catch-up scan is re-paid every \
         time. Near zero with a non-zero reconnect rate means no forward \
         progress at all.";
    counter subscription_reconnect LOG_SUBSCRIPTION_RECONNECTS_TOTAL
        "log_subscription_reconnects_total"
        "Subscription reconnects (learner side). Sustained rate = flaky \
         link or replica restarts.";
    custom  subscription_messages LOG_SUBSCRIPTION_MESSAGES_TOTAL
        "log_subscription_messages_total" Counter None
        "Records received via log subscription (learner side). \
         rate() compared with log_writes_total{log_type=\"replica\"} on \
         the upstream = per-subscription catch-up lag.";
    custom  hlc_skew  HLC_SKEW_SECONDS  "hlc_skew_seconds" Gauge Seconds
        "Per-peer HLC physical-ms skew (|local − peer| / 1000) observed on the \
         replication subscriber path. Creeps toward the configured HLC skew \
         limit (`hlc_max_clock_skew`) as a peer approaches rejection — a \
         leading indicator of the drift-reject/reconnect churn loop. \
         Labeled by peer_id.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &subscriber!();

pub fn subscription_records_retired(records: u64) {
    metrics_kit::metrics_crate::histogram!(LOG_SUBSCRIPTION_RECORDS_RETIRED)
        .record(lossy_f64(records));
}

pub fn subscription_messages(count: usize) {
    if count == 0 {
        return;
    }
    metrics_kit::metrics_crate::counter!(LOG_SUBSCRIPTION_MESSAGES_TOTAL)
        .increment(u64::try_from(count).expect("subscription message count fits u64"));
}

pub fn hlc_skew(peer_id: zslg_consensus::NodeId, skew_s: f64) {
    metrics_kit::metrics_crate::gauge!(HLC_SKEW_SECONDS, LABEL_PEER_ID => peer_id.to_string())
        .set(skew_s);
}

label_enum!(#[doc = "`reason` label of [`LOG_SUBSCRIPTION_ENDED_TOTAL`] — why one subscription attempt ended. A stream that ended because *we* shed is indistinguishable on the wire from one that ended because the peer went away, so the reason has to be recorded here or it is unrecoverable."] pub SubscriptionEndReason {
    /// The stream simply ended. On a stream that is infinite by contract this
    /// is never "the peer finished"; it is a socket drop, a replica restart, or
    /// a local teardown — all identical on the wire.
    Ended => "ended",
    /// The peer is gone, unroutable, or replaced (membership still converging).
    PeerGone => "peer_gone",
    /// The stream carried an error, or a record failed to integrate.
    Failed => "failed",
});
