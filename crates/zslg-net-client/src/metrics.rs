//! The RPC client's metrics: connection pool, round trips, and the write and subscribe streams.

use metrics_kit::{LABEL_STATE, MetricDef, metrics};

metrics!(pub "zslg_", rpc_client; {
    custom  rpc_conn_reuse RPC_CONN_REUSE_TOTAL "rpc_conn_reuse_total" Counter None
        "Unary connection-pool checkouts by outcome. Label `state`: `hit` \
         reused an idle connection, `miss` required a fresh dial. A low hit \
         rate under load = connections churning (peer closing them, or the \
         idle cap too small).";
    counter subscription_consumer_stalled LOG_SUBSCRIPTION_CONSUMER_STALLED_TOTAL
        "log_subscription_consumer_stalled_total"
        "Subscriptions ended because the consumer accepted nothing for \
         the whole stall timeout (30s). NOT backpressure: a slow \
         consumer is waited on and is not counted anywhere. This fires \
         only when an integrate loop has stopped making room entirely, \
         which is a bug — an fsync-fenced batch frees a slot in \
         milliseconds. No data is lost (the subscriber resumes from its \
         durable cursor); the thread and socket are released so a \
         wedged consumer cannot pin them forever. MUST be zero.";
    probe   rpc_roundtrip_probe RPC_ROUNDTRIP_SECONDS "rpc_roundtrip_seconds"
        "Whole client-side unary RPC: open to first response frame.";
    probe   rpc_open_stream_probe RPC_OPEN_STREAM_SECONDS "rpc_open_stream_seconds"
        "Client open: dedicated connection dial + request send for a \
         server-streaming RPC (subscribe); no response wait.";
    probe_unlabeled rpc_conn_checkout_probe RPC_CONN_CHECKOUT_SECONDS
        "rpc_conn_checkout_seconds"
        "Fresh-dial cost paid on a unary keep-alive pool miss (DNS + TCP \
         connect + socket setup). A pool hit reuses an idle connection and \
         is not recorded here; see rpc_conn_reuse_total for the hit rate.";
    hist_duration write_stream_input_wait WRITE_STREAM_INPUT_WAIT_SECONDS
        "write_stream_input_wait_seconds"
        "Time a write sat in the write stream's input channel (coordinator \
         fan pickup -> the blocking writer loop dequeues it). Directly \
         timestamped on the queued object; ~0 when the writer loop keeps up."
        (elapsed: Duration);
    hist_duration write_stream_send WRITE_STREAM_SEND_SECONDS
        "write_stream_send_seconds"
        "Time to feed + flush one batch of frames on the write-stream socket \
         (leader -> follower). Grows when the follower's server isn't reading \
         (TCP backpressure on the leader's send)."
        (elapsed: Duration);
    counter writer_outcome_dropped  WRITER_OUTCOME_DROPPED_TOTAL
        "writer_outcome_dropped_total"
        "Outcome dropped because the per-connection outcome channel was \
         full. ~0 in steady state; sustained = consumer (coordinator \
         per-replica loop) is falling behind and writes will time out.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &rpc_client!();

/// Count a unary connection-pool checkout as a reuse hit (`true`) or a miss
/// that required a fresh dial (`false`).
pub fn rpc_conn_reuse(hit: bool) {
    let state = if hit { "hit" } else { "miss" };
    metrics_kit::metrics_crate::counter!(RPC_CONN_REUSE_TOTAL, LABEL_STATE => state).increment(1);
}
