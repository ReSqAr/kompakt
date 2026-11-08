//! The RPC server's metrics: requests, handler latency, open connections.

use metrics_kit::{LABEL_STATE, MetricDef, OpState, metrics};
use std::time::Duration;

const LABEL_METHOD: &str = "method";

metrics!(pub "zslg_", rpc_server; {
    custom  rpc_server_request  RPC_SERVER_REQUESTS_TOTAL
        "rpc_server_requests_total" Counter None
        "RPC requests handled, by method and outcome.";
    custom  rpc_server_request_duration RPC_SERVER_REQUEST_DURATION_SECONDS
        "rpc_server_request_duration_seconds" Histogram Seconds
        "Server-side RPC handler latency by method.";
    gauge   rpc_connections_active RPC_CONNECTIONS_ACTIVE
        "rpc_connections_active" Count
        "Open TCP connections on the RPC server (unary keep-alive \
         connections + persistent write/subscribe connections)."
        (count: u64);
    counter subscription_reorder LOG_SUBSCRIPTION_REORDERS_TOTAL
        "log_subscription_reorders_total"
        "Subscription streams closed because a replica's delivered index \
         went BACKWARDS (a reorder or duplicate). The replica log is \
         gapless and single-writer and is read in index order, so this \
         should always be zero: any non-zero value means local \
         replica-log corruption on the serving replica, which warrants \
         the wipe + new NodeId procedure.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &rpc_server!();

pub fn rpc_server_request(method: &'static str, state: OpState) {
    metrics_kit::metrics_crate::counter!(
        RPC_SERVER_REQUESTS_TOTAL,
        LABEL_METHOD => method,
        LABEL_STATE => state.as_str(),
    )
    .increment(1);
}

pub fn rpc_server_request_duration(method: &'static str, elapsed: Duration) {
    metrics_kit::metrics_crate::histogram!(
        RPC_SERVER_REQUEST_DURATION_SECONDS,
        LABEL_METHOD => method,
    )
    .record(elapsed);
}

// ── RPC server dual-metric guard ──────────────────────────────────────

/// RAII guard for RPC server handler metrics. Records one histogram
/// (`rpc_server_request_duration_seconds`) and one counter
/// (`rpc_server_requests_total`) on drop — the two-metric pattern that
/// `Probe` is not designed for.
#[must_use = "RpcServerGuard records metrics on drop"]
pub struct RpcServerGuard {
    t0: tokio::time::Instant,
    method: &'static str,
    armed: bool,
}

impl Default for RpcServerGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcServerGuard {
    pub fn new() -> Self {
        Self {
            t0: tokio::time::Instant::now(),
            method: "handshake",
            armed: true,
        }
    }

    /// Record the dispatched request kind so a cancellation mid-dispatch is
    /// attributed to the right method.
    pub fn set_method(&mut self, method: &'static str) {
        self.method = method;
    }

    /// Suppress emission entirely (e.g. clean shutdown, not a request failure).
    pub fn disarm(&mut self) {
        self.armed = false;
    }

    /// Record both metrics with the given outcome and suppress drop emission.
    pub fn finish(mut self, ok: bool) {
        self.armed = false;
        let elapsed = self.t0.elapsed();
        rpc_server_request_duration(self.method, elapsed);
        rpc_server_request(self.method, OpState::from_ok(ok));
    }
}

impl Drop for RpcServerGuard {
    fn drop(&mut self) {
        if self.armed {
            let elapsed = self.t0.elapsed();
            rpc_server_request_duration(self.method, elapsed);
            rpc_server_request(self.method, OpState::Failure);
        }
    }
}
