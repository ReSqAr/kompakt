//! Metrics of the write path (writer side).

use std::time::Duration;

use metrics_kit::{MetricDef, label_enum, metrics, set_gauge};

const LABEL_PEER_ID: &str = "peer_id";
const LABEL_OUTCOME: &str = "outcome";

metrics!(pub "zslg_", writer; {
    custom  writes_in_flight_guard  WRITES_IN_FLIGHT  "writes_in_flight"
        Gauge Count "Writes between fan-out start and quorum resolution.";
    custom  writer_inflight_permits_set  WRITER_INFLIGHT_PERMITS
        "writer_inflight_permits" Gauge Count
        "Held in-flight permits in the write coordinator. Pinned at the \
         MAX_INFLIGHT cap = quorum can't keep up (backpressure).";
    counter writer_fan_full  WRITER_FAN_FULL_TOTAL  "writer_fan_full_total"
        "Feeder dropped an id because a replica's fan was full/gone (id \
         resent on that replica's next reconnect prime). ~0 in steady \
         state; sustained = a chronically-behind replica.";
    counter writer_submit_dropped  WRITER_SUBMIT_DROPPED_TOTAL
        "writer_submit_dropped_total"
        "Write dropped because the coordinator's submit channel was full. \
         ~0 in steady state; sustained = the coordinator feeder is blocked \
         and writes are backing up.";
    counter writer_replica_reopen  WRITER_REPLICA_REOPEN_TOTAL
        "writer_replica_reopen_total"
        "Per-replica write connections (re)opened. Steady-state growth = \
         flapping connections.";
    custom  replica_write_rpc  REPLICA_WRITE_RPC_DURATION_SECONDS
        "replica_write_rpc_duration_seconds" Histogram Seconds
        "Per-replica write-RPC latency + outcome (peer_id, outcome = \
         success / rejected / failed). A failed bucket at ~unary_timeout = a \
         black-hole/slow peer; a fast failed = refused/broken conn; rejected = \
         replica-side reject. Attributes a write-rate dip to a specific replica.";
    hist_duration coordinator_submit_wait COORDINATOR_SUBMIT_WAIT_SECONDS
        "coordinator_submit_wait_seconds"
        "Time a write sat in the coordinator's submit channel before the feed \
         loop picked it up (submit() -> feed recv). Grows when the feed loop \
         is backed up (writes not being absorbed)."
        (elapsed: Duration);
    hist_duration coordinator_permit_wait COORDINATOR_PERMIT_WAIT_SECONDS
        "coordinator_permit_wait_seconds"
        "Time the feed loop waited for an in-flight permit (the MAX_INFLIGHT \
         semaphore). THE backpressure indicator: when writes accumulate in \
         pending faster than quorum completes, this grows."
        (elapsed: Duration);
    hist_duration coordinator_fan_wait COORDINATOR_FAN_WAIT_SECONDS
        "coordinator_fan_wait_seconds"
        "Time from a write entering pending to the coordinator picking it up \
         to fan it to a follower (pending enqueue -> fan pickup)."
        (elapsed: Duration);
    hist_duration coordinator_quorum_latency COORDINATOR_QUORUM_LATENCY_SECONDS
        "coordinator_quorum_latency_seconds"
        "Time from a write entering pending to quorum being reached (the \
         writer's oneshot fires). What the client's write() actually waits \
         for. Compare to the per-follower replica_write_rpc: a gap means the \
         quorum waits on a path the per-follower round trip misses."
        (elapsed: Duration);
    probe   write_duration_probe WRITE_DURATION_SECONDS "write_duration_seconds"
        "End-to-end client write latency: fan-out start to quorum \
         resolution. Count per state doubles as the writes counter.";
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &writer!();

/// RAII guard for the `writes_in_flight` gauge: `+1` on construction, `-1` on
/// `Drop`. A guard (not bare inc/dec calls) keeps the gauge correct even when the
/// holding future is cancelled/dropped mid-flight — the previous bare pair leaked
/// `+1` on every write that hit the `write_timeout` (the drop skipped the dec).
#[must_use = "hold the guard for the write's lifetime; dropping it decrements the gauge immediately"]
pub struct WritesInFlightGuard;

impl Drop for WritesInFlightGuard {
    fn drop(&mut self) {
        metrics_kit::metrics_crate::gauge!(WRITES_IN_FLIGHT).decrement(1.0);
    }
}

pub fn writes_in_flight_guard() -> WritesInFlightGuard {
    metrics_kit::metrics_crate::gauge!(WRITES_IN_FLIGHT).increment(1.0);
    WritesInFlightGuard
}

/// Held in-flight permits (`quorum::coordinator`). Pinned at `MAX_INFLIGHT` = the
/// global cap is saturated (quorum can't keep up): the backpressure signal.
/// Set from the coordinator's semaphore rather than tracked incrementally, so
/// the gauge is re-derived every tick instead of accumulating drift.
pub fn writer_inflight_permits_set(held: usize) {
    set_gauge(
        WRITER_INFLIGHT_PERMITS,
        u64::try_from(held).unwrap_or(u64::MAX),
    );
}

/// Latency + outcome of a single write RPC to ONE replica (the enqueue→response
/// exchange in the per-peer write pool), keyed by target peer. Distinguishes a
/// slow-to-ack replica from a fast-failing one, so a write-rate dip during churn
/// can be attributed to a specific replica rather than the writer.
pub fn replica_write_rpc(
    peer_id: zslg_consensus::NodeId,
    outcome: WriteRpcOutcome,
    latency: Duration,
) {
    metrics_kit::metrics_crate::histogram!(
        REPLICA_WRITE_RPC_DURATION_SECONDS,
        LABEL_PEER_ID => peer_id.to_string(),
        LABEL_OUTCOME => outcome.as_str(),
    )
    .record(latency);
}

label_enum!(#[doc = "`outcome` label of [`REPLICA_WRITE_RPC_DURATION_SECONDS`]. A \
`failed` bucket at ~`unary_timeout` latency = a black-hole/timed-out peer; a fast \
`failed` = a refused/broken connection; `rejected` = a replica-side reject (HLC cutoff \
etc.)."] pub WriteRpcOutcome {
    Success => "success",
    Rejected => "rejected",
    Failed => "failed",
});
