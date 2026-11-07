//! What the replica log emits: append and flush latency, queue depth and dropped outcomes.

use metrics_kit::{MetricDef, label_enum, metrics, set_gauge, unix_now_secs};

const LABEL_REASON: &str = "reason";

metrics!(pub "zslg_", replica_log; {
    probe_unlabeled replica_append_postcommit_probe REPLICA_APPEND_POSTCOMMIT_SECONDS
        "replica_append_postcommit_seconds"
        "Post-fsync work per replica-log flush: frontier update, broadcast, \
         oneshot ACKs, guard drop.";
    hist_duration replica_append_queue_wait REPLICA_APPEND_QUEUE_WAIT_SECONDS
        "replica_append_queue_wait_seconds"
        "Time a write waited in the mpsc channel from append() enqueue to \
         flush-loop dequeue (the batching wait). The non-storage part of \
         replica_log_append_duration_seconds."
        (elapsed: Duration);
    probe   replica_log_append_probe REPLICA_LOG_APPEND_DURATION_SECONDS
        "replica_log_append_duration_seconds"
        "ReplicaLogStore::append latency by outcome (enqueue to durable + \
         broadcast). _count doubles as the append throughput.";
    probe replica_log_batch_build REPLICA_LOG_BATCH_BUILD_SECONDS
        "replica_log_batch_build_seconds"
        "Time to drain + encode one batch (BatchBuilder::build). The phase \
         of the flush cycle no metric covered before."
        ;
    hist_duration replica_log_commit_elapsed REPLICA_LOG_COMMIT_SECONDS
        "replica_log_commit_seconds"
        "Segment-WAL append + fsync latency per replica-log flush batch \
         (the engine's own durability fence). High values point at the \
         disk, not at compaction."
        (elapsed: Duration);
    hist    replica_log_flush_bytes REPLICA_LOG_FLUSH_BYTES
        "replica_log_flush_bytes" Bytes
        "Payload bytes per replica-log flush (one engine append batch). \
         _sum is byte throughput, _count the number of flushes."
        (bytes: usize);
    probe replica_log_flush_cycle REPLICA_LOG_FLUSH_CYCLE_SECONDS
        "replica_log_flush_cycle_seconds"
        "Total time of one flush_one_batch_blocking body — batch build + \
         append/fsync + fanout + completions. The flush thread's serial \
         per-batch cost; the write-path serialization suspect."
        ;
    hist    replica_log_flush_entries REPLICA_LOG_FLUSH_ENTRIES
        "replica_log_flush_entries" Count
        "Entries batched into one replica-log flush. Distribution near 1 \
         under load = batching starvation; near the cap = healthy batching."
        (count: usize);
    labeled_counter replica_log_flush_trigger REPLICA_LOG_FLUSH_TRIGGER_TOTAL
        "replica_log_flush_trigger_total"
        "Replica-log flushes by trigger reason (batch_full vs idle). A high \
         idle share under load = the writer is starving the batch."
        (reason: FlushTrigger) [LABEL_REASON => reason.as_str()];
    custom  replica_log_flushed REPLICA_LOG_LAST_FLUSH_TIMESTAMP_SECONDS
        "replica_log_last_flush_timestamp_seconds" Gauge Seconds
        "Unix timestamp of the last replica-log flush. Grafana shows \
         'time since last flush' as time() minus this; a stall = stuck writer.";
    gauge_usize replica_log_queue_depth REPLICA_LOG_QUEUE_DEPTH
        "replica_log_queue_depth" Count
        "Pending writes queued to the replica-log flush loop. Sustained \
         growth = the flush loop cannot keep up (durability backpressure)."
        (depth: usize);
    counter replica_outcome_dropped REPLICA_OUTCOME_DROPPED_TOTAL
        "replica_outcome_dropped_total"
        "Write outcomes the flush loop could not hand back because the \
         connection's outcome channel was full. The write IS durable — \
         the writer resends it on reconnect and the downstream dedups by \
         data_id — so this costs a resend, never correctness. Non-zero \
         means a connection's consumer stalled a full in-flight window \
         deep, or its outcome channel is sized below the client's \
         in-flight cap.";
    gauge   subscription_catchup_backlog LOG_SUBSCRIPTION_CATCHUP_BACKLOG_ENTRIES
        "log_subscription_catchup_backlog_entries" Count
        "Entries a subscriber had to replay at its most recent connect \
         (serving replica side): the replica's frontier minus the \
         subscriber's resume cursor. Rising across successive connects \
         means the deficit is diverging — shedding is no longer just \
         protecting the node, it is preventing catch-up. Flat or falling \
         means shedding is working as intended."
        (entries: u64);
});

/// Every metric this crate declares.
pub const METRICS: &[MetricDef] = &replica_log!();

pub fn replica_log_flushed() {
    set_gauge(REPLICA_LOG_LAST_FLUSH_TIMESTAMP_SECONDS, unix_now_secs());
}

label_enum!(#[doc = "`reason` label of the per-store flush-trigger counters: why a flush ran."] pub FlushTrigger {
    /// The pending batch reached the max batch size.
    BatchFull => "batch_full",
    /// The flush ran because the channel went idle (fewer than a full batch).
    Idle => "idle",
});
