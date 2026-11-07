//! Canonical metric registry (design doc §"Observability").
//!
//! Every metric the crate emits is declared here as a [`MetricDef`] in
//! [`ALL_METRICS`] — name, kind, unit, and help text. Emission sites refer to
//! the name constants and the label-value enums below; nothing constructs a
//! metric name or label value ad hoc. [`describe_all`] registers the help
//! text and units with the installed recorder (call it once after
//! `observability::init_prometheus`).
//!
//! Conventions:
//! - every metric this crate owns is exported under the `zslg_` namespace,
//!   applied by the registry macro so a row never writes it. The one exemption
//!   is the Prometheus-standard `process_*` collector namespace
//!   (`metrics!(process, unprefixed; ..)`), which every client library exports
//!   under exactly these names — prefixing them would hide this process from
//!   tooling that expects the standard. `tokio_*` comes from `tokio-metrics`
//!   and is not ours to name at all.
//! - counters end in `_total`; histograms end in a unit/quantity suffix
//!   (`_seconds`, `_bytes`, or `_entries` for count distributions); gauges carry
//!   a unit suffix where one applies (`_bytes`, `_ms`, `_seconds`).
//! - durations are recorded as `tokio::time::Duration` /
//!   `std::time::Duration` via `metrics`' native `IntoF64` impl — no float
//!   arithmetic at call sites.
//! - HLC-valued gauges export the **physical-millisecond part**
//!   (`Hlc::parts().physical_ms`, ≤ 2^48), never `Hlc::raw()`: the raw
//!   `u64` exceeds f64's 53-bit integer range and would silently lose
//!   precision in the gauge.
//! - label keys are the `LABEL_*` constants; label values come from the
//!   enums below via `as_str()`, so every `{label="value"}` combination that
//!   can occur is enumerable at compile time.

// `expect` calls here are `u64::try_from(usize)` on bounded values (counts,
// lengths) that cannot exceed u64::MAX.
#![allow(
    clippy::expect_used,
    reason = "u64::try_from(usize) on bounded metric values (counts, lengths)"
)]

use metrics_kit::{
    LABEL_STATE, MetricDef, Unit, label_enum, lossy_f64, metrics, set_gauge, unix_now_secs,
};
use std::time::Duration;

pub(crate) use metrics_kit::OpState;
#[cfg(test)]
use metrics_kit::{LABEL_NONE, MetricKind, set_flag};

// ---------------------------------------------------------------------------
// Registry types
// ---------------------------------------------------------------------------

// Metrics using manual Instant are now either Pattern 5 (snapshot multi-lap
// timing, `leader/snapshot/create.rs`) or Pattern 6 (stored-Instant fields for
// queue-wait measurement). Everything else uses Probe or RpcServerGuard.

// ---------------------------------------------------------------------------
// Registry macros
// ---------------------------------------------------------------------------

/// The namespace every metric this crate owns is exported under.
///
/// Test-only: the live value is the literal in [`metrics`], and this exists so
/// `tests::every_metric_is_namespaced` asserts against a name rather than
/// re-deriving one. The two are kept in step by that test.
#[cfg(test)]
pub(crate) const METRIC_PREFIX: &str = "zslg_";

// ---------------------------------------------------------------------------
// Label keys
// ---------------------------------------------------------------------------

const LABEL_STEP: &str = "step";
/// Peer identity (`NodeId::to_string()`). High-cardinality but bounded by cluster
/// size; the enum-valued-label rule targets bounded vocabularies, not node ids.
const LABEL_NODE_ID: &str = "node_id";
/// Which RocksDB column family a per-CF storage gauge describes. Value comes
/// from `Subsystem::cf_name()` (CLAUDE.md §7: label values from an enum,
/// never ad-hoc strings) — a node has one DB, so `cf` is the axis that
/// distinguishes one memtable/SST/compaction reading from another.
const LABEL_CF: &str = "cf";

/// Every label key this crate emits. A new `LABEL_*` constant belongs here, so
/// that `no_label_key_collides_with_prometheus_scrape_labels` keeps checking
/// the whole set rather than whichever subset was current when it was written.
#[cfg(test)]
const ALL_LABEL_KEYS: &[&str] = &[LABEL_STATE, LABEL_STEP, LABEL_NODE_ID, LABEL_CF, LABEL_NONE];

// ---------------------------------------------------------------------------
// Metrics: each name constant is immediately followed by its emission
// helper(s). Helpers are `pub(crate)` on purpose — an unwired metric is then
// an unused function, which `clippy -D warnings` rejects (dead_code).
// ---------------------------------------------------------------------------

// ── raft / consensus ────────────────────────────────────────────────────────

metrics!("zslg_", raft; {
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

// ── raft / consensus: hand-written emission helpers ────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

/// Unix-epoch seconds when this node last received a successful liveness probe
/// reply from `node_id`. Emitted from the liveness worker; Grafana derives
/// "time since last seen" as `time() - node_last_seen_timestamp_seconds`.
pub(crate) fn node_last_seen(node_id: zslg_node_types::NodeId) {
    metrics::gauge!(NODE_LAST_SEEN_TIMESTAMP_SECONDS, LABEL_NODE_ID => node_id.to_string())
        .set(lossy_f64(unix_now_secs()));
}

/// 1 while this node considers `node_id` live (probes succeeding), 0 once it has
/// been dropped from the live set (probe failures past threshold, or evicted).
pub(crate) fn node_live(node_id: zslg_node_types::NodeId, live: bool) {
    metrics::gauge!(NODE_LIVE, LABEL_NODE_ID => node_id.to_string()).set(if live {
        1.0
    } else {
        0.0
    });
}

// ── write path (replica side) + HLC cutoff ──────────────────────────────────

// ── write path (replica side) + HLC cutoff: hand-written emission helpers ───
// (custom rows above; each fn below emits the metrics named in its row)

// ── logs and replication ────────────────────────────────────────────────────

metrics!("zslg_", logs; {
});

// ── logs and replication: hand-written emission helpers ─────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

// ── subscription load shedding ──────────────────────────────────────────────
//
// The replication path sheds deliberately: a subscriber whose buffer is full is
// dropped rather than allowed to stall the replica's shared flush thread (or to
// buffer without bound). That is the intended availability trade — one slow
// consumer must not take the node down. These metrics make the trade *visible*,
// so a shed that is protecting the node can be told apart from a shed loop that
// is preventing a subscriber from ever catching up. Nothing here changes the
// shedding behaviour.

metrics!("zslg_", subscription; {
});

// ── subscription load shedding: hand-written emission helpers ───────────────
// (custom rows above; each fn below emits the metrics named in its row)

// ── snapshots / leader background tasks ─────────────────────────────────────

metrics!("zslg_", snapshots; {
    labeled_counter snapshot_creation SNAPSHOT_CREATION_TOTAL "snapshot_creation_total"
        "Snapshot creation attempts by outcome (see SnapshotState). \
         waiting_for_stable_log is backpressure, not failure: the leader's \
         local stable log has not re-ingested its own last snapshot yet."
        (state: SnapshotState) [LABEL_STATE => state.as_str()];
    custom  snapshot_creation_step SNAPSHOT_CREATION_DURATION_SECONDS
        "snapshot_creation_duration_seconds" Histogram Seconds
        "Snapshot creation latency per step (set_cutoff, await_quorum, \
         write_object, commit) plus total.";
    gauge_usize snapshot_entries SNAPSHOT_ENTRIES_COUNT "snapshot_entries_count" Count
        "Entries in the last successful snapshot." (n: usize);
    labeled_counter compaction_run COMPACTION_RUNS_TOTAL "compaction_runs_total"
        "Manifest compaction runs by outcome."
        (state: OpState) [LABEL_STATE => state.as_str()];
});

// ── snapshots / leader background tasks: hand-written emission helpers ──────
// (custom rows above; each fn below emits the metrics named in its row)

pub(crate) fn snapshot_creation_step(step: SnapshotStep, elapsed: Duration) {
    metrics::histogram!(
        SNAPSHOT_CREATION_DURATION_SECONDS,
        LABEL_STEP => step.as_str(),
    )
    .record(elapsed);
}

// ── HLC / clock ──────────────────────────────────────────────────────────────

metrics!("zslg_", hlc; {
});

// ── HLC / clock: hand-written emission helpers ──────────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

// ── read path / learner stores ───────────────────────────────────────────────

metrics!("zslg_", read_path; {
});

// ── read path / learner stores: hand-written emission helpers ───────────────
// (custom rows above; each fn below emits the metrics named in its row)

// ── RPC server ───────────────────────────────────────────────────────────────

metrics!("zslg_", rpc_server; {
});

// ── RPC server: hand-written emission helpers ────────────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

// ── RPC client (writer/learner outbound; async-path investigation) ───────────
// Additive decomposition of one client-side unary RPC:
//   roundtrip = conn_checkout(on miss: dial) + send + response_wait
// `conn_checkout` is the fresh-dial cost paid on a keep-alive pool miss (a hit
// reuses an idle connection and pays ~0); `rpc_conn_reuse_total{state}` counts
// hits vs misses; `open_stream` is a dedicated stream/subscribe dial; `roundtrip`
// is the whole `send_unary` exchange.

// ── RPC client: hand-written emission helpers ────────────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

// ── storage write paths: the replicated logs (stable log, replica log) ─────────
// Wrapper-level metrics keep their stable_log_*/replica_log_* names;
// throughput + entry count ride on the existing `log_writes{log_type}` /
// `log_index_frontier{log_type}`. Engine-level behavior is on the storage_* series.

metrics!("zslg_", storage; {
    // RocksDB engine-state gauges. Per-CF: a node has one DB, so `cf`
    // (Subsystem::cf_name()) is the only axis that distinguishes one
    // memtable/SST/compaction reading from another.
    custom  storage_num_immutable_memtable STORAGE_NUM_IMMUTABLE_MEMTABLE
        "storage_num_immutable_memtable" Gauge Count
        "rocksdb.num-immutable-mem-table: memtables that are full and \
         waiting to be flushed, per column family (cf). A sustained climb \
         means flush cannot keep up with the write rate.";
    custom  storage_memtable_flush_pending STORAGE_MEMTABLE_FLUSH_PENDING
        "storage_memtable_flush_pending" Gauge Count
        "rocksdb.mem-table-flush-pending: 1 if a memtable flush is queued \
         for this column family (cf), 0 otherwise.";
    custom  storage_compaction_pending STORAGE_COMPACTION_PENDING
        "storage_compaction_pending" Gauge Count
        "rocksdb.compaction-pending: 1 if compaction is queued for this \
         column family (cf), 0 otherwise. Sustained 1 alongside a growing \
         storage_live_sst_files_size_bytes means compaction is falling \
         behind.";
    custom  storage_estimate_num_keys STORAGE_ESTIMATE_NUM_KEYS
        "storage_estimate_num_keys" Gauge Count
        "rocksdb.estimate-num-keys: estimated live key count per column \
         family (cf). An estimate (tombstones and duplicates are not \
         resolved), not an exact count.";
    custom  storage_live_sst_files_size_bytes STORAGE_LIVE_SST_FILES_SIZE_BYTES
        "storage_live_sst_files_size_bytes" Gauge Bytes
        "rocksdb.live-sst-files-size: on-disk size of the live SST files in \
         this column family (cf). The disk-space counterpart to \
         storage_memory_charged_bytes, which covers only the in-memory \
         budget.";
    custom  storage_block_cache_usage_bytes STORAGE_BLOCK_CACHE_USAGE_BYTES
        "storage_block_cache_usage_bytes" Gauge Bytes
        "rocksdb.block-cache-usage: bytes currently held by the block \
         cache, DB-wide (one cache per node, not per column family). A \
         breakdown of storage_memory_charged_bytes, which also folds in \
         memtables charged to the same cache.";
    custom  storage_actual_delayed_write_rate STORAGE_ACTUAL_DELAYED_WRITE_RATE
        "storage_actual_delayed_write_rate" Gauge Count
        "rocksdb.actual-delayed-write-rate: the write-rate cap RocksDB is \
         currently enforcing on this node's DB (bytes/sec), 0 when no \
         throttle is active. Non-zero means the ENGINE is throttling \
         writers — backpressure from the memtable/L0 limits, not the host. \
         Cross-check against scrape duration to tell this apart from host \
         starvation: the 2026-08-17 harness run could not, because nothing \
         exported this.";
});

// ── storage write paths: hand-written emission helpers ───────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

/// `rocksdb.num-immutable-mem-table` for one column family.
pub(crate) fn storage_num_immutable_memtable(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_NUM_IMMUTABLE_MEMTABLE, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.mem-table-flush-pending` for one column family.
pub(crate) fn storage_memtable_flush_pending(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_MEMTABLE_FLUSH_PENDING, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.compaction-pending` for one column family.
pub(crate) fn storage_compaction_pending(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_COMPACTION_PENDING, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.estimate-num-keys` for one column family.
pub(crate) fn storage_estimate_num_keys(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_ESTIMATE_NUM_KEYS, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.live-sst-files-size` for one column family.
pub(crate) fn storage_live_sst_files_size_bytes(cf: &'static str, value: u64) {
    metrics::gauge!(STORAGE_LIVE_SST_FILES_SIZE_BYTES, LABEL_CF => cf).set(lossy_f64(value));
}

/// `rocksdb.block-cache-usage`, DB-wide (one cache per node).
pub(crate) fn storage_block_cache_usage_bytes(value: u64) {
    set_gauge(STORAGE_BLOCK_CACHE_USAGE_BYTES, value);
}

/// `rocksdb.actual-delayed-write-rate`, DB-wide. Non-zero means the engine
/// itself is throttling writers.
pub(crate) fn storage_actual_delayed_write_rate(value: u64) {
    set_gauge(STORAGE_ACTUAL_DELAYED_WRITE_RATE, value);
}

// The replica-log / local-store probe factories live in the `probes!` group
// (generated from its rows), along with the consts they emit.

// ---------------------------------------------------------------------------
// Label-value vocabularies
// ---------------------------------------------------------------------------

label_enum!(#[doc = "`state` label of [`SNAPSHOT_CREATION_TOTAL`]."] SnapshotState {
    Success => "success",
    /// The (watermark, cutoff] window was empty — nothing to snapshot.
    NoopEmptyWindow => "noop_empty_window",
    /// The replica quorum did not confirm the cutoff in time.
    FailureQuorumTimeout => "failure_quorum_timeout",
    /// The final fenced commit lost the generation CAS (leadership churn).
    FailureStaleGeneration => "failure_stale_generation",
    FailureObjectStore => "failure_object_store",
    FailureStorage => "failure_storage",
    /// The leader's local stable log has not caught up with the manifest yet,
    /// so index assignment cannot proceed. Backpressure, not failure — it
    /// clears once the leader re-ingests its own last snapshot.
    WaitingForStableLog => "waiting_for_stable_log",
});

label_enum!(#[doc = "`step` label of [`SNAPSHOT_CREATION_DURATION_SECONDS`]."] SnapshotStep {
    SetCutoff => "set_cutoff",
    AwaitQuorum => "await_quorum",
    WriteObject => "write_object",
    Commit => "commit",
    Total => "total",
});

// ---------------------------------------------------------------------------
// RAII timing probe
// ---------------------------------------------------------------------------

// ── Process-level resource gauges ──────────────────────────────────────

// ── Memory budget gauges: zslg's own, so namespaced — but read beside the
// unprefixed process_* gauges below, which is why they sit here.

metrics!("zslg_", storage_budget; {
    gauge   storage_memory_charged_bytes STORAGE_MEMORY_CHARGED_BYTES "storage_memory_charged_bytes" Bytes
        "Bytes held by the storage engine's block cache — block, index and \
         filter blocks plus the memtables charged to it. This is what the \
         configured budget is judged against; read it beside \
         process_memory_rss_bytes, whose excess over this is everything \
         outside the budget (table readers, compaction buffers, iterator-\
         pinned blocks, allocator fragmentation)."
        (bytes: u64);
    gauge   storage_queue_capacity_bytes STORAGE_QUEUE_CAPACITY_BYTES "storage_queue_capacity_bytes" Bytes
        "Bytes this node's write-pipeline queues can hold at full depth: the \
         replica write queue and one subscriber's live-tail ring at a 2 KiB \
         reference record, plus the learner integrate channel and the \
         residency of its DataId dedup ring. Deliberately NOT part of \
         storage_memory_charged_bytes — that budget bounds RocksDB alone, and \
         the two are separate sizing decisions (the engine stalls on its \
         write-buffer manager, the pipeline blocks at submit). A planning \
         figure, not a measurement: the queues are sized in slots, so bytes \
         are depth times an assumed record size, and the per-subscriber ring \
         term multiplies with the live subscriber count."
        (bytes: u64);
});

metrics!("", process; {
    custom  process_cpu_seconds PROCESS_CPU_SECONDS "process_cpu_seconds" Gauge Seconds
        "Cumulative user+system CPU seconds consumed by this process \
         (read from /proc/self/stat). Delta / wall-clock delta = CPU \
         utilisation ratio (1.0 = one full core).";
    gauge   process_memory_rss_bytes PROCESS_MEMORY_RSS_BYTES "process_memory_rss_bytes" Bytes
        "Resident set size (VmRSS, /proc/self/status) of this process — the \
         RAM working set. Climbs under write-stall backpressure and precedes \
         an OOM kill (137) if it reaches the container/cgroup limit."
        (bytes: u64);
    custom  process_memory_split_bytes PROCESS_MEMORY_ANON_BYTES "process_memory_anon_bytes"
        Gauge Bytes
        "Anonymous resident memory (/proc/self/smaps_rollup Anonymous): heap \
         + thread stacks — the pages the allocator and code own. The series \
         to chase in a leak hunt; file-backed RSS is reclaimable, this isn't.";
    custom  process_memory_split_bytes PROCESS_MEMORY_FILE_BYTES "process_memory_file_bytes"
        Gauge Bytes
        "Non-anonymous resident memory (RSS minus Anonymous, \
         /proc/self/smaps_rollup): file-backed mappings + shmem — the \
         binary, mapped page cache. Kernel-reclaimable under pressure.";
    gauge   process_threads_count PROCESS_THREADS_COUNT "process_threads_count" Count
        "OS threads in this process (Threads:, /proc/self/status). A climb \
         means a growing tokio blocking pool or leaked spawn_blocking work; \
         each thread's touched stack pages are anonymous RSS."
        (threads: u64);
    gauge   process_open_fds PROCESS_OPEN_FDS "process_open_fds" Count
        "Open file descriptors (/proc/self/fd). Sockets, listeners and \
         every RocksDB SST and WAL file. Compare against process_max_fds: at \
         the limit, dials AND accepts fail for a local reason, which \
         looks exactly like an unreachable peer but is not one."
        (fds: u64);
    gauge   process_max_fds PROCESS_MAX_FDS "process_max_fds" Count
        "RLIMIT_NOFILE soft limit (/proc/self/limits). Exported so \
         process_open_fds / process_max_fds is a query, not a hardcoded \
         alert threshold."
        (fds: u64);
});

// ── Process-level resource gauges: hand-written emission helpers ───────────
// (custom rows above; each fn below emits the metrics named in its row)

/// Cumulative user+system CPU seconds consumed by this process, read from
/// `/proc/self/stat` on a periodic tick. Delta between two readings divided
/// by wall-clock delta = CPU utilisation ratio (1.0 = one full core).
pub(crate) fn process_cpu_seconds(seconds: f64) {
    metrics::gauge!(PROCESS_CPU_SECONDS).set(seconds);
}

/// RSS split from `/proc/self/smaps_rollup`: anonymous (heap + thread stacks —
/// what the allocator and the code own) vs everything else (file-backed
/// mappings + shmem: the binary, mapped page cache). `anon` is the number to
/// chase in a leak hunt; `file` is reclaimable by the kernel under pressure.
pub(crate) fn process_memory_split_bytes(anon: u64, file: u64) {
    set_gauge(PROCESS_MEMORY_ANON_BYTES, anon);
    set_gauge(PROCESS_MEMORY_FILE_BYTES, file);
}

// ── jemalloc allocator gauges (only emitted with the `jemalloc` feature) ──

metrics!("zslg_", jemalloc; {
    custom  jemalloc_stats JEMALLOC_ALLOCATED_BYTES "jemalloc_allocated_bytes"
        Gauge Bytes
        "jemalloc stats.allocated: bytes the application has live-allocated \
         (the true heap working set). Only emitted when the binary is built \
         with the `jemalloc` feature (the harness default).";
    custom  jemalloc_stats JEMALLOC_ACTIVE_BYTES "jemalloc_active_bytes"
        Gauge Bytes
        "jemalloc stats.active: bytes in whole pages backing allocations. \
         active - allocated = internal fragmentation. Only emitted with the \
         `jemalloc` feature.";
    custom  jemalloc_stats JEMALLOC_METADATA_BYTES "jemalloc_metadata_bytes"
        Gauge Bytes
        "jemalloc stats.metadata: allocator bookkeeping overhead (page maps, \
         arena structures). Only emitted with the `jemalloc` feature.";
    custom  jemalloc_stats JEMALLOC_RESIDENT_BYTES "jemalloc_resident_bytes"
        Gauge Bytes
        "jemalloc stats.resident: physical memory jemalloc holds (its share \
         of RSS). resident - active = dirty pages awaiting decay; RSS - \
         resident = non-heap memory (thread stacks, binary, mmaps). Only \
         emitted with the `jemalloc` feature.";
    custom  jemalloc_stats JEMALLOC_MAPPED_BYTES "jemalloc_mapped_bytes"
        Gauge Bytes
        "jemalloc stats.mapped: total virtual address space jemalloc has \
         mapped (not physical). Only emitted with the `jemalloc` feature.";
    custom  jemalloc_stats JEMALLOC_RETAINED_BYTES "jemalloc_retained_bytes"
        Gauge Bytes
        "jemalloc stats.retained: virtual address space retained after unmap \
         (not physical memory; cheap to hold). Only emitted with the \
         `jemalloc` feature.";
});

/// One reading of jemalloc's global stats (`stats.*` via mallctl), taken after
/// an `epoch` advance. All in bytes. The ladder for a leak hunt:
/// `allocated` (live application heap) ≤ `active` (whole pages backing it;
/// gap = internal fragmentation) ≤ `resident` (physical memory jemalloc holds;
/// gap = dirty pages awaiting decay) — and `process_memory_rss_bytes` minus
/// `resident` ≈ non-heap RSS (thread stacks, binary, mmapped engine files…).
#[cfg(feature = "jemalloc")]
pub(crate) struct JemallocStats {
    pub(crate) allocated: u64,
    pub(crate) active: u64,
    pub(crate) metadata: u64,
    pub(crate) resident: u64,
    pub(crate) mapped: u64,
    pub(crate) retained: u64,
}

#[cfg(feature = "jemalloc")]
pub(crate) fn jemalloc_stats(s: &JemallocStats) {
    set_gauge(JEMALLOC_ALLOCATED_BYTES, s.allocated);
    set_gauge(JEMALLOC_ACTIVE_BYTES, s.active);
    set_gauge(JEMALLOC_METADATA_BYTES, s.metadata);
    set_gauge(JEMALLOC_RESIDENT_BYTES, s.resident);
    set_gauge(JEMALLOC_MAPPED_BYTES, s.mapped);
    set_gauge(JEMALLOC_RETAINED_BYTES, s.retained);
}

// ── Raft storage probes (inside spawn_blocking — Probe uses Instant) ─
// Consts + factories generated from the rows; the two non-family factories
// (`object_store_request_probe`, `rpc_conn_reuse`) are hand-written below.

// ── probes: hand-written emission helpers ──────────────────────────────────
// (custom rows above; each fn below emits the metrics named in its row)

// ---------------------------------------------------------------------------
// The canonical registry
// ---------------------------------------------------------------------------

const ALL_METRICS: &[&[MetricDef]] = &[
    // --- leader job heartbeat (zslg-consensus-types) ---
    zslg_consensus_types::metrics::METRICS,
    // --- leader integrator (zslg-leader-integrator) ---
    zslg_leader_integrator::metrics::METRICS,
    // --- writer (zslg-writer) ---
    zslg_writer::metrics::METRICS,
    // --- replica log and service (zslg-replica-log, zslg-replica-service) ---
    zslg_replica_log::metrics::METRICS,
    zslg_replica_service::metrics::METRICS,
    // --- reader (zslg-reader) ---
    zslg_reader::metrics::METRICS,
    // --- replication subscriber (zslg-subscriber) ---
    zslg_subscriber::metrics::METRICS,
    // --- stable log (zslg-stable-log) ---
    zslg_stable_log::metrics::METRICS,
    // --- local store (zslg-local-store) ---
    zslg_local_store::metrics::METRICS,
    // --- startup (zslg-raft-startup) ---
    zslg_raft_startup::metrics::METRICS,
    // --- raft node (zslg-raft-node) ---
    zslg_raft_node::metrics::METRICS,
    // --- raft state (zslg-raft-state) ---
    zslg_raft_state::metrics::METRICS,
    // --- raft storage (zslg-raft-storage) ---
    zslg_raft_storage::metrics::METRICS,
    // --- shared log metrics (zslg-log) ---
    zslg_log::metrics::METRICS,
    // --- HLC clock and gate (zslg-hlc-clock, zslg-hlc-gate) ---
    zslg_hlc_clock::metrics::METRICS,
    zslg_hlc_gate::metrics::METRICS,
    // --- object store (zslg-object-store) ---
    zslg_object_store::metrics::METRICS,
    // --- quorum fan-out (zslg-quorum-read) ---
    zslg_quorum_read::metrics::METRICS,
    // --- RPC client and server (zslg-net-client, zslg-net-server) ---
    zslg_net_client::metrics::METRICS,
    zslg_net_server::metrics::METRICS,
    // --- RPC wire (zslg-net-wire) ---
    zslg_net_wire::metrics::METRICS,
    // --- storage memory budget (namespaced) ---
    &storage_budget!(),
    // --- process resource gauges (unprefixed: Prometheus standard) ---
    &process!(),
    // --- jemalloc allocator gauges ---
    &jemalloc!(),
    // --- raft / consensus ---
    &raft!(),
    // --- write path (writer side) ---
    // --- write path (replica side) + HLC cutoff ---
    // --- logs and replication ---
    &logs!(),
    // --- subscription load shedding ---
    &subscription!(),
    // --- snapshots / leader background tasks ---
    &snapshots!(),
    // --- object store ---
    // --- HLC / clock ---
    &hlc!(),
    // --- read path / learner stores ---
    &read_path!(),
    // --- RPC server ---
    &rpc_server!(),
    // --- RPC client ---
    // --- storage write paths: stable log / replica log / local store ---
    &storage!(),
    // --- timing probes (latency histograms by outcome/op/state) ---
];

// ---------------------------------------------------------------------------
// Recorder registration
// ---------------------------------------------------------------------------

/// Every registered histogram, as `(name, declared unit)`.
///
/// The unit is what `observability` derives bucket ladders from, so a `_bytes`
/// histogram never lands on a latency scale.
pub(crate) fn histograms() -> impl Iterator<Item = (&'static str, Option<Unit>)> {
    metrics_kit::histograms(ALL_METRICS)
}

/// Register help text and units for every metric with the installed recorder.
/// Call once after `observability::init_prometheus`.
pub(crate) fn describe_all() {
    metrics_kit::describe_all(ALL_METRICS);
}

/// Whether `name` is a registered histogram. For `observability`'s check that
/// every exported bucket override names one.
#[cfg(test)]
pub(crate) fn is_registered_histogram(name: &str) -> bool {
    ALL_METRICS
        .iter()
        .copied()
        .flatten()
        .any(|m| m.name == name && m.kind == MetricKind::Histogram)
}

// ---------------------------------------------------------------------------
// Lint-safe emission helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_metric_names_are_unique_and_non_empty() {
        let mut seen = std::collections::HashSet::new();
        for def in ALL_METRICS.iter().copied().flatten() {
            assert!(!def.name.is_empty(), "metric name must not be empty");
            assert!(seen.insert(def.name), "duplicate metric name: {}", def.name);
        }
    }

    /// Prometheus stamps `job` and `instance` on every scraped series from the
    /// scrape config. Without `honor_labels: true` (the harness does not set it,
    /// and should not have to) an exposed label of the same name is silently
    /// renamed to `exported_<name>`, so every dashboard selector on it breaks in
    /// production while passing locally. No emitted label key may collide.
    #[test]
    fn no_label_key_collides_with_prometheus_scrape_labels() {
        const RESERVED: [&str; 2] = ["job", "instance"];
        for key in ALL_LABEL_KEYS {
            assert!(
                !RESERVED.contains(key),
                "label key {key:?} collides with a Prometheus scrape label and \
                 would be exposed as exported_{key}",
            );
        }
    }

    #[test]
    fn rocksdb_properties_are_registered() {
        // `actual-delayed-write-rate` is the one that answers "was the seize-up
        // the 20 MiB budget or the host?" — the question the 2026-08-17 harness
        // run could not answer, because nothing exported it.
        for want in [
            "zslg_storage_num_immutable_memtable",
            "zslg_storage_memtable_flush_pending",
            "zslg_storage_compaction_pending",
            "zslg_storage_estimate_num_keys",
            "zslg_storage_live_sst_files_size_bytes",
            "zslg_storage_block_cache_usage_bytes",
            "zslg_storage_actual_delayed_write_rate",
        ] {
            assert!(
                ALL_METRICS
                    .iter()
                    .copied()
                    .flatten()
                    .any(|m| m.name == want),
                "{want} is not registered in ALL_METRICS",
            );
        }
    }

    /// The namespace is a contract with every dashboard and alert, so it is
    /// asserted rather than left to the macro's good behaviour. `process_*` is
    /// the documented exemption: those names are the Prometheus-standard
    /// collector namespace, shared with every other client library.
    #[test]
    fn every_metric_is_namespaced() {
        for def in ALL_METRICS.iter().copied().flatten() {
            assert!(
                def.name.starts_with(METRIC_PREFIX) || def.name.starts_with("process_"),
                "{} is outside the {METRIC_PREFIX} namespace and is not a \
                 standard process_* metric",
                def.name,
            );
        }
    }

    #[test]
    fn no_metric_refers_to_a_deleted_engine() {
        // The engine is gone from the binary; this stops its names creeping back
        // into the registry, where they would look alive to a dashboard.
        for m in ALL_METRICS.iter().copied().flatten() {
            assert!(
                !m.name.contains("strata"),
                "{} names an engine that no longer exists",
                m.name,
            );
        }
    }

    #[test]
    fn naming_conventions_hold() {
        for def in ALL_METRICS.iter().copied().flatten() {
            match def.kind {
                MetricKind::Counter => assert!(
                    def.name.ends_with("_total"),
                    "counter must end in _total: {}",
                    def.name,
                ),
                MetricKind::Histogram => assert!(
                    def.name.ends_with("_seconds")
                        || def.name.ends_with("_bytes")
                        || def.name.ends_with("_entries"),
                    "histogram must end in a unit suffix (_seconds/_bytes/_entries): {}",
                    def.name,
                ),
                MetricKind::Gauge => assert!(
                    !def.name.ends_with("_total"),
                    "gauge must not end in _total: {}",
                    def.name,
                ),
            }
        }
    }

    #[test]
    fn histograms_declare_units() {
        for def in ALL_METRICS.iter().copied().flatten() {
            if def.kind == MetricKind::Histogram {
                assert!(
                    def.unit.is_some(),
                    "histogram must declare a unit: {}",
                    def.name,
                );
            }
        }
    }

    #[test]
    fn every_metric_has_help_text() {
        for def in ALL_METRICS.iter().copied().flatten() {
            assert!(
                def.help.len() >= 20,
                "help text too short to be useful: {}",
                def.name,
            );
        }
    }

    #[test]
    fn all_design_doc_metrics_present() {
        let set: std::collections::HashSet<&str> = ALL_METRICS
            .iter()
            .copied()
            .flatten()
            .map(|d| d.name)
            .collect();
        // The design doc §"Observability" list, verbatim.
        for required in [
            "zslg_raft_is_member",
            "zslg_raft_is_leader",
            "zslg_raft_has_leader",
            "zslg_raft_term",
            "zslg_raft_applied_log_index",
            "zslg_raft_leader_changes_total",
            "zslg_raft_in_joint_config",
            "zslg_raft_config_generation",
            "zslg_raft_fenced_writes_total",
            "zslg_replica_nodes_count",
            "zslg_membership_changes_total",
            "zslg_node_rejoins_total",
            "zslg_write_duration_seconds",
            "zslg_write_fanout_responses_total",
            "zslg_writes_in_flight",
            "zslg_replica_write_requests_total",
            "zslg_hlc_guards_in_flight",
            "zslg_hlc_cutoff_sets_total",
            "zslg_hlc_cutoff_drain_duration_seconds",
            "zslg_log_writes_total",
            "zslg_log_index_frontier",
            "zslg_log_subscription_reconnects_total",
            "zslg_local_store_integrate_queue_depth",
            "zslg_purged_entries_total",
            "zslg_manifest_watermark_advanced_timestamp_seconds",
            "zslg_stable_log_end_hlc_advanced_timestamp_seconds",
            "zslg_snapshot_creation_total",
            "zslg_snapshot_creation_duration_seconds",
            "zslg_snapshot_size_bytes",
            "zslg_snapshot_entries_count",
            "zslg_manifest_entries_count",
            "zslg_compaction_runs_total",
            "zslg_leader_tasks_active",
            "zslg_object_store_request_duration_seconds",
            "zslg_object_store_transfer_bytes_total",
            "zslg_hlc_now_physical_ms",
            "zslg_hlc_logical",
            "zslg_hlc_wall_clock_drift_ms",
            "zslg_hlc_cutoff_physical_ms",
            "zslg_manifest_end_hlc_physical_ms",
            "zslg_reads_total",
            "zslg_read_token_wait_duration_seconds",
            "zslg_read_token_wait_timeouts_total",
            "zslg_quorum_reads_total",
            "zslg_rpc_server_requests_total",
            "zslg_rpc_server_request_duration_seconds",
            "zslg_rpc_connections_active",
            "zslg_rpc_conn_checkout_seconds",
            "zslg_rpc_conn_reuse_total",
            "zslg_rpc_open_stream_seconds",
            "zslg_rpc_roundtrip_seconds",
        ] {
            assert!(
                set.contains(required),
                "missing required metric: {required}"
            );
        }
    }

    // ── emission smoke tests ─────────────────────────────────────────────

    #[test]
    fn set_flag_and_set_gauge_are_observable() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            set_flag(zslg_raft_state::metrics::RAFT_IS_LEADER, true);
            set_gauge(zslg_raft_state::metrics::RAFT_TERM, 7);
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map
            .keys()
            .any(|ck| ck.key().name() == zslg_raft_state::metrics::RAFT_IS_LEADER);
        assert!(found, "raft_is_leader gauge should be present");
    }

    #[test]
    fn counter_emission_is_observable() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            ::metrics::counter!(zslg_raft_node::metrics::RAFT_FENCED_WRITES_TOTAL).increment(1);
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map
            .keys()
            .any(|ck| ck.key().name() == zslg_raft_node::metrics::RAFT_FENCED_WRITES_TOTAL);
        assert!(found, "raft_fenced_writes_total counter should be present");
    }

    #[test]
    fn labeled_histogram_emission_is_observable() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            ::metrics::histogram!(
                SNAPSHOT_CREATION_DURATION_SECONDS,
                LABEL_STEP => SnapshotStep::Total.as_str(),
            )
            .record(std::time::Duration::from_millis(42));
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map
            .keys()
            .any(|ck| ck.key().name() == SNAPSHOT_CREATION_DURATION_SECONDS);
        assert!(
            found,
            "snapshot_creation_duration_seconds histogram should be present"
        );
    }

    #[test]
    fn probe_finish_records_chosen_outcome() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            zslg_stable_log::metrics::stable_log_append_probe().finish(OpState::Success);
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map.keys().any(|ck| {
            ck.key().name() == zslg_stable_log::metrics::STABLE_LOG_APPEND_DURATION_SECONDS
                && ck
                    .key()
                    .labels()
                    .any(|l| l.key() == LABEL_STATE && l.value() == OpState::Success.as_str())
        });
        assert!(found, "probe.finish should record the chosen outcome label");
    }

    #[test]
    fn probe_dropped_without_finish_records_default_outcome() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let _probe = zslg_stable_log::metrics::stable_log_append_probe();
            // dropped at end of scope without finish -> default outcome (Failure).
        });
        let snap = snapshotter.snapshot();
        let map = snap.into_hashmap();
        let found = map.keys().any(|ck| {
            ck.key().name() == zslg_stable_log::metrics::STABLE_LOG_APPEND_DURATION_SECONDS
                && ck
                    .key()
                    .labels()
                    .any(|l| l.key() == LABEL_STATE && l.value() == OpState::Failure.as_str())
        });
        assert!(
            found,
            "a dropped probe should record its default outcome label"
        );
    }

    #[test]
    fn watermark_advance_stamps_emit_epoch_timestamps() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            zslg_raft_state::metrics::manifest_watermark_advanced();
            zslg_stable_log::metrics::stable_log_end_advanced();
        });
        let map = snapshotter.snapshot().into_hashmap();
        // Each stamp must be a wall-clock epoch value. A near-zero reading
        // means it is measuring a duration instead, and Prometheus renders the
        // subtraction as nonsense.
        let is_epoch = |name: &str| {
            map.iter().any(|(ck, (_, _, v))| {
                ck.key().name() == name
                    && matches!(
                        v,
                        metrics_util::debugging::DebugValue::Gauge(g) if g.0 > 1_700_000_000.0
                    )
            })
        };
        assert!(
            is_epoch(zslg_raft_state::metrics::MANIFEST_WATERMARK_ADVANCED_TIMESTAMP_SECONDS),
            "manifest_watermark_advanced should emit a Unix-epoch timestamp"
        );
        assert!(
            is_epoch(zslg_stable_log::metrics::STABLE_LOG_END_HLC_ADVANCED_TIMESTAMP_SECONDS),
            "stable_log_end_advanced should emit a Unix-epoch timestamp"
        );
    }

    #[test]
    fn node_liveness_gauges_carry_node_id_label() {
        let node = zslg_node_types::NodeId::new_v4();
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
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
        let node = zslg_node_types::NodeId::new_v4();
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::format_push_string, reason = "test code")]
mod registry_golden {
    use super::*;

    const GOLDEN_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/metrics.golden");

    fn dump() -> String {
        // Sorted: which crate declares a metric (and so the registry order) is
        // not part of the contract; the set is.
        let mut lines: Vec<String> = ALL_METRICS
            .iter()
            .copied()
            .flatten()
            .map(|def| {
                format!(
                    "{}\t{:?}\t{:?}\t{}\n",
                    def.name, def.kind, def.unit, def.help
                )
            })
            .collect();
        lines.sort();
        lines.concat()
    }

    /// The exported metric set — names, kinds, units and help text — is a
    /// contract with dashboards and alerts. A restructuring of the code that
    /// emits them must not change it. `UPDATE_GOLDEN=1` rewrites the file.
    #[test]
    fn the_registry_matches_the_golden_dump() {
        let now = dump();
        if std::env::var_os("UPDATE_GOLDEN").is_some() {
            std::fs::write(GOLDEN_PATH, &now).unwrap();
            return;
        }
        let golden = std::fs::read_to_string(GOLDEN_PATH).unwrap();
        assert!(
            golden == now,
            "metric registry differs from src/metrics.golden"
        );
    }
}
