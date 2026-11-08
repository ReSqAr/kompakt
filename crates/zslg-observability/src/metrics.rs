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

#[cfg(test)]
use metrics_kit::{LABEL_NONE, LABEL_STATE, MetricKind, OpState, set_flag};
use metrics_kit::{MetricDef, Unit, lossy_f64, metrics, set_gauge};

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

const LABEL_CF: &str = "cf";

/// Every label key this crate emits. A new `LABEL_*` constant belongs here, so
/// that `no_label_key_collides_with_prometheus_scrape_labels` keeps checking
/// the whole set rather than whichever subset was current when it was written.
#[cfg(test)]
const ALL_LABEL_KEYS: &[&str] = &[LABEL_STATE, LABEL_CF, LABEL_NONE];

// ---------------------------------------------------------------------------
// Metrics: each name constant is immediately followed by its emission
// helper(s). Helpers are `pub(crate)` on purpose — an unwired metric is then
// an unused function, which `clippy -D warnings` rejects (dead_code).
// ---------------------------------------------------------------------------

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
    zslg_consensus::metrics::METRICS,
    // --- leader membership jobs (zslg-leader-membership) ---
    zslg_leader_membership::metrics::METRICS,
    // --- leader snapshot jobs (zslg-leader-snapshot) ---
    zslg_leader_snapshot::metrics::METRICS,
    // --- leader compaction (zslg-leader-compaction) ---
    zslg_leader_compaction::metrics::METRICS,
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
    // --- storage write paths: stable log / replica log / local store ---
    &storage!(),
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
mod tests;

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
