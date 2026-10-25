//! The gauges this crate publishes, and the exporter configuration a binary
//! needs to publish them well.
//!
//! `tracing` and `metrics` are facades: with nothing installed every macro is a
//! no-op, so this crate emits and installs nothing. Both globals are
//! install-once and process-wide, so the choice belongs to the binary —
//! `crates/zslg/examples/common/mod.rs` is the worked example. Filtering needs
//! nothing from us either: `tracing`'s target defaults to the module path, so
//! `RUST_LOG=zslg=debug` works as it stands.
//!
//! What a library does own is the knowledge, exported here as data:
//! [`describe_metrics`], and [`METRIC_NAMESPACE`] / [`NAMESPACE_BUCKETS`] /
//! [`BUCKET_OVERRIDES`], whose values were chosen against this crate's latency
//! distributions.

use std::time::Duration;

use ::metrics::Unit;

use tokio_util::sync::CancellationToken;

use crate::Error;
use crate::metrics;

/// The namespace every metric this crate owns is exported under.
pub const METRIC_NAMESPACE: &str = "zslg_";

/// Latency ladder, in seconds.
const SECONDS_BUCKETS: &[f64] = &[
    0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    60.0,
];

/// Size ladder, in bytes: one record through one flush batch.
const BYTES_BUCKETS: &[f64] = &[
    128.0,
    512.0,
    2_048.0,
    8_192.0,
    32_768.0,
    131_072.0,
    524_288.0,
    2_097_152.0,
    8_388_608.0,
    33_554_432.0,
    134_217_728.0,
];

/// Count ladder: batch sizes and backlogs.
const COUNT_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 2_048.0, 8_192.0, 65_536.0,
];

/// Buckets for one histogram, keyed by the unit it declares.
///
/// Deriving from the unit rather than naming a default is what keeps a
/// `_bytes` histogram off a latency ladder — the registry already knows, and
/// `metrics::tests::histograms_declare_units` makes sure it always does.
fn buckets_for(unit: Option<Unit>) -> &'static [f64] {
    match unit {
        Some(Unit::Bytes) => BYTES_BUCKETS,
        Some(Unit::Count) => COUNT_BUCKETS,
        _ => SECONDS_BUCKETS,
    }
}

/// Histograms whose unit-derived ladder is wrong for them, by exact name.
///
/// HLC peer drift is the only one: [`SECONDS_BUCKETS`] jumps 0.1 → 0.25,
/// straight past the 200 ms skew limit, so a heatmap could not show which side
/// of it drift sits on. ~25 ms resolution across the 0.1–0.3 s band.
const BUCKET_OVERRIDES: &[(&str, &[f64])] = &[(
    metrics::HLC_DRIFT_SECONDS,
    &[
        0.005, 0.01, 0.02, 0.05, 0.075, 0.1, 0.125, 0.15, 0.175, 0.2, 0.225, 0.25, 0.3, 0.4, 0.5,
        0.75, 1.0, 2.0, 5.0,
    ],
)];

/// Every histogram this crate records, with the buckets it should get.
///
/// Recorder-agnostic, for an exporter this crate knows nothing about. Users of
/// `metrics-exporter-prometheus` want [`configure_prometheus`] instead, which
/// applies exactly this.
pub fn histogram_buckets() -> impl Iterator<Item = (&'static str, &'static [f64])> {
    metrics::histograms().map(|(name, unit)| {
        let buckets = BUCKET_OVERRIDES
            .iter()
            .find(|(n, _)| *n == name)
            .map_or_else(|| buckets_for(unit), |(_, b)| *b);
        (name, buckets)
    })
}

/// Apply this crate's histogram buckets to a Prometheus builder.
///
/// Configuration only — the builder is yours, and installing it stays your
/// call. Composes with any other crate that offers the same, and touches
/// nothing global: each histogram gets an exact `Matcher::Full`, so a bucket
/// choice here can never reach another crate's metrics.
///
/// ```no_run
/// # fn main() -> Result<(), zslg::Error> {
/// use metrics_exporter_prometheus::PrometheusBuilder;
///
/// let builder = PrometheusBuilder::new().with_http_listener(([0, 0, 0, 0], 9090));
/// let builder = zslg::configure_prometheus(builder)?;
/// builder.install().expect("install recorder");
///
/// // After the recorder exists: descriptions sent to the no-op recorder are
/// // dropped, not replayed.
/// zslg::describe_metrics();
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "prometheus")]
pub fn configure_prometheus(
    mut builder: metrics_exporter_prometheus::PrometheusBuilder,
) -> Result<metrics_exporter_prometheus::PrometheusBuilder, Error> {
    use metrics_exporter_prometheus::Matcher;

    for (name, buckets) in histogram_buckets() {
        builder = builder
            .set_buckets_for_metric(Matcher::Full(name.to_owned()), buckets)
            .map_err(|e| Error::Observability(format!("bucket config for {name}: {e}")))?;
    }
    Ok(builder)
}

/// Register every metric's help text and unit with the installed recorder.
///
/// Call once, **after** installing a recorder: descriptions sent to the no-op
/// recorder are dropped, not replayed.
pub fn describe_metrics() {
    metrics::describe_all();
}

/// Cadence of [`run_process_gauges`].
const PROCESS_GAUGE_INTERVAL: Duration = Duration::from_secs(1);

/// Future that reads `/proc/self/{stat,status}` every [`PROCESS_GAUGE_INTERVAL`]
/// and emits [`metrics::process_cpu_seconds`] and
/// [`metrics::process_memory_rss_bytes`]. Pass to
/// [`crate::shutdown::Shutdown::spawn`] for lifecycle management.
///
/// CPU is cumulative *clock ticks* converted to seconds via `sysconf(_SC_CLK_TCK)`
/// (100 Hz on Linux). Memory is resident set size (VmRSS), which climbs toward the
/// cgroup limit before an OOM kill. On non-Linux or if `/proc` is unreadable the
/// tick silently idles.
///
/// `queue_capacity_bytes` is the write pipeline's own number, computed by the
/// configs that own those depths (`ReplicaLogConfig::queue_capacity_bytes`,
/// `LearnerConfig::queue_capacity_bytes`) and passed in already summed — it is
/// static for the process's life, so it is stamped from the owner rather than
/// recomputed on a scheduling tick. `None` means this node type owns no such
/// queues (the writer), and publishes nothing rather than a 0 that would read
/// as "measured, and empty".
pub(crate) async fn run_process_gauges(
    storage: Option<std::sync::Arc<crate::database::storage::Storage>>,
    queue_capacity_bytes: Option<u64>,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let mut ticker = tokio::time::interval(PROCESS_GAUGE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = ticker.tick() => {}
        }
        if let Some(cpu_s) = read_proc_self_stat_cpu_seconds() {
            metrics::process_cpu_seconds(cpu_s);
        }
        if let Some(rss_bytes) = read_proc_self_status_memory() {
            metrics::process_memory_rss_bytes(rss_bytes);
        }
        // Emitted beside RSS deliberately: the budget is only meaningful as a
        // pair with what the process actually holds.
        //
        // ONE term, and exact: a node opens exactly one RocksDB, so this reads
        // that node's block cache and nothing else. It used to be a sum of this
        // plus a process-wide registry of every DB the process had open — which
        // conflated two scopes (each of six in-process loadgen nodes published a
        // different value to the same gauge) *and* under-reported, because two
        // stores had their own DBs outside the registry.
        //
        // `None` is the writer node, which has no durable state and opens no
        // DB: it publishes no reading rather than a zero that would read as
        // "holding nothing".
        if let Some(storage) = storage.as_ref() {
            metrics::storage_memory_charged_bytes(
                u64::try_from(storage.charged_bytes()).unwrap_or(u64::MAX),
            );
            emit_rocksdb_property_gauges(storage);
        }
        // Beside the engine's charged bytes, never inside it: `storage_memory`
        // bounds RocksDB and nothing else. The queues hold payloads and are the
        // larger part of the gap between that budget and RSS — at 2 KiB records
        // a 4,096-deep write queue alone is ~8 MB against a 20 MiB budget.
        if let Some(bytes) = queue_capacity_bytes {
            metrics::storage_queue_capacity_bytes(bytes);
        }
        if let Some(threads) = read_proc_self_status_threads() {
            metrics::process_threads_count(threads);
        }
        if let Some(fds) = read_proc_self_open_fds() {
            metrics::process_open_fds(fds);
        }
        if let Some(max_fds) = read_proc_self_max_fds() {
            metrics::process_max_fds(max_fds);
        }
        if let Some((anon, file)) = read_proc_self_smaps_rollup_split() {
            metrics::process_memory_split_bytes(anon, file);
        }
        #[cfg(feature = "jemalloc")]
        emit_jemalloc_stats();
    }
}

/// Read the RocksDB engine-state properties and publish them as gauges: the
/// per-CF ones (memtable/compaction/SST/key-count backlog, one reading per
/// column family this node has open) plus the two DB-wide ones (block-cache
/// usage, and the write-rate cap RocksDB is currently enforcing).
///
/// Stamped here, beside `zslg_storage_memory_charged_bytes`, because this is where
/// the state lives: the `Storage` that owns the DB, not whichever scheduling
/// tick happens to be convenient (CLAUDE.md §7).
///
/// A property that is missing or fails to parse as `u64` is skipped, never
/// published as a fabricated 0 — see `Storage::property_u64`.
fn emit_rocksdb_property_gauges(storage: &crate::database::storage::Storage) {
    for subsystem in storage.column_families() {
        let cf = subsystem.cf_name();
        if let Some(v) = storage.property_u64(subsystem, "rocksdb.num-immutable-mem-table") {
            metrics::storage_num_immutable_memtable(cf, v);
        }
        if let Some(v) = storage.property_u64(subsystem, "rocksdb.mem-table-flush-pending") {
            metrics::storage_memtable_flush_pending(cf, v);
        }
        if let Some(v) = storage.property_u64(subsystem, "rocksdb.compaction-pending") {
            metrics::storage_compaction_pending(cf, v);
        }
        if let Some(v) = storage.property_u64(subsystem, "rocksdb.estimate-num-keys") {
            metrics::storage_estimate_num_keys(cf, v);
        }
        if let Some(v) = storage.property_u64(subsystem, "rocksdb.live-sst-files-size") {
            metrics::storage_live_sst_files_size_bytes(cf, v);
        }
    }
    if let Some(v) = storage.property_u64_db("rocksdb.block-cache-usage") {
        metrics::storage_block_cache_usage_bytes(v);
    }
    // Non-zero means RocksDB itself is throttling writers — engine
    // backpressure, distinct from host starvation (which shows in scrape
    // duration). This is what the 2026-08-17 harness run could not tell
    // apart, because nothing exported it.
    if let Some(v) = storage.property_u64_db("rocksdb.actual-delayed-write-rate") {
        metrics::storage_actual_delayed_write_rate(v);
    }
}

/// Read jemalloc's global `stats.*` counters and publish them as gauges.
/// The `epoch` advance refreshes jemalloc's internal snapshot (its stats are
/// cached between epochs); each read is a cheap mallctl lookup. Any failure
/// (allocator built without stats, or a process whose binary did not declare jemalloc) silently skips the tick.
#[cfg(feature = "jemalloc")]
fn emit_jemalloc_stats() {
    use tikv_jemalloc_ctl::{epoch, stats};
    if epoch::advance().is_err() {
        return;
    }
    let read = |r: tikv_jemalloc_ctl::Result<usize>| r.ok().and_then(|v| u64::try_from(v).ok());
    let (
        Some(allocated),
        Some(active),
        Some(metadata),
        Some(resident),
        Some(mapped),
        Some(retained),
    ) = (
        read(stats::allocated::read()),
        read(stats::active::read()),
        read(stats::metadata::read()),
        read(stats::resident::read()),
        read(stats::mapped::read()),
        read(stats::retained::read()),
    )
    else {
        return;
    };
    metrics::jemalloc_stats(&metrics::JemallocStats {
        allocated,
        active,
        metadata,
        resident,
        mapped,
        retained,
    });
}

/// Read the current resident set size (bytes) from `/proc/self/status`.
/// `None` if the file is unreadable or `VmRSS` is missing/unparsable.
fn read_proc_self_status_memory() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_status_memory(&status)
}

/// Parse `VmRSS` (resident set size) from the contents of `/proc/self/status`,
/// returning `rss_bytes`. The field is reported in kB (`"VmRSS:\t   12345 kB"`);
/// we convert to bytes with a checked `* 1024` (CLAUDE.md §2 — no bare
/// arithmetic, no `as`).
fn parse_status_memory(status: &str) -> Option<u64> {
    field_u64(status, "VmRSS:")?.checked_mul(1024)
}

/// First whitespace-separated value after a `key` line prefix — the layout of
/// every `/proc/self/{status,smaps_rollup}` field this module reads.
fn field_u64(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let rest = line.strip_prefix(key)?;
        rest.split_whitespace().next()?.parse::<u64>().ok()
    })
}

/// Read the OS thread count (`Threads:`) from `/proc/self/status`.
fn read_proc_self_status_threads() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    field_u64(&status, "Threads:")
}

/// Count entries in `/proc/self/fd` — the process's open file descriptors.
/// `None` on a platform without procfs. The directory read itself opens an fd,
/// which is closed before we count, so it does not inflate the reading.
fn read_proc_self_open_fds() -> Option<u64> {
    let entries = std::fs::read_dir("/proc/self/fd").ok()?;
    u64::try_from(entries.count()).ok()
}

/// The `RLIMIT_NOFILE` soft limit from `/proc/self/limits`. Parsed rather than
/// read via `getrlimit` to keep this module free of unsafe/libc.
fn read_proc_self_max_fds() -> Option<u64> {
    let limits = std::fs::read_to_string("/proc/self/limits").ok()?;
    parse_max_open_files(&limits)
}

/// Extract the soft limit from the `Max open files` row of `/proc/self/limits`.
/// Columns are `Limit / Soft Limit / Hard Limit / Units`; `unlimited` yields
/// `None` (nothing useful to compare a ratio against).
fn parse_max_open_files(limits: &str) -> Option<u64> {
    let row = limits
        .lines()
        .find(|l| l.starts_with("Max open files"))?
        .strip_prefix("Max open files")?;
    row.split_whitespace().next()?.parse().ok()
}

/// Read `(anonymous, file_backed)` resident bytes from
/// `/proc/self/smaps_rollup`. `None` if the file is unreadable (non-Linux,
/// restricted /proc) or a field is missing.
fn read_proc_self_smaps_rollup_split() -> Option<(u64, u64)> {
    let rollup = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
    parse_smaps_rollup_split(&rollup)
}

/// Parse the anon/file RSS split from `smaps_rollup` contents. Both fields are
/// kB. `file = Rss - Anonymous` (saturating: the two are sampled from one read,
/// but be safe) covers file-backed mappings and shmem.
fn parse_smaps_rollup_split(rollup: &str) -> Option<(u64, u64)> {
    let rss = field_u64(rollup, "Rss:")?.checked_mul(1024)?;
    let anon = field_u64(rollup, "Anonymous:")?.checked_mul(1024)?;
    Some((anon, rss.saturating_sub(anon)))
}

fn read_proc_self_stat_cpu_seconds() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Format: "pid (comm) state ..." — fields are space-separated after
    // the comm (which may contain spaces and is wrapped in parens). We
    // want fields 14 (utime) and 15 (stime), 0-indexed from after the
    // closing paren.
    let after_paren = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after_paren.split_whitespace().collect();
    // Fields: state(0) ppid(1) ... utime(11) stime(12)
    if fields.len() < 13 {
        return None;
    }
    let utime: u64 = fields[11].parse().ok()?;
    let stime: u64 = fields[12].parse().ok()?;
    // sysconf(_SC_CLK_TCK) is 100 on all Linux architectures.
    let total_ticks = utime.checked_add(stime)?;
    Some(ticks_to_seconds(total_ticks))
}

/// Convert clock ticks to seconds (`sysconf(_SC_CLK_TCK)` = 100 Hz).
/// The `as` cast is lossless for realistic process CPU tick counts.
#[expect(
    clippy::as_conversions,
    reason = "u64-to-f64 is lossless for realistic CPU tick counts"
)]
#[expect(
    clippy::float_arithmetic,
    reason = "tick-to-second conversion is inherently float"
)]
fn ticks_to_seconds(ticks: u64) -> f64 {
    ticks as f64 / 100.0_f64
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::{
        BUCKET_OVERRIDES, METRIC_NAMESPACE, histogram_buckets, parse_max_open_files,
        parse_status_memory,
    };

    /// An override naming a metric that does not exist is silently
    /// ineffective, which is the worst kind of configuration bug.
    #[test]
    fn bucket_overrides_name_registered_histograms() {
        for (name, buckets) in BUCKET_OVERRIDES {
            assert!(
                crate::metrics::is_registered_histogram(name),
                "{name} is not a registered histogram, so buckets do nothing",
            );
            assert!(!buckets.is_empty(), "{name} has an empty bucket set");
        }
    }

    /// The point of deriving from the declared unit: a `_bytes` or `_entries`
    /// histogram must not land on a latency ladder, and every histogram must
    /// get buckets without anyone naming it.
    #[test]
    fn every_histogram_gets_buckets_matching_its_unit() {
        let mut seen = 0usize;
        for (name, buckets) in histogram_buckets() {
            seen = seen.saturating_add(1);
            assert!(name.starts_with(METRIC_NAMESPACE), "{name} is unnamespaced");
            assert!(!buckets.is_empty(), "{name} got no buckets");

            let top = buckets.last().copied().unwrap_or_default();
            if name.ends_with("_bytes") {
                assert!(top > 1_000.0, "{name} is bytes but tops out at {top}");
            } else if name.ends_with("_entries") {
                assert!(top >= 64.0, "{name} counts entries but tops out at {top}");
            } else if name.ends_with("_seconds") {
                assert!(top <= 3_600.0, "{name} is seconds but tops out at {top}");
            }
        }
        assert!(seen > 20, "expected the registry's histograms, saw {seen}");
    }

    /// The soft limit is the middle-ish column and the one that actually binds;
    /// picking the hard limit by mistake would make the fd ratio read as
    /// healthy right up to the point everything fails.
    #[test]
    fn parse_max_open_files_takes_the_soft_limit() {
        let limits = "\
Limit                     Soft Limit           Hard Limit           Units
Max stack size            8388608              unlimited            bytes
Max open files            1024                 1048576              files
Max locked memory         8388608              8388608              bytes
";
        assert_eq!(parse_max_open_files(limits), Some(1024));
    }

    #[test]
    fn parse_max_open_files_reports_none_when_unlimited() {
        let limits = "Max open files            unlimited            unlimited            files\n";
        assert_eq!(parse_max_open_files(limits), None);
    }

    #[test]
    fn parse_status_memory_extracts_rss_as_bytes() {
        // Abridged /proc/self/status. VmRSS is in kB with tab+space padding, as
        // the kernel formats it, surrounded by unrelated Vm* lines it must skip.
        let status = "\
Name:\treplica
VmSize:\t  850000 kB
VmHWM:\t   65536 kB
VmRSS:\t   40960 kB
Threads:\t12
";
        let rss = parse_status_memory(status).expect("VmRSS present");
        assert_eq!(rss, 40960 * 1024);
    }

    #[test]
    fn parse_status_memory_missing_field_is_none() {
        // VmRSS absent → cannot report RSS → None.
        let status = "Name:\treplica\nVmHWM:\t   1024 kB\n";
        assert!(parse_status_memory(status).is_none());
    }

    #[test]
    fn parse_status_memory_ignores_unrelated_vm_prefixes() {
        // `VmRSS:` must not be confused with e.g. `VmRSSanon:`-style keys or
        // the VmSize line; strip_prefix is exact on the "VmRSS:" token.
        let status = "VmSize:\t 123 kB\nVmHWM:\t 2 kB\nVmRSS:\t 1 kB\n";
        assert_eq!(parse_status_memory(status), Some(1024));
    }

    #[test]
    fn parse_smaps_rollup_splits_anon_and_file() {
        // Abridged /proc/self/smaps_rollup as the kernel formats it: kB units,
        // Anonymous ≤ Rss; file = Rss - Anonymous.
        let rollup = "\
00400000-7fff0000000 ---p 00000000 00:00 0    [rollup]
Rss:              100000 kB
Pss:               99000 kB
Shared_Clean:        512 kB
Anonymous:         90000 kB
Swap:                  0 kB
";
        let (anon, file) = super::parse_smaps_rollup_split(rollup).expect("fields present");
        assert_eq!(anon, 90000 * 1024);
        assert_eq!(file, 10000 * 1024);
    }

    #[test]
    fn parse_smaps_rollup_missing_field_is_none() {
        assert!(super::parse_smaps_rollup_split("Rss: 10 kB\n").is_none());
    }
}
