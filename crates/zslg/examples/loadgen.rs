//! Standalone, single-process load harness for `zslg`.
//!
//! Brings up a self-contained cluster of `--replicas` [`ReplicaNode`]s and
//! `--writers` [`FollowerNode`]s over real plain-TCP connections on loopback,
//! drives a sustained write workload, and captures **raw** metric samples to
//! `<out-dir>/<timestamp>/metrics.jsonl` for offline analysis.
//!
//! Design decisions (see `docs/PERFORMANCE.md`):
//!   * **Public API only** — uses `ReplicaNode` / `FollowerNode` exactly as an
//!     external operator would. No reaching into crate internals.
//!   * **On-disk object store** (`LocalFileSystem`) shared by all replicas, and a
//!     real per-replica RocksDB — fsync costs are realistic.
//!   * **No TLS** — the transport is plain TCP on a trusted network; peer
//!     identity is the `node_id` in every RPC envelope.
//!   * **No Prometheus server** — installs the metrics recorder *in-process*
//!     (no HTTP listener) and samples `handle.render()` itself, ~1 Hz, into JSONL.
//!   * **No reduction** — the binary writes raw time-series only. Compute
//!     throughput / percentiles / breakdowns offline (DuckDB `read_json_auto`,
//!     polars, pandas, …).
//!
//! Example:
//!   cargo run --release --example loadgen -- \
//!     --replicas 3 --writers 3 --saturate --max-inflight 512 --duration-secs 300
//!   cargo run --release --example loadgen -- --replicas 3 --writers 3 --target-rps 0   # idle floor

#![allow(
    unused_crate_dependencies,
    reason = "example crates inherit all package deps and use only a subset"
)]

mod write_tracker;

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clap::Parser;
use futures::FutureExt as _;
use rand::{RngExt, SeedableRng};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use write_tracker::WriteTracker;

use zslg::{
    FollowerNode, FollowerNodeConfig, HostPort, ObjectStoreConfig, PeerAddr, ReplicaNode,
    ReplicaNodeConfig, ServerName, Writer,
};

#[derive(Parser, Debug, Clone)]
#[command(name = "loadgen", about = "zslg single-process load harness")]
struct Args {
    /// Number of replica nodes. Required (no default) — cluster size changes
    /// the memory/disk footprint enough that it shouldn't be silently implied.
    #[arg(long)]
    replicas: usize,

    /// Number of writer nodes (load drivers). Required (no default) — see
    /// `--replicas`.
    #[arg(long)]
    writers: usize,

    /// Target total writes/sec across all writers. 0 = no writes (idle floor),
    /// unless --saturate is set.
    #[arg(long, default_value_t = 0.0)]
    target_rps: f64,

    /// Ignore --target-rps; keep the in-flight window saturated (measures max
    /// sustainable throughput).
    #[arg(long, default_value_t = false)]
    saturate: bool,

    /// Total maximum in-flight writes across all writers.
    #[arg(long, default_value_t = 2048)]
    max_inflight: usize,

    /// Steady-state measurement duration, seconds.
    #[arg(long, default_value_t = 300)]
    duration_secs: u64,

    /// Warm-up duration before steady state, seconds (drivers run; the
    /// `loadgen_steady_state` gauge is 0 during warm-up, 1 after).
    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,

    /// Minimum payload size, bytes.
    #[arg(long, default_value_t = 64)]
    payload_min: u64,

    /// Maximum payload size, bytes.
    #[arg(long, default_value_t = 4096)]
    payload_max: u64,

    /// Metric sample interval, milliseconds.
    #[arg(long, default_value_t = 1000)]
    sample_interval_ms: u64,

    /// Directory for run outputs; a `<timestamp>/` subdir is created inside.
    #[arg(long, default_value = "results")]
    out_dir: PathBuf,

    /// Base directory for the per-node RocksDB data + the shared object store.
    ///
    /// **Must be on a fast, local filesystem** (tmpfs / NVMe).  Do NOT point
    /// this at a Docker overlay / FUSE / network mount — RocksDB fsync latency
    /// will spike under contention and can trigger write-stall cascades.
    #[arg(long)]
    data_dir: PathBuf,

    /// RNG seed for payload generation (reproducibility).
    #[arg(long, default_value_t = 0xC0FFEE)]
    seed: u64,

    /// Override `ReplicaLogConfig::purge_grace_period` (seconds; config default
    /// 300). Retirement only unlinks data older than `manifest watermark −
    /// grace`, so exercising the replica purge path within a short run needs a
    /// small grace (e.g. 30). Node validation rejects anything below
    /// `learner_quorum_read_timeout` (default 10).
    #[arg(long)]
    replica_purge_grace_secs: Option<u64>,

    /// Override the TCP connect timeout (milliseconds) — deadline for a
    /// single TCP connection attempt. Under CPU contention a shorter timeout
    /// fails fast instead of tying up a task for the full default 10 s. Must
    /// be ≤ `unary_timeout`.
    #[arg(long)]
    connect_timeout_ms: Option<u64>,

    /// Override the per-peer write-pool channel capacity — per-peer write-pool
    /// job-channel depth. Smaller = faster backpressure when writes stall
    /// (recommended: 64 for tmpfs, higher for slower storage).
    #[arg(long)]
    pool_channel_capacity: Option<usize>,

    /// Override the local store's recent-`DataId` filter size (entries). `0`
    /// disables the filter, which is what the A/B control uses. Every learner
    /// subscribes to every replica, so each write arrives once per replica and
    /// this drops copies 2..n before the LSM write.
    #[arg(long)]
    local_store_dedup_ring: Option<usize>,

    /// Storage memory budget per node, in MiB (block cache + charged
    /// memtables). Everything else scales off it.
    #[arg(long)]
    storage_memory_mib: Option<u64>,

    /// Override the replica log's per-subscriber live-channel capacity
    /// (entries). Must stay >= the flush batch cap (1024) or the node refuses
    /// to start — below it, one flush batch sheds every subscriber.
    #[arg(long)]
    replica_log_subscribe_capacity: Option<usize>,

    /// Override the *client-side* subscription receive buffer (entries). This
    /// is the hop after the replica's live channel; it blocks rather than
    /// sheds, so an undersized value stalls a stream instead of collapsing it.
    #[arg(long)]
    client_subscribe_capacity: Option<usize>,

    /// Compress the replica log and local store with zstd-3 (the pre-Task-6
    /// setting). Off by default — both ship uncompressed. This is the A/B
    /// control; the stable log keeps zstd either way.
    #[arg(long)]
    compress_high_volume_stores: bool,

    /// Run ALL nodes on the single shared `#[tokio::main]` runtime (the old
    /// behavior). By default each node gets its OWN runtime + OS thread, which
    /// mirrors a real deployment (separate processes) and removes cross-node
    /// scheduler contention — co-locating 6 nodes on one runtime inflated task
    /// wakeups and capped write throughput ~3× (docs/PERFORMANCE entries 5, 21).
    #[arg(long)]
    shared_runtime: bool,

    /// Deprecated: per-node runtime isolation is now the default. No-op kept so
    /// existing scripts don't break; use `--shared-runtime` to opt out.
    #[arg(long, hide = true)]
    isolate_runtimes: bool,

    /// Worker threads per node runtime (ignored with `--shared-runtime`).
    /// Default 1: a near–single-threaded runtime per node keeps task wakeups
    /// on-thread (fsync still offloads to the blocking pool) — the most
    /// CPU-efficient point. Raise only to give one node more async parallelism.
    #[arg(long)]
    node_runtime_threads: Option<usize>,

    /// If set, run an in-process CPU sampling profiler for the whole run and
    /// write a flamegraph SVG to this path on exit (perf is unavailable in the
    /// container; this is the realistic flamegraph path).
    #[arg(long)]
    pprof: Option<PathBuf>,

    /// Multi-phase load schedule: comma-separated `name:rps@dur` entries run
    /// in order after warm-up, e.g. `"burst:1000@2m,rest:0@15m"`. `dur` is an
    /// integer with an optional `s` (default) or `m` suffix. Overrides
    /// --target-rps/--duration-secs for the measurement window; incompatible
    /// with --saturate. Each phase is recorded to `<run-dir>/phases.json` and
    /// exposed live via the `loadgen_phase` gauge, for the analyzer to segment
    /// the run on.
    #[arg(long)]
    load_profile: Option<String>,

    /// Heap-profile sample interval, seconds. Only takes effect in a
    /// `--features heap-profiling` build: every interval a jemalloc in-use-heap
    /// dump is triggered (time-based, not allocation-based, so it keeps
    /// sampling through idle/rest phases) into `<run-dir>/heap/`, indexed by
    /// `<run-dir>/heap-index.jsonl`.
    #[arg(long)]
    heap_profile_interval_secs: Option<u64>,
}

/// One step of a `--load-profile` schedule.
#[derive(Debug, Clone)]
struct Phase {
    name: String,
    rps: f64,
    dur: Duration,
}

/// Parse `--load-profile`: comma-separated `name:rps@dur` entries.
fn parse_load_profile(s: &str) -> Result<Vec<Phase>, String> {
    s.split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|part| {
            let (name, rest) = part
                .split_once(':')
                .ok_or_else(|| format!("load-profile phase {part:?}: missing 'name:rps@dur'"))?;
            let (rps_str, dur_str) = rest
                .split_once('@')
                .ok_or_else(|| format!("load-profile phase {part:?}: missing '@dur'"))?;
            let rps: f64 = rps_str
                .trim()
                .parse()
                .map_err(|_| format!("load-profile phase {part:?}: bad rps {rps_str:?}"))?;
            let dur = parse_duration_suffix(dur_str.trim())
                .ok_or_else(|| format!("load-profile phase {part:?}: bad duration {dur_str:?}"))?;
            Ok(Phase {
                name: name.trim().to_string(),
                rps,
                dur,
            })
        })
        .collect()
}

/// Parse a duration with an optional trailing `s` (default) or `m` suffix.
fn parse_duration_suffix(s: &str) -> Option<Duration> {
    let (num, mult) = if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else {
        (s, 1)
    };
    let secs: u64 = num.parse().ok()?;
    Some(Duration::from_secs(secs.checked_mul(mult)?))
}

/// A shared, lock-free `f64` the load profile writes to and drivers poll each
/// tick — lets one process step through phases at different offered rates.
struct RateCell(std::sync::atomic::AtomicU64);

impl RateCell {
    fn new(v: f64) -> Self {
        Self(std::sync::atomic::AtomicU64::new(v.to_bits()))
    }

    fn set(&self, v: f64) {
        self.0.store(v.to_bits(), Ordering::Relaxed);
    }

    fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
}

/// One completed (or interrupted) phase, for `phases.json`.
struct PhaseRecord {
    index: usize,
    name: String,
    target_rps: f64,
    start_ms: u128,
    end_ms: u128,
}

/// Step through the load profile, updating `rate` + the `loadgen_phase` gauge
/// before each phase and recording start/end timestamps. Stops early (and
/// records the truncated phase) on ctrl-c, mirroring the single-phase path.
async fn run_load_profile(phases: &[Phase], rate: &RateCell) -> Vec<PhaseRecord> {
    let mut records = Vec::with_capacity(phases.len());
    for (index, phase) in phases.iter().enumerate() {
        rate.set(phase.rps);
        metrics::gauge!("loadgen_phase").set(index as f64);
        let start_ms = now_ms();
        info!(
            index,
            name = %phase.name,
            rps = phase.rps,
            dur_secs = phase.dur.as_secs(),
            "loadgen: phase"
        );
        let interrupted = wait_or_ctrl_c(phase.dur).await;
        let end_ms = now_ms();
        records.push(PhaseRecord {
            index,
            name: phase.name.clone(),
            target_rps: phase.rps,
            start_ms,
            end_ms,
        });
        if interrupted {
            info!("loadgen: ctrl-c during load profile");
            break;
        }
    }
    records
}

/// Write `phases.json`: `[{index,name,target_rps,start_ms,end_ms}]`.
fn write_phases_json(path: &std::path::Path, records: &[PhaseRecord]) {
    let mut out = String::from("[");
    for (i, r) in records.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"index\":");
        out.push_str(&r.index.to_string());
        out.push_str(",\"name\":\"");
        push_json_str(&r.name, &mut out);
        out.push_str("\",\"target_rps\":");
        out.push_str(&r.target_rps.to_string());
        out.push_str(",\"start_ms\":");
        out.push_str(&r.start_ms.to_string());
        out.push_str(",\"end_ms\":");
        out.push_str(&r.end_ms.to_string());
        out.push('}');
    }
    out.push(']');
    if let Err(e) = std::fs::write(path, out) {
        warn!(error = %e, path = %path.display(), "loadgen: failed to write phases.json");
    }
}

/// Best-effort `git <args>` capture; empty string on any failure (no git, not
/// a repo, non-UTF8 output — never fatal to the run).
fn git_capture(args: &[&str]) -> String {
    std::process::Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// Write `run-meta.json`: provenance + resolved config, so `report.py` is
/// self-describing without re-shelling to git or re-parsing CLI args.
fn write_run_meta_json(path: &std::path::Path, args: &Args, phases: &[Phase]) {
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| {
        std::process::Command::new("hostname")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_default()
    });
    let git_commit = git_capture(&["rev-parse", "HEAD"]);
    let git_branch = git_capture(&["rev-parse", "--abbrev-ref", "HEAD"]);
    let git_dirty = !git_capture(&["status", "--porcelain"]).is_empty();

    let mut out = String::from("{\"hostname\":\"");
    push_json_str(&hostname, &mut out);
    out.push_str("\",\"git_commit\":\"");
    push_json_str(&git_commit, &mut out);
    out.push_str("\",\"git_branch\":\"");
    push_json_str(&git_branch, &mut out);
    out.push_str("\",\"git_dirty\":");
    out.push_str(if git_dirty { "true" } else { "false" });
    out.push_str(",\"data_dir\":\"");
    push_json_str(&args.data_dir.to_string_lossy(), &mut out);
    out.push_str("\",\"replicas\":");
    out.push_str(&args.replicas.to_string());
    out.push_str(",\"writers\":");
    out.push_str(&args.writers.to_string());
    out.push_str(",\"target_rps\":");
    out.push_str(&args.target_rps.to_string());
    out.push_str(",\"saturate\":");
    out.push_str(if args.saturate { "true" } else { "false" });
    out.push_str(",\"max_inflight\":");
    out.push_str(&args.max_inflight.to_string());
    out.push_str(",\"duration_secs\":");
    out.push_str(&args.duration_secs.to_string());
    out.push_str(",\"warmup_secs\":");
    out.push_str(&args.warmup_secs.to_string());
    out.push_str(",\"payload_min\":");
    out.push_str(&args.payload_min.to_string());
    out.push_str(",\"payload_max\":");
    out.push_str(&args.payload_max.to_string());
    out.push_str(",\"seed\":");
    out.push_str(&args.seed.to_string());
    out.push_str(",\"shared_runtime\":");
    out.push_str(if args.shared_runtime { "true" } else { "false" });
    out.push_str(",\"load_profile\":[");
    for (i, p) in phases.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"name\":\"");
        push_json_str(&p.name, &mut out);
        out.push_str("\",\"rps\":");
        out.push_str(&p.rps.to_string());
        out.push_str(",\"dur_secs\":");
        out.push_str(&p.dur.as_secs().to_string());
        out.push('}');
    }
    out.push_str("]}");
    if let Err(e) = std::fs::write(path, out) {
        warn!(error = %e, path = %path.display(), "loadgen: failed to write run-meta.json");
    }
}

/// Apply the optional write-pool CLI overrides to a node config.
/// Applied to both replica and writer configs so whichever side drives the
/// pooled write path picks them up.
/// Apply the pool-tuning overrides to a node config's flat `network_*` fields.
/// Takes the four disjoint fields by `&mut` so it works uniformly across the
/// (now flat) replica and follower configs.
/// The write-pool network tunables loadgen can override, exposed as mutable
/// references so [`apply_pool_overrides`] can take a whole node config instead of
/// its loose fields. Implemented by every node config that carries them.
trait PoolOverridable {
    fn connect_timeout(&mut self) -> &mut Duration;
    fn pool_channel_capacity(&mut self) -> &mut usize;
    fn local_store_dedup_ring(&mut self) -> &mut Option<std::num::NonZeroUsize>;
    fn storage_memory(&mut self) -> &mut zslg::ByteSize;
    /// `None` for node types that do not host a replica log.
    fn replica_log_subscribe_capacity(&mut self) -> Option<&mut std::num::NonZeroUsize>;
    fn client_subscribe_capacity(&mut self) -> &mut std::num::NonZeroUsize;
    /// (local store, replica log if this node hosts one)
    fn high_volume_compression(&mut self) -> (&mut bool, Option<&mut bool>);
}

impl PoolOverridable for ReplicaNodeConfig {
    fn connect_timeout(&mut self) -> &mut Duration {
        &mut self.connect_timeout
    }
    fn pool_channel_capacity(&mut self) -> &mut usize {
        &mut self.pool_channel_capacity
    }
    fn local_store_dedup_ring(&mut self) -> &mut Option<std::num::NonZeroUsize> {
        &mut self.learner_local_store_dedup_ring
    }
    fn storage_memory(&mut self) -> &mut zslg::ByteSize {
        &mut self.storage_memory
    }
    fn replica_log_subscribe_capacity(&mut self) -> Option<&mut std::num::NonZeroUsize> {
        Some(&mut self.replica_log_subscribe_channel_capacity)
    }
    fn client_subscribe_capacity(&mut self) -> &mut std::num::NonZeroUsize {
        &mut self.subscribe_channel_capacity
    }
    fn high_volume_compression(&mut self) -> (&mut bool, Option<&mut bool>) {
        (
            &mut self.learner_local_store_compression,
            Some(&mut self.replica_log_compression),
        )
    }
}

impl PoolOverridable for FollowerNodeConfig {
    fn connect_timeout(&mut self) -> &mut Duration {
        &mut self.connect_timeout
    }
    fn pool_channel_capacity(&mut self) -> &mut usize {
        &mut self.pool_channel_capacity
    }
    fn local_store_dedup_ring(&mut self) -> &mut Option<std::num::NonZeroUsize> {
        &mut self.learner_local_store_dedup_ring
    }
    fn storage_memory(&mut self) -> &mut zslg::ByteSize {
        &mut self.storage_memory
    }
    fn replica_log_subscribe_capacity(&mut self) -> Option<&mut std::num::NonZeroUsize> {
        None // followers host no replica log
    }
    fn client_subscribe_capacity(&mut self) -> &mut std::num::NonZeroUsize {
        &mut self.subscribe_channel_capacity
    }
    fn high_volume_compression(&mut self) -> (&mut bool, Option<&mut bool>) {
        (&mut self.learner_local_store_compression, None)
    }
}

fn apply_pool_overrides(cfg: &mut impl PoolOverridable, args: &Args) {
    if let Some(ms) = args.connect_timeout_ms {
        *cfg.connect_timeout() = Duration::from_millis(ms);
        info!(
            connect_timeout_ms = ms,
            "loadgen: overriding connect timeout"
        );
    }
    if let Some(cap) = args.pool_channel_capacity {
        *cfg.pool_channel_capacity() = cap;
        info!(
            pool_channel_capacity = cap,
            "loadgen: overriding pool channel capacity"
        );
    }
    if let Some(cap) = args.replica_log_subscribe_capacity
        && let Some(slot) = cfg.replica_log_subscribe_capacity()
    {
        *slot = std::num::NonZeroUsize::new(cap).unwrap_or(std::num::NonZeroUsize::MIN);
        info!(
            replica_log_subscribe_capacity = cap,
            "loadgen: overriding replica-log subscribe capacity"
        );
    }
    if args.compress_high_volume_stores {
        let (local, replica) = cfg.high_volume_compression();
        *local = true;
        if let Some(r) = replica {
            *r = true;
        }
        info!("loadgen: compressing the replica log and local store (A/B control)");
    }
    if let Some(cap) = args.client_subscribe_capacity {
        *cfg.client_subscribe_capacity() =
            std::num::NonZeroUsize::new(cap).unwrap_or(std::num::NonZeroUsize::MIN);
        info!(
            client_subscribe_capacity = cap,
            "loadgen: overriding client subscribe buffer"
        );
    }
    if let Some(mib) = args.storage_memory_mib {
        *cfg.storage_memory() = zslg::ByteSize::from_mib(mib);
        info!(
            storage_memory_mib = mib,
            "loadgen: overriding storage memory budget"
        );
    }
    if let Some(entries) = args.local_store_dedup_ring {
        // `0` means "disabled" at the CLI only — the config itself is an
        // Option, never a sentinel. This is the A/B control.
        *cfg.local_store_dedup_ring() = std::num::NonZeroUsize::new(entries);
        info!(
            local_store_dedup_ring = entries,
            "loadgen: overriding local-store dedup ring"
        );
    }
}

#[allow(
    clippy::large_stack_frames,
    reason = "one-shot example main; see comment"
)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.replicas == 0 || args.writers == 0 {
        return Err("--replicas and --writers must both be >= 1".into());
    }
    if args.payload_min == 0 || args.payload_max < args.payload_min {
        return Err("require 0 < --payload-min <= --payload-max".into());
    }
    if args.load_profile.is_some() && args.saturate {
        return Err("--load-profile is incompatible with --saturate".into());
    }
    let phases: Vec<Phase> = match &args.load_profile {
        Some(s) => parse_load_profile(s)?,
        None => Vec::new(),
    };
    if args.load_profile.is_some() && phases.is_empty() {
        return Err("--load-profile parsed to zero phases".into());
    }

    // ── Watchdog: hard self-destruct if the run overruns its budget ────────────
    // Belt-and-suspenders against a hang (e.g. a full data dir wedging the write
    // path): a detached thread force-exits the whole process past a generous
    // deadline. NB a *panic* on a spawned thread would only unwind that thread
    // (default panic=unwind), so we `process::exit` to guarantee the process dies
    // even without an external `timeout` wrapper.
    let profile_secs: u64 = phases
        .iter()
        .map(|p| p.dur.as_secs())
        .fold(0u64, u64::saturating_add);
    let run_secs = if phases.is_empty() {
        args.duration_secs
    } else {
        profile_secs
    };
    let watchdog_secs = args.warmup_secs.saturating_add(run_secs).saturating_add(60);
    std::thread::Builder::new()
        .name("loadgen-watchdog".into())
        .spawn(move || {
            std::thread::sleep(Duration::from_secs(watchdog_secs));
            eprintln!(
                "loadgen-watchdog: run exceeded {watchdog_secs}s budget — assuming hang, aborting process"
            );
            std::process::exit(2);
        })?;

    // ── Optional in-process CPU sampling profiler (perf is unavailable here) ───
    let profiler = match &args.pprof {
        Some(_) => Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(997)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()?,
        ),
        None => None,
    };

    common::init_tracing()?;

    // ── In-process metrics recorder (no HTTP listener, nothing to launch) ──────
    let handle = install_recorder()?;

    // ── Run output dir ─────────────────────────────────────────────────────────
    let stamp = timestamp();
    let run_dir = args.out_dir.join(&stamp);
    std::fs::create_dir_all(&run_dir)?;
    let jsonl_path = run_dir.join("metrics.jsonl");
    info!(run_dir = %run_dir.display(), "loadgen: writing raw samples");
    write_run_meta_json(&run_dir.join("run-meta.json"), &args, &phases);

    // ── Data dirs (one RocksDB per replica + one shared object store) ─────────────
    let data_root = args.data_dir.join(format!("loadgen-{stamp}"));
    let object_store_root = data_root.join("object-store");
    std::fs::create_dir_all(&object_store_root)?;
    info!(data_root = %data_root.display(), "loadgen: data + object store on disk");

    let replica_names: Vec<String> = (0..args.replicas).map(|i| format!("node-{i}")).collect();
    let writer_names: Vec<String> = (0..args.writers).map(|j| format!("writer-{j}")).collect();
    let bootstrap_name = ServerName::from(replica_names[0].clone());

    // ── Reserve ports + build the full replica peer list ───────────────────────
    let replica_addrs: Vec<SocketAddr> = (0..args.replicas).map(|_| reserve_port()).collect();
    let replica_peers: Vec<PeerAddr> = replica_names
        .iter()
        .zip(&replica_addrs)
        .map(|(name, addr)| PeerAddr {
            addr: HostPort::from(*addr),
            server_name: ServerName::from(name.clone()),
        })
        .collect();

    // ── Runtime isolation (default ON; --shared-runtime opts out) ──────────────
    let isolate = !args.shared_runtime;
    if args.isolate_runtimes {
        warn!(
            "--isolate-runtimes is deprecated and now the default; pass --shared-runtime to opt out"
        );
    }
    let total_nodes = args.replicas + args.writers;
    let per_node_workers = args.node_runtime_threads.unwrap_or(1).max(1);
    if isolate {
        info!(
            per_node_workers,
            total_nodes,
            "loadgen: per-node runtimes (default; --shared-runtime to opt out, drivers on main)"
        );
    } else {
        info!("loadgen: shared runtime (all nodes on the main runtime)");
    }

    // ── Start replicas (node-0 bootstraps; the rest join via the shared fence) ──
    let mut replica_tokens = Vec::with_capacity(args.replicas);
    let mut replica_runs: Vec<NodeRun> = Vec::with_capacity(args.replicas);
    for (i, (name, addr)) in replica_names.iter().zip(&replica_addrs).enumerate() {
        let node_dir = data_root.join(format!("replica-{i}"));
        std::fs::create_dir_all(&node_dir)?;
        let mut cfg = ReplicaNodeConfig::new(
            ServerName::from(name.clone()),
            replica_peers.clone(),
            bootstrap_name.clone(),
            *addr,
            node_dir.join("node"),
            ObjectStoreConfig::LocalFs {
                root: object_store_root.clone(),
            },
        )?;
        if let Some(secs) = args.replica_purge_grace_secs {
            cfg.replica_log_purge_grace_period = Duration::from_secs(secs);
            info!(
                replica_purge_grace_secs = secs,
                "loadgen: overriding replica purge grace"
            );
        }
        apply_pool_overrides(&mut cfg, &args);
        info!(node = %name, addr = %addr, "loadgen: starting replica");
        if isolate {
            let tname = name.clone();
            let (htx, hrx) = tokio::sync::oneshot::channel::<Result<CancellationToken, String>>();
            let th = std::thread::Builder::new()
                .name(format!("{tname}-rt"))
                .spawn(move || {
                    let rt = match build_node_runtime(per_node_workers, &tname) {
                        Ok(rt) => rt,
                        Err(e) => {
                            let _ = htx.send(Err(format!("runtime build: {e}")));
                            return;
                        }
                    };
                    rt.block_on(async move {
                        spawn_runtime_metrics("replica", tname);
                        let start =
                            async { ReplicaNode::start(cfg).await?.wait_until_ready().await };
                        match start.boxed().await {
                            Ok(node) => {
                                let _ = htx.send(Ok(node.shutdown_token()));
                                let _ = node.run().boxed().await;
                            }
                            Err(e) => {
                                let _ = htx.send(Err(e.to_string()));
                            }
                        }
                    });
                    // Don't block the thread on teardown: dropping the runtime
                    // would wait for the fsync/flush blocking tasks (parked in
                    // blocking_recv) to finish. Detach them — the process exits
                    // right after, so it's harmless.
                    rt.shutdown_background();
                })?;
            match hrx.await {
                Ok(Ok(token)) => {
                    replica_tokens.push(token);
                    replica_runs.push(NodeRun::Thread(th));
                }
                Ok(Err(e)) => return Err(format!("replica {name} start failed: {e}").into()),
                Err(_) => return Err(format!("replica {name} thread exited early").into()),
            }
        } else {
            let node = ReplicaNode::start(cfg)
                .await?
                .wait_until_ready()
                .boxed()
                .await?;
            replica_tokens.push(node.shutdown_token());
            replica_runs.push(NodeRun::Task(tokio::spawn(async move { node.run().await })));
        }
    }

    // ── Start followers (merged learner + writer) ────────────────────────────
    let mut writers: Vec<Arc<dyn Writer>> = Vec::with_capacity(args.writers);
    let mut writer_readers: Vec<Arc<dyn zslg::Reader>> = Vec::with_capacity(args.writers);
    let mut writer_tokens = Vec::with_capacity(args.writers);
    let mut writer_runs: Vec<NodeRun> = Vec::with_capacity(args.writers);
    for (j, name) in writer_names.iter().enumerate() {
        let node_dir = data_root.join(format!("follower-{j}"));
        std::fs::create_dir_all(&node_dir)?;
        let mut cfg = FollowerNodeConfig::new(
            replica_peers.clone(),
            node_dir.join("node"),
            ObjectStoreConfig::LocalFs {
                root: object_store_root.clone(),
            },
        )?;
        cfg.startup_timeout = Duration::from_secs(60);
        cfg.raft_tracker_min_backoff = Duration::from_millis(100);
        apply_pool_overrides(&mut cfg, &args);
        // Apply storage overrides (same pattern as replicas).
        info!(node = %name, "loadgen: starting follower");
        if isolate {
            let tname = name.clone();
            let (htx, hrx) = tokio::sync::oneshot::channel::<Result<FollowerHandles, String>>();
            let th = std::thread::Builder::new()
                .name(format!("{tname}-rt"))
                .spawn(move || {
                    let rt = match build_node_runtime(per_node_workers, &tname) {
                        Ok(rt) => rt,
                        Err(e) => {
                            let _ = htx.send(Err(format!("runtime build: {e}")));
                            return;
                        }
                    };
                    rt.block_on(async move {
                        spawn_runtime_metrics("writer", tname);
                        match FollowerNode::start(cfg).await {
                            Ok(node) => {
                                let _ = htx.send(Ok((
                                    node.writer(),
                                    node.reader(),
                                    node.shutdown_token(),
                                )));
                                let _ = node.run().await;
                            }
                            Err(e) => {
                                let _ = htx.send(Err(e.to_string()));
                            }
                        }
                    });
                    // See replica path: detach blocking tasks instead of letting
                    // the runtime drop block on them.
                    rt.shutdown_background();
                })?;
            match hrx.await {
                Ok(Ok((writer, reader, token))) => {
                    writers.push(writer);
                    writer_readers.push(reader);
                    writer_tokens.push(token);
                    writer_runs.push(NodeRun::Thread(th));
                }
                Ok(Err(e)) => return Err(format!("follower {name} start failed: {e}").into()),
                Err(_) => return Err(format!("follower {name} thread exited early").into()),
            }
        } else {
            let node = FollowerNode::start(cfg).await?;
            writer_readers.push(node.reader());
            writers.push(node.writer());
            writer_tokens.push(node.shutdown_token());
            writer_runs.push(NodeRun::Task(tokio::spawn(async move { node.run().await })));
        }
    }

    // ── WriteTracker: one per follower, each with its own reader ────────────
    let write_trackers: Vec<Arc<WriteTracker>> = writer_readers
        .iter()
        .enumerate()
        .map(|(i, reader)| {
            let tracker = Arc::new(WriteTracker::new());
            tracker
                .clone()
                .spawn(reader.clone(), writer_tokens[i].clone());
            tracker
        })
        .collect();

    // ── Start the in-process sampler thread (raw JSONL) ────────────────────────
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let handle = handle.clone();
        let stop = stop.clone();
        let interval = Duration::from_millis(args.sample_interval_ms.max(1));
        std::thread::spawn(move || sampler_loop(&handle, &jsonl_path, &stop, interval))
    };

    // ── Start the periodic heap-profile thread (heap-profiling build only) ─────
    let heap_stop = Arc::new(AtomicBool::new(false));
    let heap_thread = spawn_heap_profile_thread(
        run_dir.clone(),
        args.heap_profile_interval_secs,
        heap_stop.clone(),
    );

    // ── Wait for the cluster to accept writes (public-API readiness probe) ─────
    metrics::gauge!("loadgen_steady_state").set(0.0);
    if let Err(e) = wait_ready(&writers[0], Duration::from_secs(90)).await {
        warn!(error = %e, "loadgen: cluster did not become ready; shutting down");
        shutdown(
            stop,
            sampler,
            &run_dir,
            writer_tokens,
            writer_runs,
            replica_tokens,
            replica_runs,
        )
        .await;
        return Err(e);
    }
    info!("loadgen: cluster ready, starting load drivers");

    // ── Launch load drivers ─────────────────────────────────────────────────────
    let load_cancel = CancellationToken::new();
    let inflight_per = (args.max_inflight / args.writers).max(1);
    let initial_rate = if phases.is_empty() {
        args.target_rps
    } else {
        phases[0].rps
    };
    let rate = Arc::new(RateCell::new(initial_rate));
    let inflight = Arc::new(AtomicUsize::new(0));
    let mut driver_handles = Vec::with_capacity(args.writers);
    for (idx, w) in writers.iter().enumerate() {
        let params = DriverParams {
            inflight: inflight_per,
            rate: rate.clone(),
            writer_count: args.writers,
            saturate: args.saturate,
            seed: args.seed.wrapping_add(idx as u64),
            pmin: args.payload_min,
            pmax: args.payload_max,
        };
        driver_handles.push(tokio::spawn(run_driver(
            w.clone(),
            params,
            load_cancel.clone(),
            inflight.clone(),
            write_trackers[idx].clone(),
        )));
    }

    // ── Warm-up → steady state → measurement window ────────────────────────────
    info!(secs = args.warmup_secs, "loadgen: warm-up");
    let mut phase_records = Vec::new();
    if wait_or_ctrl_c(Duration::from_secs(args.warmup_secs)).await {
        info!("loadgen: ctrl-c during warm-up");
    } else {
        metrics::gauge!("loadgen_steady_state").set(1.0);
        if phases.is_empty() {
            info!(
                secs = args.duration_secs,
                steady_start_ms = now_ms() as u64,
                "loadgen: STEADY STATE — measuring"
            );
            if wait_or_ctrl_c(Duration::from_secs(args.duration_secs)).await {
                info!("loadgen: ctrl-c during steady state");
            }
        } else {
            info!(
                phases = phases.len(),
                steady_start_ms = now_ms() as u64,
                "loadgen: STEADY STATE — running load profile"
            );
            phase_records = run_load_profile(&phases, &rate).await;
        }
    }
    if !phase_records.is_empty() {
        write_phases_json(&run_dir.join("phases.json"), &phase_records);
    }

    // ── Drain drivers, then shut down all nodes, then stop sampling ────────────
    // Close the measurement window NOW (before teardown). The report bounds its
    // window by `loadgen_steady_state==1`; leaving it set through the slow
    // node/raft teardown would inflate `dt` and understate every rate.
    metrics::gauge!("loadgen_steady_state").set(0.0);
    info!("loadgen: stopping load drivers");
    load_cancel.cancel();
    for h in driver_handles {
        let _ = tokio::time::timeout(Duration::from_secs(10), h).await;
    }
    // Let client-observed in-flight writes finish before tearing nodes down, so
    // shutdown doesn't reset live streams (cleaner final state + far less noise).
    let drain_deadline = Instant::now() + Duration::from_secs(10);
    while inflight.load(Ordering::Acquire) > 0 && Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown(
        stop,
        sampler,
        &run_dir,
        writer_tokens,
        writer_runs,
        replica_tokens,
        replica_runs,
    )
    .await;

    // Stop the heap-profile thread last (mirrors the sampler): one final dump
    // captures end-of-run state (e.g. drain after a rest phase) before teardown.
    if let Some(h) = heap_thread {
        heap_stop.store(true, Ordering::Release);
        let _ = h.join();
    }

    // Dump the CPU flamegraph (if profiling) before tearing down the data dir.
    if let (Some(guard), Some(path)) = (profiler, &args.pprof) {
        match guard.report().build() {
            Ok(report) => {
                let f = std::fs::File::create(path)?;
                report.flamegraph(f)?;
                // Also emit collapsed/folded stacks (root;...;leaf count) for
                // offline self/inclusive aggregation — easier than parsing SVG.
                use std::fmt::Write as _;
                let mut folded = String::new();
                for (frames, count) in &report.data {
                    let mut names: Vec<String> = Vec::new();
                    for frame in frames.frames.iter().rev() {
                        for sym in frame {
                            names.push(sym.to_string());
                        }
                    }
                    let _ = writeln!(folded, "{} {count}", names.join(";"));
                }
                std::fs::write(path.with_extension("folded"), folded)?;
                info!(path = %path.display(), "loadgen: wrote pprof flamegraph + .folded");
            }
            Err(e) => warn!("loadgen: pprof report build failed: {e}"),
        }
    }

    // Remove the data dir loadgen created (replica DBs + object store) now that
    // every node is shut down and its handles are closed. Repeated runs would
    // otherwise accumulate and fill the RAM-backed tmpfs. Metrics live under
    // out_dir (separate), so they survive this.
    match std::fs::remove_dir_all(&data_root) {
        Ok(()) => info!(data_root = %data_root.display(), "loadgen: removed data dir"),
        Err(e) => {
            warn!(data_root = %data_root.display(), "loadgen: failed to remove data dir: {e}")
        }
    }
    Ok(())
}

// ── Load driver ───────────────────────────────────────────────────────────────

#[derive(Clone)]
struct DriverParams {
    inflight: usize,
    rate: Arc<RateCell>,
    writer_count: usize,
    saturate: bool,
    seed: u64,
    pmin: u64,
    pmax: u64,
}

/// Drive writes for one writer node. Backpressure is bounded by a per-writer
/// semaphore; in paced mode, tokens that cannot acquire a permit are counted as
/// `loadgen_backpressure_total` so offered-vs-issued is visible.
async fn run_driver(
    writer: Arc<dyn Writer>,
    p: DriverParams,
    cancel: CancellationToken,
    inflight: Arc<AtomicUsize>,
    tracker: Arc<WriteTracker>,
) {
    let sem = Arc::new(Semaphore::new(p.inflight));
    let mut rng = rand::rngs::StdRng::seed_from_u64(p.seed);

    if p.saturate {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                permit = sem.clone().acquire_owned() => {
                    let Ok(permit) = permit else { break };
                    metrics::counter!("loadgen_offered_total").increment(1);
                    metrics::counter!("loadgen_issued_total").increment(1);
                    let bytes = make_blob(&mut rng, p.pmin, p.pmax);
                    spawn_write(
                        writer.clone(),
                        bytes,
                        permit,
                        inflight.clone(),
                        tracker.clone(),
                    );
                }
            }
        }
    } else {
        let mut tokens = 0.0f64;
        let mut last = Instant::now();
        let backlog_cap = (p.inflight as f64) * 4.0;
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
            let now = Instant::now();
            let rate_per = p.rate.get() / p.writer_count as f64;
            tokens = (tokens + now.duration_since(last).as_secs_f64() * rate_per).min(backlog_cap);
            last = now;
            while tokens >= 1.0 {
                metrics::counter!("loadgen_offered_total").increment(1);
                match sem.clone().try_acquire_owned() {
                    Ok(permit) => {
                        metrics::counter!("loadgen_issued_total").increment(1);
                        let bytes = make_blob(&mut rng, p.pmin, p.pmax);
                        spawn_write(
                            writer.clone(),
                            bytes,
                            permit,
                            inflight.clone(),
                            tracker.clone(),
                        );
                    }
                    Err(_) => {
                        metrics::counter!("loadgen_backpressure_total").increment(1);
                        debug!("loadgen: backpressure — semaphore fully acquired");
                    }
                }
                tokens -= 1.0;
            }
        }
    }
}

fn make_blob(rng: &mut rand::rngs::StdRng, pmin: u64, pmax: u64) -> Bytes {
    let size = rng.random_range(pmin..=pmax) as usize;
    let mut blob = vec![0u8; size];
    rng.fill(&mut blob[..]);
    Bytes::from(blob)
}

fn spawn_write(
    writer: Arc<dyn Writer>,
    bytes: Bytes,
    permit: tokio::sync::OwnedSemaphorePermit,
    inflight: Arc<AtomicUsize>,
    tracker: Arc<WriteTracker>,
) {
    tokio::spawn(async move {
        inflight.fetch_add(1, Ordering::AcqRel);
        metrics::gauge!("loadgen_inflight").increment(1.0);
        let t0 = Instant::now();
        match writer.write(bytes).await {
            Ok(data_id) => {
                tracker.record_write(data_id);
                metrics::counter!("loadgen_writes_total").increment(1);
                metrics::histogram!("loadgen_write_latency_seconds")
                    .record(t0.elapsed().as_secs_f64());
            }
            Err(e) => {
                // Label by the real error variant. The serializable `Error`
                // round-trips across the wire intact, so this `kind` is the
                // actual replica-side cause (e.g. quorum_unreachable / hlc_cutoff),
                // not a flattened `rpc`. Prometheus sums labels, so the unlabeled
                // total is still recoverable.
                let kind = e.kind();
                warn!(error = %e, kind, "loadgen: write error");
                metrics::counter!("loadgen_errors_total", "kind" => kind).increment(1);
            }
        }
        metrics::gauge!("loadgen_inflight").decrement(1.0);
        inflight.fetch_sub(1, Ordering::AcqRel);
        drop(permit);
    });
}

/// Probe the cluster until a single write succeeds (leader elected + replica set
/// converged) or `deadline` elapses. Uses only the public writer API.
async fn wait_ready(
    writer: &Arc<dyn Writer>,
    deadline: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let stop = Instant::now() + deadline;
    let mut attempt = 0u64;
    loop {
        match writer
            .write(Bytes::from_static(b"loadgen-readiness-probe"))
            .await
        {
            Ok(_data_id) => return Ok(()),
            Err(e) => {
                attempt += 1;
                if Instant::now() >= stop {
                    return Err(format!(
                        "cluster not ready after {deadline:?} ({attempt} probes): {e}"
                    )
                    .into());
                }
                if attempt.is_multiple_of(10) {
                    info!(attempt, error = %e, "loadgen: still waiting for cluster readiness");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

// ── Per-node runtime isolation ──────────────────────────────────────────────────

/// Handles a follower hands back to the main runtime (the driver + readiness
/// probe + write-tracker run on main even under `--isolate-runtimes`).
type FollowerHandles = (Arc<dyn Writer>, Arc<dyn zslg::Reader>, CancellationToken);

/// A running node's lifetime handle: a task on the shared runtime (default), or a
/// dedicated OS thread owning the node's private runtime (`--isolate-runtimes`).
enum NodeRun {
    Task(tokio::task::JoinHandle<Result<(), zslg::Error>>),
    Thread(std::thread::JoinHandle<()>),
}

/// Spawn a tokio-metrics runtime reporter on the CURRENT runtime, tagging every
/// `tokio_*` metric with `role` + `node` labels so the co-located node runtimes
/// don't collide in the shared `metrics` recorder. The rich scheduler metrics
/// (`tokio_busy_ratio`, `tokio_mean_polls_per_park`, queue depth, park/steal
/// counts) only populate under `--cfg tokio_unstable`; build the loadgen with
/// `RUSTFLAGS="--cfg tokio_unstable"` to capture the wakeup-bound collapse
/// signature (PERFORMANCE entry 23). Must be called from inside the node's own
/// runtime so it monitors that runtime.
fn spawn_runtime_metrics(role: &'static str, node: String) {
    tokio::spawn(
        tokio_metrics::RuntimeMetricsReporterBuilder::default()
            .with_interval(Duration::from_secs(1))
            .with_metrics_transformer(move |name| {
                metrics::Key::from_parts(
                    name,
                    vec![
                        metrics::Label::new("role", role),
                        metrics::Label::new("node", node.clone()),
                    ],
                )
            })
            .describe_and_run(),
    );
}

/// Build a per-node multi-thread runtime, sized to avoid oversubscription.
fn build_node_runtime(
    worker_threads: usize,
    name: &str,
) -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads.max(1))
        .thread_name(format!("{name}-w"))
        .enable_all()
        .build()
}

/// Await a node's run handle, bounded by `timeout`. A node thread is joined off
/// the async runtime (`spawn_blocking`) so we never block a worker on `join()`.
async fn join_node_run(run: NodeRun, timeout: Duration) {
    match run {
        NodeRun::Task(h) => {
            let _ = tokio::time::timeout(timeout, h).await;
        }
        NodeRun::Thread(h) => {
            let _ = tokio::time::timeout(
                timeout,
                tokio::task::spawn_blocking(move || {
                    let _ = h.join();
                }),
            )
            .await;
        }
    }
}

// ── Shutdown ──────────────────────────────────────────────────────────────────

#[allow(
    clippy::too_many_arguments,
    reason = "single call site; explicit is clearer than a struct here"
)]
async fn shutdown(
    stop: Arc<AtomicBool>,
    sampler: std::thread::JoinHandle<()>,
    run_dir: &std::path::Path,
    writer_tokens: Vec<CancellationToken>,
    writer_runs: Vec<NodeRun>,
    replica_tokens: Vec<CancellationToken>,
    replica_runs: Vec<NodeRun>,
) {
    info!("loadgen: shutting down writers");
    for t in writer_tokens {
        t.cancel();
    }
    for h in writer_runs {
        join_node_run(h, Duration::from_secs(35)).await;
    }
    info!("loadgen: shutting down replicas");
    for t in replica_tokens {
        t.cancel();
    }
    for h in replica_runs {
        join_node_run(h, Duration::from_secs(35)).await;
    }
    // Stop sampling last so the final state is captured.
    stop.store(true, Ordering::Release);
    let _ = sampler.join();
    info!(run_dir = %run_dir.display(), "loadgen: done — raw samples in metrics.jsonl");
}

/// Sleep for `dur` or return `true` early if ctrl-c arrives.
async fn wait_or_ctrl_c(dur: Duration) -> bool {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => true,
        () = tokio::time::sleep(dur) => false,
    }
}

// ── Periodic jemalloc heap snapshots (heap-profiling build only) ───────────────
//
// jemalloc's own interval dumping (`lg_prof_interval`, `MALLOC_CONF`) is
// allocation-driven: it stops advancing once allocation stops, so it cannot
// see memory *drain* during a rest phase. We instead trigger dumps on a wall
// clock from a dedicated thread via the `prof.dump` mallctl.
//
// Profiling itself can only be turned on at process start (`MALLOC_CONF=prof:true`,
// e.g. via `just perf-scenario`, mirroring `harness/entrypoint.sh`) — it cannot
// be enabled later from within the process.

/// True if jemalloc was started with profiling enabled (`opt.prof`, set only via
/// `MALLOC_CONF` at process init).
#[cfg(feature = "heap-profiling")]
fn jemalloc_profiling_active() -> bool {
    // SAFETY: `opt.prof` is a read-only jemalloc mallctl documented to return
    // `bool`; the requested type matches the key's actual value type.
    unsafe { tikv_jemalloc_ctl::raw::read::<bool>(b"opt.prof\0") }.unwrap_or(false)
}

/// Trigger one jemalloc in-use-heap dump to `path`.
#[cfg(feature = "heap-profiling")]
fn jemalloc_dump_heap(path: &std::path::Path) -> Result<(), String> {
    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|e| format!("heap dump path has an interior NUL: {e}"))?;
    // SAFETY: `prof.dump` is a write-only jemalloc mallctl expecting a
    // `*const c_char` to a null-terminated path. jemalloc opens and writes the
    // file synchronously within this call and does not retain the pointer, so
    // `c_path`'s stack lifetime is sufficient.
    unsafe { tikv_jemalloc_ctl::raw::write(b"prof.dump\0", c_path.as_ptr()) }
        .map_err(|e| format!("prof.dump mallctl failed: {e}"))
}

/// After this many *consecutive* dump failures, `heap_profile_loop` gives up
/// for the rest of the run instead of spamming a warning every interval. Real
/// case this guards: jemalloc 5.3.x's backtrace/unwind is broken against
/// macOS's dyld shared cache during `prof.dump` (fixed only on jemalloc's
/// `dev` branch as of April 2024 — jemalloc/jemalloc#2610 — not in any
/// released version yet, including the 5.3.1 `tikv-jemalloc-sys` vendors), so
/// every attempt fails identically for the whole process lifetime on affected
/// macOS builds; there is nothing a retry can fix.
#[cfg(feature = "heap-profiling")]
const MAX_CONSECUTIVE_HEAP_DUMP_FAILURES: u32 = 3;

/// Background thread: every `interval`, dump the in-use heap to
/// `<run_dir>/heap/<seq>.heap` and append `{ts_ms,seq,file}` to
/// `<run_dir>/heap-index.jsonl`. Always takes one final dump once `stop` is set
/// (mirrors `sampler_loop`), so end-of-run state (e.g. post-rest-phase drain)
/// is captured before teardown — unless it already gave up on repeated
/// failures (see `MAX_CONSECUTIVE_HEAP_DUMP_FAILURES`).
#[cfg(feature = "heap-profiling")]
fn heap_profile_loop(run_dir: &std::path::Path, interval: Duration, stop: &AtomicBool) {
    let heap_dir = run_dir.join("heap");
    if let Err(e) = std::fs::create_dir_all(&heap_dir) {
        warn!(error = %e, dir = %heap_dir.display(), "loadgen: cannot create heap dir");
        return;
    }
    let index_path = run_dir.join("heap-index.jsonl");
    let index_file = match std::fs::File::create(&index_path) {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, path = %index_path.display(), "loadgen: cannot create heap-index.jsonl");
            return;
        }
    };
    use std::io::Write as _;
    let mut index = std::io::BufWriter::new(index_file);
    let mut seq: u64 = 0;
    let mut consecutive_failures: u32 = 0;

    // Returns true on a successful dump — kept outside the closure (rather
    // than mutating a captured counter) so the loop below can still read
    // `consecutive_failures` between calls without fighting the borrow checker.
    let take = |seq: &mut u64, index: &mut std::io::BufWriter<std::fs::File>| -> bool {
        let file_name = format!("{seq:06}.heap");
        let ts_ms = now_ms();
        let ok = match jemalloc_dump_heap(&heap_dir.join(&file_name)) {
            Ok(()) => {
                let _ = writeln!(
                    index,
                    "{{\"ts_ms\":{ts_ms},\"seq\":{seq},\"file\":\"{file_name}\"}}"
                );
                let _ = index.flush();
                true
            }
            Err(e) => {
                warn!(error = %e, "loadgen: heap dump failed");
                false
            }
        };
        *seq = seq.wrapping_add(1);
        ok
    };

    loop {
        if take(&mut seq, &mut index) {
            consecutive_failures = 0;
        } else {
            consecutive_failures = consecutive_failures.saturating_add(1);
        }
        if consecutive_failures >= MAX_CONSECUTIVE_HEAP_DUMP_FAILURES {
            warn!(
                "loadgen: heap dumps failed {MAX_CONSECUTIVE_HEAP_DUMP_FAILURES} times in a row \
                 — giving up for the rest of this run instead of retrying every {interval:?}. \
                 On macOS this is a known upstream jemalloc limitation (backtrace/unwind vs. the \
                 dyld shared cache — jemalloc/jemalloc#2610, not in any released jemalloc yet); \
                 heap profiling works on Linux. The rest of the report is unaffected."
            );
            break;
        }
        let mut waited = Duration::ZERO;
        while waited < interval && !stop.load(Ordering::Acquire) {
            let chunk = interval
                .saturating_sub(waited)
                .min(Duration::from_millis(200));
            if chunk.is_zero() {
                break;
            }
            std::thread::sleep(chunk);
            waited = waited.saturating_add(chunk);
        }
        if stop.load(Ordering::Acquire) {
            take(&mut seq, &mut index);
            break;
        }
    }
}

/// Spawn the heap-profile dump thread if `--heap-profile-interval-secs` is set
/// and this is a `--features heap-profiling` build with profiling actually
/// active (`MALLOC_CONF=prof:true`). Returns `None` otherwise — every caller
/// treats that as "nothing to join", so the flag degrades to a no-op warning
/// on a build/config that can't honor it instead of failing the run.
fn spawn_heap_profile_thread(
    run_dir: std::path::PathBuf,
    interval_secs: Option<u64>,
    stop: Arc<AtomicBool>,
) -> Option<std::thread::JoinHandle<()>> {
    let interval_secs = interval_secs?;
    #[cfg(feature = "heap-profiling")]
    {
        if !jemalloc_profiling_active() {
            warn!(
                "loadgen: --heap-profile-interval-secs given but jemalloc profiling is inactive \
                 (run with MALLOC_CONF=prof:true,prof_active:true — see `just perf-scenario`); skipping"
            );
            return None;
        }
        let interval = Duration::from_secs(interval_secs.max(1));
        Some(
            std::thread::Builder::new()
                .name("loadgen-heap-profile".into())
                .spawn(move || heap_profile_loop(&run_dir, interval, &stop))
                .expect("spawn heap-profile thread"),
        )
    }
    #[cfg(not(feature = "heap-profiling"))]
    {
        let _ = (run_dir, stop, interval_secs);
        warn!(
            "loadgen: --heap-profile-interval-secs given but this binary was not built with \
             --features heap-profiling; skipping"
        );
        None
    }
}

// ── Metrics recorder + sampler ─────────────────────────────────────────────────

fn install_recorder()
-> Result<metrics_exporter_prometheus::PrometheusHandle, Box<dyn std::error::Error>> {
    use metrics_exporter_prometheus::PrometheusBuilder;
    // Same latency buckets as the crate's own exporter so histograms render as
    // cumulative Prometheus buckets (offline-computable percentiles).
    // Fine-grained buckets: the original set jumped 2.5→5→10→25ms, which hides
    // everything interesting at low concurrency (sub-10ms). Dense resolution
    // below 10ms, coarser above.
    let recorder = PrometheusBuilder::new()
        .set_buckets(&[
            0.000_1, 0.000_2, 0.000_3, 0.000_5, 0.000_75, 0.001, 0.001_5, 0.002, 0.003, 0.004,
            0.005, 0.006, 0.008, 0.01, 0.012_5, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.075, 0.1,
            0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
        ])?
        .build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder).map_err(|_| "global metrics recorder already set")?;
    Ok(handle)
}

/// Sample `handle.render()` at `interval`, parsing each Prometheus sample line
/// into a JSONL row `{ts_ms, name, labels, value}`. Always takes a final sample
/// once `stop` is set.
fn sampler_loop(
    handle: &metrics_exporter_prometheus::PrometheusHandle,
    path: &std::path::Path,
    stop: &AtomicBool,
    interval: Duration,
) {
    use std::io::Write as _;
    let file = match std::fs::File::create(path) {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, path = %path.display(), "loadgen: cannot create metrics.jsonl");
            return;
        }
    };
    let mut writer = std::io::BufWriter::new(file);
    let mut buf = String::new();

    let mut sample = |w: &mut std::io::BufWriter<std::fs::File>| {
        buf.clear();
        append_rows(&handle.render(), now_ms(), &mut buf);
        if let Err(e) = w.write_all(buf.as_bytes()).and_then(|()| w.flush()) {
            warn!(error = %e, "loadgen: metrics sample write failed");
        }
    };

    loop {
        sample(&mut writer);
        let mut waited = Duration::ZERO;
        while waited < interval && !stop.load(Ordering::Acquire) {
            let chunk = interval
                .saturating_sub(waited)
                .min(Duration::from_millis(50));
            if chunk.is_zero() {
                break;
            }
            std::thread::sleep(chunk);
            waited = waited.saturating_add(chunk);
        }
        if stop.load(Ordering::Acquire) {
            sample(&mut writer);
            break;
        }
    }
}

/// Parse Prometheus exposition text into JSONL rows appended to `out`.
///
/// Assumes label values contain no spaces or commas (true for this crate's enum
/// label values) so the value is the final whitespace token and labels split on
/// `,`. Prometheus string escapes (`\\`, `\"`, `\n`) are decoded then re-encoded
/// as JSON.
fn append_rows(render: &str, ts_ms: u128, out: &mut String) {
    for line in render.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((series, value_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let series = series.trim_end();
        let value = match value_str.trim() {
            "NaN" | "+Inf" | "-Inf" => continue, // non-finite → skip (JSON has no Inf/NaN)
            v => match v.parse::<f64>() {
                Ok(v) if v.is_finite() => v,
                _ => continue,
            },
        };
        let (name, labels) = match series.split_once('{') {
            Some((name, rest)) => (name, rest.strip_suffix('}').unwrap_or(rest)),
            None => (series, ""),
        };

        out.push_str("{\"ts_ms\":");
        out.push_str(&ts_ms.to_string());
        out.push_str(",\"name\":\"");
        push_json_str(name, out);
        out.push_str("\",\"labels\":{");
        let mut first = true;
        for pair in labels.split(',').filter(|p| !p.is_empty()) {
            let Some((k, v)) = pair.split_once('=') else {
                continue;
            };
            if !first {
                out.push(',');
            }
            first = false;
            out.push('"');
            push_json_str(k.trim(), out);
            out.push_str("\":\"");
            push_prom_value_as_json(v.trim().trim_matches('"'), out);
            out.push('"');
        }
        out.push_str("},\"value\":");
        out.push_str(&value.to_string());
        out.push_str("}\n");
    }
}

/// JSON-escape a plain string (metric/label names).
fn push_json_str(s: &str, out: &mut String) {
    for c in s.chars() {
        push_json_char(c, out);
    }
}

/// Decode Prometheus string escapes then JSON-escape (label values).
fn push_prom_value_as_json(s: &str, out: &mut String) {
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        let real = if c == '\\' {
            match chars.next() {
                Some('\\') => '\\',
                Some('"') => '"',
                Some('n') => '\n',
                Some(other) => other,
                None => '\\',
            }
        } else {
            c
        };
        push_json_char(real, out);
    }
}

fn push_json_char(c: char, out: &mut String) {
    match c {
        '"' => out.push_str("\\\""),
        '\\' => out.push_str("\\\\"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        '\t' => out.push_str("\\t"),
        c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
        c => out.push(c),
    }
}

// ── Small helpers ───────────────────────────────────────────────────────────────

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn timestamp() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

/// Reserve an OS-assigned TCP port on loopback (mirrors the test harness helper).
fn reserve_port() -> SocketAddr {
    let sock = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback tcp");
    let addr = sock.local_addr().expect("local_addr");
    drop(sock);
    addr
}
