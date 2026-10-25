//! Example binary: start a replica node with CLI-configured peers, TLS, and storage.
//!
//! Usage:
//!   cargo run --example replica -- \
//!     --node-name replica-1 \
//!     --listen-addr 0.0.0.0:9000 \
//!     --peers "replica-1:9000,replica-2:9001,replica-3:9002" \
//!     --bootstrap-server-name replica-1 \
//!     --metrics-addr 0.0.0.0:9090 \
//!     --ca-cert-file ca.pem \
//!     --data-dir /tmp/replica-1 \
//!     --object-store in-memory

#![allow(
    unused_crate_dependencies,
    reason = "example crates inherit all package deps and use only a subset"
)]

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use futures::FutureExt;
use tracing::{error, info, warn};

use zslg::{Error, ObjectStoreConfig, PeerAddr, ReplicaNode, ReplicaNodeConfig, ServerName};

#[derive(Parser, Debug)]
#[command(name = "replica")]
struct Args {
    /// Node name (also used as the TLS SNI server name).
    #[arg(long)]
    node_name: String,

    /// Address to listen on for TCP RPC (e.g. "0.0.0.0:9000").
    #[arg(long)]
    listen_addr: SocketAddr,

    /// Comma-separated list of peers in "name:port" format.
    #[arg(long)]
    peers: String,

    /// Server name of the bootstrap node.
    #[arg(long)]
    bootstrap_server_name: String,

    /// Address for the Prometheus HTTP metrics listener.
    #[arg(long)]
    metrics_addr: SocketAddr,

    /// Path to the data directory (the node's RocksDB lives here).
    #[arg(long)]
    data_dir: PathBuf,

    /// Object store backend: "s3", "in-memory", or "local-fs".
    #[arg(long)]
    object_store: String,

    /// S3 endpoint URL (required when --object-store=s3).
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 bucket name (required when --object-store=s3).
    #[arg(long)]
    s3_bucket: Option<String>,

    /// S3 access key (required when --object-store=s3).
    #[arg(long)]
    s3_access_key: Option<String>,

    /// S3 secret key (required when --object-store=s3).
    #[arg(long)]
    s3_secret_key: Option<String>,

    /// S3 region (required when --object-store=s3).
    #[arg(long)]
    s3_region: Option<String>,

    /// Allow plain HTTP connections to S3.
    #[arg(long)]
    s3_allow_http: bool,

    /// Total memory the storage engine may hold on this node, in MiB — block
    /// cache, index and filter blocks, and the memtables charged to it.
    /// Everything else (memtable size, SST target, retained WAL, open files)
    /// scales off it. Leave unset for the built-in default.
    #[arg(long)]
    storage_memory_mib: Option<u64>,

    /// Write a CPU flamegraph (pprof, in-process) to this path on shutdown.
    /// Also writes a `.folded` sibling for offline aggregation.
    #[arg(long)]
    pprof: Option<PathBuf>,
}

/// Apply a `--storage-memory-mib` override. One number for the node: the stores
/// are column families in a single DB, so the budget is not a per-store choice.
fn apply_storage_memory(mut config: ReplicaNodeConfig, mib: Option<u64>) -> ReplicaNodeConfig {
    if let Some(mib) = mib {
        config.storage_memory = zslg::ByteSize::from_mib(mib);
    }
    config
}

#[allow(
    clippy::large_stack_frames,
    reason = "one-shot example main; see comment"
)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("BINARY: replica main() entered");
    let args = Args::parse();
    eprintln!(
        "BINARY: args parsed, node_name={} listen_addr={}",
        args.node_name, args.listen_addr
    );

    eprintln!("BINARY: calling init_tracing...");
    common::init_tracing()?;
    eprintln!("BINARY: init_tracing OK");
    eprintln!(
        "BINARY: calling init_prometheus on {}...",
        args.metrics_addr
    );
    common::init_prometheus(args.metrics_addr)?;
    eprintln!("BINARY: init_prometheus OK");

    // ── Tokio runtime metrics ───────────────────────────────────────────────
    // Emit per-runtime scheduler metrics (`tokio_*`) through the same `metrics`
    // facade the Prometheus exporter above is installed on. The richer per-worker
    // metrics only populate when built with `--cfg tokio_unstable` (see
    // harness/Dockerfile); the stable subset works without it.
    tokio::spawn(
        tokio_metrics::RuntimeMetricsReporterBuilder::default()
            .with_interval(std::time::Duration::from_secs(5))
            .describe_and_run(),
    );

    // ── CPU profiler (optional) ────────────────────────────────────────────
    let profiler = args.pprof.as_ref().map(|_| {
        pprof::ProfilerGuardBuilder::default()
            .frequency(997)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .expect("pprof profiler")
    });

    // ── Peers ──────────────────────────────────────────────────────────────
    eprintln!("BINARY: parsing peers...");
    let mut peers = Vec::new();
    for entry in args.peers.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (name, port_str) = entry
            .split_once(':')
            .ok_or_else(|| format!("invalid peer entry '{entry}': expected name:port"))?;
        let port: u16 = port_str
            .parse()
            .map_err(|_| format!("invalid port in peer entry '{entry}'"))?;

        peers.push(PeerAddr {
            addr: zslg::HostPort::new(name, port),
            server_name: ServerName::from(name),
        });
    }
    eprintln!("BINARY: {} peers parsed", peers.len());

    // ── Object store ───────────────────────────────────────────────────────
    eprintln!(
        "BINARY: building object store config (backend={})...",
        args.object_store
    );
    let object_store_config = match args.object_store.as_str() {
        "in-memory" => ObjectStoreConfig::InMemory,
        "local-fs" => ObjectStoreConfig::LocalFs {
            root: args.data_dir.clone(),
        },
        "s3" => ObjectStoreConfig::S3 {
            endpoint: args.s3_endpoint.ok_or("--s3-endpoint required for s3")?,
            bucket: args.s3_bucket.ok_or("--s3-bucket required for s3")?,
            access_key: args
                .s3_access_key
                .ok_or("--s3-access-key required for s3")?,
            secret_key: args
                .s3_secret_key
                .ok_or("--s3-secret-key required for s3")?,
            region: args.s3_region.ok_or("--s3-region required for s3")?,
            allow_http: args.s3_allow_http,
        },
        other => return Err(format!("unknown object store: {other}").into()),
    };
    eprintln!("BINARY: object store config done");

    let db_path = args.data_dir.join("db");
    eprintln!("BINARY: db_path={}", db_path.display());
    tokio::fs::create_dir_all(&args.data_dir).await?;
    eprintln!("BINARY: data dir created");

    let server_name = ServerName::from(args.node_name.clone());
    eprintln!("BINARY: building ReplicaNodeConfig...");
    let config = ReplicaNodeConfig::new(
        server_name,
        peers,
        ServerName::from(args.bootstrap_server_name),
        args.listen_addr,
        db_path,
        object_store_config,
    )?;
    let config = apply_storage_memory(config, args.storage_memory_mib);
    eprintln!("BINARY: config built");

    eprintln!("BINARY: calling ReplicaNode::start...");
    let initialising = ReplicaNode::start(config).boxed().await?;
    eprintln!(
        "BINARY: ReplicaNode::start OK, node_id={}, now waiting for quorum freshness...",
        initialising.node_id()
    );

    // Captured — and the signal handler installed — BEFORE `wait_until_ready`,
    // which can block indefinitely waiting on a quorum that never forms (e.g.
    // cold start racing a rolling restart). Without a handler in place before
    // that call, the default SIGTERM disposition kills the process outright,
    // hitting the hard-kill path `wait_until_ready`'s docs warn against
    // (aborting the startup `JoinSet` mid-flight) instead of the graceful
    // drain that cancelling the token gives us. The same token is read by the
    // node before and after it becomes ready, so one handler installed here
    // covers both phases.
    eprintln!("BINARY: setting up signal handler...");
    let shutdown_token = initialising.shutdown_token();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        let signal = tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
        };
        // WARN so it's visible at the harness's RUST_LOG=warn: a graceful stop must
        // never be a silent mystery.
        warn!(
            signal,
            "shutdown signal received; cancelling shutdown token (graceful stop)"
        );
        shutdown_token.cancel();
    });
    eprintln!("BINARY: signal handler spawned");

    eprintln!("BINARY: waiting until ready (quorum freshness gate)...");
    let node = match initialising.wait_until_ready().boxed().await {
        Ok(node) => node,
        Err(Error::StartupCancelled) => {
            // Not a fault: the operator asked us to stop (SIGTERM/SIGINT)
            // before the node ever became ready. `wait_until_ready` has
            // already drained and shut Raft down gracefully by the time this
            // error reaches us — exit cleanly rather than as an error.
            warn!(
                "replica node startup cancelled by shutdown signal before reaching quorum \
                 freshness; node has been shut down gracefully"
            );
            eprintln!("BINARY: startup cancelled by shutdown signal, exiting cleanly");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    eprintln!("BINARY: node ready, node_id={}", node.node_id());
    info!(node_id = %node.node_id(), "replica node started");

    eprintln!("BINARY: entering node.run()...");
    match node.run().await {
        Ok(()) => warn!(
            "replica node shut down gracefully (run() returned Ok — the shutdown token was \
             cancelled; see the preceding 'shutdown signal received' log for the signal)"
        ),
        Err(e) => {
            error!(
                error = %e,
                "replica node exited on an UNEXPECTED fault — a background task stopped without a \
                 shutdown request; see the preceding 'background task …' log for which task and why"
            );
            return Err(e.into());
        }
    }
    eprintln!("BINARY: node.run() returned, exiting");

    // Dump CPU flamegraph if profiling was enabled.
    if let (Some(guard), Some(path)) = (profiler, &args.pprof) {
        eprintln!("BINARY: building pprof flamegraph...");
        match guard.report().build() {
            Ok(report) => {
                let svg_file = std::fs::File::create(path).expect("pprof output file");
                report.flamegraph(svg_file).expect("write flamegraph SVG");
                // Collapsed/folded stacks for offline aggregation.
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
                let _ = std::fs::write(path.with_extension("folded"), folded);
                eprintln!("BINARY: wrote pprof flamegraph to {}", path.display());
            }
            Err(e) => eprintln!("BINARY: pprof report build failed: {e}"),
        }
    }

    Ok(())
}
