//! Example binary: start a follower node (learner + writer merged) with
//! CLI-configured peers, TLS, storage, and write load.
//!
//! Usage:
//!   cargo run --example follower -- \
//!     --node-name follower-1 \
//!     --peers "replica-1:9000,replica-2:9001,replica-3:9002" \
//!     --metrics-addr 0.0.0.0:9090 \
//!     --ca-cert-file ca.pem \
//!     --data-dir /tmp/follower-1 \
//!     --object-store in-memory

#![allow(
    unused_crate_dependencies,
    reason = "example crates inherit all package deps and use only a subset"
)]

mod write_tracker;

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rand::RngExt;
use tracing::{error, info, warn};
use write_tracker::WriteTracker;

use zslg::{FollowerNode, FollowerNodeConfig, ObjectStoreConfig, PeerAddr, ServerName};

fn index_html(node_name: &str, rps: f64, size_min: u64, size_max: u64) -> String {
    let rps_str = format!("{rps:.0}");
    include_str!("res/writer_control.html")
        .replace("{node_name}", node_name)
        .replace("{rps_str}", &rps_str)
        .replace("{size_min_str}", &size_min.to_string())
        .replace("{size_max_str}", &size_max.to_string())
}

fn stats_fragment(
    target_rps: f64,
    actual_rps: f64,
    errors: u64,
    uptime_secs: u64,
    size_min: u64,
    size_max: u64,
) -> String {
    let minutes = uptime_secs / 60;
    let seconds = uptime_secs % 60;
    format!(
        r#"<table>
<tr><td>Target RPS:</td><td>{target_rps:.1}</td></tr>
<tr><td>Actual RPS:</td><td>{actual_rps:.1}</td></tr>
<tr><td>Errors:</td><td>{errors}</td></tr>
<tr><td>Uptime:</td><td>{minutes}m{seconds}s</td></tr>
<tr><td>Payload size:</td><td>{size_min}–{size_max} bytes</td></tr>
</table>"#,
    )
}

fn parse_form_value(body: &str, key: &str) -> Option<String> {
    for pair in body.split('&') {
        let (k, v) = pair.split_once('=')?;
        if k == key {
            return Some(urlencoding(v).unwrap_or_else(|| v.to_owned()));
        }
    }
    None
}

fn urlencoding(s: &str) -> Option<String> {
    let mut out = String::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        match b {
            b'+' => out.push(' '),
            b'%' => {
                let hi = bytes.next()?;
                let lo = bytes.next()?;
                let byte = u8::from_str_radix(&String::from_utf8_lossy(&[hi, lo]), 16).ok()?;
                out.push(byte as char);
            }
            _ => out.push(b as char),
        }
    }
    Some(out)
}

#[derive(Parser, Debug)]
#[command(name = "follower")]
struct Args {
    /// Node name (also used as the TLS SNI server name).
    #[arg(long)]
    node_name: String,

    /// Comma-separated list of replica addresses in "name:port" format.
    #[arg(long)]
    peers: String,

    /// Address for the Prometheus HTTP metrics listener.
    #[arg(long)]
    metrics_addr: SocketAddr,

    /// Address for the htmx control panel.
    #[arg(long)]
    control_addr: SocketAddr,

    /// Initial target writes per second.
    #[arg(long, default_value = "10")]
    target_rps: f64,

    /// Initial minimum payload size in bytes.
    #[arg(long, default_value = "64")]
    data_size_min: u64,

    /// Initial maximum payload size in bytes.
    #[arg(long, default_value = "4096")]
    data_size_max: u64,

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
}

/// Apply a `--storage-memory-mib` override. One number for the node: the stores
/// are column families in a single DB, so the budget is not a per-store choice.
fn apply_storage_memory(mut config: FollowerNodeConfig, mib: Option<u64>) -> FollowerNodeConfig {
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
    eprintln!("BINARY: follower main() entered");
    let args = Args::parse();
    eprintln!("BINARY: args parsed, node_name={}", args.node_name);

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
            .with_interval(Duration::from_secs(5))
            .describe_and_run(),
    );

    let target_rps_bits = Arc::new(AtomicU64::new(args.target_rps.to_bits()));
    let error_count = Arc::new(AtomicU64::new(0));
    let min_size = Arc::new(AtomicU64::new(args.data_size_min));
    let max_size = Arc::new(AtomicU64::new(args.data_size_max));

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

    let db_path = args.data_dir.join("node");
    eprintln!("BINARY: db_path={}", db_path.display());
    tokio::fs::create_dir_all(&args.data_dir).await?;
    eprintln!("BINARY: data dir created");

    eprintln!("BINARY: building FollowerNodeConfig...");
    let config = FollowerNodeConfig::new(peers, db_path, object_store_config)?;
    let config = apply_storage_memory(config, args.storage_memory_mib);
    eprintln!("BINARY: config built, calling FollowerNode::start...");

    let node = FollowerNode::start(config).await?;
    eprintln!("BINARY: FollowerNode::start OK, node_id={}", node.node_id());
    info!(node_id = %node.node_id(), "follower node started");

    // ── WriteTracker ────────────────────────────────────────────────────────
    // A lagged event stream replays the gap from the durable stable log
    // (resubscribe at the last processed index), so verification keeps full
    // fidelity through chaos/restart churn — no policy knob needed.
    let write_tracker = Arc::new(WriteTracker::new());
    write_tracker
        .clone()
        .spawn(node.reader(), node.shutdown_token());

    // ── Write loop ─────────────────────────────────────────────────────────
    //
    // Bounded mpsc channel (10 k slots) + 100 worker tasks.  The rate limiter
    // pushes unit tokens onto the channel; workers pull them, generate a random
    // blob, and issue a write.  If the channel is full, `try_send` drops the
    // token — memory is bounded by `CAPACITY × max_blob_size`.
    let writer = node.writer();
    let token = node.shutdown_token();
    let target_rps_writer = target_rps_bits.clone();
    let error_count_writer = error_count.clone();
    let min_size_writer = min_size.clone();
    let max_size_writer = max_size.clone();
    let actual_rps = Arc::new(AtomicU64::new(0u64)); // achieved writes/s × 100
    let actual_rps_writer = actual_rps.clone();

    const CAPACITY: usize = 10_000;
    const WORKERS: usize = 100;
    const TICK: Duration = Duration::from_millis(10);

    let (job_tx, job_rx) = tokio::sync::mpsc::channel::<()>(CAPACITY);
    let job_rx = Arc::new(tokio::sync::Mutex::new(job_rx));

    // Spawn worker pool — each worker contends on the shared receiver.
    let mut worker_handles = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let seed = rand::random::<u64>();
        let rx = job_rx.clone();
        let w = writer.clone();
        let ec = error_count_writer.clone();
        let lo = min_size_writer.clone();
        let hi = max_size_writer.clone();
        let cancel = token.clone();
        let tracker = write_tracker.clone();
        let handle = tokio::spawn(async move {
            use rand::SeedableRng;
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            loop {
                let tk = {
                    let mut guard = rx.lock().await;
                    tokio::select! {
                        _ = cancel.cancelled() => None,
                        t = guard.recv() => t,
                    }
                };
                let Some(()) = tk else { break };
                let s = rng.random_range(lo.load(Ordering::Relaxed)..=hi.load(Ordering::Relaxed))
                    as usize;
                let mut blob = vec![0u8; s];
                rng.fill(&mut blob[..]);
                let bytes = Bytes::from(blob);
                match w.write(bytes).await {
                    Ok(data_id) => {
                        tracker.record_write(data_id);
                        info!(s, "write succeeded");
                    }
                    Err(e) => {
                        ec.fetch_add(1, Ordering::Relaxed);
                        warn!(%e, "write failed");
                    }
                }
            }
        });
        worker_handles.push(handle);
    }

    // Rate-limiter: tick at 100 Hz, push tokens into the channel.
    let write_handle = tokio::spawn(async move {
        let mut tokens: f64 = 0.0;
        let mut last_tick = tokio::time::Instant::now();
        let mut writes_this_second: u64 = 0;
        let mut second_start = last_tick;

        loop {
            let now = tokio::time::Instant::now();
            let rps = f64::from_bits(target_rps_writer.load(Ordering::Relaxed));

            let elapsed = now.saturating_duration_since(last_tick);
            last_tick = now;
            tokens = (tokens + elapsed.as_secs_f64() * rps).min(CAPACITY as f64);

            while tokens >= 1.0 {
                if job_tx.try_send(()).is_ok() {
                    tokens -= 1.0;
                    writes_this_second = writes_this_second.saturating_add(1);
                } else {
                    break; // channel full — backpressure
                }
            }

            let since_second = now.saturating_duration_since(second_start);
            if since_second >= Duration::from_secs(1) {
                let rate = (writes_this_second as f64 / since_second.as_secs_f64() * 100.0) as u64;
                actual_rps_writer.store(rate, Ordering::Relaxed);
                writes_this_second = 0;
                second_start = now;
            }

            tokio::select! {
                _ = token.cancelled() => {
                    info!("rate limiter received shutdown, stopping");
                    break;
                }
                _ = tokio::time::sleep_until(now.checked_add(TICK).expect("tick time")) => {}
            }
        }
    });

    // ── htmx control panel ─────────────────────────────────────────────────
    {
        let target_rps_ctrl = target_rps_bits.clone();
        let error_count_ctrl = error_count.clone();
        let actual_rps_ctrl = actual_rps.clone();
        let node_name = args.node_name.clone();
        let min_size_ctrl = min_size.clone();
        let max_size_ctrl = max_size.clone();
        let ctrl_addr = args.control_addr;
        let start = std::time::Instant::now();
        let shutdown = node.shutdown_token();

        tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(ctrl_addr).await {
                Ok(l) => l,
                Err(e) => {
                    warn!(%e, "follower-control: failed to bind to {ctrl_addr}");
                    return;
                }
            };
            info!("follower-control panel: http://{ctrl_addr}");

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("follower-control server stopping");
                        break;
                    }
                    result = listener.accept() => {
                        let (stream, _) = match result {
                            Ok(c) => c,
                            Err(_) => continue,
                        };
                        let t = target_rps_ctrl.clone();
                        let e = error_count_ctrl.clone();
                        let ar = actual_rps_ctrl.clone();
                        let name = node_name.clone();
                        let st = start;
                        let mn = min_size_ctrl.clone();
                        let mx = max_size_ctrl.clone();

                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let svc = hyper::service::service_fn(move |req: hyper::Request<Incoming>| {
                                let t = t.clone();
                                let e = e.clone();
                                let ar = ar.clone();
                                let name = name.clone();
                                let mn = mn.clone();
                                let mx = mx.clone();
                                async move {
                                    match (req.method(), req.uri().path()) {
                                        (&Method::GET, "/") => {
                                            let rps = f64::from_bits(t.load(Ordering::Relaxed));
                                            let lo = mn.load(Ordering::Relaxed);
                                            let hi = mx.load(Ordering::Relaxed);
                                            let html = index_html(&name, rps, lo, hi);
                                            Ok::<_, std::convert::Infallible>(
                                                Response::new(Full::new(Bytes::from(html))),
                                            )
                                        }
                                        (&Method::GET, "/stats") => {
                                            let rps = f64::from_bits(t.load(Ordering::Relaxed));
                                            // `ar` holds a plain integer (writes/s × 100), NOT an
                                            // f64 bit pattern like `t` — reading it via from_bits
                                            // rendered a subnormal ≈0.0 forever.
                                            let act = ar.load(Ordering::Relaxed) as f64 / 100.0;
                                            let errors = e.load(Ordering::Relaxed);
                                            let up = st.elapsed().as_secs();
                                            let lo = mn.load(Ordering::Relaxed);
                                            let hi = mx.load(Ordering::Relaxed);
                                            let html =
                                                stats_fragment(rps, act, errors, up, lo, hi);
                                            Ok(Response::new(Full::new(Bytes::from(html))))
                                        }
                                        // Plain-text programmatic API — script-friendly.
                                        (&Method::GET, "/rate") => {
                                            let rps = f64::from_bits(t.load(Ordering::Relaxed));
                                            Ok(Response::new(Full::new(Bytes::from(
                                                format!("{rps:.1}"),
                                            ))))
                                        }
                                        (&Method::POST, "/rate") => {
                                            let body = req
                                                .collect()
                                                .await
                                                .map(|b| b.to_bytes())
                                                .unwrap_or_default();
                                            let body_str =
                                                String::from_utf8_lossy(&body).into_owned();
                                            if let Ok(new_rps) = body_str.trim().parse::<f64>() {
                                                t.store(new_rps.to_bits(), Ordering::Relaxed);
                                            }
                                            let rps = f64::from_bits(t.load(Ordering::Relaxed));
                                            Ok(Response::new(Full::new(Bytes::from(
                                                format!("{rps:.1}"),
                                            ))))
                                        }
                                        (&Method::POST, "/config") => {
                                            let body = req
                                                .collect()
                                                .await
                                                .map(|b| b.to_bytes())
                                                .unwrap_or_default();
                                            let body_str =
                                                String::from_utf8_lossy(&body).into_owned();
                                            if let Some(val) =
                                                parse_form_value(&body_str, "target_rps")
                                                && let Ok(new_rps) = val.parse::<f64>()
                                            {
                                                t.store(new_rps.to_bits(), Ordering::Relaxed);
                                            }
                                            if let Some(val) =
                                                parse_form_value(&body_str, "data_size_min")
                                                && let Ok(new_min) = val.parse::<u64>()
                                                && new_min > 0
                                            {
                                                mn.store(new_min, Ordering::Relaxed);
                                            }
                                            if let Some(val) =
                                                parse_form_value(&body_str, "data_size_max")
                                                && let Ok(new_max) = val.parse::<u64>()
                                                && new_max > 0
                                            {
                                                mx.store(new_max, Ordering::Relaxed);
                                            }
                                            let rps = f64::from_bits(t.load(Ordering::Relaxed));
                                            // Integer × 100 store — see the /stats handler.
                                            let act = ar.load(Ordering::Relaxed) as f64 / 100.0;
                                            let errors = e.load(Ordering::Relaxed);
                                            let up = st.elapsed().as_secs();
                                            let lo = mn.load(Ordering::Relaxed);
                                            let hi = mx.load(Ordering::Relaxed);
                                            let html =
                                                stats_fragment(rps, act, errors, up, lo, hi);
                                            Ok(Response::new(Full::new(Bytes::from(html))))
                                        }
                                        _ => {
                                            let mut resp =
                                                Response::new(Full::new(Bytes::from("not found")));
                                            *resp.status_mut() = StatusCode::NOT_FOUND;
                                            Ok(resp)
                                        }
                                    }
                                }
                            });
                            if let Err(e) = hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, svc)
                                .await
                            {
                                warn!(%e, "follower-control: connection error");
                            }
                        });
                    }
                }
            }
        });
    }

    // ── Shutdown ────────────────────────────────────────────────────────────
    let shutdown_token = node.shutdown_token();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        let signal = tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
        };
        // WARN so it's visible at RUST_LOG=warn: a graceful stop must never be silent.
        warn!(
            signal,
            "shutdown signal received; cancelling shutdown token (graceful stop)"
        );
        shutdown_token.cancel();
    });

    eprintln!("BINARY: entering node.run()...");
    match node.run().await {
        Ok(()) => warn!(
            "follower node shut down gracefully (run() returned Ok — shutdown token cancelled; \
             see the preceding 'shutdown signal received' log for the signal)"
        ),
        Err(e) => {
            error!(
                error = %e,
                "follower node exited on an UNEXPECTED fault — a background task stopped without a \
                 shutdown request; see the preceding 'background task …' log for which task and why"
            );
            return Err(e.into());
        }
    }

    match write_handle.await {
        Ok(()) => {}
        Err(e) if e.is_cancelled() => info!("write loop task was cancelled"),
        Err(e) => error!(%e, "write loop task panicked"),
    }

    eprintln!("BINARY: node.run() returned, exiting");
    Ok(())
}
