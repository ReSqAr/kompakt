//! Exporting the metrics zslg and nvzn already emit.
//!
//! Both crates are instrumented throughout and both follow the same rule: a
//! library *emits*, an application *installs*. Neither touches the global
//! recorder, so until a binary installs one every `counter!` and `histogram!`
//! in the process is a no-op writing to nowhere. This is that binary, and this
//! is the install.
//!
//! ```text
//!   PrometheusBuilder
//!     -> zslg::configure_prometheus     its histograms' buckets
//!     -> nvzn::configure_prometheus     its histograms' buckets
//!     -> install_recorder()             one global recorder, once
//!     -> zslg::describe_metrics()       help text and units, after the install
//!     -> nvzn::describe_metrics()
//! ```
//!
//! Buckets are threaded through both crates before the install because each
//! sets an exact `Matcher::Full` per histogram, so neither can reach the
//! other's series and the order does not matter. The descriptions come *after*
//! the install: sent to the no-op recorder they are dropped, not replayed.
//!
//! # Why a second listener
//!
//! `/metrics` says how big the store is, how far the fold has got, how many
//! sessions were minted and how long logins take. None of that belongs on the
//! public listener, and a path on the main router is one misplaced reverse
//! proxy rule away from being served to the internet. It binds to loopback by
//! default, so exposing it is something an operator has to choose and type.

use axum::response::IntoResponse;
use axum::{Router, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

pub(crate) fn install(apps: &[crate::Entry]) -> Result<PrometheusHandle, Box<dyn Error>> {
    let builder = PrometheusBuilder::new();
    let builder = zslg::configure_prometheus(builder)?;
    let builder = nvzn::configure_prometheus(builder)?;
    let builder = declared_buckets(builder, apps)?;
    let builder = http_buckets(builder)?;
    let builder = read_after_write_buckets(builder)?;

    let handle = builder.install_recorder()?;

    zslg::describe_metrics();
    nvzn::describe_metrics();
    crate::describe_metrics(apps);
    metrics::describe_gauge!(
        "process_memory_rss_bytes",
        "Resident set size of this process, in bytes"
    );
    metrics::describe_counter!(
        "process_cpu_seconds_total",
        metrics::Unit::Seconds,
        "CPU time this process has used, user and system, in whole seconds"
    );
    metrics::describe_gauge!(
        "btrb_fold_up",
        "1 while the nvzn projection fold is advancing, 0 once it has stopped"
    );
    metrics::describe_counter!(
        "btrb_http_requests_total",
        "HTTP requests, by method, matched route and status"
    );
    metrics::describe_histogram!(
        "btrb_read_after_write_wait_seconds",
        metrics::Unit::Seconds,
        "Read-after-write waits, by app, site (write: the writer's own view; read: \
         the next request's) and outcome (seen, timed_out)"
    );
    metrics::describe_histogram!(
        "btrb_http_request_duration_seconds",
        metrics::Unit::Seconds,
        "HTTP request duration, by method and matched route"
    );

    Ok(handle)
}

/// HTTP request latency buckets: a fast static asset through a slow streamed
/// body's response head.
fn http_buckets(builder: PrometheusBuilder) -> Result<PrometheusBuilder, Box<dyn Error>> {
    use metrics_exporter_prometheus::Matcher;
    const SECONDS: &[f64] = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    ];
    Ok(builder.set_buckets_for_metric(
        Matcher::Full("btrb_http_request_duration_seconds".to_owned()),
        SECONDS,
    )?)
}

/// Read-after-write wait buckets: an entry already held answers in
/// microseconds, the cap (`BTRB_READ_AFTER_WRITE_WAIT`) a fraction of a second.
fn read_after_write_buckets(
    builder: PrometheusBuilder,
) -> Result<PrometheusBuilder, Box<dyn Error>> {
    use metrics_exporter_prometheus::Matcher;
    const SECONDS: &[f64] = &[
        0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0,
    ];
    Ok(builder.set_buckets_for_metric(
        Matcher::Full("btrb_read_after_write_wait_seconds".to_owned()),
        SECONDS,
    )?)
}

/// The bounds every app and the cache declare for their histograms.
fn declared_buckets(
    builder: PrometheusBuilder,
    apps: &[crate::Entry],
) -> Result<PrometheusBuilder, Box<dyn Error>> {
    use metrics_exporter_prometheus::Matcher;
    let mut builder = builder;
    for h in crate::histograms(apps) {
        builder = builder.set_buckets_for_metric(Matcher::Full(h.metric.to_owned()), h.bounds)?;
    }
    Ok(builder)
}

/// Install the recorder and serve `/metrics` on `addr`, or say that export is
/// off. Called once, by the role that was started.
pub(super) fn start(addr: Option<SocketAddr>, apps: &[crate::Entry]) -> Result<(), Box<dyn Error>> {
    match addr {
        Some(addr) => {
            let handle = install(apps)?;
            tokio::spawn(async move {
                if let Err(e) = serve(addr, handle).await {
                    tracing::error!(%e, "the metrics listener stopped");
                }
            });
        }
        None => tracing::info!("metrics export is off"),
    }
    Ok(())
}

pub(crate) async fn serve(
    addr: SocketAddr,
    handle: PrometheusHandle,
) -> Result<(), Box<dyn Error>> {
    let handle = Arc::new(handle);

    let app = Router::new().route(
        "/metrics",
        get(move || {
            let handle = Arc::clone(&handle);
            async move {
                record_process_memory();
                record_process_cpu();
                match tokio::task::spawn_blocking(move || handle.render()).await {
                    Ok(body) => (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "text/plain; version=0.0.4; charset=utf-8",
                        )],
                        body,
                    )
                        .into_response(),
                    Err(e) => {
                        tracing::error!(?e, "rendering metrics failed");
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                }
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "serving metrics");
    axum::serve(listener, app).await?;

    Ok(())
}

/// The process's resident set size, sampled at scrape time so every exposition
/// carries the current value. A platform metric (not app's): it says how much
/// memory the process itself holds. The `instance` label is added by the
/// scraper, which knows which target this was.
/// The CPU time used so far, as the counter it is. zslg exports the same as a
/// gauge (`process_cpu_seconds`), which a rate cannot rely on; this is the
/// standard counter. Whole seconds (a `metrics` counter is an integer): a
/// rate over a minute is off by at most 1/60 of a core.
fn record_process_cpu() {
    if let Some(seconds) =
        cpu_seconds(&std::fs::read_to_string("/proc/self/stat").unwrap_or_default())
    {
        metrics::counter!("process_cpu_seconds_total").absolute(seconds);
    }
}

/// User plus system time from `/proc/self/stat`, in whole seconds. The
/// fields after the command's closing parenthesis (the command may hold
/// spaces): utime is the 12th, stime the 13th, in clock ticks of 1/100 s
/// (`_SC_CLK_TCK` is 100 on every Linux architecture).
fn cpu_seconds(stat: &str) -> Option<u64> {
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime.checked_add(stime)? / 100)
}

#[allow(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "byte counts would only lose precision beyond 8 PiB"
)]
fn record_process_memory() {
    if let Some(stats) = memory_stats::memory_stats() {
        metrics::gauge!("process_memory_rss_bytes").set(stats.physical_mem as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::cpu_seconds;

    /// utime 250 + stime 120 ticks: 3.7 s, counted as 3; a command with a
    /// space and a parenthesis does not shift the fields.
    #[test]
    fn cpu_time_is_read_from_proc_stat() {
        let stat = "42 (host (srv) x) S 1 42 42 0 -1 4194560 100 0 0 0 250 120 0 0 20 0 9 0 5";
        assert_eq!(cpu_seconds(stat), Some(3));
        assert_eq!(cpu_seconds("garbage"), None);
    }
}
