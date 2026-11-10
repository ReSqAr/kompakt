//! The metrics capability: installing the recorder, and exporting what the process emits.
//!
//! ```text
//!   PrometheusBuilder
//!     -> zslg::configure_prometheus     its histograms' buckets
//!     -> nvzn::configure_prometheus     its histograms' buckets
//!     -> set_buckets_for_metric(h)      every histogram the capabilities and apps declared
//!     -> install_recorder()             one global recorder, once
//!     -> zslg::describe_metrics()       help text and units, after the install
//!     -> nvzn::describe_metrics()
//! ```
//!
//! A library *emits*, an application *installs*: neither zslg nor nvzn touches the global recorder, so until
//! a binary installs one every `counter!` and `histogram!` in the process writes to nowhere. This crate knows
//! no other capability: each declares its histograms as data (`HISTOGRAMS`) and describes its own series, and
//! the host hands the list in. Descriptions come *after* the install: sent to the no-op recorder they are dropped.
//!
//! # Why a second listener
//!
//! `/metrics` says how big the store is, how far the fold has got, how many sessions were minted. None of
//! that belongs on the public listener, and a path on the main router is one misplaced reverse proxy rule
//! away from being served to the internet. It binds to loopback by default, so exposing it is something an
//! operator has to choose and type.

pub mod settings;

use axum::response::IntoResponse;
use axum::{Router, routing::get};
use btrb_types::Histogram;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

/// Install the recorder with the bounds of every histogram in `histograms`, then describe zslg's, nvzn's and
/// the process's own series. A capability describes its own series after this returns.
pub fn install(histograms: &[Histogram]) -> Result<PrometheusHandle, Box<dyn Error>> {
    let builder = PrometheusBuilder::new();
    let builder = zslg::configure_prometheus(builder)?;
    let mut builder = nvzn::configure_prometheus(builder)?;
    for h in histograms {
        builder = builder.set_buckets_for_metric(Matcher::Full(h.metric.to_owned()), h.bounds)?;
    }

    let handle = builder.install_recorder()?;

    zslg::describe_metrics();
    nvzn::describe_metrics();
    metrics::describe_gauge!(
        "process_memory_rss_bytes",
        "Resident set size of this process, in bytes"
    );
    metrics::describe_counter!(
        "process_cpu_seconds_total",
        metrics::Unit::Seconds,
        "CPU time this process has used, user and system, in whole seconds"
    );
    Ok(handle)
}

/// Serve `/metrics` on `addr` until the listener fails. Process memory and CPU are sampled at scrape time.
pub async fn serve(addr: SocketAddr, handle: PrometheusHandle) -> Result<(), Box<dyn Error>> {
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
