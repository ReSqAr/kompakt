//! Process setup for the example binaries: one subscriber, one recorder.
//!
//! ```text
//!   tracing:     EnvFilter(RUST_LOG, default info) + fmt|json + panic hook
//!   prometheus:  PrometheusBuilder -> zslg::configure_prometheus -> install
//!                                  -> zslg::describe_metrics()
//! ```
//!
//! Both globals are install-once and process-wide, which is why a binary
//! configures them and `zslg` does not: a library installing either would
//! decide for the whole process, including every other library in it. What the
//! library does own — which histogram gets which buckets — it applies to a
//! builder the caller still holds.
//!
//! A second instrumented crate composes by threading the same builder through
//! its own `configure_prometheus` before `install`.

#![allow(
    dead_code,
    reason = "each example compiles this module and uses only the part it needs"
)]
#![allow(
    unreachable_pub,
    reason = "a private module in a binary crate; `pub` is how the binary reaches it"
)]

use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use zslg::Error;

// The harness runs on jemalloc (`--features jemalloc`): freed pages go back to
// the OS far more readily under churn than with glibc's per-arena retention,
// tunable through `MALLOC_CONF`. `heap-profiling` additionally builds jemalloc
// with `--enable-prof` (`MALLOC_CONF=prof:true`, `harness/entrypoint.sh`).
// zslg publishes jemalloc's stats under the same feature but leaves the
// allocator to the binary, which is this module's.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub fn init_tracing() -> Result<(), Error> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let use_json = std::env::var("LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    // Base registry. In the harness build (feature `tokio-console` + the
    // `--cfg tokio_unstable` it sets) attach the tokio-console layer when the
    // `TOKIO_CONSOLE` env var is set. It binds 127.0.0.1:6669 by default
    // (override via `TOKIO_CONSOLE_BIND`); connect from inside the pod with the
    // `tokio-console` CLI. The console layer is deliberately left unfiltered,
    // so the `filter` below is applied *per-layer* on the fmt layer only —
    // a `RUST_LOG=warn` setting must not starve console of the `tokio`/`runtime`
    // trace events it needs.
    let registry = tracing_subscriber::registry();
    #[cfg(feature = "tokio-console")]
    let registry = registry
        .with(std::env::var_os("TOKIO_CONSOLE").map(|_| console_subscriber::spawn().boxed()));

    let fmt_layer = fmt::layer().with_target(true);
    let fmt_layer = if use_json {
        fmt_layer.json().with_filter(filter).boxed()
    } else {
        fmt_layer.with_filter(filter).boxed()
    };
    registry
        .with(fmt_layer)
        .try_init()
        .map_err(|e| Error::Observability(format!("tracing init failed: {e}")))?;

    install_panic_logger();
    Ok(())
}

/// Route panics through `tracing` (ERROR) with location + message + backtrace, then
/// chain the previous hook (keeps the default stderr print). A panic in a spawned
/// task otherwise surfaces only as an opaque `JoinError` with the cause lost; this
/// makes it land in the log pipeline (Loki) so a crash is diagnosable.
fn install_panic_logger() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map_or_else(|| "unknown".to_owned(), std::string::ToString::to_string);
        let message = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic payload>");
        let backtrace = std::backtrace::Backtrace::force_capture();
        tracing::error!(
            location = %location,
            message = %message,
            backtrace = %backtrace,
            "thread panicked"
        );
        prev(info);
    }));
}

/// Install the Prometheus exporter on `addr`.
///
/// The whole recipe, and the reason [`zslg::configure_prometheus`] exists: a
/// consumer writes these four lines, not a bucket table.
pub fn init_prometheus(addr: SocketAddr) -> Result<(), Error> {
    let builder = PrometheusBuilder::new().with_http_listener(addr);
    zslg::configure_prometheus(builder)?
        .install()
        .map_err(|e| Error::Observability(format!("prometheus exporter install failed: {e}")))?;
    zslg::describe_metrics();
    Ok(())
}
