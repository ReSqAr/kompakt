#![forbid(unsafe_code)]
#![deny(
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    dead_code
)]

// These dev-dependencies exist only for `examples/` (Cargo has no example-only
// dep section, so they sit in `[dev-dependencies]` and are linked into the test
// build too). Whitelist exactly these as used, so `unused_crate_dependencies`
// still flags any *other* dev-dep that goes unused.
#[cfg(test)]
use tracing_subscriber as _;
#[cfg(test)]
use {
    bytes as _, clap as _, crossbeam_combinators as _, dashmap as _, futures as _,
    http_body_util as _, hyper as _, hyper_util as _, metrics as _, object_store as _, pprof as _,
    shutdown as _, time as _, tokio as _, tokio_metrics as _, tokio_util as _,
};

// Used by `examples/common`, not here; a normal dependency only because Cargo
// has no optional dev-dependencies.
#[cfg(feature = "tokio-console")]
use console_subscriber as _;

// The `jemalloc` feature publishes jemalloc's `stats.*` as gauges
// (`observability.rs`). It does not declare the allocator: that is the
// process's, so the binary declares `#[global_allocator]` (`examples/common`
// for the harness) and the gauges mean something only when it did. The
// dependency is used there, not here; a normal dependency only because Cargo
// has no optional dev-dependencies.
#[cfg(feature = "jemalloc")]
use tikv_jemalloc_ctl as _;
#[cfg(feature = "jemalloc")]
use tikv_jemallocator as _;

// Used by the examples only.
#[cfg(test)]
use rand as _;
#[cfg(test)]
use tracing as _;

pub use bytesize::ByteSize;
pub use zslg_consensus::NodeId;
pub use zslg_consensus::RaftStateView;
pub use zslg_consensus::{
    ConsensusError, Exit, RaftGeneration, RaftManifestState, RaftState, RaftStateHandle,
    ReplicaMembership, SpareSelectionStrategy,
};
pub use zslg_writer_node::Error;

/// An in-process log serving [`Reader`] and [`Writer`], so a consumer can test
/// against zslg without a cluster. Behind the `testing` feature.
#[cfg(feature = "testing")]
pub mod testing {
    pub use zslg_log_fake::{TestLog, data_id};
}
pub use zslg_follower_node::{FollowerNode, FollowerNodeConfig};
pub use zslg_learner_node::{LearnerNode, LearnerNodeConfig};
pub use zslg_log::{Data, DataId, LogError, LogIndex, StableLogEvent, UnstableEvent};
pub use zslg_log::{Error as LogApiError, Reader, StableStream, Writer};
pub use zslg_net::{HostPort, NetError, PeerAddr, ServerName};
pub use zslg_object_store_config::{IntoArcObjectStore, ObjectStoreConfig};
pub use zslg_observability::{METRIC_NAMESPACE, describe_metrics, histogram_buckets};
pub use zslg_replica_node::{InitialisingReplicaNode, ReplicaNode, ReplicaNodeConfig};
pub use zslg_snapshot::{
    CompactionTier, CompressionLevel, SnapshotError, SnapshotId, SnapshotMetadata,
};
pub use zslg_time::{ClockError, Hlc};
pub use zslg_writer_node::{ConfigError, LifecycleError};
pub use zslg_writer_node::{WriterNode, WriterNodeConfig};

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
    builder: metrics_exporter_prometheus::PrometheusBuilder,
) -> Result<metrics_exporter_prometheus::PrometheusBuilder, Error> {
    zslg_observability::configure_prometheus(builder).map_err(Error::from)
}
