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
    clap as _, http_body_util as _, hyper as _, hyper_util as _, pprof as _, time as _,
    tokio_metrics as _,
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

// `rand` is used by the `loadgen` example only.
#[cfg(test)]
use rand as _;

mod database;
pub(crate) mod error;
pub(crate) mod follower_node;
#[cfg(test)]
mod layering;
pub(crate) mod leader;
pub(crate) mod learner;
pub(crate) use zslg_net_setup as network;
pub(crate) mod raft;
pub(crate) mod replica;
pub(crate) mod replica_node;
#[cfg(test)]
#[cfg(test)]
mod tests;

pub use crate::error::Error;
pub use crate::follower_node::{FollowerNode, FollowerNodeConfig};
pub use crate::replica_node::{InitialisingReplicaNode, ReplicaNode, ReplicaNodeConfig};
pub use bytesize::ByteSize;
pub use zslg_consensus::RaftStateView;
pub use zslg_consensus_types::{
    ConsensusError, Exit, RaftGeneration, RaftManifestState, RaftState, RaftStateHandle,
    ReplicaMembership, SpareSelectionStrategy,
};
pub use zslg_hlc_clock::HlcClock;
pub use zslg_learner_node::{LearnerNode, LearnerNodeConfig};
pub use zslg_lifecycle_types::{ConfigError, LifecycleError};
pub use zslg_log::{Error as LogApiError, Reader, StableStream, Writer};
pub use zslg_log_types::{Data, DataId, LogError, LogIndex, StableLogEvent, UnstableEvent};
pub use zslg_net_types::{HostPort, NetError, PeerAddr, ServerName};
pub use zslg_node_types::NodeId;
pub use zslg_object_store_config::{IntoArcObjectStore, ObjectStoreConfig};
pub use zslg_observability::{METRIC_NAMESPACE, describe_metrics, histogram_buckets};
pub use zslg_snapshot_types::{
    CompactionTier, CompressionLevel, SnapshotError, SnapshotId, SnapshotMetadata,
};
pub use zslg_time_types::{ClockError, Hlc};
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
