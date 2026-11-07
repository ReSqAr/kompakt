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
use tikv_jemallocator as _;

mod database;
pub(crate) mod error;
pub(crate) mod follower_node;
pub(crate) mod hlc;
#[cfg(test)]
mod layering;
pub(crate) mod leader;
pub(crate) mod learner;
pub(crate) mod learner_node;
pub(crate) mod metrics;
pub(crate) mod network;
pub(crate) mod observability;
pub(crate) mod quorum;
pub(crate) mod raft;
pub(crate) mod replica;
pub(crate) mod replica_node;
#[cfg(test)]
#[cfg(test)]
mod tests;
pub(crate) mod writer;
pub(crate) mod writer_node;

pub use crate::error::Error;
pub use crate::follower_node::{FollowerNode, FollowerNodeConfig};
pub use crate::hlc::HlcClock;
pub use crate::learner_node::{LearnerNode, LearnerNodeConfig};
#[cfg(feature = "prometheus")]
pub use crate::observability::configure_prometheus;
pub use crate::observability::{METRIC_NAMESPACE, describe_metrics, histogram_buckets};
pub use crate::replica_node::{InitialisingReplicaNode, ReplicaNode, ReplicaNodeConfig};
pub use crate::writer_node::{WriterNode, WriterNodeConfig};
pub use bytesize::ByteSize;
pub use zslg_consensus::RaftStateView;
pub use zslg_consensus_types::{
    ConsensusError, Exit, RaftGeneration, RaftManifestState, RaftState, RaftStateHandle,
    ReplicaMembership, SpareSelectionStrategy,
};
pub use zslg_lifecycle_types::{ConfigError, LifecycleError};
pub use zslg_log::{Error as LogApiError, Reader, StableStream, Writer};
pub use zslg_log_types::{Data, DataId, LogError, LogIndex, StableLogEvent, UnstableEvent};
pub use zslg_net_types::{HostPort, NetError, PeerAddr, ServerName};
pub use zslg_node_types::NodeId;
pub use zslg_object_store_config::{IntoArcObjectStore, ObjectStoreConfig};
pub use zslg_snapshot_types::{
    CompactionTier, CompressionLevel, SnapshotError, SnapshotId, SnapshotMetadata,
};
pub use zslg_time_types::{ClockError, Hlc};

pub(crate) fn jitter(d: std::time::Duration) -> std::time::Duration {
    let ms = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let quarter = ms.checked_div(4).unwrap_or(1).max(1);
    let spread = quarter.saturating_mul(2);
    let range = spread.saturating_add(1);
    let delta = rand::random::<u64>().checked_rem(range).unwrap_or(0);
    let ms = ms.saturating_sub(quarter).saturating_add(delta);
    std::time::Duration::from_millis(ms)
}
