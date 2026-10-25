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

pub(crate) mod api;
pub(crate) mod channel;
mod database;
pub(crate) mod error;
pub(crate) mod follower_node;
pub(crate) mod framing;
pub(crate) mod hlc;
pub(crate) mod ids;
pub(crate) mod leader;
pub(crate) mod learner;
pub(crate) mod learner_node;
pub(crate) mod meta;
pub(crate) mod metrics;
pub(crate) mod network;
pub(crate) mod object_store;
pub(crate) mod observability;
pub(crate) mod quorum;
pub(crate) mod raft;
pub(crate) mod replica;
pub(crate) mod replica_node;
pub(crate) mod shutdown;
#[cfg(test)]
mod tests;
mod unit;
pub(crate) mod warn_throttle;
pub(crate) mod writer;
pub(crate) mod writer_node;

pub use crate::api::{RaftStateView, Reader, Writer};
pub use crate::channel::{CancelHandle, Cancellation, Channels, End as ChannelEnd};
pub use crate::error::Error;
pub use crate::follower_node::{FollowerNode, FollowerNodeConfig};
pub use crate::hlc::{Hlc, HlcClock};
pub use crate::ids::{
    DataId, HostPort, LogIndex, NodeId, PeerAddr, RaftGeneration, ServerName, SnapshotId,
};
pub use crate::leader::CompactionTier;
pub use crate::learner_node::{LearnerNode, LearnerNodeConfig};
pub use crate::meta::{
    Data, Exit, RaftManifestState, RaftState, ReplicaMembership, SnapshotMetadata, StableLogEvent,
    UnstableEvent,
};
pub use crate::object_store::{CompressionLevel, IntoArcObjectStore, ObjectStoreConfig};
#[cfg(feature = "prometheus")]
pub use crate::observability::configure_prometheus;
pub use crate::observability::{METRIC_NAMESPACE, describe_metrics, histogram_buckets};
pub use crate::raft::{RaftStateHandle, SpareSelectionStrategy};
pub use crate::replica_node::{InitialisingReplicaNode, ReplicaNode, ReplicaNodeConfig};
pub use crate::unit::ByteSize;
pub use crate::writer_node::{WriterNode, WriterNodeConfig};

pub(crate) fn jitter(d: std::time::Duration) -> std::time::Duration {
    let ms = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let quarter = ms.checked_div(4).unwrap_or(1).max(1);
    let spread = quarter.saturating_mul(2);
    let range = spread.saturating_add(1);
    let delta = rand::random::<u64>().checked_rem(range).unwrap_or(0);
    let ms = ms.saturating_sub(quarter).saturating_add(delta);
    std::time::Duration::from_millis(ms)
}
