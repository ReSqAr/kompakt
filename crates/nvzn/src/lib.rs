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

use crossbeam_channel as _;
use {async_trait as _, futures as _, tokio as _, tokio_util as _};
#[cfg(test)]
use {metrics_util as _, proptest as _, tempfile as _, tracing_subscriber as _};

mod dispatcher;
#[cfg(test)]
mod events_tests;
mod live;
mod metrics;
#[cfg(test)]
mod projection_tests;
mod reader;
mod resultset;
#[cfg(test)]
mod store_tests;

/// An in-process log and store, for testing against nvzn without a cluster.
///
/// Behind the `testing` feature, which a consumer enables in its
/// `[dev-dependencies]`.
#[cfg(any(test, feature = "testing"))]
pub mod testing;

#[cfg(test)]
pub(crate) mod testsupport;

pub use dispatcher::{AssociatedProjectorManager, Dispatcher, Encoder, EventWriter, Syncer};
pub use live::{Live, LiveLimits, Wait, is_live};
#[cfg(feature = "prometheus")]
pub use metrics::configure_prometheus;
pub use metrics::{METRIC_NAMESPACE, describe_metrics, histogram_buckets};
pub use nvzn_projector::ProjectorManager;
pub use nvzn_store::{AnyIndex, Index, Indices, Projection, Ref, View, Write};
pub use nvzn_store::{
    ArcReadTransaction, ArcStore, CommitBatch, ConfigHash, ReadTransaction, ReadTransactionExt,
    Row, Rows, Store, StoreConfig, Watermark, WriteTransaction, WriteTransactionExt,
    WriteTransactionImpl,
};
pub use nvzn_store_fake::InMemoryStore;
pub use nvzn_store_rocksdb::RocksDBStore;
pub use nvzn_types::{
    APP_ID_LEN, Error, EventSet, INDEX_ID_LEN, IndexId, PROJECTION_ID_LEN, PROJECTION_TYPE_ID_LEN,
    ProjectionId, ProjectionTypeId, StopReason, SubEventOf, Tenant, TenantId, WithWireFormat,
    chained_from, define_events,
};
pub(crate) use nvzn_types::{FoldState, SubscriptionItem, Subscriptions, subset};
pub use reader::{Reader, Snapshot};
pub use resultset::{IndexJoin, Lazy, LazyRows, ResultSet};
