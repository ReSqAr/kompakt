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
#[cfg(test)]
use tracing as _;
use {async_trait as _, futures as _, tokio as _, tokio_util as _};
#[cfg(test)]
use {metrics_util as _, proptest as _, tempfile as _, tracing_subscriber as _};

#[cfg(test)]
mod events_tests;
mod metrics;
#[cfg(test)]
mod projection_tests;
#[cfg(test)]
mod resultset_tests;
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

#[cfg(feature = "prometheus")]
pub use metrics::configure_prometheus;
pub use metrics::{METRIC_NAMESPACE, describe_metrics, histogram_buckets};
#[cfg(any(test, feature = "testing"))]
pub use nvzn_dispatcher::InMemoryStore;
pub use nvzn_dispatcher::{
    AssociatedProjectorManager, Dispatcher, IndexJoin, Lazy, LazyRows, Live, LiveLimits,
    ProjectorManager, Reader, ResultSet, RocksDBStore, Snapshot, Wait, assert_live, is_live,
};
pub use nvzn_store::{
    APP_ID_LEN, AnyIndex, ArcReadTransaction, ArcStore, CommitBatch, ConfigHash, Encoder, Error,
    EventSet, EventWriter, INDEX_ID_LEN, Index, IndexId, Indices, PROJECTION_ID_LEN,
    PROJECTION_TYPE_ID_LEN, Projection, ProjectionId, ProjectionTypeId, ReadTransaction,
    ReadTransactionExt, Ref, Row, Rows, StopReason, Store, StoreConfig, SubEventOf, Syncer, Tenant,
    TenantId, View, Watermark, WithWireFormat, Write, WriteTransaction, WriteTransactionExt,
    WriteTransactionImpl, chained_from, define_events,
};
