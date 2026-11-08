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
mod projection;
mod reader;
mod resultset;
mod store;
mod view;

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
pub use nvzn_types::{
    APP_ID_LEN, Error, EventSet, INDEX_ID_LEN, IndexId, PROJECTION_ID_LEN, PROJECTION_TYPE_ID_LEN,
    ProjectionId, ProjectionTypeId, StopReason, SubEventOf, Tenant, TenantId, WithWireFormat,
    chained_from, define_events,
};
pub(crate) use nvzn_types::{FoldState, SubscriptionItem, Subscriptions, subset};
pub use projection::{AnyIndex, Index, Indices, Projection, ProjectorManager, Ref, Write};
pub use reader::{Reader, Snapshot};
pub use resultset::{IndexJoin, Lazy, LazyRows, ResultSet};
pub use store::{
    ArcReadTransaction, ArcStore, CommitBatch, ConfigHash, InMemoryStore, ReadTransaction,
    ReadTransactionExt, RocksDBStore, Row, Rows, Store, StoreConfig, Watermark, WriteTransaction,
    WriteTransactionExt, WriteTransactionImpl,
};
pub use view::View;
