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

use {async_trait as _, futures as _, tokio as _, tokio_util as _};
#[cfg(test)]
use {metrics_util as _, proptest as _, tempfile as _, tracing_subscriber as _};

mod dispatcher;
mod events;
mod ids;
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
pub use events::{SubEventOf, WithWireFormat};
pub use ids::{IndexId, ProjectionId, ProjectionTypeId, Tenant, TenantId};
#[cfg(feature = "prometheus")]
pub use metrics::configure_prometheus;
pub use metrics::{METRIC_NAMESPACE, StopReason, describe_metrics, histogram_buckets};
pub use projection::{AnyIndex, Index, Indices, Projection, ProjectorManager, Ref, Write};
pub use reader::{Reader, Snapshot};
pub use resultset::{IndexJoin, Lazy, LazyRows, ResultSet};
pub use store::{
    ArcReadTransaction, ArcStore, CommitBatch, ConfigHash, InMemoryStore, ReadTransaction,
    ReadTransactionExt, RocksDBStore, Row, Rows, Store, StoreConfig, Watermark, WriteTransaction,
    WriteTransactionExt, WriteTransactionImpl,
};
pub use view::View;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("postcard error {0}")]
    PostCard(#[from] postcard::Error),
    #[error(
        "data corruption: data ID needs to be monotonic - went from {max_data_id:?} to {data_id:?} at index {index:?}"
    )]
    NonMonotoneDataId {
        max_data_id: zslg::DataId,
        data_id: zslg::DataId,
        index: zslg::LogIndex,
    },
    #[error("poisoned lock")]
    Poison(String),
    #[error("rocksdb error {0}")]
    RocksDB(#[from] rust_rocksdb::Error),
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("observability setup: {0}")]
    Observability(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("dispatcher task failed: {0}")]
    TaskJoin(String),
    #[error("rocksdb column family {0} is missing")]
    RocksDBColumnFamilyMissing(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("an tenant with ID {0} is already registered")]
    TenantAlreadyRegistered(u128),
    #[error("payload is {0} bytes, too short to carry the {1}-byte tenant ID prefix")]
    PayloadTooShort(usize, usize),
    #[error("config is {0} bytes")]
    ConfigTooShort(usize),
    #[error("no handler is registered for tenant ID {0}")]
    UnknownTenant(u128),
    #[error("projection {type_name} keyed its row {expected:?} but built one with ID {actual:?}")]
    IncorrectProjectionID {
        type_name: &'static str,
        expected: ProjectionId,
        actual: ProjectionId,
    },
    #[error("a projection with ID {0} is already registered")]
    ProjectionAlreadyRegistered(u32),
    #[error(
        "projection {type_name} (type ID {type_id}) is not registered for tenant ID {tenant_id}, \
         so nothing would ever be delivered for it"
    )]
    ProjectionNotRegistered {
        type_name: &'static str,
        type_id: u32,
        tenant_id: u128,
    },
    #[error("could not look up projection ID {pid:?} of type {type_name}")]
    ReferenceNotFound {
        type_name: &'static str,
        pid: ProjectionId,
    },
    #[error(transparent)]
    Zslg(#[from] zslg::Error),
    #[error("subscription for dispatcher was aborted")]
    SubscriptionAborted,
    #[error(
        "the projection fold stopped ({}): this store no longer follows the log, and will not \
         resume without a new dispatcher",
        .0.as_str()
    )]
    FoldStopped(StopReason),
    #[error(
        "{0} subscriptions are already waiting for the fold to take them up after its next \
         commit"
    )]
    TooManyPendingSubscriptions(usize),
    #[error(transparent)]
    TokioRecv(#[from] tokio::sync::watch::error::RecvError),
    #[error(transparent)]
    TokioSend(#[from] tokio::sync::watch::error::SendError<Option<zslg::Hlc>>),
    #[error("an index with ID {0} is already registered")]
    IndexAlreadyRegistered(u32),
}
