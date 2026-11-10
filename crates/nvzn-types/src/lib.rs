//! Vocabulary shared by every nvzn crate: identifiers, event sets, the error,
//! why a fold stops, and the cells the dispatcher shares with its readers.

pub mod metrics;

mod events;
mod fold_state;
mod handles;
mod ids;
mod subscriptions;

pub use events::{EventSet, SubEventOf, WithWireFormat, contains, subset};
pub use fold_state::FoldState;
pub use handles::{Encoder, EventWriter, Syncer};
pub use ids::{
    APP_ID_LEN, INDEX_ID_LEN, IndexId, PROJECTION_ID_LEN, PROJECTION_TYPE_ID_LEN, ProjectionId,
    ProjectionTypeId, Tenant, TenantId,
};
pub use subscriptions::{SubscriptionItem, SubscriptionKey, Subscriptions};

/// Why the fold loop stopped. Every variant means the read model is frozen
/// from that point on; `cancelled` is the one that was asked for. Carried to readers as [`crate::Error::FoldStopped`], so a
/// call that cannot be served says which of these ended the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    SubscriptionAborted,
    Cancelled,
    /// A `data_id` went backwards: zslg's ordering guarantee is broken.
    NonMonotoneDataId,
    /// A payload this dispatcher cannot decode. Permanent — see the crate
    /// review's §1: the entry stays in the log and a restart re-reads it.
    PoisonEntry,
    StoreError,
    /// The store went away under the fold — the dispatcher was dropped.
    StoreDropped,
    /// zslg handed the fold an error item instead of an entry.
    Upstream,
    /// The fold task itself died — a projection handler panicked under
    /// `panic = "unwind"` — without reaching any of the other exits.
    Panicked,
    /// A live view's unstable-tail subscription ended: zslg resyncs a slow
    /// consumer inside the stream, so an end means the node's reader broke,
    /// and a live view that stopped integrating would serve a stale world as
    /// fresh. Stops the dispatcher so the process goes down.
    UnstableTailEnded,
    /// A live view's cold-start scan of the stable log ended before reaching
    /// the stable watermark it set out for; as `UnstableTailEnded`.
    LiveScanEnded,
    /// A live view's merged scan/tail stream ended; the `End` names the source.
    LiveStreamEnded,
}

impl StopReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SubscriptionAborted => "subscription_aborted",
            Self::Cancelled => "cancelled",
            Self::NonMonotoneDataId => "non_monotone_data_id",
            Self::PoisonEntry => "poison_entry",
            Self::StoreError => "store_error",
            Self::StoreDropped => "store_dropped",
            Self::Upstream => "upstream",
            Self::Panicked => "panicked",
            Self::UnstableTailEnded => "unstable_tail_ended",
            Self::LiveScanEnded => "live_scan_ended",
            Self::LiveStreamEnded => "live_stream_ended",
        }
    }
}

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

impl From<zslg::LogApiError> for Error {
    fn from(error: zslg::LogApiError) -> Self {
        Self::Zslg(error.into())
    }
}
