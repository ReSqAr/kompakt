//! The log group's ports: how the replicated log is read, written, stored
//! locally and archived, and how a replica serves it. Re-exports the group's
//! types, so callers import `Data`, `LogIndex` and friends from here.

use error_union::error_union;
use zslg_consensus_types::ConsensusError;
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_net_types::NetError;
use zslg_snapshot_types::SnapshotError;
use zslg_time_types::ClockError;

mod local_store_mut;
mod local_store_view;
mod reader;
mod replica;
mod replica_log;
mod stable_archive;
mod stable_log;
mod writer;

pub use local_store_mut::LocalStoreMut;
pub use local_store_view::{LocalStoreStream, LocalStoreView};
pub use reader::{Reader, StableStream};
pub use replica::{Replica, ReplicaStream, StampedWrite, WriteOutcome};
pub use replica_log::{Completion, ReplicaLog};
pub use stable_archive::{ArchiveBounds, ArchiveStream, ArchiveUpperBound, StableArchiveReader};
pub use stable_log::StableLogSnapshot;
pub use writer::Writer;
pub use zslg_log_types::metrics;
pub use zslg_log_types::{
    Data, DataId, LogError, LogIndex, SnapshotEntry, StableLogEvent, ToCodec, ToStorage,
    UnstableEvent,
};
pub use zslg_time_types::Admission;

error_union! {
    /// What the ports of this crate can fail with. All seven leaves: the
    /// implementations of these ports raise every one of them (a transport
    /// fails with `Net`, the object store with `Log`, a shut-down node with
    /// `Lifecycle`, ...), and a port's error type must hold what any of them
    /// returns. Narrowing needs per-method error types; see
    /// `docs/logs/2026-11-07_post_crate_split.md`.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(LifecycleError),
        Config(ConfigError),
        Clock(ClockError),
        Log(LogError),
        Snapshot(SnapshotError),
        Net(NetError),
        Consensus(ConsensusError),
    }
    bridge zslg_error::Error
}
