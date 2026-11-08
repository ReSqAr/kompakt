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
    /// What the ports of this crate can fail with. Holds every leaf error for
    /// now; narrowed to the leaves its implementations actually raise once they
    /// have moved out of `zslg`.
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
    from zslg_error::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    into zslg_error::Error
}
