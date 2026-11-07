//! The consensus engine, as the rest of the workspace sees it. This is the only
//! crate that depends on `openraft`; everything else imports it from here, the
//! way storage code imports RocksDB from `rocksdb-kit`. Replacing or upgrading
//! the engine therefore touches this crate and the adapters that implement its
//! traits, not every signature that mentions a request or an error.
//!
//! The modules are re-exported under openraft's own names so engine
//! documentation stays applicable.

pub use openraft::{error, network, raft, storage};

pub use openraft::declare_raft_types;
pub use openraft::{
    CommittedLeaderId, Config, Entry, EntryPayload, LogId, LogState, Membership, OptionalSend,
    Raft, RaftLogId, RaftLogReader, RaftMetrics, RaftNetwork, RaftNetworkFactory,
    RaftSnapshotBuilder, RaftStorage, Snapshot, SnapshotMeta, SnapshotPolicy, StorageError,
    StorageIOError, StoredMembership, TokioRuntime, Vote,
};
