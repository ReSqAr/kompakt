//! Node-local storage: laying out the storage root, and the boot step that must
//! happen before anything opens under it.
//!
//! A node opens **one** RocksDB at the storage root ([`storage::Storage`]) and
//! every node-local store — including the write-once node identity — is a
//! column family in it. [`Subsystem`] is the only place those family names are
//! written down; there is nothing under the root but the one DB.

mod node_id;
pub(crate) mod storage;

pub(crate) use self::node_id::load_or_create_node_id;

use crate::Error;
use crate::error::ToStorage;
use std::path::Path;

/// A node-local store — one column family of the node's one RocksDB.
///
/// The variants are the complete schema: a node names the subset it runs in its
/// `COLUMN_FAMILIES` constant, and [`storage::Storage::open`] creates exactly
/// those.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Subsystem {
    /// The stable log: the settled prefix, keyed by global `LogIndex`.
    StableLog,
    /// The replica log: this replica's dense local log.
    ReplicaLog,
    /// Local-store data rows, keyed by `DataId`.
    LocalStoreData,
    /// Local-store subscription cursors, keyed by upstream `NodeId`.
    LocalStoreCursors,
    /// The raft log.
    RaftLog,
    /// The raft meta singletons.
    RaftMeta,
    /// The write-once node identity: one row, written once and never
    /// rewritten (`database::node_id`).
    NodeId,
}

impl Subsystem {
    /// The column-family name. Stable on disk, but per CLAUDE.md §10 formats
    /// are free to change — there is no deployed data to stay compatible with.
    #[must_use]
    pub(crate) const fn cf_name(self) -> &'static str {
        match self {
            Subsystem::ReplicaLog => "replica_log",
            Subsystem::StableLog => "stable_log",
            Subsystem::LocalStoreData => "local_store_data",
            Subsystem::LocalStoreCursors => "local_store_cursors",
            Subsystem::RaftLog => "raft_log",
            Subsystem::RaftMeta => "raft_meta",
            Subsystem::NodeId => "node_id",
        }
    }
}

/// Create the storage root and fsync its *parent*, so the root's own directory
/// entry is durable before any store opens beneath it. Without that, a crash can
/// take the root with it and orphan everything the stores below believe they
/// persisted. Called once per node, at boot.
pub(crate) async fn create_storage_root(root: &Path) -> Result<(), Error> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(), Error> {
        std::fs::create_dir_all(&root).to_storage("create storage root")?;
        if let Some(parent) = root.parent() {
            std::fs::File::open(parent)
                .and_then(|d| d.sync_all())
                .to_storage("fsync storage-root parent")?;
        }
        Ok(())
    })
    .await
    .to_storage("storage-root creation panicked")?
}
