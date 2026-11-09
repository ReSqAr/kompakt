//! The openraft `RaftStorage` shim over two column families of the node's
//! [`Storage`]: the **raft log** (one `Entry` per index — primary = index =
//! retire rank, so purge is literally one `DeleteRange` over `[0, index]`)
//! and the **raft meta** (the [`RaftMetaSlot`]
//! LWW singletons — vote, committed, last-applied, last-purged, membership,
//! snapshot counter, snapshot blob; a multi-slot batch is atomic, the openraft
//! apply requirement).
//!
//! Applying is a **value-rejecting fence**, never an `Err`: a Normal entry
//! whose `FencedStateWrite` fails a check (stale generation, manifest shape via
//! [`validate_replicated_state`], membership transition via
//! [`validate_transition`]) is skipped in place — the state machine does not
//! advance while `last_applied` still moves past it. `Err` is fatal here: the
//! fence runs on openraft's post-commit apply path and the recovery replay,
//! where a `StorageError` halts the state machine and — the entry being already
//! committed — bricks every node's restart forever. The fence therefore
//! re-validates on every open, replaying committed-but-illegal entries to the
//! same verdict.
//!
//! The one deliberate `Err` is `limited_get_log_entries` on a wholly-purged
//! non-empty range. openraft's default delegates to `try_get_log_entries`
//! (which may legally answer empty) and then unwraps `logs.first()` — with our
//! `panic = "abort"` release profile that is a whole-node crash
//! (`openraft-0.9.24/src/replication/mod.rs:399`). Refusing the request instead
//! makes openraft fall back to snapshot replication;
//! `max_in_snapshot_log_to_keep` is deliberately *not* the fix — a retention
//! constant only moves the load at which the race is reachable, never removes
//! it.
//!
//! Reads here are **pinned**: `get_pinned_cf` and the raw iterators hand back
//! borrowed block-cache blocks rather than copies. That block stays pinned for
//! as long as the borrow lives, so no pinned value may cross an `.await` or a
//! batch commit — every such site states the rule where it is taken.
use rocksdb_kit::ColumnFamily;
use std::fmt::Debug;
use std::sync::Arc;
use zslg_log::LogError;

use parking_lot::Mutex as SyncMutex;
use raft_kit::Entry;
use raft_kit::EntryPayload;
use raft_kit::LogId;
use raft_kit::SnapshotMeta;
use raft_kit::StorageError;
use raft_kit::StorageIOError;
use raft_kit::StoredMembership;
use raft_kit::Vote;
use raft_kit::storage::Adaptor;
use rocksdb_kit::{CfSpec, Storage};
use serde::{Deserialize, Serialize};
use zslg_raft::NodeInfo;

use crate::Error;
use zslg_consensus::NodeId;
use zslg_consensus::RaftGeneration;
use zslg_consensus::RaftLogIndex;
use zslg_consensus::RaftStateHandle;
use zslg_log::ToCodec;
use zslg_log::ToStorage;
use zslg_raft::TypeConfig;

/// The raft log.
pub const RAFT_LOG_CF: ColumnFamily = ColumnFamily::new("raft_log");

/// The raft meta singletons.
pub const RAFT_META_CF: ColumnFamily = ColumnFamily::new("raft_meta");

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests;

mod fence;
mod keys;
mod log_reader;
mod log_scan;
mod meta;
mod raft_storage;
mod snapshot_builder;

use self::fence::apply_normal_fenced;
use self::keys::{
    LOG_KEY_WIDTH, LOG_PRIMARY_WIDTH, RaftMetaSlot, decode_log_key, encode_log_key,
    encode_log_primary, encode_meta_key,
};
use self::log_scan::{bounds_to_pair, for_each_log_entry, newest_live_entry, newest_live_index};
use self::meta::{put_meta, read_meta, write_meta_durable};

#[cfg(feature = "testing")]
mod testing;
#[cfg(feature = "testing")]
pub use self::testing::{build_raft_for_test, seed_foreign_membership_for_test};

/// The two column families this store owns. Both are committed raft state at
/// low volume, so compression is worth its CPU on both.
#[must_use]
pub fn raft_cf_specs() -> [CfSpec; 2] {
    [
        CfSpec {
            cf: RAFT_LOG_CF,
            compress: true,
        },
        CfSpec {
            cf: RAFT_META_CF,
            compress: true,
        },
    ]
}

#[derive(Debug)]
pub struct RocksRaftStorage {
    inner: Arc<SyncMutex<Inner>>,
}

struct Inner {
    storage: Arc<Storage>,
    snapshot_index: u64,
    raft_state: RaftStateHandle,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("snapshot_index", &self.snapshot_index)
            .field("raft_state", &self.raft_state)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct SnapshotPayload {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, NodeInfo>,
    apply: RaftStateHandle,
    generation: RaftGeneration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, NodeInfo>,
    data: Vec<u8>,
}

impl RocksRaftStorage {
    /// Adopt the node's [`Storage`] as the raft store's two column families and
    /// replay the raft log from disk. The DB is already open; this only reads
    /// it. Blocking I/O runs inside `spawn_blocking` — safe to call from an
    /// async context.
    pub async fn open(raft_state: RaftStateHandle, storage: Arc<Storage>) -> Result<Self, Error> {
        tokio::task::spawn_blocking(move || {
            let mut snapshot_index = 0u64;
            if let Some(idx) = read_meta::<u64>(&storage, RaftMetaSlot::SnapshotIndex)? {
                snapshot_index = idx;
            }
            let mut blob_last_applied: Option<u64> = None;
            if let Some(stored) = read_meta::<StoredSnapshot>(&storage, RaftMetaSlot::SnapshotBlob)?
            {
                let payload =
                    postcard::from_bytes::<SnapshotPayload>(&stored.data).map_err(|e| {
                        Error::Log(LogError::Storage(format!(
                            "corrupt raft snapshot blob (snapshot_id {id}): {e}; \
                         refusing to start — wipe state and rejoin as a new node",
                            id = stored.meta.snapshot_id
                        )))
                    })?;
                blob_last_applied = payload.last_applied.map(|id| id.index);
                let mut restored = (*payload.apply.snapshot()).clone();
                restored.generation = payload.generation;
                raft_state.replace(restored);
            }
            if let Some(last_applied_id) =
                read_meta::<LogId<NodeId>>(&storage, RaftMetaSlot::LastApplied)?
            {
                let lower = blob_last_applied
                    .map(RaftLogIndex::from)
                    .map(|i| {
                        i.checked_next().ok_or_else(|| {
                            Error::Log(LogError::Storage("raft log index overflow".into()))
                        })
                    })
                    .transpose()?
                    .unwrap_or(RaftLogIndex::ZERO);
                let upper = RaftLogIndex::from(last_applied_id.index).next();
                for_each_log_entry(&storage, lower, Some(upper), &mut |_index, payload| {
                    let entry: Entry<TypeConfig> =
                        postcard::from_bytes::<Entry<TypeConfig>>(payload)
                            .to_codec("decode raft entry")?;
                    if let EntryPayload::Normal(state) = entry.payload {
                        apply_normal_fenced(
                            &raft_state,
                            &state,
                            RaftLogIndex::from(entry.log_id.index),
                        )?;
                    }
                    Ok(true)
                })?;
            }

            Ok(Self {
                inner: Arc::new(SyncMutex::new(Inner {
                    storage,
                    snapshot_index,
                    raft_state,
                })),
            })
        })
        .await
        .to_storage("raft storage open panicked")?
    }

    /// Check whether durable raft state exists (vote or log entries).
    /// All blocking I/O runs inside `spawn_blocking`.
    pub async fn has_durable_state(&self) -> Result<bool, Error> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner
                .try_lock()
                .ok_or_else(|| Error::Log(LogError::Storage("storage busy".into())))?;
            let vote = read_meta::<Vote<NodeId>>(&inner.storage, RaftMetaSlot::Vote)?;
            if vote.is_some() {
                return Ok(true);
            }
            Ok(newest_live_index(&inner.storage)?.is_some())
        })
        .await
        .to_storage("has_durable_state spawn_blocking panicked")?
    }

    /// Split this one v1 `RaftStorage` into the pair of v2 traits `Raft::new`
    /// wants.
    ///
    /// Implementing v1 and adapting is deliberate: v1 is a single ~12-method
    /// trait against v2's ~16 methods across `RaftLogStorage` +
    /// `RaftStateMachine`, and the log and state machine here share one storage
    /// instance and one fence, so splitting them buys nothing. The adaptor
    /// serialises through an `RwLock`, which is free at the doc's ≤100 writes/s
    /// target.
    #[must_use]
    pub fn into_adaptor(self) -> (Adaptor<TypeConfig, Self>, Adaptor<TypeConfig, Self>) {
        Adaptor::new(self)
    }
}

fn storage_io_read<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::read(&e).into()
}

fn storage_io_write<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::write(&e).into()
}
