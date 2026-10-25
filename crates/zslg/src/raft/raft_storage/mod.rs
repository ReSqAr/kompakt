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
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use rust_rocksdb::{BoundColumnFamily, WriteBatch};

use crate::database::Subsystem;
use crate::database::storage::{CfSpec, Storage};
use crate::raft::NodeInfo;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogId;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;
use openraft::storage::Adaptor;
use parking_lot::Mutex as SyncMutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, ToCodec, ToStorage};
use crate::hlc::Hlc;
use crate::ids::{NodeId, RaftGeneration};
use crate::meta::ReplicaMembership;
use crate::metrics::{self, LogType};
use crate::raft::RaftStateHandle;
use crate::raft::{RaftLogIndex, RaftResponse, TypeConfig};

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests;
mod validators;

#[cfg(test)]
pub(crate) use self::tests::{
    build_raft_for_test, open_test_storage, seed_foreign_membership_for_test,
};
pub(crate) use self::validators::{
    validate_cutoff_advance, validate_replicated_state, validate_transition,
};

/// The two column families this store owns. Both are committed raft state at
/// low volume, so compression is worth its CPU on both.
#[must_use]
pub(crate) fn raft_cf_specs() -> [CfSpec; 2] {
    [
        CfSpec {
            subsystem: Subsystem::RaftLog,
            compress: true,
        },
        CfSpec {
            subsystem: Subsystem::RaftMeta,
            compress: true,
        },
    ]
}

// ── key codec ───────────────────────────────────────────────────────────────
//
// `key = primary ‖ secondary`, both big-endian so the byte ordering IS the
// tuple ordering — load-bearing, because retirement is a `DeleteRange` over
// encoded-byte bounds (`key_order_matches_tuple_order`); if the byte order
// were not the tuple order, `purge_logs_upto` would drop the wrong entries.
// Fixed width, so `decode_log_key` splits at a constant.
//
// Folding the secondary into the key is lossless because no store here ever
// holds two live versions of one primary with differing secondaries — see
// docs/ROCKSDB_MIGRATION.md §1.2. For the raft log the secondary is the *same*
// index as the primary: it is the retire rank, which is what makes
// `purge_logs_upto(i)` a single `DeleteRange` over the prefix `[0, i]`.

pub(crate) const LOG_PRIMARY_WIDTH: usize = 8;
const LOG_RANK_WIDTH: usize = 8;
pub(crate) const LOG_KEY_WIDTH: usize = LOG_PRIMARY_WIDTH + LOG_RANK_WIDTH;

/// The raft-meta key is the slot byte alone: seven LWW singletons, never
/// retired, so there is no secondary to fold in.
pub(crate) const META_KEY_WIDTH: usize = 1;

#[must_use]
fn encode_log_key(primary: RaftLogIndex, rank: RaftLogIndex) -> [u8; LOG_KEY_WIDTH] {
    let mut key = [0u8; LOG_KEY_WIDTH];
    let (head, tail) = key.split_at_mut(LOG_PRIMARY_WIDTH);
    head.copy_from_slice(&primary.raw().to_be_bytes());
    tail.copy_from_slice(&rank.raw().to_be_bytes());
    key
}

#[must_use]
fn encode_log_primary(primary: RaftLogIndex) -> [u8; LOG_PRIMARY_WIDTH] {
    primary.raw().to_be_bytes()
}

/// The retire rank on its own. Purge no longer arms an in-memory clamp
/// against it (that's a durable `DeleteRange` now), so this is test-only:
/// it pins that the rank half of the key still round-trips.
#[cfg(test)]
#[must_use]
fn encode_log_rank(rank: RaftLogIndex) -> [u8; LOG_RANK_WIDTH] {
    rank.raw().to_be_bytes()
}

#[must_use]
fn decode_log_key(bytes: &[u8]) -> Option<(RaftLogIndex, RaftLogIndex)> {
    let primary = <[u8; LOG_PRIMARY_WIDTH]>::try_from(bytes.get(..LOG_PRIMARY_WIDTH)?).ok()?;
    let rank =
        <[u8; LOG_RANK_WIDTH]>::try_from(bytes.get(LOG_PRIMARY_WIDTH..LOG_KEY_WIDTH)?).ok()?;
    Some((
        RaftLogIndex::from(u64::from_be_bytes(primary)),
        RaftLogIndex::from(u64::from_be_bytes(rank)),
    ))
}

#[must_use]
fn encode_meta_key(slot: RaftMetaSlot) -> [u8; META_KEY_WIDTH] {
    [slot.as_byte()]
}

/// The raft-meta singletons — the seven former string keys, now a fixed-width
/// (1-byte) enum key. Never retired; every value is overwritten in place (LWW),
/// and a multi-slot `WriteBatch` is atomic — the openraft apply/snapshot
/// atomicity requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RaftMetaSlot {
    Vote,
    Committed,
    LastApplied,
    LastPurged,
    LastMembership,
    SnapshotIndex,
    SnapshotBlob,
}

impl RaftMetaSlot {
    const fn as_byte(self) -> u8 {
        match self {
            Self::Vote => 0,
            Self::Committed => 1,
            Self::LastApplied => 2,
            Self::LastPurged => 3,
            Self::LastMembership => 4,
            Self::SnapshotIndex => 5,
            Self::SnapshotBlob => 6,
        }
    }
}

#[derive(Debug)]
pub(in crate::raft) struct RocksRaftStorage {
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
pub(in crate::raft) struct SnapshotPayload {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, NodeInfo>,
    apply: RaftStateHandle,
    generation: RaftGeneration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::raft) struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, NodeInfo>,
    data: Vec<u8>,
}

impl RocksRaftStorage {
    /// Adopt the node's [`Storage`] as the raft store's two column families and
    /// replay the raft log from disk. The DB is already open; this only reads
    /// it. Blocking I/O runs inside `spawn_blocking` — safe to call from an
    /// async context.
    pub(in crate::raft) async fn open(
        raft_state: RaftStateHandle,
        storage: Arc<Storage>,
    ) -> Result<Self, Error> {
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
                        Error::Storage(format!(
                            "corrupt raft snapshot blob (snapshot_id {id}): {e}; \
                         refusing to start — wipe state and rejoin as a new node",
                            id = stored.meta.snapshot_id
                        ))
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
                        i.checked_next()
                            .ok_or_else(|| Error::Storage("raft log index overflow".into()))
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
    pub(in crate::raft) async fn has_durable_state(&self) -> Result<bool, Error> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner
                .try_lock()
                .ok_or_else(|| Error::Storage("storage busy".into()))?;
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
    pub(in crate::raft) fn into_adaptor(
        self,
    ) -> (Adaptor<TypeConfig, Self>, Adaptor<TypeConfig, Self>) {
        Adaptor::new(self)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "openraft::StorageError is inherently large (240+ bytes); we cannot change it"
)]
impl RaftLogReader<TypeConfig> for RocksRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.clone();
        let (lo, hi) = bounds_to_pair(&range);
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            let mut out = Vec::new();
            for_each_log_entry(&inner.storage, lo, Some(hi), &mut |_index, payload| {
                out.push(
                    postcard::from_bytes::<Entry<TypeConfig>>(payload)
                        .to_codec("decode raft entry")?,
                );
                Ok(true)
            })
            .map_err(storage_io_read)?;
            // A non-empty request that yielded nothing means the whole range
            // sat below the purge floor (retired records are dropped on
            // read) — legal here, so counted, not failed; it is an error in
            // `limited_get_log_entries` (header).
            if out.is_empty() && lo < hi {
                metrics::raft_log_read_empty_range();
            }
            Ok(out)
        })
        .await
        .map_err(|e| {
            StorageIOError::read(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    /// Upholds the contract `try_get_log_entries` cannot: a non-empty range
    /// must never answer empty. The header explains why the default's empty
    /// answer is a whole-node crash under `panic = "abort"`, and why refusing
    /// (rather than a retention constant) is the fix. A partially-purged range
    /// stays a short read — openraft tolerates `logs.len() <= end - start`.
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let entries = self.try_get_log_entries(start..end).await?;
        if entries.is_empty() && start < end {
            // The counter was already incremented by `try_get_log_entries`.
            tracing::warn!(
                log_index = start,
                end,
                "raft log: replication asked for [{start}, {end}), which is \
                 entirely below the purge floor — the logs were already purged \
                 and snapshotted. Returning a storage error so openraft \
                 re-syncs this follower from a snapshot; returning the empty \
                 result openraft's default would produce makes it unwrap None \
                 and abort the process. If this is frequent, the follower is \
                 falling behind faster than max_in_snapshot_log_to_keep covers.",
            );
            return Err(storage_io_read(Error::Storage(format!(
                "raft log range [{start}, {end}) is entirely purged; \
                 replication must fall back to a snapshot"
            ))));
        }
        Ok(entries)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "openraft::StorageError is inherently large (240+ bytes); we cannot change it"
)]
impl RaftStorage<TypeConfig> for RocksRaftStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let vote = *vote;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            write_meta_durable(&inner.storage, RaftMetaSlot::Vote, &vote)
                .map_err(storage_io_write)?;
            Ok(())
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            read_meta::<Vote<NodeId>>(&inner.storage, RaftMetaSlot::Vote).map_err(storage_io_read)
        })
        .await
        .map_err(|e| {
            StorageIOError::read(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            write_meta_durable(&inner.storage, RaftMetaSlot::Committed, &committed)
                .map_err(storage_io_write)?;
            Ok(())
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            let raw = read_meta::<Option<LogId<NodeId>>>(&inner.storage, RaftMetaSlot::Committed)
                .map_err(storage_io_read)?;
            Ok(raw.flatten())
        })
        .await
        .map_err(|e| {
            StorageIOError::read(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            let last_purged_log_id =
                read_meta::<LogId<NodeId>>(&inner.storage, RaftMetaSlot::LastPurged)
                    .map_err(storage_io_read)?;
            let floor = last_purged_log_id.map(|id| RaftLogIndex::from(id.index));
            // The newest *stored* key may sit in the purged zone, so the newest
            // live entry needs the backwards walk in `newest_live_entry`.
            let last_log_id = newest_live_entry(&inner.storage, floor).map_err(storage_io_read)?;
            let last_log_id = last_log_id.or(last_purged_log_id);
            Ok(LogState {
                last_purged_log_id,
                last_log_id,
            })
        })
        .await
        .map_err(|e| {
            StorageIOError::read(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            inner: self.inner.clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let inner = self.inner.clone();
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        tokio::task::spawn_blocking(move || {
            let _probe = metrics::raft_log_append_probe();
            let inner = inner.lock();
            let cf = inner
                .storage
                .cf(Subsystem::RaftLog)
                .map_err(storage_io_write)?;
            let mut batch = WriteBatch::default();
            for entry in &entries {
                let idx = RaftLogIndex::from(entry.log_id.index);
                let bytes = postcard::to_allocvec(&entry).map_err(storage_io_write)?;
                batch.put_cf(&cf, encode_log_key(idx, idx), &bytes);
            }
            // One atomic batch + fsync — the ACK fence. A re-append over a
            // truncated suffix overwrites the same key.
            inner.storage.write(batch).map_err(storage_io_write)?;
            inner.storage.fence().map_err(storage_io_write)?;
            Ok(())
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            // Delete every live entry at or above the conflict point. The scan
            // skips the purged zone and already-deleted entries, so this is
            // idempotent. Bounded by the divergent suffix — a few entries at a
            // leader change. Collected first because the batch that deletes
            // them must not be built while the iterator holds its blocks.
            let mut victims: Vec<RaftLogIndex> = Vec::new();
            for_each_log_entry(
                &inner.storage,
                RaftLogIndex::from(log_id.index),
                None,
                &mut |idx, _payload| {
                    victims.push(idx);
                    Ok(true)
                },
            )
            .map_err(storage_io_write)?;
            if victims.is_empty() {
                return Ok(());
            }
            let cf = inner
                .storage
                .cf(Subsystem::RaftLog)
                .map_err(storage_io_write)?;
            let mut batch = WriteBatch::default();
            for idx in victims {
                batch.delete_cf(&cf, encode_log_key(idx, idx));
            }
            inner.storage.write(batch).map_err(storage_io_write)?;
            inner.storage.fence().map_err(storage_io_write)?;
            Ok(())
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            let current = read_meta::<LogId<NodeId>>(&inner.storage, RaftMetaSlot::LastPurged)
                .map_err(storage_io_write)?;
            // One batch across two column families: `set_atomic_flush(true)`
            // makes a cross-CF WriteBatch all-or-nothing through recovery
            // (CLAUDE.md §8), so there is no ordering to reason about — the
            // meta floor and the deletion of everything below it land together
            // or not at all. That is what the previous "last_purged must land
            // FIRST, because retirement is only armed in memory" comment was
            // protecting against, and it no longer applies.
            //
            // Guarded on monotonicity: openraft should not call this backwards,
            // and a lowered floor would be a floor regression rather than a
            // no-op.
            if current.is_none_or(|p| p < log_id) {
                let meta_cf = inner
                    .storage
                    .cf(Subsystem::RaftMeta)
                    .map_err(storage_io_write)?;
                let log_cf = inner
                    .storage
                    .cf(Subsystem::RaftLog)
                    .map_err(storage_io_write)?;
                let mut batch = WriteBatch::default();
                put_meta(&mut batch, &meta_cf, RaftMetaSlot::LastPurged, &log_id)
                    .map_err(storage_io_write)?;
                let purge_upto = RaftLogIndex::from(log_id.index);
                // `rank == index`, so retirement is the prefix [0, purge_upto].
                // The 8-byte primary of purge_upto+1 sorts after every 16-byte
                // key beginning with purge_upto and before (n ‖ n) — exactly
                // the exclusive upper bound. Exhausting the index space is
                // unreachable (2^64 raft entries); on that impossible input
                // there is nothing above the floor to keep, so an all-0xFF key
                // longer than any real one covers the family.
                let end: Vec<u8> = match purge_upto.checked_next() {
                    Some(next) => encode_log_primary(next).to_vec(),
                    None => vec![0xFFu8; LOG_KEY_WIDTH.saturating_add(1)],
                };
                batch.delete_range_cf(&log_cf, &[0u8; LOG_PRIMARY_WIDTH][..], &end[..]);
                inner.storage.write(batch).map_err(storage_io_write)?;
                inner.storage.fence().map_err(storage_io_write)?;
            }
            // The purge span is dense (indices gapless above the previous
            // floor), so the count is pure arithmetic.
            let prev_floor =
                current.map_or(RaftLogIndex::ZERO, |p| RaftLogIndex::from(p.index).next());
            let purged = RaftLogIndex::from(log_id.index)
                .next()
                .raw()
                .saturating_sub(prev_floor.raw());
            metrics::purged_entries(LogType::Raft, usize::try_from(purged).unwrap_or(usize::MAX));
            Ok(())
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, NodeInfo>), StorageError<NodeId>>
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            let last_applied =
                read_meta::<LogId<NodeId>>(&inner.storage, RaftMetaSlot::LastApplied)
                    .map_err(storage_io_read)?;
            let last_membership = read_meta::<StoredMembership<NodeId, NodeInfo>>(
                &inner.storage,
                RaftMetaSlot::LastMembership,
            )
            .map_err(storage_io_read)?
            .unwrap_or_default();
            Ok((last_applied, last_membership))
        })
        .await
        .map_err(|e| {
            StorageIOError::read(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }

    #[tracing::instrument(level = "debug", skip_all, name = "raft_apply")]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<RaftResponse>, StorageError<NodeId>> {
        let entries: Vec<Entry<TypeConfig>> = entries.to_vec();
        let last_index = entries.last().map(|e| RaftLogIndex::from(e.log_id.index));
        let inner = self.inner.clone();
        let (out, staged, raft_state) = tokio::task::spawn_blocking(move || {
            let _probe = metrics::raft_apply_probe();
            let inner = inner.lock();
            let mut out = Vec::with_capacity(entries.len());
            let applied = inner.raft_state.snapshot();
            let mut next_generation = applied.generation;
            // The membership the *next* entry is validated against. Advances
            // with each accepted entry in this batch, so a batch containing
            // several membership steps is checked step by step.
            let mut next_replica = applied.replica.clone();
            // Tracked across the batch for the same reason as `next_replica`:
            // entry N is validated against entry N-1's state, not the stale
            // applied one.
            let mut next_cutoff = applied.manifest_state.hlc_cutoff;
            let mut staged: Vec<(crate::api::ReplicatedRaftState, RaftGeneration)> = Vec::new();
            let mut last_applied: Option<LogId<NodeId>> = None;
            let mut last_membership: Option<StoredMembership<NodeId, NodeInfo>> = None;
            for entry in &entries {
                last_applied = Some(entry.log_id);
                let response = match &entry.payload {
                    EntryPayload::Blank => RaftResponse::Applied,
                    EntryPayload::Normal(write) => {
                        let response = evaluate_fence(
                            next_generation,
                            &next_replica,
                            next_cutoff,
                            write,
                            RaftLogIndex::from(entry.log_id.index),
                        );
                        if matches!(response, RaftResponse::Applied) {
                            next_generation = RaftGeneration(entry.log_id.index);
                            next_replica = write.state.replica.clone();
                            next_cutoff = write.state.hlc_cutoff.or(next_cutoff);
                            staged.push((write.state.clone(), next_generation));
                        }
                        response
                    }
                    EntryPayload::Membership(m) => {
                        last_membership =
                            Some(StoredMembership::new(Some(entry.log_id), m.clone()));
                        RaftResponse::Applied
                    }
                };
                out.push(response);
            }
            // One atomic meta batch (the WAL commit marker): last_applied and
            // any membership land together or not at all — the openraft apply
            // atomicity requirement — then the fsync fence.
            let cf = inner
                .storage
                .cf(Subsystem::RaftMeta)
                .map_err(storage_io_write)?;
            let mut batch = WriteBatch::default();
            if let Some(la) = last_applied {
                put_meta(&mut batch, &cf, RaftMetaSlot::LastApplied, &la)
                    .map_err(storage_io_write)?;
            }
            if let Some(lm) = &last_membership {
                put_meta(&mut batch, &cf, RaftMetaSlot::LastMembership, lm)
                    .map_err(storage_io_write)?;
            }
            inner.storage.write(batch).map_err(storage_io_write)?;
            inner.storage.fence().map_err(storage_io_write)?;
            let raft_state = inner.raft_state.clone();
            Ok::<_, StorageError<NodeId>>((out, staged, raft_state))
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })??;
        if let Some(idx) = last_index {
            crate::metrics::raft_applied_log_index(u64::from(idx));
        }
        for (state, generation) in staged {
            raft_state
                .apply_replicated(state, generation)
                .map_err(storage_io_write)?;
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            inner: self.inner.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, NodeInfo>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let meta = meta.clone();
        let bytes = snapshot.into_inner();
        let inner = self.inner.clone();
        let (payload_apply, payload_gen, raft_state) = tokio::task::spawn_blocking(move || {
            let _probe = metrics::raft_install_snapshot_probe();
            let inner = inner.lock();
            let payload: SnapshotPayload = postcard::from_bytes::<SnapshotPayload>(&bytes)
                .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

            // One atomic meta batch: applied + membership + blob land together.
            let stored = StoredSnapshot { meta, data: bytes };
            let cf = inner
                .storage
                .cf(Subsystem::RaftMeta)
                .map_err(storage_io_write)?;
            let mut batch = WriteBatch::default();
            put_meta(
                &mut batch,
                &cf,
                RaftMetaSlot::LastApplied,
                &payload.last_applied,
            )
            .map_err(storage_io_write)?;
            put_meta(
                &mut batch,
                &cf,
                RaftMetaSlot::LastMembership,
                &payload.last_membership,
            )
            .map_err(storage_io_write)?;
            put_meta(&mut batch, &cf, RaftMetaSlot::SnapshotBlob, &stored)
                .map_err(storage_io_write)?;
            inner.storage.write(batch).map_err(storage_io_write)?;
            inner.storage.fence().map_err(storage_io_write)?;
            let raft_state = inner.raft_state.clone();
            Ok::<_, StorageError<NodeId>>((payload.apply, payload.generation, raft_state))
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })??;
        let mut restored = (*payload_apply.snapshot()).clone();
        restored.generation = payload_gen;
        raft_state.replace(restored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            let stored = read_meta::<StoredSnapshot>(&inner.storage, RaftMetaSlot::SnapshotBlob)
                .map_err(storage_io_read)?;
            Ok(stored.map(|s| Snapshot {
                meta: s.meta,
                snapshot: Box::new(Cursor::new(s.data)),
            }))
        })
        .await
        .map_err(|e| {
            StorageIOError::read(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }
}

#[allow(
    clippy::result_large_err,
    reason = "openraft::StorageError is inherently large (240+ bytes); we cannot change it"
)]
impl RaftSnapshotBuilder<TypeConfig> for RocksRaftStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _probe = metrics::raft_build_snapshot_probe();
            let mut inner = inner.lock();
            let last_applied =
                read_meta::<LogId<NodeId>>(&inner.storage, RaftMetaSlot::LastApplied)
                    .map_err(storage_io_read)?;
            let last_membership = read_meta::<StoredMembership<NodeId, NodeInfo>>(
                &inner.storage,
                RaftMetaSlot::LastMembership,
            )
            .map_err(storage_io_read)?
            .unwrap_or_default();
            let apply = inner.raft_state.clone();
            let generation = apply.snapshot().generation;

            let payload = SnapshotPayload {
                last_applied,
                last_membership: last_membership.clone(),
                apply,
                generation,
            };
            let bytes = postcard::to_allocvec(&payload).map_err(|e| {
                let signature = SnapshotMeta::<NodeId, NodeInfo> {
                    last_log_id: last_applied,
                    last_membership: last_membership.clone(),
                    snapshot_id: String::new(),
                }
                .signature();
                StorageIOError::write_snapshot(Some(signature), &e)
            })?;

            inner.snapshot_index = inner.snapshot_index.checked_add(1).unwrap_or(0);
            let snapshot_id = match last_applied {
                Some(id) => format!("{}-{}-{}", id.leader_id, id.index, inner.snapshot_index),
                None => format!("--{}", inner.snapshot_index),
            };
            let meta = SnapshotMeta {
                last_log_id: last_applied,
                last_membership,
                snapshot_id,
            };
            let stored = StoredSnapshot {
                meta: meta.clone(),
                data: bytes.clone(),
            };

            // One atomic meta batch: index + blob land together, then the fence.
            let cf = inner
                .storage
                .cf(Subsystem::RaftMeta)
                .map_err(storage_io_write)?;
            let mut batch = WriteBatch::default();
            put_meta(
                &mut batch,
                &cf,
                RaftMetaSlot::SnapshotIndex,
                &inner.snapshot_index,
            )
            .map_err(storage_io_write)?;
            put_meta(&mut batch, &cf, RaftMetaSlot::SnapshotBlob, &stored)
                .map_err(storage_io_write)?;
            inner.storage.write(batch).map_err(storage_io_write)?;
            inner.storage.fence().map_err(storage_io_write)?;

            Ok(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(bytes)),
            })
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Storage(format!("spawn_blocking panicked: {e}")))
        })?
    }
}

fn evaluate_fence(
    current: RaftGeneration,
    previous_replica: &ReplicaMembership,
    previous_cutoff: Option<Hlc>,
    write: &crate::raft::FencedStateWrite,
    log_index: RaftLogIndex,
) -> RaftResponse {
    if write.expected_generation != current {
        tracing::warn!(
            generation = ?current,
            expected_generation = ?write.expected_generation,
            log_index = %log_index,
            "fenced state write rejected: stale generation",
        );
        return RaftResponse::RejectedStaleGeneration {
            expected: write.expected_generation,
            current,
        };
    }
    // A structural-invariant violation is a *value* rejection, never an `Err`
    // — the header explains why `Err` on a committed entry is fatal. The leader
    // also refuses to *propose* such a state (the same check runs pre-flight in
    // `replicate_state`), so in correct operation this never fires.
    match validate_replicated_state(&write.state)
        .and_then(|()| validate_transition(previous_replica, &write.state))
        .and_then(|()| validate_cutoff_advance(previous_cutoff, &write.state))
    {
        Ok(()) => RaftResponse::Applied,
        Err(reason) => {
            tracing::error!(
                log_index = %log_index,
                %reason,
                "fenced state write rejected: invalid raft state",
            );
            RaftResponse::RejectedInvalidState { reason }
        }
    }
}

fn apply_normal_fenced(
    raft_state: &RaftStateHandle,
    write: &crate::raft::FencedStateWrite,
    log_index: RaftLogIndex,
) -> Result<RaftResponse, Error> {
    let applied = raft_state.snapshot();
    let response = evaluate_fence(
        applied.generation,
        &applied.replica,
        applied.manifest_state.hlc_cutoff,
        write,
        log_index,
    );
    if matches!(response, RaftResponse::Applied) {
        raft_state.apply_replicated(write.state.clone(), RaftGeneration(log_index.raw()))?;
    }
    Ok(response)
}

fn read_meta<T: DeserializeOwned>(
    storage: &Storage,
    slot: RaftMetaSlot,
) -> Result<Option<T>, Error> {
    let cf = storage.cf(Subsystem::RaftMeta)?;
    let key = encode_meta_key(slot);
    let Some(pinned) = storage
        .db()
        .get_pinned_cf(&cf, key)
        .map_err(|e| Error::Storage(format!("read raft meta slot {slot:?}: {e}")))?
    else {
        return Ok(None);
    };
    // PINNING RULE: `pinned` holds its block in the block cache for as long as
    // it lives. It must not cross an `.await` or a batch commit — decode here
    // and let it drop at the end of this expression.
    Ok(Some(
        postcard::from_bytes(&pinned).to_codec("read raft meta")?,
    ))
}

/// Stage one meta slot's overwrite into `batch` (LWW — the slot byte is the
/// whole key, so a later write simply replaces it). The caller owns the batch
/// because a multi-slot batch has to be one atomic write.
fn put_meta<T: Serialize>(
    batch: &mut WriteBatch,
    cf: &Arc<BoundColumnFamily<'_>>,
    slot: RaftMetaSlot,
    value: &T,
) -> Result<(), Error> {
    let bytes = postcard::to_allocvec(value).to_codec("encode raft meta")?;
    batch.put_cf(cf, encode_meta_key(slot), &bytes);
    Ok(())
}

/// The callback [`for_each_log_entry`] feeds: `(index, payload)`, answering
/// `false` to stop the walk. The payload is borrowed for the call only.
type LogSink<'a> = dyn FnMut(RaftLogIndex, &[u8]) -> Result<bool, Error> + 'a;

/// Primary-ordered walk of the live raft log over `[lo, hi)` (`hi = None` is
/// unbounded), skipping retired entries. `sink` answering `false` stops it.
///
/// PINNING RULE: the payload handed to `sink` borrows the iterator's pinned
/// block-cache block. It must not cross an `.await` or a batch commit, so a
/// `sink` that wants to *write* what it sees collects keys and builds its
/// `WriteBatch` after this returns (`delete_conflict_logs_since`).
fn for_each_log_entry(
    storage: &Storage,
    lo: RaftLogIndex,
    hi: Option<RaftLogIndex>,
    sink: &mut LogSink<'_>,
) -> Result<(), Error> {
    let cf = storage.cf(Subsystem::RaftLog)?;
    let mut it = storage.db().raw_iterator_cf(&cf);
    it.seek(encode_log_primary(lo));
    while let Some(key) = it.key() {
        let Some((index, _rank)) = decode_log_key(key) else {
            return Err(Error::Storage(format!(
                "undecodable key in {}",
                Subsystem::RaftLog.cf_name()
            )));
        };
        if hi.is_some_and(|end| index >= end) {
            break;
        }
        // `value()` is `None` only when `key()` is, so this is unreachable —
        // but substituting an empty payload for a missing one would feed
        // openraft an undecodable entry instead of failing (§20).
        let Some(value) = it.value() else {
            return Err(Error::Storage(format!(
                "key with no value in {} ({} bytes)",
                Subsystem::RaftLog.cf_name(),
                key.len()
            )));
        };
        if !sink(index, value)? {
            return Ok(());
        }
        it.next();
    }
    it.status()
        .map_err(|e| Error::Storage(format!("scan raft log: {e}")))
}

/// The newest live log index, or `None` if every stored entry is retired.
fn newest_live_index(storage: &Storage) -> Result<Option<RaftLogIndex>, Error> {
    let cf = storage.cf(Subsystem::RaftLog)?;
    let mut it = storage.db().raw_iterator_cf(&cf);
    it.seek_to_last();
    if let Some(key) = it.key() {
        return decode_log_key(key)
            .map(|(index, _)| Some(index))
            .ok_or_else(|| {
                Error::Storage(format!(
                    "undecodable key in {}",
                    Subsystem::RaftLog.cf_name()
                ))
            });
    }
    it.status()
        .map_err(|e| Error::Storage(format!("scan raft log: {e}")))?;
    Ok(None)
}

/// The newest live entry's log id: the newest stored key may sit in the purged
/// zone, so walk backwards until a live entry answers, flooring at the purge
/// point.
fn newest_live_entry(
    storage: &Storage,
    floor: Option<RaftLogIndex>,
) -> Result<Option<LogId<NodeId>>, Error> {
    let Some(index) = newest_live_index(storage)? else {
        return Ok(None);
    };
    if floor.is_some_and(|f| index <= f) {
        return Ok(None);
    }
    let cf = storage.cf(Subsystem::RaftLog)?;
    let Some(pinned) = storage
        .db()
        .get_pinned_cf(&cf, encode_log_key(index, index))
        .map_err(|e| Error::Storage(format!("read raft log entry {index}: {e}")))?
    else {
        return Ok(None);
    };
    // PINNING RULE: `pinned` holds its block in the block cache. It must not
    // cross an `.await` or a batch commit — decode here and let it drop.
    let entry: Entry<TypeConfig> =
        postcard::from_bytes::<Entry<TypeConfig>>(&pinned).to_codec("decode raft entry")?;
    Ok(Some(*RaftLogId::get_log_id(&entry)))
}

/// Write one meta slot and fsync — the ACK fence for a single-slot update
/// (vote / committed).
fn write_meta_durable<T: Serialize>(
    storage: &Storage,
    slot: RaftMetaSlot,
    value: &T,
) -> Result<(), Error> {
    let cf = storage.cf(Subsystem::RaftMeta)?;
    let mut batch = WriteBatch::default();
    put_meta(&mut batch, &cf, slot, value)?;
    storage.write(batch)?;
    storage.fence()
}

fn bounds_to_pair<RB: RangeBounds<u64>>(range: &RB) -> (RaftLogIndex, RaftLogIndex) {
    let lo = match range.start_bound() {
        Bound::Included(&v) => v,
        Bound::Excluded(&v) => v.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let hi = match range.end_bound() {
        Bound::Included(&v) => v.saturating_add(1),
        Bound::Excluded(&v) => v,
        Bound::Unbounded => u64::MAX,
    };
    (RaftLogIndex::from(lo), RaftLogIndex::from(hi))
}

fn storage_io_read<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::read(&e).into()
}

fn storage_io_write<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::write(&e).into()
}
