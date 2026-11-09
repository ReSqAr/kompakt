//! `RaftStorage`: the write side — votes, appends, purge, apply, snapshots.

use crate::Error;
use crate::metrics::{self};
use raft_kit::Entry;
use raft_kit::EntryPayload;
use raft_kit::LogId;
use raft_kit::LogState;
use raft_kit::OptionalSend;
use raft_kit::RaftStorage;
use raft_kit::Snapshot;
use raft_kit::SnapshotMeta;
use raft_kit::StorageError;
use raft_kit::StorageIOError;
use raft_kit::StoredMembership;
use raft_kit::Vote;
use rocksdb_kit::Batch;
use std::io::Cursor;
use zslg_consensus::NodeId;
use zslg_consensus::RaftGeneration;
use zslg_consensus::RaftLogIndex;
use zslg_log::LogError;
use zslg_log::metrics::LogType;
use zslg_raft::NodeInfo;
use zslg_raft::{RaftResponse, TypeConfig};

use super::fence::evaluate_fence;
use super::{
    LOG_KEY_WIDTH, LOG_PRIMARY_WIDTH, RAFT_LOG_CF, RAFT_META_CF, RaftMetaSlot, RocksRaftStorage,
    SnapshotPayload, StoredSnapshot, encode_log_key, encode_log_primary, for_each_log_entry,
    newest_live_entry, put_meta, read_meta, storage_io_read, storage_io_write, write_meta_durable,
};
// SUPER

#[allow(
    clippy::result_large_err,
    reason = "raft_kit::StorageError is inherently large (240+ bytes); we cannot change it"
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
            StorageIOError::write(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            StorageIOError::read(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            StorageIOError::write(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            StorageIOError::read(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            StorageIOError::read(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            let cf = inner.storage.cf(RAFT_LOG_CF).map_err(storage_io_write)?;
            let mut batch = Batch::new();
            for entry in &entries {
                let idx = RaftLogIndex::from(entry.log_id.index);
                let bytes = postcard::to_allocvec(&entry).map_err(storage_io_write)?;
                batch.put(&cf, encode_log_key(idx, idx), &bytes);
            }
            // One atomic batch + fsync — the ACK fence. A re-append over a
            // truncated suffix overwrites the same key.
            inner.storage.write(batch).map_err(storage_io_write)?;
            inner.storage.fence().map_err(storage_io_write)?;
            Ok(())
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            let cf = inner.storage.cf(RAFT_LOG_CF).map_err(storage_io_write)?;
            let mut batch = Batch::new();
            for idx in victims {
                batch.delete(&cf, encode_log_key(idx, idx));
            }
            inner.storage.write(batch).map_err(storage_io_write)?;
            inner.storage.fence().map_err(storage_io_write)?;
            Ok(())
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
                let meta_cf = inner.storage.cf(RAFT_META_CF).map_err(storage_io_write)?;
                let log_cf = inner.storage.cf(RAFT_LOG_CF).map_err(storage_io_write)?;
                let mut batch = Batch::new();
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
                batch.delete_range(&log_cf, &[0u8; LOG_PRIMARY_WIDTH][..], &end[..]);
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
            zslg_log::metrics::purged_entries(
                LogType::Raft,
                usize::try_from(purged).unwrap_or(usize::MAX),
            );
            Ok(())
        })
        .await
        .map_err(|e| {
            StorageIOError::write(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            StorageIOError::read(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            let mut staged: Vec<(zslg_consensus::ReplicatedRaftState, RaftGeneration)> = Vec::new();
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
            let cf = inner.storage.cf(RAFT_META_CF).map_err(storage_io_write)?;
            let mut batch = Batch::new();
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
            StorageIOError::write(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            let cf = inner.storage.cf(RAFT_META_CF).map_err(storage_io_write)?;
            let mut batch = Batch::new();
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
            StorageIOError::write(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
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
            StorageIOError::read(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
        })?
    }
}
