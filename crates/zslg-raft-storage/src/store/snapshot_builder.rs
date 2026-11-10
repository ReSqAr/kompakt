//! `RaftSnapshotBuilder`: building a snapshot of the applied state.

use crate::Error;
use crate::metrics::{self};
use raft_kit::LogId;
use raft_kit::RaftSnapshotBuilder;
use raft_kit::Snapshot;
use raft_kit::SnapshotMeta;
use raft_kit::StorageError;
use raft_kit::StorageIOError;
use raft_kit::StoredMembership;
use rocksdb_kit::Batch;
use std::io::Cursor;
use zslg_consensus::NodeId;
use zslg_log::LogError;
use zslg_raft::NodeInfo;
use zslg_raft::TypeConfig;

use super::{
    RAFT_META_CF, RaftMetaSlot, RocksRaftStorage, SnapshotPayload, StoredSnapshot, put_meta,
    read_meta, storage_io_read, storage_io_write,
};
// SUPER

#[allow(
    clippy::result_large_err,
    reason = "raft_kit::StorageError is inherently large (240+ bytes); we cannot change it"
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
            let cf = inner.storage.cf(RAFT_META_CF).map_err(storage_io_write)?;
            let mut batch = Batch::new();
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
            StorageIOError::write(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
        })?
    }
}
