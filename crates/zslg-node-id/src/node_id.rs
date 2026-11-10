//! The persistent node identity: one row in the [`NODE_ID_CF`] column
//! family of the node's one RocksDB.
//!
//! Write-once is a safety property, not tidiness. A `NodeId` is how the rest
//! of the cluster refers to this node's log, so re-minting one while old
//! state survives on disk is a correctness violation — which is why a
//! corrupt row fails loud instead of falling through to the mint path.
//! Discarding an identity is an operator action (wipe, then rejoin as a new
//! node), never something this code decides (CLAUDE.md §8).
//!
//! Minting goes through the same write-then-fence path as every other store
//! (CLAUDE.md §8): the row is durable — and safe to hand to a caller — only
//! after [`Storage::fence`] returns, never before.

use rocksdb_kit::Batch;
use rocksdb_kit::ColumnFamily;
use uuid::Uuid;
use zslg_log::LogError;

use crate::Error;
use rocksdb_kit::Storage;
use zslg_consensus::NodeId;
use zslg_log::ToStorage;

/// This column family's only row.
const KEY: &[u8] = b"node_id";

/// Idempotent across restarts, and durable before it returns — a caller that
/// gets an id may act on it immediately.
pub fn load_or_create_node_id(storage: &Storage) -> Result<NodeId, Error> {
    let cf = storage.cf(NODE_ID_CF)?;
    if let Some(bytes) = storage.get_pinned(&cf, KEY).to_storage("node id read")? {
        let raw: [u8; 16] = bytes.as_ref().try_into().map_err(|_| {
            Error::Log(LogError::Storage(
                "node id record corrupt: wrong length".to_owned(),
            ))
        })?;
        return Ok(NodeId::from_uuid(Uuid::from_bytes(raw)));
    }

    let id = NodeId::new_v4();
    let mut batch = Batch::new();
    batch.put(&cf, KEY, id.into_uuid().as_bytes());
    storage.write(batch)?;
    storage.fence()?;
    Ok(id)
}

/// The write-once node identity row.
pub const NODE_ID_CF: ColumnFamily = ColumnFamily::new("node_id");

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use bytesize::ByteSize;
    use rocksdb_kit::{CfSpec, StorageConfig};

    fn test_storage(root: &std::path::Path) -> std::sync::Arc<Storage> {
        Storage::open(
            root,
            StorageConfig::new(ByteSize::from_mib(16)),
            &[CfSpec {
                cf: NODE_ID_CF,
                compress: false,
            }],
        )
        .expect("open test storage")
    }

    #[tokio::test]
    async fn load_or_create_is_idempotent_across_reopens() {
        let dir = tempfile::tempdir().expect("tempdir");

        // Each call reopens the DB from scratch, so this also proves the id
        // survives a restart.
        let id1 = {
            let storage = test_storage(dir.path());
            super::load_or_create_node_id(&storage).expect("first load")
        };
        let id2 = {
            let storage = test_storage(dir.path());
            super::load_or_create_node_id(&storage).expect("second load")
        };

        assert_eq!(id1, id2, "node ID persists across loads/restarts");
    }

    #[tokio::test]
    async fn corrupt_row_refuses_to_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = test_storage(dir.path());
        let cf = storage.cf(NODE_ID_CF).unwrap();
        let mut batch = Batch::new();
        batch.put(&cf, KEY, b"not a uuid");
        storage.write(batch).unwrap();
        storage.fence().unwrap();

        assert!(
            super::load_or_create_node_id(&storage).is_err(),
            "corruption fails loud, never mints a replacement id"
        );
    }
}
