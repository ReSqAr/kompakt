//! Node-local storage glue. A node opens **one** RocksDB at the storage root
//! (`rocksdb_kit::Storage`) and every node-local store — including the
//! write-once node identity — is a column family in it. Each store declares
//! its own column family (`STABLE_LOG_CF`, `REPLICA_LOG_CF`, ...); the node
//! names the ones it wires when it opens the DB, and nothing else is created.

pub(crate) use zslg_node_id::NODE_ID_CF;

#[cfg(test)]
use std::path::Path;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use bytesize::ByteSize;
#[cfg(test)]
use rocksdb_kit::{CfSpec, Storage, StorageConfig};

#[cfg(test)]
use crate::Error;

/// Open a storage root carrying **every** column family, for tests.
///
/// A root's descriptor list must name every family it has ever held, so a test
/// helper that opened a subset would make the roots two test constructors
/// produce mutually incompatible. Naming all of them means any test store can
/// share a root with any other — which is what lets one `Storage` back a
/// stable log and a local store in one test.
#[cfg(test)]
pub(crate) fn open_for_test(root: &Path) -> Result<Arc<Storage>, Error> {
    let cfs: Vec<CfSpec> = [
        crate::replica::REPLICA_LOG_CF,
        crate::learner::STABLE_LOG_CF,
        crate::learner::LOCAL_STORE_DATA_CF,
        crate::learner::LOCAL_STORE_CURSORS_CF,
        zslg_raft_storage::RAFT_LOG_CF,
        zslg_raft_storage::RAFT_META_CF,
        NODE_ID_CF,
    ]
    .into_iter()
    .map(|cf| CfSpec {
        cf,
        compress: false,
    })
    .collect();
    Ok(Storage::open(
        root,
        StorageConfig::new(ByteSize::from_mib(64)),
        &cfs,
    )?)
}
