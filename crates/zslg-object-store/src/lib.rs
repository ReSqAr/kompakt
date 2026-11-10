//! Snapshots in object storage: the object format and the `SnapshotStore` over
//! an `ObjectStore` backend.
//!
//! Everything here is shaped by four assumptions about the backend (CLAUDE.md
//! §11): **no CAS**, **immutable objects**, **no LIST required for
//! correctness**, and the `raft-bootstrap-fence` marker as the only
//! "is this cluster initialized?" question. Hence names that are injective in
//! the snapshot id — with no compare-and-swap available, not colliding is the
//! only way not to overwrite.

mod framing;
pub mod metrics;
mod snapshot_store;

pub use snapshot_store::{RemoteSnapshotStore, SnapshotStoreConfig};

/// The one error this implementation raises: its port's.
pub use zslg_snapshot::Error;

#[cfg(test)]
#[allow(clippy::expect_used, reason = "test code")]
fn test_entry(idx: u64) -> zslg_log::SnapshotEntry {
    zslg_log::SnapshotEntry {
        global_index: zslg_log::LogIndex(idx),
        data: zslg_log::Data {
            data_id: zslg_log::DataId::test_id(zslg_time::Hlc(0), idx),
            blob: bytes::Bytes::copy_from_slice(format!("payload-{idx}").as_bytes()),
        },
    }
}
