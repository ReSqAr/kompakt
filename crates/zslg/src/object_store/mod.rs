//! Snapshots in object storage: the backend config, the object format, and the
//! store built on them.
//!
//! Everything here is shaped by four assumptions about the backend (CLAUDE.md
//! §11): **no CAS**, **immutable objects**, **no LIST required for
//! correctness**, and the `raft-bootstrap-fence` marker as the only
//! "is this cluster initialized?" question. Hence names that are injective in
//! the snapshot id — with no compare-and-swap available, not colliding is the
//! only way not to overwrite.
//!
//! | [`framing`] | the object's bytes — magic, frames, CRC, optional zstd |
//! | [`snapshot_store`] | streaming those bytes to and from a backend |

mod framing;
mod snapshot_store;

pub(crate) use snapshot_store::{RemoteSnapshotStore, SnapshotStoreConfig};

#[cfg(test)]
#[allow(clippy::expect_used, reason = "test code")]
pub(in crate::object_store) fn test_entry(idx: u64) -> zslg_log_types::SnapshotEntry {
    zslg_log_types::SnapshotEntry {
        global_index: zslg_log_types::LogIndex(idx),
        data: zslg_log_types::Data {
            data_id: zslg_log_types::DataId::test_id(zslg_time_types::Hlc(0), idx),
            blob: bytes::Bytes::copy_from_slice(format!("payload-{idx}").as_bytes()),
        },
    }
}
