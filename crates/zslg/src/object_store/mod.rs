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

use crate::Error;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) use snapshot_store::{RemoteSnapshotStore, SnapshotStoreConfig};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectStoreConfig {
    LocalFs {
        root: PathBuf,
    },
    InMemory,
    #[cfg(feature = "s3")]
    S3 {
        endpoint: String,
        bucket: String,
        access_key: String,
        secret_key: String,
        region: String,
        allow_http: bool,
    },
}

impl std::fmt::Debug for ObjectStoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalFs { root } => f.debug_struct("LocalFs").field("root", root).finish(),
            Self::InMemory => f.debug_struct("InMemory").finish(),
            #[cfg(feature = "s3")]
            Self::S3 {
                endpoint,
                bucket,
                access_key,
                region,
                allow_http,
                ..
            } => f
                .debug_struct("S3")
                .field("endpoint", endpoint)
                .field("bucket", bucket)
                .field("access_key", access_key)
                .field("region", region)
                .field("allow_http", allow_http)
                .field("secret_key", &"<redacted>")
                .finish(),
        }
    }
}

/// Bound on a single object-store HTTP *request*, not on an operation — a
/// multipart upload is many requests, so a legitimately large snapshot is never
/// penalised while a hung connection still fails.
///
/// Long on purpose: a backstop, not a latency target. Without it nothing in the
/// snapshot or compaction path has a deadline, and a stalled backend wedges the
/// job indefinitely.
#[cfg(feature = "s3")]
const S3_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

impl ObjectStoreConfig {
    pub fn build(&self) -> Result<Arc<dyn ObjectStore>, Error> {
        match self {
            Self::InMemory => Ok(Arc::new(object_store::memory::InMemory::new())),
            Self::LocalFs { root } => {
                let store = object_store::local::LocalFileSystem::new_with_prefix(root)
                    .map_err(|e| Error::ObjectStore(format!("local fs at {root:?}: {e}")))?;
                Ok(std::sync::Arc::new(store))
            }
            #[cfg(feature = "s3")]
            Self::S3 {
                endpoint,
                bucket,
                access_key,
                secret_key,
                region,
                allow_http,
            } => {
                let store = object_store::aws::AmazonS3Builder::new()
                    .with_client_options(
                        object_store::ClientOptions::new().with_timeout(S3_REQUEST_TIMEOUT),
                    )
                    .with_endpoint(endpoint)
                    .with_bucket_name(bucket)
                    .with_access_key_id(access_key)
                    .with_secret_access_key(secret_key)
                    .with_region(region)
                    .with_allow_http(*allow_http)
                    .with_virtual_hosted_style_request(false)
                    .build()
                    .map_err(|e| Error::ObjectStore(format!("s3: {e}")))?;
                Ok(Arc::new(store))
            }
        }
    }
}

/// How hard zstd works on a snapshot object. The *algorithm* is not a choice —
/// snapshot objects are always one streaming zstd frame — so this carries only
/// the level, `0..=22`, rising from cheapest to densest.
///
/// The range is checked once, here, so every downstream consumer can hand the
/// value straight to zstd without re-validating.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionLevel(u8);

impl CompressionLevel {
    /// zstd's densest setting. Levels above this are rejected by the library.
    pub const MAX: u8 = 22;

    /// Level 1 — the cheapest setting that actually compresses. Note that level
    /// *0* is not faster: zstd reads it as "use the library default", which is
    /// [`Self::standard`].
    #[must_use]
    pub const fn fast() -> Self {
        Self(1)
    }

    /// Level 3 — zstd's own default, and ours.
    #[must_use]
    pub const fn standard() -> Self {
        Self(3)
    }

    /// `Err(Error::InvalidConfiguration)` above [`Self::MAX`].
    pub fn new(level: u8) -> Result<Self, Error> {
        if level > Self::MAX {
            return Err(Error::InvalidConfiguration(format!(
                "compression zstd level must be 0–{}, got {level}",
                Self::MAX
            )));
        }
        Ok(Self(level))
    }

    /// The level in zstd's own signed representation. Widening is infallible.
    #[must_use]
    pub fn get(self) -> i32 {
        i32::from(self.0)
    }
}

impl Default for CompressionLevel {
    fn default() -> Self {
        Self::standard()
    }
}

pub trait IntoArcObjectStore {
    fn into_arc_object_store(self) -> Result<Arc<dyn ObjectStore>, Error>;
}

impl IntoArcObjectStore for ObjectStoreConfig {
    fn into_arc_object_store(self) -> Result<Arc<dyn ObjectStore>, Error> {
        self.build()
    }
}

impl<T: ObjectStore + 'static> IntoArcObjectStore for Arc<T> {
    fn into_arc_object_store(self) -> Result<Arc<dyn ObjectStore>, Error> {
        Ok(self)
    }
}

impl IntoArcObjectStore for Arc<dyn ObjectStore> {
    fn into_arc_object_store(self) -> Result<Arc<dyn ObjectStore>, Error> {
        Ok(self)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, reason = "test code")]
pub(in crate::object_store) fn test_entry(idx: u64) -> crate::meta::SnapshotEntry {
    crate::meta::SnapshotEntry {
        global_index: crate::ids::LogIndex(idx),
        data: crate::meta::Data {
            data_id: crate::ids::DataId::test_id(crate::hlc::Hlc(0), idx),
            blob: bytes::Bytes::copy_from_slice(format!("payload-{idx}").as_bytes()),
        },
    }
}
