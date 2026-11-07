//! How a node reaches its object store: the backends it can be configured with
//! and the construction of the external client. Public configuration; knows no
//! port.

use error_union::error_union;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use zslg_snapshot_types::SnapshotError;

error_union! {
    /// What building a backend can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Snapshot(SnapshotError),
    }
}

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
                let store =
                    object_store::local::LocalFileSystem::new_with_prefix(root).map_err(|e| {
                        Error::Snapshot(SnapshotError::ObjectStore(format!(
                            "local fs at {root:?}: {e}"
                        )))
                    })?;
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
                    .map_err(|e| Error::Snapshot(SnapshotError::ObjectStore(format!("s3: {e}"))))?;
                Ok(Arc::new(store))
            }
        }
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
