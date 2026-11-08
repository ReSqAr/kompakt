//! Test wiring for the crate's own tests.

use std::path::Path;
use std::sync::Arc;

use bytesize::ByteSize;
use rocksdb_kit::{CfSpec, Storage, StorageConfig};

use crate::{Error, STABLE_LOG_CF};

/// A storage root carrying this log's column family.
pub(crate) fn open_for_test(root: &Path) -> Result<Arc<Storage>, Error> {
    Ok(Storage::open(
        root,
        StorageConfig::new(ByteSize::from_mib(64)),
        &[CfSpec {
            cf: STABLE_LOG_CF,
            compress: false,
        }],
    )?)
}

impl crate::StableLog {
    /// A log on its own storage root.
    pub(crate) async fn open_engine(dir: std::path::PathBuf) -> Result<Self, Error> {
        Self::open_on(open_for_test(&dir)?).await
    }
}
