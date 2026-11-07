//! What opening or using the node's RocksDB can fail with.

/// A storage failure, carrying enough context to diagnose without re-running.
/// The text is the message alone: the caller's error type adds its own prefix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    /// The configuration is not one the engine can run with.
    #[error("{0}")]
    InvalidConfiguration(String),
    /// The engine, or the filesystem under it, refused.
    #[error("{0}")]
    Storage(String),
}

impl From<bytesize::OutOfRange> for StorageError {
    fn from(e: bytesize::OutOfRange) -> Self {
        StorageError::InvalidConfiguration(e.to_string())
    }
}
