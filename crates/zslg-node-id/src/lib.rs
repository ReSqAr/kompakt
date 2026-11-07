//! The node's persistent identity.

use error_union::error_union;
use zslg_lifecycle_types::ConfigError;
use zslg_log::LogError;

mod node_id;

pub use node_id::{NODE_ID_CF, load_or_create_node_id};

error_union! {
    /// What reading or minting the identity can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Config(ConfigError),
        Log(LogError),
    }
}

impl From<rocksdb_kit::StorageError> for Error {
    fn from(e: rocksdb_kit::StorageError) -> Self {
        match e {
            rocksdb_kit::StorageError::InvalidConfiguration(msg) => {
                Self::Config(ConfigError::InvalidConfiguration(msg))
            }
            rocksdb_kit::StorageError::Storage(msg) => Self::Log(LogError::Storage(msg)),
        }
    }
}
