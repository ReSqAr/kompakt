//! The store ports: the write side of persistence, the staging overlay both
//! stores share, and the store configuration.
//!
//! The read side (`ReadTransaction`) and the projection model live in
//! `nvzn-projection`; this crate builds on them. The overlay rides along because
//! the `Store` trait names its `CommitBatch` and `WriteTransactionImpl`.

mod config;
mod overlay;
mod store;

pub use config::{DEFAULT_FOLD_COMMIT, StoreConfig};
pub use overlay::{CommitBatch, IndexAction, WriteTransactionImpl};
pub use store::{ArcStore, ConfigHash, Store, WriteTransaction, WriteTransactionExt};
