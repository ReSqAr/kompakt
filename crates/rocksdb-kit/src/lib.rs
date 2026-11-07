//! The node's one RocksDB, with an engine-neutral surface.
//!
//! ```text
//!   Storage::open(root, StorageConfig{memory,..}, &[CfSpec{cf, compress}, ..])
//!       one DB, one block cache, one column family per store; the list IS the schema
//!   Batch::put/delete/delete_range ──> Storage::write ──> Storage::fence   (durable)
//!   Storage::scan(&cf) -> Scan        ordered, bounded reads
//!   Storage::get_pinned(&cf, key)     point reads
//! ```
//!
//! Callers never name `rust_rocksdb`; a store owns its column families
//! (`ColumnFamily::new("name")`) and the node assembly lists the ones it wires.
//! The engine-property gauges and the budget accounting live here too.

mod config;
mod error;
mod handles;
mod storage;

pub use config::StorageConfig;
pub use error::StorageError;
pub use handles::{Batch, Cf, ColumnFamily, Pinned, Scan};
pub use storage::{CfSpec, Storage, create_storage_root};
