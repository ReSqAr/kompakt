//! The node's one RocksDB, with an engine-neutral surface.
//!
//! ```text
//!   Storage::open(root, StorageConfig{memory,..}, &[CfSpec::new(family, compress), ..])
//!       one DB, one block cache, one column family per store; the list IS the schema
//!   Batch::put/delete/delete_range ──> Storage::write ──> Storage::fence   (durable)
//!   Storage::scan(&cf) -> Scan                ordered reads, whole family
//!   Storage::scan_prefix(&cf, &[u8; N])       the keys under one prefix (Prefixed families)
//!   Storage::get_pinned(&cf, key)             point reads
//!   Storage::snapshot() -> Snapshot           the same reads as of one moment
//!
//!   ColumnFamily<Plain>        no filter policy: every zslg store
//!   ColumnFamily<Lookup>       + a bloom filter, for point lookups
//!   ColumnFamily<Prefixed<N>>  prefix filter and extractor, for scans of one prefix's set
//! ```
//!
//! How a family is read is part of its type and always spelled out, so the
//! option set a store gets is visible where the family is declared and the
//! methods that only suit one mode exist only on its handle. Callers never name
//! `rust_rocksdb`; a store owns its column families
//! (`ColumnFamily::<Plain>::new("name")`) and the node assembly lists the ones
//! it wires. The kit is a typed wrapper: durability policy (when to fence, what
//! to skip) stays with the stores that own the data.

mod config;
mod error;
mod handles;
mod storage;

pub use config::StorageConfig;
pub use error::StorageError;
pub use handles::{
    Access, AccessKind, AnyFamily, Batch, Cf, ColumnFamily, Lookup, Pinned, Plain, Prefixed, Scan,
};
pub use storage::{CfSpec, Snapshot, Storage, create_storage_root};
