//! The engine-neutral vocabulary the stores speak: a named column family, a
//! write batch, an ordered scan and a pinned read. Nothing here names
//! `rust_rocksdb` in its signatures.

use std::ops::Deref;
use std::sync::Arc;

use rust_rocksdb::{
    BoundColumnFamily, DBPinnableSlice, DBRawIteratorWithThreadMode, DBWithThreadMode,
    MultiThreaded, WriteBatch,
};

use crate::error::StorageError;

/// A column family, named by the store that owns it. The node assembly passes
/// the families it wires to [`Storage::open`](crate::Storage::open); the
/// names are the on-disk schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnFamily(&'static str);

impl ColumnFamily {
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// The on-disk name, also the value of the `cf` metrics label.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.0
    }
}

/// An opened column family.
pub struct Cf<'db>(pub(crate) Arc<BoundColumnFamily<'db>>);

/// Writes applied atomically, across column families.
#[derive(Default)]
pub struct Batch(pub(crate) WriteBatch);

impl Batch {
    #[must_use]
    pub fn new() -> Self {
        Self(WriteBatch::default())
    }

    pub fn put(&mut self, cf: &Cf<'_>, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.0.put_cf(&cf.0, key, value);
    }

    /// Put a key and value each assembled from several slices, so a large
    /// value is copied into the batch buffer once rather than concatenated first.
    pub fn put_vectored(
        &mut self,
        cf: &Cf<'_>,
        key: &[std::io::IoSlice<'_>],
        value: &[std::io::IoSlice<'_>],
    ) -> Result<(), StorageError> {
        self.0
            .put_cf_vectored(&cf.0, key, value)
            .map_err(|e| StorageError::Storage(e.to_string()))
    }

    pub fn delete(&mut self, cf: &Cf<'_>, key: impl AsRef<[u8]>) {
        self.0.delete_cf(&cf.0, key);
    }

    /// Delete `[from, to)`.
    pub fn delete_range<K: AsRef<[u8]>>(&mut self, cf: &Cf<'_>, from: K, to: K) {
        self.0.delete_range_cf(&cf.0, from, to);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A point read that keeps its block pinned in the cache while it is alive.
pub struct Pinned<'db>(pub(crate) DBPinnableSlice<'db>);

impl Deref for Pinned<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

/// An ordered scan. `key` and `value` borrow the scan's current block and pin
/// it: decode there, and let the borrow end before the scan moves.
pub struct Scan<'db>(pub(crate) DBRawIteratorWithThreadMode<'db, DBWithThreadMode<MultiThreaded>>);

impl Scan<'_> {
    pub fn seek_to_first(&mut self) {
        self.0.seek_to_first();
    }

    pub fn seek_to_last(&mut self) {
        self.0.seek_to_last();
    }

    /// Position at the first key `>= key`.
    pub fn seek(&mut self, key: impl AsRef<[u8]>) {
        self.0.seek(key);
    }

    /// Position at the last key `<= key`.
    pub fn seek_for_prev(&mut self, key: impl AsRef<[u8]>) {
        self.0.seek_for_prev(key);
    }

    pub fn next(&mut self) {
        self.0.next();
    }

    pub fn prev(&mut self) {
        self.0.prev();
    }

    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.0.key()
    }

    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.0.value()
    }

    /// Whether the scan ended because it ran out of keys (`Ok`) or because the
    /// engine failed (`Err`). Check after the loop.
    pub fn status(&self) -> Result<(), StorageError> {
        self.0
            .status()
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}
