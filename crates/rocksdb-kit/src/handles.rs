//! The engine-neutral vocabulary the stores speak: a named column family, a
//! write batch, an ordered scan and a pinned read. Nothing here names
//! `rust_rocksdb` in its signatures.

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use rust_rocksdb::{
    BoundColumnFamily, DBPinnableSlice, DBRawIteratorWithThreadMode, DBWithThreadMode,
    MultiThreaded, WriteBatch,
};

use crate::error::StorageError;

/// How a column family is read, and so which options it is opened with.
///
/// The mode is part of the family's type: `ColumnFamily<Plain>`, never a bare
/// `ColumnFamily`. It is written wherever a family is declared, so which option
/// set a store gets is visible at the declaration, and the methods that only
/// make sense for one mode exist only on that mode's handle.
pub trait Access: sealed::Sealed + Copy + std::fmt::Debug + Eq + std::hash::Hash + 'static {
    /// The mode as data, for the schema list `Storage::open` takes.
    const KIND: AccessKind;
}

mod sealed {
    pub trait Sealed {}
}

/// An [`Access`] mode as a value: what [`CfSpec`](crate::CfSpec) carries and
/// `Storage::open` builds options from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccessKind {
    Plain,
    Lookup,
    Prefixed(usize),
}

/// The options every zslg store has always had: partitioned index and filter
/// blocks inside the cache budget, and no filter policy. A test pins the
/// resulting option set (`plain_options.golden`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Plain;

/// [`Plain`] plus a 10-bit bloom filter on whole keys: for a family read by
/// point lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Lookup;

/// A family read by prefix scans. The first `N` bytes of a key identify the set
/// being scanned and the rest a member of it, always at a fixed width, so the
/// cut lands on a field boundary. Whole-key filtering is off; a prefix filter
/// (and a memtable prefix bloom) lets a scan skip the files that hold nothing
/// for its prefix. Sound only if the key layout really puts a constant-width
/// prefix first: the compiler checks the width a scan is given, not the layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Prefixed<const N: usize>;

impl sealed::Sealed for Plain {}
impl sealed::Sealed for Lookup {}
impl<const N: usize> sealed::Sealed for Prefixed<N> {}

impl Access for Plain {
    const KIND: AccessKind = AccessKind::Plain;
}
impl Access for Lookup {
    const KIND: AccessKind = AccessKind::Lookup;
}
impl<const N: usize> Access for Prefixed<N> {
    const KIND: AccessKind = AccessKind::Prefixed(N);
}

/// A column family, named by the store that owns it, and typed by how it is
/// read. The node assembly passes the families it wires to
/// [`Storage::open`](crate::Storage::open); the names are the on-disk schema.
#[derive(Debug)]
pub struct ColumnFamily<A: Access>(&'static str, PhantomData<A>);

impl<A: Access> Clone for ColumnFamily<A> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<A: Access> Copy for ColumnFamily<A> {}
impl<A: Access> PartialEq for ColumnFamily<A> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl<A: Access> Eq for ColumnFamily<A> {}
impl<A: Access> std::hash::Hash for ColumnFamily<A> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<A: Access> ColumnFamily<A> {
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(name, PhantomData)
    }

    /// The on-disk name, also the value of the `cf` metrics label.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.0
    }
}

/// A column family with its access mode as data: what a schema list holds and
/// `Storage::column_families` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AnyFamily {
    name: &'static str,
    access: AccessKind,
}

impl AnyFamily {
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.name
    }

    #[must_use]
    pub const fn access(self) -> AccessKind {
        self.access
    }
}

impl<A: Access> From<ColumnFamily<A>> for AnyFamily {
    fn from(cf: ColumnFamily<A>) -> Self {
        Self {
            name: cf.0,
            access: A::KIND,
        }
    }
}

impl<A: Access> PartialEq<ColumnFamily<A>> for AnyFamily {
    fn eq(&self, other: &ColumnFamily<A>) -> bool {
        self.name == other.0 && self.access == A::KIND
    }
}

/// An opened column family.
pub struct Cf<'db, A: Access>(pub(crate) Arc<BoundColumnFamily<'db>>, PhantomData<A>);

impl<'db, A: Access> Cf<'db, A> {
    pub(crate) fn new(handle: Arc<BoundColumnFamily<'db>>) -> Self {
        Self(handle, PhantomData)
    }
}

/// Writes applied atomically, across column families.
#[derive(Default)]
pub struct Batch(pub(crate) WriteBatch);

impl Batch {
    #[must_use]
    pub fn new() -> Self {
        Self(WriteBatch::default())
    }

    pub fn put<A: Access>(
        &mut self,
        cf: &Cf<'_, A>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        self.0.put_cf(&cf.0, key, value);
    }

    /// Put a key and value each assembled from several slices, so a large
    /// value is copied into the batch buffer once rather than concatenated first.
    pub fn put_vectored<A: Access>(
        &mut self,
        cf: &Cf<'_, A>,
        key: &[std::io::IoSlice<'_>],
        value: &[std::io::IoSlice<'_>],
    ) -> Result<(), StorageError> {
        self.0
            .put_cf_vectored(&cf.0, key, value)
            .map_err(|e| StorageError::Storage(e.to_string()))
    }

    pub fn delete<A: Access>(&mut self, cf: &Cf<'_, A>, key: impl AsRef<[u8]>) {
        self.0.delete_cf(&cf.0, key);
    }

    /// Delete `[from, to)`.
    pub fn delete_range<A: Access, K: AsRef<[u8]>>(&mut self, cf: &Cf<'_, A>, from: K, to: K) {
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
