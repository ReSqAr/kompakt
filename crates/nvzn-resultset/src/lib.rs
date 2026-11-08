//! `ResultSet`: lazy, tenant-scoped query results over one read transaction.

use nvzn_store::{ReadTransaction, Rows};
use nvzn_types::{Error, TenantId};

mod filter_map;
mod join_on;
mod join_pk;
mod map;

pub use join_on::{IndexJoin, LazyRows};
pub use join_pk::Lazy;

#[must_use = "a result set reads nothing until it is iterated"]
pub struct ResultSet<'a, T> {
    tenant_id: TenantId,
    read_txn: &'a dyn ReadTransaction,
    rows: Rows<'a, T>,
}

impl<'a, T: 'a> ResultSet<'a, T> {
    pub fn new(tenant_id: TenantId, read_txn: &'a dyn ReadTransaction, rows: Rows<'a, T>) -> Self {
        Self {
            tenant_id,
            read_txn,
            rows,
        }
    }

    #[must_use]
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    pub fn collect(self) -> Result<Vec<T>, Error> {
        self.rows.collect()
    }

    fn with_rows<S: 'a>(self, f: impl FnOnce(Rows<'a, T>) -> Rows<'a, S>) -> ResultSet<'a, S> {
        ResultSet {
            tenant_id: self.tenant_id,
            read_txn: self.read_txn,
            rows: f(self.rows),
        }
    }
}

impl<'a, T> IntoIterator for ResultSet<'a, T> {
    type Item = Result<T, Error>;
    type IntoIter = Rows<'a, T>;

    fn into_iter(self) -> Rows<'a, T> {
        self.rows
    }
}
