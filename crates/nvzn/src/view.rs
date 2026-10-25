//! `View`: what a projection step may read.
//!
//! ```text
//!   Projection::next(event, &View) ──> Vec<Write>      (reads end here)
//!   Snapshot::view()               ──> the same reads over a committed snapshot
//! ```
//!
//! A tenant-scoped, borrowed, read-only window onto one transaction. Row
//! lookups go by id (`get`/`resolve`) or by a typed [`Index`] (`lookup`, the
//! reverse edge: "which `P` point at this `Q`"). It is lazy and borrows the
//! transaction, which is sound inside `next` because `next` returns an owned
//! `Vec`: every borrow ends before the framework writes.

use crate::{Error, Index, Projection, ReadTransaction, ReadTransactionExt, Ref, Rows, TenantId};

#[derive(Clone, Copy)]
pub struct View<'a> {
    tenant_id: TenantId,
    txn: &'a dyn ReadTransaction,
}

impl<'a> View<'a> {
    pub(crate) fn new(tenant_id: TenantId, txn: &'a dyn ReadTransaction) -> Self {
        Self { tenant_id, txn }
    }

    pub fn get<P: Projection>(&self, r: Ref<P>) -> Result<Option<P>, Error> {
        self.txn.get(self.tenant_id, r)
    }

    /// Like [`get`](Self::get), but a missing row is [`Error::ReferenceNotFound`].
    pub fn resolve<P: Projection>(&self, r: Ref<P>) -> Result<P, Error> {
        self.txn.resolve(self.tenant_id, r)
    }

    /// Every `P` whose index entry under `idx` points at `key`.
    pub fn lookup<P: Projection, Q: Projection>(
        &self,
        idx: Index<P, Q>,
        key: impl Into<Ref<Q>>,
    ) -> Rows<'a, Ref<P>> {
        let rows = self
            .txn
            .scan_index(self.tenant_id, P::TYPE_ID, idx.id(), key.into().id());
        Box::new(rows.map(|pid| pid.map(Ref::new)))
    }
}
