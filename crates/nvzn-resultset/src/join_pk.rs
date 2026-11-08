use crate::ResultSet;
use nvzn_projection::{Projection, ReadTransaction, ReadTransactionExt, Ref, Row};
use nvzn_types::TenantId;

impl<'a, T: 'a> ResultSet<'a, T> {
    pub fn join_pk<P: Projection>(
        self,
        mut key_fn: impl FnMut(&T) -> Ref<P> + 'a,
    ) -> ResultSet<'a, (T, P)> {
        let (txn, tenant_id) = (self.read_txn, self.tenant_id);
        self.with_rows(move |rows| {
            Box::new(rows.map(move |t| {
                let t = t?;
                let p = txn.resolve(tenant_id, key_fn(&t))?;
                Ok((t, p))
            }))
        })
    }

    /// Like [`join_pk`](Self::join_pk), but reads nothing: each row carries a
    /// [`Lazy`] that resolves `P` against this result set's snapshot only when
    /// [`load`](Lazy::load)ed.
    pub fn join_pk_lazy<P: Projection>(
        self,
        mut key_fn: impl FnMut(&T) -> Ref<P> + 'a,
    ) -> ResultSet<'a, (T, Lazy<'a, P>)> {
        let (txn, tenant_id) = (self.read_txn, self.tenant_id);
        self.map(move |t| {
            let lazy = Lazy::new(tenant_id, txn, key_fn(&t));
            (t, lazy)
        })
    }
}

/// A reference to a `P`, resolved against the snapshot it came from on
/// [`load`](Self::load). It holds that snapshot, so it cannot outlive it and
/// always reads it — never a later commit.
#[must_use = "a lazy reference reads nothing until it is loaded"]
pub struct Lazy<'a, P: Projection> {
    tenant_id: TenantId,
    read_txn: &'a dyn ReadTransaction,
    r: Ref<P>,
}

impl<'a, P: Projection> Lazy<'a, P> {
    pub(super) fn new(tenant_id: TenantId, read_txn: &'a dyn ReadTransaction, r: Ref<P>) -> Self {
        Self {
            tenant_id,
            read_txn,
            r,
        }
    }

    #[must_use]
    pub fn reference(&self) -> Ref<P> {
        self.r
    }

    /// Reads `P` from the snapshot; a dangling reference is
    /// [`Error::ReferenceNotFound`](nvzn_types::Error::ReferenceNotFound), exactly
    /// as it is for [`join_pk`](ResultSet::join_pk).
    #[must_use = "loading reads the row; discarding it drops the result"]
    pub fn load(&self) -> Row<P> {
        self.read_txn.resolve(self.tenant_id, self.r)
    }
}
