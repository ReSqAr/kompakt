//! `Snapshot`: one tenant's consistent view over a read transaction, from
//! which result sets are started.

use crate::ResultSet;
use nvzn_projection::{ArcReadTransaction, Index, Projection, ReadTransactionExt as _, Ref, View};
use nvzn_types::{Error, TenantId};

#[derive(Clone)]
pub struct Snapshot<'a> {
    tenant_id: TenantId,
    snapshot: ArcReadTransaction<'a>,
}

impl<'a> Snapshot<'a> {
    /// Wrap one read transaction as a tenant's snapshot. Plumbing for the
    /// crates that hand snapshots out (the reader and the live view); a caller
    /// takes a snapshot from those, not from here.
    #[doc(hidden)]
    pub fn from_txn(tenant_id: TenantId, snapshot: ArcReadTransaction<'a>) -> Self {
        Self {
            tenant_id,
            snapshot,
        }
    }

    #[must_use]
    pub fn view(&self) -> View<'_> {
        View::new(self.tenant_id, &*self.snapshot)
    }

    pub fn get<P>(&self, r: Ref<P>) -> Result<Option<P>, Error>
    where
        P: Projection + Send + Sync,
    {
        self.view().get(r)
    }

    pub fn select<P: Projection>(&self) -> ResultSet<'_, P> {
        let rows = self.snapshot.scan::<P>(self.tenant_id);
        ResultSet::new(self.tenant_id, &*self.snapshot, rows)
    }

    pub fn with_iter<'s, I>(&'s self, rows: I) -> ResultSet<'s, I::Item>
    where
        I: IntoIterator,
        I::IntoIter: 's,
        I::Item: 's,
    {
        ResultSet::new(
            self.tenant_id,
            &*self.snapshot,
            Box::new(rows.into_iter().map(Ok)),
        )
    }

    pub fn lookup<P: Projection, Q: Projection>(
        &self,
        idx: Index<P, Q>,
        key: impl Into<Ref<Q>>,
    ) -> ResultSet<'_, P> {
        let view = self.view();
        let rows = view.lookup(idx, key).map(move |r| view.resolve(r?));
        ResultSet::new(self.tenant_id, &*self.snapshot, Box::new(rows))
    }
}
