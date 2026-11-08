use crate::{Lazy, ResultSet};
use nvzn_projection::{Index, Projection, Ref, Rows};

impl<'a, T: 'a> ResultSet<'a, T> {
    pub fn join_idx<P: Projection, Q: Projection>(
        self,
        idx: Index<P, Q>,
    ) -> IndexJoin<'a, T, P, Q> {
        IndexJoin { base: self, idx }
    }
}

#[must_use = "an index join reads nothing until `on` or `on_collected` names its key"]
pub struct IndexJoin<'a, T, P, Q: Projection> {
    base: ResultSet<'a, T>,
    idx: Index<P, Q>,
}

impl<'a, T: 'a, P: Projection, Q: Projection> IndexJoin<'a, T, P, Q> {
    pub fn on(self, key_fn: impl FnMut(&T) -> Ref<Q> + 'a) -> ResultSet<'a, (T, Rows<'a, P>)> {
        self.on_lazy(key_fn).map(|(t, bucket)| {
            let rows: Rows<'a, P> = Box::new(bucket.map(|lazy| lazy?.load()));
            (t, rows)
        })
    }

    /// Like [`on`](Self::on), but reads no `P`: each row's bucket is a
    /// [`LazyRows`], an iterator of [`Lazy`] references that resolve against
    /// this result set's snapshot only when [`load`](Lazy::load)ed.
    pub fn on_lazy(
        self,
        mut key_fn: impl FnMut(&T) -> Ref<Q> + 'a,
    ) -> ResultSet<'a, (T, LazyRows<'a, P>)> {
        let IndexJoin { base, idx } = self;
        let (txn, tenant_id, index_id) = (base.read_txn, base.tenant_id, idx.id());
        base.map(move |t| {
            let bucket: LazyRows<'a, P> = Box::new(
                txn.scan_index(tenant_id, P::TYPE_ID, index_id, key_fn(&t).id())
                    .map(move |pid| Ok(Lazy::new(tenant_id, txn, Ref::new(pid?)))),
            );
            (t, bucket)
        })
    }

    pub fn on_collected(self, key_fn: impl FnMut(&T) -> Ref<Q> + 'a) -> ResultSet<'a, (T, Vec<P>)> {
        self.on(key_fn)
            .try_map(|(t, ps)| Ok((t, ps.collect::<Result<_, _>>()?)))
    }
}

/// An index bucket whose rows are not yet read: the scan yields one [`Lazy`]
/// per entry, each bound to the snapshot the bucket came from.
pub type LazyRows<'a, P> = Rows<'a, Lazy<'a, P>>;
