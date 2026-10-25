use crate::{Projection, ReadTransaction, ReadTransactionExt, Ref, ResultSet, Row, TenantId};

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
    /// [`Error::ReferenceNotFound`](crate::Error::ReferenceNotFound), exactly
    /// as it is for [`join_pk`](ResultSet::join_pk).
    #[must_use = "loading reads the row; discarding it drops the result"]
    pub fn load(&self) -> Row<P> {
        self.read_txn.resolve(self.tenant_id, self.r)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::{
        resultset::test_support::{rig, seed, todo, user},
        testsupport::{Todo, TodoId, UserId, commit_row, tenant_id},
    };

    #[tokio::test]
    async fn join_pk_resolves_and_errors_on_missing() {
        let (store, reader, _idx) = rig().await;
        seed(&store);
        commit_row(
            &store,
            tenant_id(),
            Ref::new(UserId(200).into()),
            &user(200, "bobb"),
        );

        let resolved: Vec<(u128, u128)> = reader
            .snapshot()
            .unwrap()
            .select::<Todo>()
            .join_pk(Todo::user)
            .collect()
            .unwrap()
            .into_iter()
            .map(|(t, u)| (t.id.0, u.id.0))
            .collect();
        assert_eq!(resolved, vec![(1, 100), (2, 100), (3, 200)]);

        commit_row(
            &store,
            tenant_id(),
            Ref::new(TodoId(9).into()),
            &todo(9, 999, "dangling"),
        );
        let snapshot = reader.snapshot().unwrap();
        let err = snapshot
            .select::<Todo>()
            .join_pk(Todo::user)
            .collect()
            .expect_err("dangling reference must err");
        assert!(matches!(err, crate::Error::ReferenceNotFound { .. }));
    }

    #[tokio::test]
    async fn join_pk_lazy_defers_the_read_and_errors_only_on_load() {
        let (store, reader, _idx) = rig().await;
        seed(&store);
        commit_row(
            &store,
            tenant_id(),
            Ref::new(TodoId(9).into()),
            &todo(9, 999, "dangling"),
        );

        // The dangling todo does not fail the result set: nothing is read yet.
        let snapshot = reader.snapshot().unwrap();
        let rows = snapshot
            .select::<Todo>()
            .join_pk_lazy(Todo::user)
            .collect()
            .expect("a lazy join reads no target");
        assert_eq!(rows.len(), 4);

        // User 200 lands after the snapshot; the lazy load must not see it.
        commit_row(
            &store,
            tenant_id(),
            Ref::new(UserId(200).into()),
            &user(200, "bobb"),
        );

        let loaded: Vec<(u128, Option<u128>)> = rows
            .iter()
            .map(|(t, u)| {
                assert_eq!(u.reference(), Todo::user(t));
                match u.load() {
                    Ok(u) => (t.id.0, Some(u.id.0)),
                    Err(crate::Error::ReferenceNotFound { .. }) => (t.id.0, None),
                    Err(e) => panic!("unexpected error {e:?}"),
                }
            })
            .collect();
        assert_eq!(
            loaded,
            vec![(1, Some(100)), (2, Some(100)), (3, None), (9, None)],
            "loads read the snapshot the join came from"
        );
    }
}
