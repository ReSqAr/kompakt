use crate::{
    ArcReadTransaction, Error, IndexId, ProjectionId, ProjectionTypeId, ReadTransaction, Rows,
    Store, TenantId, Watermark, WriteTransaction,
};
use bytes::Bytes;
use itertools::EitherOrBoth;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

#[derive(Clone, Copy)]
pub(crate) enum IndexAction {
    Add,
    Remove,
}

type OverlayStore = BTreeMap<(TenantId, ProjectionTypeId, ProjectionId), Option<Bytes>>;
type OverlayIndex = BTreeMap<
    (TenantId, ProjectionTypeId, IndexId, ProjectionId),
    BTreeMap<ProjectionId, IndexAction>,
>;

pub struct CommitBatch {
    pub(in crate::store) overlay_store: OverlayStore,
    pub(in crate::store) overlay_index: OverlayIndex,
}

/// What a staged row costs beyond its payload: the key and the map's node.
const ROW_OVERHEAD: usize = 96;
/// What a staged index entry costs.
const INDEX_OVERHEAD: usize = 64;

pub struct WriteTransactionImpl<'a> {
    pub(in crate::store) read_txn: ArcReadTransaction<'a>,
    store: &'a dyn Store,
    pub(in crate::store) overlay_store: OverlayStore,
    pub(in crate::store) overlay_index: OverlayIndex,
    /// [`Self::staged_bytes`].
    staged: usize,
}

impl<'a> WriteTransactionImpl<'a> {
    pub(crate) fn new(read_txn: ArcReadTransaction<'a>, store: &'a dyn Store) -> Self {
        Self {
            read_txn,
            store,
            overlay_store: Default::default(),
            overlay_index: Default::default(),
            staged: 0,
        }
    }

    /// What the transaction holds in memory until it commits, estimated: every
    /// staged row's payload and key, every staged index entry. A row staged
    /// twice counts once, at its last payload.
    pub(crate) fn staged_bytes(&self) -> usize {
        self.staged
    }

    /// Every row written, deletions included.
    pub(crate) fn written(&self) -> Vec<(TenantId, ProjectionTypeId, ProjectionId)> {
        self.overlay_store.keys().copied().collect()
    }

    pub fn commit(self, watermark: Watermark) -> Result<(), Error> {
        let Self {
            read_txn,
            store,
            overlay_store,
            overlay_index,
            ..
        } = self;

        drop(read_txn);

        store.commit_batch(
            CommitBatch {
                overlay_store,
                overlay_index,
            },
            watermark,
        )
    }
}

impl<'a> ReadTransaction for WriteTransactionImpl<'a> {
    fn watermark(&self) -> Result<Watermark, Error> {
        self.read_txn.watermark()
    }

    fn get_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        pid: ProjectionId,
    ) -> Result<Option<Bytes>, Error> {
        match self.overlay_store.get(&(tenant_id, ptid, pid)) {
            Some(Some(b)) => Ok(Some(b.clone())),
            Some(None) => Ok(None),
            None => self.read_txn.get_raw(tenant_id, ptid, pid),
        }
    }

    fn scan_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
    ) -> Rows<'_, (ProjectionTypeId, ProjectionId, Bytes)> {
        let it_store = self.read_txn.scan_raw(tenant_id, ptid);
        let range = (
            Bound::Included((tenant_id, ptid, ProjectionId::MIN)),
            Bound::Included((tenant_id, ptid, ProjectionId::MAX)),
        );
        let it_overlay = self
            .overlay_store
            .range(range)
            .map(|(&k, v)| (k, v.clone()));
        let it_merge = itertools::merge_join_by(
            it_store,
            it_overlay,
            move |r, ((tid, optid, opid), _)| match r {
                Ok((sptid, spid, _)) => {
                    Ord::cmp(&(tenant_id, *sptid, *spid), &(*tid, *optid, *opid))
                }
                Err(_) => Ordering::Less,
            },
        );

        Box::new(it_merge.filter_map(|e| match e {
            EitherOrBoth::Both(_, ((_, optid, opid), Some(b))) => Some(Ok((optid, opid, b))),
            EitherOrBoth::Both(_, (_, None)) => None,
            EitherOrBoth::Left(Ok((sptid, spid, sb))) => Some(Ok((sptid, spid, sb))),
            EitherOrBoth::Left(Err(e)) => Some(Err(e)),
            EitherOrBoth::Right(((_, optid, opid), Some(b))) => Some(Ok((optid, opid, b))),
            EitherOrBoth::Right((_, None)) => None,
        }))
    }

    fn scan_index(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        q: ProjectionId,
    ) -> Rows<'_, ProjectionId> {
        let it_store = self.read_txn.scan_index(tenant_id, ptid, index_id, q);
        let bucket = self
            .overlay_index
            .get(&(tenant_id, ptid, index_id, q))
            .into_iter()
            .flatten();
        let it_overlay = bucket.map(|(&pid, &action)| (pid, action));
        let it_merge = itertools::merge_join_by(it_store, it_overlay, |r, (pid, _)| match r {
            Ok(rpid) => Ord::cmp(rpid, pid),
            Err(_) => Ordering::Less,
        });

        Box::new(it_merge.filter_map(|e| match e {
            EitherOrBoth::Both(_, (pid, IndexAction::Add)) => Some(Ok(pid)),
            EitherOrBoth::Both(_, (_, IndexAction::Remove)) => None,
            EitherOrBoth::Left(r) => Some(r),
            EitherOrBoth::Right((pid, IndexAction::Add)) => Some(Ok(pid)),
            EitherOrBoth::Right((_, IndexAction::Remove)) => None,
        }))
    }
}

impl<'a> WriteTransaction for WriteTransactionImpl<'a> {
    fn set_raw(
        &mut self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        pid: ProjectionId,
        payload: Option<Bytes>,
    ) -> Result<(), Error> {
        let len = payload.as_ref().map_or(0, Bytes::len);
        match self.overlay_store.insert((tenant_id, ptid, pid), payload) {
            None => self.staged = self.staged.saturating_add(ROW_OVERHEAD).saturating_add(len),
            Some(old) => {
                let old = old.as_ref().map_or(0, Bytes::len);
                self.staged = self.staged.saturating_sub(old).saturating_add(len);
            }
        }

        Ok(())
    }

    fn add_index(
        &mut self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        p: ProjectionId,
        q: ProjectionId,
    ) -> Result<(), Error> {
        let staged = self
            .overlay_index
            .entry((tenant_id, ptid, index_id, q))
            .or_default()
            .insert(p, IndexAction::Add);
        if staged.is_none() {
            self.staged = self.staged.saturating_add(INDEX_OVERHEAD);
        }

        Ok(())
    }

    fn remove_index(
        &mut self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        p: ProjectionId,
        q: ProjectionId,
    ) -> Result<(), Error> {
        let staged = self
            .overlay_index
            .entry((tenant_id, ptid, index_id, q))
            .or_default()
            .insert(p, IndexAction::Remove);
        if staged.is_none() {
            self.staged = self.staged.saturating_add(INDEX_OVERHEAD);
        }

        Ok(())
    }
}
