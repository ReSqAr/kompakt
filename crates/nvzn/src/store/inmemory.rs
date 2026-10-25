use crate::store::IndexAction;
use crate::{
    ArcReadTransaction, CommitBatch, ConfigHash, Error, IndexId, ProjectionId, ProjectionTypeId,
    ReadTransaction, Rows, Store, TenantId, Watermark, WriteTransactionImpl,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, RwLock};

#[derive(Default, Clone)]
struct Inner {
    config_hash: Option<ConfigHash>,
    watermark: Watermark,
    store: BTreeMap<(TenantId, ProjectionTypeId, ProjectionId), Bytes>,
    index: BTreeMap<(TenantId, ProjectionTypeId, IndexId, ProjectionId), BTreeSet<ProjectionId>>,
}

#[derive(Clone, Default)]
pub struct InMemoryStore {
    guard: Arc<RwLock<Inner>>,
}

impl Store for InMemoryStore {
    fn config_hash(&self) -> Result<Option<ConfigHash>, Error> {
        Ok(self
            .guard
            .read()
            .map_err(|e| Error::Poison(e.to_string()))?
            .config_hash)
    }

    fn set_config_hash(&self, hash: &ConfigHash) -> Result<(), Error> {
        self.guard
            .write()
            .map_err(|e| Error::Poison(e.to_string()))?
            .config_hash = Some(*hash);
        Ok(())
    }

    fn read(&self) -> Result<ArcReadTransaction<'_>, Error> {
        let guard = self
            .guard
            .read()
            .map_err(|e| Error::Poison(e.to_string()))?;
        Ok(Arc::new(InMemoryReadTransaction::new(guard.clone())))
    }

    fn write(&self) -> Result<WriteTransactionImpl<'_>, Error> {
        let read_txn = self.read()?;

        Ok(WriteTransactionImpl::<'_>::new(read_txn, self))
    }

    fn commit_batch(&self, batch: CommitBatch, watermark: Watermark) -> Result<(), Error> {
        let CommitBatch {
            overlay_store,
            overlay_index,
        } = batch;

        let mut guard = self
            .guard
            .write()
            .map_err(|e| Error::Poison(e.to_string()))?;

        for (k, v) in overlay_store.into_iter() {
            match v {
                Some(v) => {
                    guard.store.insert(k, v);
                }
                None => {
                    guard.store.remove(&k);
                }
            }
        }

        for (k, v) in overlay_index.into_iter() {
            let s = guard.index.entry(k).or_default();
            for (pid, a) in v.into_iter() {
                match a {
                    IndexAction::Add => {
                        s.insert(pid);
                    }
                    IndexAction::Remove => {
                        s.remove(&pid);
                    }
                }
            }
        }

        guard.watermark = watermark;

        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }

    fn reset(&self) -> Result<(), Error> {
        let mut guard = self
            .guard
            .write()
            .map_err(|e| Error::Poison(e.to_string()))?;
        *guard = Inner::default();
        Ok(())
    }
}

struct InMemoryReadTransaction {
    inner: Inner,
}

impl InMemoryReadTransaction {
    fn new(inner: Inner) -> Self {
        Self { inner }
    }
}

impl ReadTransaction for InMemoryReadTransaction {
    fn watermark(&self) -> Result<Watermark, Error> {
        Ok(self.inner.watermark.clone())
    }

    fn get_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        pid: ProjectionId,
    ) -> Result<Option<Bytes>, Error> {
        let b = self.inner.store.get(&(tenant_id, ptid, pid));
        Ok(b.cloned())
    }

    fn scan_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
    ) -> Rows<'_, (ProjectionTypeId, ProjectionId, Bytes)> {
        let range = (
            Bound::Included((tenant_id, ptid, ProjectionId::MIN)),
            Bound::Included((tenant_id, ptid, ProjectionId::MAX)),
        );
        Box::new(
            self.inner
                .store
                .range(range)
                .map(|(&(_, ptid, pid), v)| Ok((ptid, pid, v.clone()))),
        )
    }

    fn scan_index(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        q: ProjectionId,
    ) -> Rows<'_, ProjectionId> {
        let bucket = self.inner.index.get(&(tenant_id, ptid, index_id, q));
        Box::new(bucket.into_iter().flatten().map(|&pid| Ok(pid)))
    }
}
