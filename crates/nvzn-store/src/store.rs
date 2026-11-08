use bytes::Bytes;
use nvzn_projection::{ArcReadTransaction, Projection, ReadTransaction, Ref, Watermark};
use nvzn_types::{Error, IndexId, ProjectionId, ProjectionTypeId, TenantId};
use std::sync::Arc;

use crate::overlay::{CommitBatch, WriteTransactionImpl};

pub trait WriteTransaction: ReadTransaction {
    /// Stage a row, or stage its removal with `None`.
    ///
    /// Takes an owned [`Bytes`] rather than a slice so the overlay can adopt
    /// the buffer the caller already encoded into. A `&[u8]` here would force
    /// every implementation to copy, once per row per event, purely to satisfy
    /// the signature.
    fn set_raw(
        &mut self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        pid: ProjectionId,
        payload: Option<Bytes>,
    ) -> Result<(), Error>;

    fn add_index(
        &mut self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        p: ProjectionId,
        q: ProjectionId,
    ) -> Result<(), Error>;
    fn remove_index(
        &mut self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        p: ProjectionId,
        q: ProjectionId,
    ) -> Result<(), Error>;
}

pub trait WriteTransactionExt: WriteTransaction {
    fn set<P: Projection>(
        &mut self,
        tenant_id: TenantId,
        r: Ref<P>,
        p: Option<&P>,
    ) -> Result<(), Error> {
        // `Bytes::from(Vec<u8>)` adopts the allocation `encode` just made; it
        // does not copy. The encoded row travels to the overlay untouched.
        let encoded = p.map(P::encode).transpose()?.map(Bytes::from);
        self.set_raw(tenant_id, r.type_id(), r.id(), encoded)
    }
}
impl<T: WriteTransaction + ?Sized> WriteTransactionExt for T {}

pub type ConfigHash = [u8; 32];

pub trait Store: Send + Sync + 'static {
    fn config_hash(&self) -> Result<Option<ConfigHash>, Error>;
    fn set_config_hash(&self, hash: &ConfigHash) -> Result<(), Error>;
    fn read(&self) -> Result<ArcReadTransaction<'_>, Error>;
    fn write(&self) -> Result<WriteTransactionImpl<'_>, Error>;
    fn commit_batch(&self, batch: CommitBatch, watermark: Watermark) -> Result<(), Error>;
    fn flush(&self) -> Result<(), Error>;
    fn reset(&self) -> Result<(), Error>;
}

pub type ArcStore = Arc<dyn Store>;
