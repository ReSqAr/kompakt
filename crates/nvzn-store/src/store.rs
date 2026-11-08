use crate::{Projection, Ref};
use bytes::Bytes;
use nvzn_types::{Error, IndexId, ProjectionId, ProjectionTypeId, TenantId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zslg::{DataId, Hlc, LogIndex};

use crate::overlay::{CommitBatch, WriteTransactionImpl};

pub type Row<T> = Result<T, Error>;
pub type Rows<'a, T> = Box<dyn Iterator<Item = Row<T>> + 'a>;

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    pub index: Option<LogIndex>,
    pub data_id: Option<DataId>,
    pub hlc: Option<Hlc>,
}

impl Watermark {
    /// Whether a store folded up to this watermark already accounts for the
    /// entry `data_id`: either it was folded, or it sits at or below the
    /// settled prefix without being part of it — a write that never made
    /// quorum, which the blended view must not show.
    ///
    /// Everything at or below `hlc` is covered, with one tie. The stable log
    /// is HLC-ordered, so every stable entry below `hlc` has been folded; at
    /// `hlc` itself that holds only when `hlc` came from a `Watermark` event
    /// (which settles everything at or below it). When it came from an
    /// entry's own HLC instead — `hlc` equals `data_id.hlc()` — a later stable
    /// entry may share it, so at the tie only `data_id` itself decides.
    #[must_use]
    pub fn covers(&self, data_id: DataId) -> bool {
        if self.data_id.is_some_and(|folded| data_id <= folded) {
            return true;
        }
        let Some(folded_hlc) = self.hlc else {
            return false;
        };
        let hlc = data_id.hlc();
        hlc < folded_hlc || (hlc == folded_hlc && self.data_id.is_none_or(|d| d.hlc() < folded_hlc))
    }
}

pub trait ReadTransaction: Send + Sync {
    fn watermark(&self) -> Result<Watermark, Error>;
    fn get_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        pid: ProjectionId,
    ) -> Result<Option<Bytes>, Error>;
    fn scan_raw(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
    ) -> Rows<'_, (ProjectionTypeId, ProjectionId, Bytes)>;
    fn scan_index(
        &self,
        tenant_id: TenantId,
        ptid: ProjectionTypeId,
        index_id: IndexId,
        q: ProjectionId,
    ) -> Rows<'_, ProjectionId>;
}

pub type ArcReadTransaction<'a> = Arc<dyn ReadTransaction + 'a>;

pub trait ReadTransactionExt: ReadTransaction {
    fn get<P: Projection>(&self, tenant_id: TenantId, r: Ref<P>) -> Result<Option<P>, Error> {
        self.get_raw(tenant_id, r.type_id(), r.id())?
            .map(|b| P::decode(&b))
            .transpose()
    }

    fn resolve<P: Projection>(&self, tenant_id: TenantId, r: Ref<P>) -> Result<P, Error> {
        self.get(tenant_id, r)?
            .ok_or_else(|| reference_not_found(r))
    }

    fn scan<P: Projection>(&self, tenant_id: TenantId) -> Rows<'_, P> {
        Box::new(
            self.scan_raw(tenant_id, P::TYPE_ID)
                .map(|r| r.and_then(|(_, _, b)| P::decode(&b))),
        )
    }
}
impl<T: ReadTransaction + ?Sized> ReadTransactionExt for T {}

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

/// The error for a `resolve` that found no row at `r`.
#[must_use]
pub fn reference_not_found<P: Projection>(r: Ref<P>) -> Error {
    Error::ReferenceNotFound {
        pid: r.id(),
        type_name: std::any::type_name::<P>(),
    }
}
