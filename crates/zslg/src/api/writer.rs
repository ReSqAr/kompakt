//! The client's entire write API: hand over bytes, get back an identity.
//!
//! The caller never chooses the [`DataId`] — it is minted here from 64 random
//! bits plus the creation HLC, which is what makes semantic reuse impossible by
//! construction rather than by a dedup check (CLAUDE.md §8). Resending the same
//! write is therefore always safe and always idempotent, and the writer's
//! reconnect path relies on that: it resends every un-acked write.

use bytes::Bytes;

use crate::Error;
use crate::ids::DataId;

#[async_trait::async_trait]
pub trait Writer: Send + Sync + 'static {
    /// Returns once a quorum of replicas has durably acknowledged the blob. An
    /// error means quorum was not observed, not that nothing was written — a
    /// caller-level retry mints a fresh `DataId`, so both copies may survive.
    async fn write(&self, blob: Bytes) -> Result<DataId, Error>;
}
