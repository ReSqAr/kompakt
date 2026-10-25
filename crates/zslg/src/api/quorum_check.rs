//! Catch up with a quorum of replicas — the one operation behind
//! [`Reader::sync_quorum`](crate::api::Reader::sync).
//!
//! Its own trait rather than a `Reader` method because the debouncer that paces
//! it (`learner::quorum::QuorumReadDebouncer`) is a `QuorumCheck` too: one
//! in-flight check is shared by every caller that arrives during it, and the
//! decorator only composes if the thing decorated is this narrow.

use async_trait::async_trait;

use crate::Error;

#[async_trait]
pub(crate) trait QuorumCheck: Send + Sync + 'static {
    /// `Ok(())` means every entry that was on a quorum of replicas when the call
    /// started is now locally visible. Fails if no quorum answers — never
    /// returns early with a partial catch-up.
    async fn check(&self) -> Result<(), Error>;
}
