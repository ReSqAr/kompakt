//! How leader jobs hand the integrator a change to make.

use zslg_consensus_types::{IntentOutcome, RaftIntent};

/// The queue in front of the single writer of the replicated Raft state.
///
/// Jobs never write that state themselves: they submit what they want and the
/// implementation enforces the invariants at the write point.
#[async_trait::async_trait]
pub trait IntentSubmitter: Send + Sync + 'static {
    /// Submit an intent and wait for its outcome. `LifecycleError::Shutdown` if
    /// the writer is gone, which can only mean shutdown — same cancellation tree
    /// as its callers.
    async fn submit(&self, intent: RaftIntent) -> Result<IntentOutcome, crate::Error>;
}
