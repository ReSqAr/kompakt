//! Writing into the local store — ingest from upstream, and TTL purge behind it.
//!
//! Ingest is a *loop*, not a call: one call per upstream subscription, feeding
//! the store's single integration worker so a drained batch becomes one commit
//! and one fsync (CLAUDE.md §8). A per-record entry point would make batching
//! impossible and turn ingest into a per-entry fsync storm.
//!
//! The stream carries `Result`, and an `Err` **ends the call**: a subscription
//! error is in-band, so the reason reaches the caller directly rather than
//! arriving out of band while the loop reports a clean end.
//!
//! No per-record acknowledgement exists, and none is needed: records dedup by
//! `data_id`, so the durable subscription cursor written inside each batch is
//! the only progress signal — a crash re-delivers the suffix after it and the
//! duplicates collapse (CLAUDE.md §20). The call therefore returns only once
//! everything it forwarded has committed, or the cursor re-read that measures
//! an attempt's progress would under-report it.

use crate::api::LocalStoreView;
use crate::channel::Channels;
use crate::hlc::Hlc;
use crate::ids::{LogIndex, NodeId};
use crate::meta::Data;

#[async_trait::async_trait]
pub(crate) trait LocalStoreMut: LocalStoreView {
    /// Feed `records` into the store until it ends, then return once
    /// everything forwarded has committed. An `Err` item ends the call early
    /// and is returned as-is.
    ///
    /// Concurrent calls are safe: every item names its own source node, and
    /// they converge on one writer, so per-upstream ordering is preserved
    /// while batches span upstreams. Backpressure is consequently shared — a
    /// slow commit stalls every caller, not just the one behind it.
    async fn integrate_loop(
        &self,
        records: Channels<Result<(NodeId, LogIndex, Data), crate::Error>>,
    ) -> Result<(), crate::Error>;

    /// Purge all local-store rows whose HLC is strictly below `threshold`.
    ///
    /// A durable range delete, not a scan — there is no count of rows removed
    /// to report, only whether the delete committed.
    async fn purge_below_hlc(&self, threshold: Hlc) -> Result<(), crate::Error>;
}
