//! Has the local store caught up to one upstream replica? Shared by the
//! reader's quorum check and the leader's snapshot job.

use std::sync::Arc;

use crate::Error;
use zslg_consensus::{NodeId, RaftState};
use zslg_log::{LocalStoreView, LogError, LogIndex, Replica};

/// Checks whether the local store is caught up to a single upstream replica.
pub struct ReplicaPoller;

impl ReplicaPoller {
    /// Fetch the replica's status, then wait for the local store's
    /// subscription cursor to reach its frontier. Returns the replica's
    /// `RaftState` on success, `None` if the replica is unreachable.
    pub async fn check(
        transport: Arc<dyn Replica>,
        local_store: Arc<dyn LocalStoreView>,
        target: NodeId,
    ) -> Result<Option<RaftState>, Error> {
        // Nothing below runs until this returns, and it has no deadline of its
        // own — the caller's `check_timeout` is the only bound. Logged either
        // side so a quorum read parked here is distinguishable from one that
        // never started.
        tracing::debug!(?target, "quorum read: fetching replica status");
        let (end_index, config) = match transport.fetch_status().await {
            Ok(status) => (status.frontier, status.raft_state),
            Err(e) => {
                tracing::debug!(?target, error = %e, "quorum read: replica status fetch failed");
                return Ok(None);
            }
        };
        tracing::debug!(
            ?target,
            frontier = ?end_index.map(LogIndex::raw),
            "quorum read: replica status fetched"
        );

        let Some(end_index) = end_index else {
            tracing::debug!(
                ?target,
                "quorum read: replica log is empty; nothing to catch up to"
            );
            return Ok(Some(config));
        };

        // Subscribe before the first check, so an integration racing it is a
        // version bump this receiver has not seen — `changed()` then returns
        // immediately rather than waiting for the next one.
        let mut integrated = local_store.subscribe_integrated();
        // The cursor is in the replica's own index space and advances only as
        // this node integrates that replica's stream, so a frontier it never
        // delivers parks this loop until the caller's deadline.
        let mut waited = false;
        loop {
            if Self::upstream_caught_up(local_store.as_ref(), target, end_index).await? {
                if waited {
                    tracing::debug!(
                        ?target,
                        end_index = end_index.raw(),
                        "quorum read: upstream caught up"
                    );
                }
                return Ok(Some(config));
            }
            if !waited {
                waited = true;
                let cursor = local_store.subscription_cursor(target).await?;
                tracing::debug!(
                    ?target,
                    end_index = end_index.raw(),
                    ?cursor,
                    "quorum read: waiting for the local store to reach this replica's frontier"
                );
            }
            integrated.changed().await.map_err(|_| {
                Error::Log(LogError::Storage(format!(
                    "quorum check: local store closed before {target:?} caught up"
                )))
            })?;
        }
    }

    /// Whether the local store's cursor for `upstream` has reached `end_index`.
    pub async fn upstream_caught_up(
        local_store: &dyn LocalStoreView,
        upstream: NodeId,
        end_index: LogIndex,
    ) -> Result<bool, Error> {
        let cursor = local_store.subscription_cursor(upstream).await?;
        Ok(cursor.is_some_and(|c| c >= end_index))
    }
}
