//! Keeps the RPC router's peer table in sync with the raft membership:
//! registers every member's address as the membership changes, and `warn!`s
//! periodically while the membership is empty — a node stuck with no routes
//! can reach no one, so it must be surfaced even when no event fires.
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::api::Router;
use crate::error::Error;
use crate::ids::NodeId;
use crate::raft::NodeInfo;

/// Cadence for re-checking (and `warn!`ing about) empty raft membership. The
/// first tick lands one interval in, giving a natural startup grace before the
/// first "no peer routes" warning.
const EMPTY_MEMBERSHIP_WARN_INTERVAL: Duration = Duration::from_secs(5);

pub(in crate::raft) struct PeerRegistrationWatcher {
    raft: Arc<crate::raft::RaftNode>,
    net: Arc<dyn Router>,
}

impl PeerRegistrationWatcher {
    #[must_use]
    pub(in crate::raft) fn new(raft: Arc<crate::raft::RaftNode>, net: Arc<dyn Router>) -> Self {
        Self { raft, net }
    }

    pub(in crate::raft) async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        tracing::info!("peer registration watcher: starting");
        let mut rx = self.raft.inner().metrics();
        self.sync(&rx.borrow());

        // Periodic re-check so an *empty* membership (no peer routes registered)
        // is surfaced even when no membership-change event fires — a node stuck
        // with no routes cannot reach anyone. First tick is one interval in.
        let mut warn_tick = tokio::time::interval(EMPTY_MEMBERSHIP_WARN_INTERVAL);
        warn_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    tracing::info!("peer registration watcher: shutdown");
                    return Ok(());
                }
                changed = rx.changed() => {
                    if changed.is_err() {
                        tracing::info!("peer registration watcher: raft metrics closed");
                        return Ok(());
                    }
                    self.sync(&rx.borrow());
                }
                _ = warn_tick.tick() => {
                    if self.sync(&rx.borrow()) == 0 {
                        tracing::warn!(
                            "peer registration watcher: raft membership is empty — no peer \
                             routes registered (needs a leader to push membership, or local \
                             state is empty); this node cannot reach any peer"
                        );
                    }
                }
            }
        }
    }

    /// Register every peer address from the current membership. Returns the
    /// number of nodes seen so the caller can detect an empty membership.
    fn sync(&self, metrics: &openraft::RaftMetrics<NodeId, NodeInfo>) -> usize {
        let mut count = 0usize;
        for (node_id, node) in metrics.membership_config.nodes() {
            self.net.register_peer(*node_id, (**node).clone());
            count = count.saturating_add(1);
        }
        count
    }
}
