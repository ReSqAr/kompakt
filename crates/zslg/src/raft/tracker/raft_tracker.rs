//! A non-Raft node's view of the cluster's Raft state, learned over RPC.
//!
//! Neither a `WriterNode` nor a `LearnerNode` participates in Raft or the
//! replica log, yet both need the current replica set + generation — the writer
//! to route `fan_out_quorum` writes, the learner to gate quorum reads.
//! [`RaftTracker`] is the non-Raft counterpart to the replica's
//! [`RaftStateHandle`](crate::raft::RaftStateHandle): instead of being fed by
//! consensus apply, it is fed by the per-peer stream workers
//! ([`super::streamer`]) and by write ACKs, integrating the highest generation
//! seen. The "integrate an observed state" logic lives here, next to the
//! write path.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::debug;

use crate::api::{PeersWatcher, RaftStateView};
use crate::ids::NodeId;
use crate::meta::RaftState;
use crate::{Error, quorum};

/// Everything [`spawn_raft_tracker_streamer`](crate::raft::spawn_raft_tracker_streamer)
/// needs beyond the ambient shutdown + router: the peer-set watcher plus the
/// per-peer failure backoff and startup readiness bound. Assembled by the owning
/// node config's `raft_tracker_config()` producer.
#[derive(Debug, Clone)]
pub(crate) struct RaftTrackerConfig {
    /// The expected replica-set watcher; one stream worker is run per peer it
    /// yields.
    pub(crate) peers: Arc<dyn PeersWatcher>,
    /// First backoff after a failed `subscribe_status` connect or a dropped
    /// stream that never went live; doubles up to `max_backoff`.
    pub(crate) min_backoff: Duration,
    /// Upper bound on the per-peer failure backoff.
    pub(crate) max_backoff: Duration,
    /// How long the streamer blocks for a quorum of write targets to be
    /// discovered before giving up (`wait_for_ready`). Filled from the owning
    /// node config's `startup_timeout`.
    pub(crate) startup_timeout: Duration,
}

impl RaftTrackerConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.min_backoff > self.max_backoff {
            return Err(Error::InvalidConfiguration(format!(
                "raft_tracker.min_backoff ({:?}) must be <= max_backoff ({:?})",
                self.min_backoff, self.max_backoff
            )));
        }
        if self.min_backoff.is_zero() {
            return Err(Error::InvalidConfiguration(
                "raft_tracker.min_backoff must be non-zero".to_owned(),
            ));
        }
        if self.startup_timeout.is_zero() {
            return Err(Error::InvalidConfiguration(
                "startup_timeout must be non-zero".to_owned(),
            ));
        }
        Ok(())
    }
}

/// A non-Raft node's cache of the cluster's `RaftState`, plus the set of replica
/// `NodeId`s whose `addr` has been discovered (registered in the router). Fed
/// by RPC, not consensus.
pub(crate) struct RaftTracker {
    discovered_tx: watch::Sender<BTreeSet<NodeId>>,
    /// The integrated state and its change notification in one primitive, so
    /// `subscribe()` callers (e.g. `ReplicationManager`) read the current
    /// membership rather than a stream of edges they might lag behind.
    state_tx: watch::Sender<Arc<RaftState>>,
}

impl Default for RaftTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftTracker {
    #[must_use]
    pub(in crate::raft) fn new() -> Self {
        let (discovered_tx, _) = watch::channel(BTreeSet::new());
        let (state_tx, _) = watch::channel(Arc::new(RaftState::empty()));
        Self {
            discovered_tx,
            state_tx,
        }
    }

    /// Record that `node_id`'s address has been registered in the router and is
    /// therefore routable by `NodeId`. Notifies any waiter on
    /// [`wait_for_ready`](Self::wait_for_ready) only when the id was not already
    /// known.
    pub(in crate::raft::tracker) fn note_discovered(&self, node_id: NodeId) {
        self.discovered_tx
            .send_if_modified(|set| set.insert(node_id));
    }

    /// Ready once the replica set is known **and** a quorum of write targets
    /// have been discovered — so the first write has a quorum-routable set and
    /// no start-up race can drop it below quorum. Uses the union of the old and
    /// new replica sets so a joint config is handled correctly.
    #[must_use]
    fn is_ready(&self) -> bool {
        let snap = self.snapshot();
        if quorum::all_targets(&snap.replica).is_empty() {
            return false;
        }
        let discovered = self.discovered_tx.borrow();
        quorum::is_quorum(&snap.replica, &discovered)
    }

    /// Wait until the tracker reports ready (quorum of targets discovered), or
    /// the `timeout` elapses. Event-driven: sleeps until a peer is noted
    /// discovered, then rechecks — no busy-loop.
    pub(in crate::raft) async fn wait_for_ready(&self, timeout: Duration) -> Result<(), Error> {
        let mut rx = self.discovered_tx.subscribe();
        if self.is_ready() {
            return Ok(());
        }
        tokio::time::timeout(timeout, async {
            loop {
                rx.changed()
                    .await
                    .map_err(|_| Error::Rpc("raft tracker shut down before ready".into()))?;
                if self.is_ready() {
                    return Ok::<_, Error>(());
                }
            }
        })
        .await
        .map_err(|_| {
            Error::Rpc(format!(
                "did not discover a quorum of replicas within {timeout:?}",
            ))
        })?
    }
}

impl RaftStateView for RaftTracker {
    fn snapshot(&self) -> Arc<RaftState> {
        Arc::clone(&self.state_tx.borrow())
    }

    fn subscribe(&self) -> watch::Receiver<Arc<RaftState>> {
        self.state_tx.subscribe()
    }
}

impl crate::api::RaftStateTracker for RaftTracker {
    fn integrate(&self, observed: &RaftState) {
        // The compare-and-store and the notification are the same operation
        // under the watch's own lock, so the separate write lock this needed is
        // gone with it.
        let integrated = self.state_tx.send_if_modified(|slot| {
            if observed.generation <= slot.generation {
                return false;
            }
            debug!(
                generation = ?observed.generation,
                replica_node_ids = ?observed.replica.current(),
                "RaftTracker integrated newer generation",
            );
            *slot = Arc::new(observed.clone());
            true
        });
        if integrated {
            // Wake wait_for_ready: the new replica set may make a quorum
            // reachable with the already-discovered peers.
            self.discovered_tx.send_modify(|_| {});
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::api::RaftStateTracker;
    use crate::ids::RaftGeneration;
    use crate::meta::{RaftManifestState, ReplicaMembership};
    use crate::tests::nid;

    fn state(generation: u64, replicas: &[NodeId]) -> RaftState {
        RaftState {
            generation: RaftGeneration(generation),
            leader: None,
            raft_term: 0,
            replica: ReplicaMembership::single(replicas.iter().copied().collect()),
            manifest_state: RaftManifestState::default(),
            voters: BTreeSet::new(),
        }
    }

    #[test]
    fn integrate_adopts_higher_generation() {
        let view = RaftTracker::new();
        let (n1, n2) = (nid(1), nid(2));
        view.integrate(&state(3, &[n1, n2]));
        assert_eq!(view.snapshot().generation, RaftGeneration(3));
        assert_eq!(view.snapshot().replica.current().len(), 2);
    }

    #[test]
    fn integrate_ignores_lower_or_equal_generation() {
        let view = RaftTracker::new();
        let n1 = nid(1);
        view.integrate(&state(5, &[n1]));
        // Equal generation with a different replica set must not overwrite.
        view.integrate(&state(5, &[nid(9)]));
        // Strictly lower generation must not overwrite.
        view.integrate(&state(4, &[nid(8)]));
        let snap = view.snapshot();
        assert_eq!(snap.generation, RaftGeneration(5));
        assert_eq!(
            snap.replica.current().clone(),
            [n1].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn is_ready_requires_quorum_discovered() {
        let view = RaftTracker::new();
        let (n1, n2, n3) = (nid(1), nid(2), nid(3));
        assert!(!view.is_ready(), "empty replica set is not ready");
        view.integrate(&state(1, &[n1, n2, n3]));
        assert!(!view.is_ready(), "no peers discovered yet");
        view.note_discovered(n1);
        assert!(!view.is_ready(), "only 1 of 3 — not a quorum");
        view.note_discovered(n2);
        assert!(view.is_ready(), "2 of 3 — quorum reached");
    }

    #[test]
    fn raft_tracker_config_validate() {
        let ok = RaftTrackerConfig {
            peers: crate::network::static_peers(vec![]),
            min_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(5),
            startup_timeout: Duration::from_secs(30),
        };
        ok.validate().expect("defaults valid");
        let bad = RaftTrackerConfig {
            min_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(1),
            ..ok
        };
        assert!(bad.validate().is_err());
    }
}
