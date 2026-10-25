//! Dynamic source of expected Raft member addresses.
//!
//! The [`PeersWatcher`] trait is defined in [`crate::api`]. This module
//! provides the concrete [`StaticPeers`] implementation and the
//! [`static_peers`] convenience constructor.

use std::collections::BTreeSet;
use std::sync::Arc;

use tokio::sync::watch;

use crate::api::{MembershipGate, PeersWatcher};
use crate::ids::{NodeId, PeerAddr, ServerName};

/// A static, never-changing implementation of [`PeersWatcher`].
///
/// This is the default when peers are provided as a hard-coded list at startup.
/// A file-backed implementation that watches a JSON file and broadcasts changes
/// via the `watch::Sender` channel can be added later.
struct StaticPeers {
    tx: watch::Sender<Vec<PeerAddr>>,
}

impl std::fmt::Debug for StaticPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticPeers")
            .field("len", &self.tx.borrow().len())
            .finish_non_exhaustive()
    }
}

impl StaticPeers {
    /// Create a new static peers watcher from the given list.
    ///
    /// The list must be non-empty (validated by
    /// `RpcClientConfig::validate()`.
    #[must_use]
    #[allow(dead_code, reason = "constructor used in tests")]
    fn new(peers: Vec<PeerAddr>) -> Self {
        let (tx, _) = watch::channel(peers);
        Self { tx }
    }
}

impl PeersWatcher for StaticPeers {
    fn subscribe(&self) -> watch::Receiver<Vec<PeerAddr>> {
        self.tx.subscribe()
    }

    fn current(&self) -> Vec<PeerAddr> {
        self.tx.borrow().clone()
    }
}

/// Convenience: convert a `Vec<PeerAddr>` into an
/// `Arc<dyn PeersWatcher>` backed by [`StaticPeers`].
#[must_use]
pub(crate) fn static_peers(peers: Vec<PeerAddr>) -> Arc<dyn PeersWatcher> {
    Arc::new(StaticPeers::new(peers))
}

// ── MembershipGate over peer config ────────────────────────────────────

/// [`MembershipGate`] that admits joiners whose `server_name` appears in the
/// configured peer list. This is the single source of truth for membership
/// admission: the check lives in [`RaftNode::add_member`](crate::raft::RaftNode),
/// not in the network dispatch layer.
pub(crate) struct PeersMembershipGate {
    peers: Arc<dyn PeersWatcher>,
}

impl PeersMembershipGate {
    #[must_use]
    pub(crate) fn new(peers: Arc<dyn PeersWatcher>) -> Self {
        Self { peers }
    }
}

impl MembershipGate for PeersMembershipGate {
    fn admit(&self, node_id: NodeId, peer_addr: &PeerAddr) -> Result<(), crate::Error> {
        let allowed: BTreeSet<ServerName> = self
            .peers
            .current()
            .iter()
            .map(|p| p.server_name.clone())
            .collect();
        if allowed.contains(&peer_addr.server_name) {
            Ok(())
        } else {
            tracing::warn!(
                ?node_id,
                %peer_addr.server_name,
                "AddMember denied: server_name not in config.peers",
            );
            Err(crate::Error::MembershipDenied { node_id })
        }
    }
}

/// Permissive gate (admits everything). Used in tests where membership policy
/// is not under test.
#[cfg(test)]
#[must_use]
pub(crate) fn permissive_gate() -> Arc<dyn MembershipGate> {
    Arc::new(PermissiveGate)
}

#[cfg(test)]
struct PermissiveGate;

#[cfg(test)]
impl MembershipGate for PermissiveGate {
    fn admit(&self, _node_id: NodeId, _peer_addr: &PeerAddr) -> Result<(), crate::Error> {
        Ok(())
    }
}
