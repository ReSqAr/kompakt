//! Where a node looks for the cluster's *expected* Raft members.
//!
//! Configuration, not consensus: this is the bootstrap/join answer to "who
//! should be here", distinct from the membership openraft has actually applied
//! (that is [`RaftLeadership::voter_ids`](crate::api::RaftLeadership)). A
//! watch-shaped seam so a future config source can push changes to a running
//! node; the current impl is the static `config.peers` list.

use tokio::sync::watch;

use crate::ids::PeerAddr;

pub(crate) trait PeersWatcher: Send + Sync + std::fmt::Debug + 'static {
    /// Notified on every change; the receiver starts primed with the current
    /// list, so a subscriber never has to also call `current`.
    #[allow(
        dead_code,
        reason = "subscribe is part of the interface for future dynamic peers"
    )]
    fn subscribe(&self) -> watch::Receiver<Vec<PeerAddr>>;

    fn current(&self) -> Vec<PeerAddr>;
}
