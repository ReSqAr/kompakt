//! The engine's live view of itself: leader, term, membership, applied index.
//! Background watchers (peer registration, state gauges) read it without
//! needing the node that owns the engine.

use raft_kit::RaftMetrics;
use tokio::sync::watch;
use zslg_node_types::NodeId;
use zslg_raft_types::NodeInfo;

pub trait RaftMetricsSource: Send + Sync + 'static {
    /// A receiver primed with the current metrics; notified on every change.
    #[must_use]
    fn metrics(&self) -> watch::Receiver<RaftMetrics<NodeId, NodeInfo>>;
}
