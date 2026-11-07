//! The verbs the startup procedure needs from the consensus engine, and nothing
//! else: form a cluster, wait for it to settle, and write the first fenced
//! state. Startup decides; an implementation of this port executes.

use std::collections::BTreeMap;

use async_trait::async_trait;
use zslg_node_types::NodeId;
use zslg_raft_types::{FencedStateWrite, NodeInfo, RaftResponse};

use crate::{Error, RaftMetricsSource};

#[async_trait]
pub trait ClusterBootstrap: RaftMetricsSource {
    /// Create the cluster with `members` as its first voters.
    async fn initialize(&self, members: BTreeMap<NodeId, NodeInfo>) -> Result<(), Error>;

    /// Wait until `leader` is the elected leader. `context` names the caller's
    /// step in the error.
    async fn wait_leader(&self, leader: NodeId, context: &'static str) -> Result<(), Error>;

    /// Wait until the local state machine has applied at least `index`.
    async fn wait_applied_at_least(&self, index: u64, context: &'static str) -> Result<(), Error>;

    /// Propose a fenced write of the replicated state.
    async fn write_fenced(&self, write: FencedStateWrite) -> Result<RaftResponse, Error>;
}
