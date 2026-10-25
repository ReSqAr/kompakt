//! Who leads, who votes — the Raft facts the leader jobs act on.
//!
//! Everything here reads *applied* state. openraft's metrics watch is
//! deliberately not exposed: it also carries in-flight and speculative values,
//! and a job that acted on one could propose against a config that never
//! commits. The single consumer that does need the raw stream
//! (`raft::state::state_publisher`) holds the concrete node instead of this trait.
//!
//! Voters ≠ the replica set. These are Raft's members; the replica set that
//! serves writes is `ReplicaMembership` in the replicated state, and a
//! membership job's whole task is moving one toward the other.

use std::collections::BTreeSet;

use crate::ids::{NodeId, ServerName};

#[async_trait::async_trait]
pub(crate) trait RaftLeadership: Send + Sync + 'static {
    #[must_use]
    fn current_leader(&self) -> Option<NodeId>;

    #[must_use]
    fn voter_ids(&self) -> BTreeSet<NodeId>;

    #[must_use]
    fn voter_server_names(&self) -> Vec<(NodeId, ServerName)>;

    async fn remove_member(&self, node_id: NodeId) -> Result<(), crate::Error>;
}
