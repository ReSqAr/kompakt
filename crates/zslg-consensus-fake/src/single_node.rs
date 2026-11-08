//! A one-node consensus that is always the leader and applies every proposal
//! at once, with the same fence the real state machine applies: a write whose
//! expected generation is stale is refused, an invalid state or transition is
//! refused, and each accepted write advances the generation by one (the real
//! one uses the log index; both only ever move forward).

use std::collections::BTreeSet;

use async_trait::async_trait;
use parking_lot::Mutex;
use zslg_consensus::{
    ConsensusError, NodeId, RaftGeneration, RaftLeadership, RaftStateHandle, ReplicatedRaftState,
    StateReplicator, validate_cutoff_advance, validate_replicated_state, validate_transition,
};
use zslg_net::ServerName;

/// Leadership and state replication for one node, over a [`RaftStateHandle`]
/// that the code under test reads.
pub struct SingleNodeConsensus {
    me: NodeId,
    state: RaftStateHandle,
    next_index: Mutex<u64>,
}

impl SingleNodeConsensus {
    #[must_use]
    pub fn new(me: NodeId) -> Self {
        Self {
            me,
            state: RaftStateHandle::new(),
            next_index: Mutex::new(1),
        }
    }

    /// The state the code under test reads and this fake writes.
    #[must_use]
    pub fn state(&self) -> RaftStateHandle {
        self.state.clone()
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.me
    }
}

#[async_trait]
impl RaftLeadership for SingleNodeConsensus {
    fn current_leader(&self) -> Option<NodeId> {
        Some(self.me)
    }

    fn voter_ids(&self) -> BTreeSet<NodeId> {
        BTreeSet::from([self.me])
    }

    fn voter_server_names(&self) -> Vec<(NodeId, ServerName)> {
        Vec::new()
    }

    async fn remove_member(&self, _node_id: NodeId) -> Result<(), zslg_consensus::Error> {
        Ok(())
    }
}

#[async_trait]
impl StateReplicator for SingleNodeConsensus {
    async fn replicate_state(
        &self,
        state: ReplicatedRaftState,
        expected_generation: RaftGeneration,
        context: &'static str,
    ) -> Result<(), zslg_consensus::Error> {
        validate_replicated_state(&state)
            .map_err(|source| ConsensusError::invalid_raft_state(context, source))?;
        // One critical section: the check and the apply must not interleave.
        let mut index = self.next_index.lock();
        let applied = self.state.snapshot();
        if expected_generation != applied.generation {
            return Err(ConsensusError::stale_state_write(
                context,
                expected_generation,
                applied.generation,
            )
            .into());
        }
        validate_transition(&applied.replica, &state)
            .and_then(|()| validate_cutoff_advance(applied.manifest_state.hlc_cutoff, &state))
            .map_err(|source| ConsensusError::invalid_raft_state(context, source))?;
        let generation = RaftGeneration(*index);
        *index = index.saturating_add(1);
        self.state.apply_replicated(state, generation)?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests {
    use super::*;
    use zslg_consensus::ReplicaMembership;

    fn state(replicas: &[NodeId]) -> ReplicatedRaftState {
        ReplicatedRaftState {
            replica: ReplicaMembership::single(replicas.iter().copied().collect()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        }
    }

    #[tokio::test]
    async fn a_write_at_the_current_generation_applies_and_advances_it() {
        let me = NodeId::new_v4();
        let c = SingleNodeConsensus::new(me);
        let before = c.state().snapshot().generation;
        c.replicate_state(state(&[me]), before, "seed")
            .await
            .unwrap();
        assert!(c.state().snapshot().generation > before);
        assert_eq!(c.current_leader(), Some(me));
    }

    #[tokio::test]
    async fn a_write_fenced_on_a_stale_generation_is_refused_and_changes_nothing() {
        let me = NodeId::new_v4();
        let c = SingleNodeConsensus::new(me);
        let genesis = c.state().snapshot().generation;
        c.replicate_state(state(&[me]), genesis, "seed")
            .await
            .unwrap();
        let after_seed = c.state().snapshot().generation;

        let err = c
            .replicate_state(state(&[me]), genesis, "stale")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            zslg_consensus::Error::Consensus(ConsensusError::StaleStateWrite(_))
        ));
        assert_eq!(c.state().snapshot().generation, after_seed);
    }
}
