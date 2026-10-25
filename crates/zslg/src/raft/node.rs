//! The `RaftNode`: a thin owned wrapper around `openraft::Raft` implementing
//! the crate's `api` traits — `RaftConsensus` RPC passthroughs, `RaftLeadership`
//! queries, and `StateReplicator` fenced writes.
//!
//! `add_member` is the one non-thin path: it gates on the membership policy
//! (`MembershipGate`), removes DNS-colliding node ids at the same address
//! atomically with the add, and retries the membership change across a lost
//! leadership mid-change.
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt as _;

use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tracing::info;

use crate::api::{
    MembershipGate, RaftConsensus, RaftLeadership, ReplicatedRaftState, StateReplicator,
};
use crate::error::{Error, ToRaft};
use crate::ids::{NodeId, PeerAddr, RaftGeneration, ServerName};
use crate::metrics;
use crate::raft::{FencedStateWrite, NodeInfo, RaftResponse};

pub(crate) struct RaftNode {
    inner: openraft::Raft<crate::raft::TypeConfig>,
    gate: Arc<dyn MembershipGate>,
    client_write_timeout: std::time::Duration,
}

impl RaftNode {
    #[must_use]
    pub(crate) fn new(
        inner: openraft::Raft<crate::raft::TypeConfig>,
        gate: Arc<dyn MembershipGate>,
        client_write_timeout: std::time::Duration,
    ) -> Self {
        Self {
            inner,
            gate,
            client_write_timeout,
        }
    }

    #[must_use]
    pub(in crate::raft) fn inner(&self) -> &openraft::Raft<crate::raft::TypeConfig> {
        &self.inner
    }

    /// Demote `node_id` from voter to learner (`retain: true`), rather than
    /// dropping it from the membership map outright as
    /// [`RaftLeadership::remove_member`] does (`retain: false`, matching
    /// production eviction). Test-only: a learner keeps receiving
    /// `AppendEntries`, so — unlike a fully-dropped node, whose own view of
    /// `voters` can legitimately lag forever once the leader stops
    /// replicating to it — its own metrics promptly reflect the demotion,
    /// which is what a same-process observability test needs.
    #[cfg(test)]
    pub(crate) async fn demote_to_learner_for_test(&self, node_id: NodeId) -> Result<(), Error> {
        let members: BTreeSet<NodeId> = self
            .inner
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .filter(|id| *id != node_id)
            .collect();
        self.inner
            .change_membership(members, true)
            .await
            .to_raft("change_membership demote (test, retain=true)")?;
        Ok(())
    }

    pub(crate) async fn shutdown(&self) {
        let _ = self.inner.shutdown().await;
    }
}

#[async_trait::async_trait]
impl RaftConsensus for RaftNode {
    async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<crate::raft::TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> {
        self.inner.append_entries(rpc).await
    }

    async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RaftError<NodeId>> {
        self.inner.vote(rpc).await
    }

    async fn install_snapshot(
        &self,
        rpc: InstallSnapshotRequest<crate::raft::TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> {
        self.inner.install_snapshot(rpc).await
    }

    async fn add_member(&self, new_node_id: NodeId, peer_addr: PeerAddr) -> Result<(), Error> {
        self.gate.admit(new_node_id, &peer_addr)?;

        let start = tokio::time::Instant::now();
        let metrics = self.inner.metrics().borrow().clone();

        if metrics.current_leader != Some(metrics.id) {
            return Err(Error::Raft(format!(
                "add_member: not the leader (current leader: {:?})",
                metrics.current_leader
            )));
        }

        let membership = metrics.membership_config;
        let already_voter = membership
            .membership()
            .voter_ids()
            .any(|id| id == new_node_id);
        let same_address = membership
            .nodes()
            .any(|(id, info)| *id == new_node_id && info.addr == peer_addr.addr);
        if already_voter && same_address {
            return Ok(());
        }

        // DNS collisions: other node IDs at the same address — remove them atomically.
        let colliding: BTreeSet<NodeId> = membership
            .nodes()
            .filter_map(|(id, node)| {
                if *id != new_node_id && node.addr == peer_addr.addr {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        if !colliding.is_empty() {
            info!(
                node_id = ?new_node_id,
                addr = %peer_addr.addr,
                ?colliding,
                "add_member: DNS collision detected — old node(s) at same address will be replaced atomically",
            );
        }
        drop(membership);

        let t0 = tokio::time::Instant::now();
        self.inner
            .add_learner(new_node_id, NodeInfo::from(peer_addr), true)
            .boxed()
            .await
            .to_raft("add_learner")?;
        let learner_ms = t0.elapsed().as_millis();

        let t0 = tokio::time::Instant::now();
        let mut change_retries: u32 = 0;
        loop {
            // Re-check leadership on each iteration — may have been demoted.
            let metrics = self.inner.metrics().borrow().clone();
            if metrics.current_leader != Some(metrics.id) {
                return Err(Error::Raft(format!(
                    "add_member: lost leadership during membership change (current leader: {:?})",
                    metrics.current_leader
                )));
            }
            let mut members: BTreeSet<NodeId> =
                metrics.membership_config.membership().voter_ids().collect();
            for old in &colliding {
                members.remove(old);
            }
            members.insert(new_node_id);
            drop(metrics);

            match self.inner.change_membership(members, false).boxed().await {
                Ok(_) => {
                    info!(
                        node_id = ?new_node_id,
                        colliding_removed = colliding.len(),
                        learner_ms,
                        change_ms = t0.elapsed().as_millis(),
                        change_retries,
                        total_ms = ?start.elapsed().as_millis(),
                        "add_member: committed new voter",
                    );
                    return Ok(());
                }
                Err(e)
                    if e.to_string()
                        .contains("already undergoing a configuration change") =>
                {
                    change_retries = change_retries.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => return Err(Error::Raft(format!("change_membership: {e}"))),
            }
        }
    }
}

// ── RaftLeadership (leader queries + membership remove) ─────────────────

#[async_trait::async_trait]
impl RaftLeadership for RaftNode {
    fn current_leader(&self) -> Option<NodeId> {
        self.inner.metrics().borrow().current_leader
    }

    fn voter_ids(&self) -> BTreeSet<NodeId> {
        self.inner
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect()
    }

    fn voter_server_names(&self) -> Vec<(NodeId, ServerName)> {
        self.inner
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .map(|(id, info)| (*id, info.server_name.clone()))
            .collect()
    }

    async fn remove_member(&self, node_id: NodeId) -> Result<(), Error> {
        let metrics = self.inner.metrics().borrow().clone();
        let members: BTreeSet<NodeId> = metrics
            .membership_config
            .membership()
            .voter_ids()
            .filter(|id| *id != node_id)
            .collect();
        self.inner
            .change_membership(members, false)
            .await
            .to_raft("change_membership remove")?;
        Ok(())
    }
}

// ── StateReplicator (fenced cluster-state write) ─────────────────────────

#[async_trait::async_trait]
impl StateReplicator for RaftNode {
    async fn replicate_state(
        &self,
        state: ReplicatedRaftState,
        expected_generation: RaftGeneration,
        context: &'static str,
    ) -> Result<(), Error> {
        // See `StateReplicator::replicate_state` for the pre-flight contract.
        crate::raft::raft_storage::validate_replicated_state(&state)
            .map_err(|source| Error::invalid_raft_state(context, source))?;
        // openraft's call has no deadline, and the integrator is a single
        // serial task — one wedged wait would stall every leader job
        // (`RaftConfig::client_write_timeout`). Time out instead; the
        // submitting job re-derives on its next iteration.
        let response = tokio::time::timeout(
            self.client_write_timeout,
            self.inner.client_write(FencedStateWrite {
                expected_generation,
                state,
            }),
        )
        .await
        .map_err(|_| {
            crate::metrics::raft_client_write_timeout();
            Error::Rpc(format!(
                "raft client_write ({context}) timed out after {:?}",
                self.client_write_timeout
            ))
        })?
        .to_raft(context)?;
        match response.data {
            RaftResponse::Applied => Ok(()),
            RaftResponse::RejectedStaleGeneration { expected, current } => {
                metrics::raft_fenced_write();
                Err(Error::stale_state_write(context, expected, current))
            }
            RaftResponse::RejectedInvalidState { reason } => {
                metrics::raft_fenced_write();
                Err(Error::invalid_raft_state(context, reason))
            }
        }
    }
}
