//! Joining an existing cluster: retried attempts with backoff.

use super::STARTUP_WARN_INTERVAL;
use crate::Error;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use zslg_consensus::AddMemberOutcome;
use zslg_consensus::ConsensusError;
use zslg_consensus::NodeId;
use zslg_net::PeerAddr;
use zslg_net::{PeersWatcher, Router};

pub(crate) const JOIN_INITIAL_BACKOFF: Duration = Duration::from_millis(50);

pub(crate) const JOIN_MAX_BACKOFF: Duration = Duration::from_secs(10);

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) async fn join_existing(
    router: &dyn Router,
    node_id: NodeId,
    peers_watcher: &Arc<dyn PeersWatcher>,
    self_peer_addr: &PeerAddr,
) -> Result<(), Error> {
    let start = tokio::time::Instant::now();
    let mut backoff = JOIN_INITIAL_BACKOFF;
    let mut attempt_num: u32 = 0;
    let mut last_warn = tokio::time::Instant::now();
    loop {
        attempt_num = attempt_num.saturating_add(1);
        let t_attempt = tokio::time::Instant::now();
        let peers_snapshot = peers_watcher.current();
        match attempt_join_once(router, &peers_snapshot, node_id, self_peer_addr.clone()).await {
            JoinAttempt::Accepted => {
                info!(
                    node_id = ?node_id,
                    attempt_num,
                    attempt_ms = ?t_attempt.elapsed().as_millis(),
                    total_ms = ?start.elapsed().as_millis(),
                    "join accepted by peer",
                );
                return Ok(());
            }
            JoinAttempt::Denied => {
                return Err(Error::Consensus(ConsensusError::MembershipDenied {
                    node_id,
                }));
            }
            JoinAttempt::Retry => {
                if last_warn.elapsed() >= STARTUP_WARN_INTERVAL {
                    last_warn = tokio::time::Instant::now();
                    warn!(
                        node_id = ?node_id,
                        attempt_num,
                        elapsed_s = start.elapsed().as_secs(),
                        peers = peers_snapshot.len(),
                        "raft startup: still trying to join an existing cluster \
                         (no reachable leader accepted the add-member yet); see \
                         debug logs for per-peer outcomes"
                    );
                }
                debug!(
                    ?backoff,
                    attempt_num,
                    attempt_ms = ?t_attempt.elapsed().as_millis(),
                    "retrying RaftAddMember after backoff",
                );
                let jittered = backoff_kit::jitter(backoff);
                tokio::time::sleep(jittered).await;
                backoff = backoff.saturating_mul(2).min(JOIN_MAX_BACKOFF);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinAttempt {
    Accepted,
    Denied,
    Retry,
}

pub(crate) async fn attempt_join_once(
    net: &dyn Router,
    peers: &[PeerAddr],
    node_id: NodeId,
    self_peer_addr: PeerAddr,
) -> JoinAttempt {
    for peer_addr in peers {
        let transport = net.remote_direct(peer_addr.clone());
        match tokio::time::timeout(
            CONNECT_TIMEOUT,
            transport.add_member(node_id, self_peer_addr.clone()),
        )
        .await
        {
            Err(_) => {
                debug!(server_name = %peer_addr.server_name, addr = %peer_addr.addr, "RaftAddMember timed out; trying next peer");
            }
            Ok(Ok(AddMemberOutcome::Accepted { .. })) => {
                info!(server_name = %peer_addr.server_name, ?node_id, "RaftAddMember accepted");
                return JoinAttempt::Accepted;
            }
            Ok(Ok(AddMemberOutcome::Rejected { state })) => {
                debug!(server_name = %peer_addr.server_name, ?node_id, leader_node_id = ?state.leader, "RaftAddMember rejected; will retry");
            }
            Ok(Err(zslg_consensus::Error::Consensus(ConsensusError::MembershipDenied {
                ..
            }))) => {
                // `debug`, not `warn`: this fires on every call once denied, and
                // `RejoinMonitor::run` is a continuous retry loop (never fatal —
                // see its module doc) that calls this every
                // `REJOIN_RECONCILE_INTERVAL` for as long as the node stays
                // decommissioned. The monitor already emits its own `warn` at
                // the point the outcome CHANGES (`EvictionStatus`), which is the
                // signal worth alerting on; this line would otherwise repeat
                // forever at `warn` for a state that no longer causes an exit.
                debug!(
                    server_name = %peer_addr.server_name,
                    ?node_id,
                    "RaftAddMember denied: this node is not in config.peers",
                );
                return JoinAttempt::Denied;
            }
            Ok(Err(e)) => {
                debug!(server_name = %peer_addr.server_name, error = %e, "RaftAddMember failed; trying next peer");
            }
        }
    }
    JoinAttempt::Retry
}
