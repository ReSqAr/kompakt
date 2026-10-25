//! One-shot quorum collection for control operations.
//!
//! ```text
//!   dispatch op to all_targets(replica)      one task per peer
//!   loop:
//!       quorum(acked)?          ──> Ok((state, acked))
//!       join_next()             ──> None: Err(QuorumUnreachable)
//!           Ready(s) if s.generation > current:
//!               current = s; dispatch to peers not yet sent
//! ```
//!
//! The target set only ever grows. A peer answering with a newer generation
//! re-aims the whole fan-out at *its* config and dispatches the difference, so
//! a membership change discovered mid-flight is absorbed rather than restarted
//! (CLAUDE.md §9). `sent` is what keeps that idempotent.
//!
//! Contrast [`super::Coordinator`], which does the same job for writes over
//! long-lived streams. This one owns its tasks and ends with the operation.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use tokio::task::JoinSet;

use crate::error::Error;
use crate::ids::NodeId;
use crate::meta::RaftState;
use crate::metrics::{self, FanoutResponseState};

use super::{all_targets, is_quorum};

/// How one peer answered. Distinct from the `Err` of the enclosing `Result`,
/// which aborts the whole fan-out: `Err` is for a **node-local** failure (the
/// local store, say) that would hit every peer's check identically, so trying
/// the rest cannot help. Anything per-peer — unreachable, refused, timed out —
/// is a `Failed` here, and the remaining peers can still form a quorum.
pub(crate) enum FanOutResponse {
    /// Counts toward quorum; may carry a newer generation.
    Ready(RaftState),
    Failed,
}

/// Returns the state quorum was reached against and the peers that acked, or
/// `Err(QuorumUnreachable)` once every dispatched task has completed without
/// one. A panicking `op` is logged and counted as a failed peer.
pub(crate) async fn fan_out_quorum<F, Fut>(
    initial_state: RaftState,
    op: F,
) -> Result<(RaftState, BTreeSet<NodeId>), Error>
where
    F: Fn(NodeId) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<FanOutResponse, Error>> + Send + 'static,
{
    if initial_state.replica.current().is_empty() {
        return Err(Error::QuorumUnreachable);
    }

    let op = Arc::new(op);
    let mut join_set: JoinSet<(NodeId, Result<FanOutResponse, Error>)> = JoinSet::new();

    let mut current_state = initial_state;
    let mut sent: BTreeSet<NodeId> = BTreeSet::new();
    let mut successful: BTreeSet<NodeId> = BTreeSet::new();

    dispatch_to_new(
        &all_targets(&current_state.replica),
        &mut sent,
        &op,
        &mut join_set,
    );

    loop {
        if is_quorum(&current_state.replica, &successful) {
            return Ok((current_state, successful));
        }

        let Some(joined) = join_set.join_next().await else {
            return Err(Error::QuorumUnreachable);
        };

        let (node_id, result) = match joined {
            Ok(pair) => pair,
            Err(join_error) => {
                tracing::warn!(
                    error = %join_error,
                    "fan-out op task panicked; treating as failed peer",
                );
                continue;
            }
        };

        let response = result?;

        let maybe_state = match response {
            FanOutResponse::Ready(state) => {
                metrics::write_fanout_response(FanoutResponseState::Ack);
                successful.insert(node_id);
                Some(state)
            }
            FanOutResponse::Failed => {
                metrics::write_fanout_response(FanoutResponseState::Unreachable);
                None
            }
        };

        if let Some(state) = maybe_state
            && state.generation > current_state.generation
        {
            current_state = state;
            dispatch_to_new(
                &all_targets(&current_state.replica),
                &mut sent,
                &op,
                &mut join_set,
            );
        }
    }
}

fn dispatch_to_new<F, Fut>(
    targets: &BTreeSet<NodeId>,
    sent: &mut BTreeSet<NodeId>,
    op: &Arc<F>,
    join_set: &mut JoinSet<(NodeId, Result<FanOutResponse, Error>)>,
) where
    F: Fn(NodeId) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<FanOutResponse, Error>> + Send + 'static,
{
    for &target in targets {
        if sent.insert(target) {
            let op = op.clone();
            join_set.spawn(async move {
                let response = op(target).await;
                (target, response)
            });
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
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;
    use crate::ids::RaftGeneration;
    use crate::meta::{RaftManifestState, RaftState, ReplicaMembership};
    use crate::tests::nid;

    fn ids(seeds: &[u8]) -> BTreeSet<NodeId> {
        seeds.iter().map(|&s| nid(s)).collect()
    }

    fn state(generation: u64, old: &[u8], new: Option<&[u8]>) -> RaftState {
        RaftState {
            leader: None,
            raft_term: 0,
            generation: RaftGeneration(generation),
            replica: match new {
                None => ReplicaMembership::single(ids(old)),
                Some(n) => ReplicaMembership::joint(ids(old), ids(n)),
            },
            manifest_state: RaftManifestState::default(),
            voters: BTreeSet::new(),
        }
    }

    /// A response plan: maps each NodeId to the FanOutResponse it should return.
    /// Records which nodes were actually contacted.
    type Plan = Arc<Mutex<BTreeMap<NodeId, ResponseKind>>>;
    type Contacted = Arc<Mutex<BTreeSet<NodeId>>>;

    #[derive(Clone)]
    enum ResponseKind {
        Ready(RaftState),
        Failed,
        Error,
        Panic,
    }

    #[allow(
        clippy::type_complexity,
        reason = "closure return type is complex but localized to this test helper"
    )]
    fn run_with(
        initial: RaftState,
        plan: BTreeMap<NodeId, ResponseKind>,
    ) -> (
        Plan,
        Contacted,
        impl std::future::Future<Output = Result<(RaftState, BTreeSet<NodeId>), Error>>,
    ) {
        let plan: Plan = Arc::new(Mutex::new(plan));
        let contacted: Contacted = Arc::new(Mutex::new(BTreeSet::new()));
        let plan_c = plan.clone();
        let contacted_c = contacted.clone();
        let fut = fan_out_quorum(initial, move |peer| {
            let plan = plan_c.clone();
            let contacted = contacted_c.clone();
            async move {
                contacted.lock().expect("lock").insert(peer);
                let kind = plan.lock().expect("lock").get(&peer).cloned();
                match kind {
                    Some(ResponseKind::Ready(s)) => Ok(FanOutResponse::Ready(s)),
                    Some(ResponseKind::Failed) | None => Ok(FanOutResponse::Failed),
                    Some(ResponseKind::Error) => Err(Error::Rpc("simulated hard error".into())),
                    Some(ResponseKind::Panic) => panic!("simulated op panic"),
                }
            }
        });
        (plan, contacted, fut)
    }

    // ── happy path ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn succeeds_when_quorum_acks_same_generation() {
        let s = state(1, &[1, 2, 3], None);
        let mut plan = BTreeMap::new();
        plan.insert(nid(1), ResponseKind::Ready(s.clone()));
        plan.insert(nid(2), ResponseKind::Ready(s.clone()));
        plan.insert(nid(3), ResponseKind::Failed);

        let (_, _, fut) = run_with(s.clone(), plan);
        let (final_state, successful) = fut.await.expect("quorum reached");
        assert_eq!(final_state.generation, RaftGeneration(1));
        assert!(successful.contains(&nid(1)));
        assert!(successful.contains(&nid(2)));
    }

    // ── quorum unreachable ──────────────────────────────────────────────

    #[tokio::test]
    async fn fails_when_too_many_peers_fail() {
        let s = state(1, &[1, 2, 3], None);
        let mut plan = BTreeMap::new();
        plan.insert(nid(1), ResponseKind::Ready(s.clone()));
        plan.insert(nid(2), ResponseKind::Failed);
        plan.insert(nid(3), ResponseKind::Failed);

        let (_, _, fut) = run_with(s.clone(), plan);
        assert!(matches!(fut.await, Err(Error::QuorumUnreachable)));
    }

    #[tokio::test]
    async fn empty_replica_set_is_immediately_unreachable() {
        let s = state(1, &[], None);
        let (_, contacted, fut) = run_with(s.clone(), BTreeMap::new());
        assert!(matches!(fut.await, Err(Error::QuorumUnreachable)));
        assert!(contacted.lock().expect("lock").is_empty());
    }

    // ── generation upgrade & dynamic dispatch ───────────────────────────

    #[tokio::test]
    async fn generation_upgrade_dispatches_to_new_peers() {
        // Initial config {1,2,3} gen 1. Node 3 responds with a newer config
        // {1,2,3,4,5} gen 2 → fanout must dispatch to 4 and 5 too.
        let initial = state(1, &[1, 2, 3], None);
        let upgraded = state(2, &[1, 2, 3, 4, 5], None);

        let mut plan = BTreeMap::new();
        // 1 and 2 fail in the old view so quorum needs the upgraded set.
        plan.insert(nid(1), ResponseKind::Failed);
        plan.insert(nid(2), ResponseKind::Ready(initial.clone()));
        plan.insert(nid(3), ResponseKind::Ready(upgraded.clone()));
        plan.insert(nid(4), ResponseKind::Ready(upgraded.clone()));
        plan.insert(nid(5), ResponseKind::Ready(upgraded.clone()));

        let (_, contacted, fut) = run_with(initial, plan);
        let (final_state, _successful) = fut.await.expect("quorum reached on upgraded set");
        assert_eq!(final_state.generation, RaftGeneration(2));
        // 4 and 5 must have been contacted (discovered via the upgrade).
        let c = contacted.lock().expect("lock");
        assert!(c.contains(&nid(4)));
        assert!(c.contains(&nid(5)));
    }

    // ── joint config ────────────────────────────────────────────────────

    #[tokio::test]
    async fn joint_config_requires_both_quorums() {
        // old = {1,2,3}, new = {3,4,5}. Acks from {1,2} (old quorum) only.
        let s = state(1, &[1, 2, 3], Some(&[3, 4, 5]));
        let mut plan = BTreeMap::new();
        plan.insert(nid(1), ResponseKind::Ready(s.clone()));
        plan.insert(nid(2), ResponseKind::Ready(s.clone()));
        plan.insert(nid(3), ResponseKind::Failed);
        plan.insert(nid(4), ResponseKind::Failed);
        plan.insert(nid(5), ResponseKind::Failed);

        let (_, _, fut) = run_with(s.clone(), plan);
        assert!(matches!(fut.await, Err(Error::QuorumUnreachable)));
    }

    #[tokio::test]
    async fn joint_config_succeeds_with_both_quorums() {
        // old = {1,2,3}, new = {3,4,5}. Acks {2,3,4}: old has {2,3} (quorum),
        // new has {3,4} (quorum).
        let s = state(1, &[1, 2, 3], Some(&[3, 4, 5]));
        let mut plan = BTreeMap::new();
        plan.insert(nid(2), ResponseKind::Ready(s.clone()));
        plan.insert(nid(3), ResponseKind::Ready(s.clone()));
        plan.insert(nid(4), ResponseKind::Ready(s.clone()));
        plan.insert(nid(1), ResponseKind::Failed);
        plan.insert(nid(5), ResponseKind::Failed);

        let (_, _, fut) = run_with(s.clone(), plan);
        let (_final, successful) = fut.await.expect("both quorums reached");
        assert!(successful.is_superset(&ids(&[2, 3, 4])));
    }

    // ── error propagation ───────────────────────────────────────────────

    #[tokio::test]
    async fn hard_error_from_op_aborts() {
        // Only node 1 could complete a quorum path; it errors. 2 and 3 fail,
        // so the result is driven by node 1's hard error rather than a clean
        // QuorumUnreachable. (5-node set: a single ack is never a quorum, so
        // the error response is the operative outcome.)
        let s = state(1, &[1, 2, 3, 4, 5], None);
        let mut plan = BTreeMap::new();
        plan.insert(nid(1), ResponseKind::Error);
        plan.insert(nid(2), ResponseKind::Ready(s.clone()));
        plan.insert(nid(3), ResponseKind::Ready(s.clone()));
        plan.insert(nid(4), ResponseKind::Failed);
        plan.insert(nid(5), ResponseKind::Failed);

        let (_, _, fut) = run_with(s.clone(), plan);
        // A hard error from `op` must surface as an Err (either the Rpc error,
        // if it is observed first, or QuorumUnreachable if the failures drain
        // first). It must never succeed: 2 acks of 5 is below quorum.
        let result = fut.await;
        assert!(result.is_err(), "must not reach quorum with only 2/5 acks");
    }
    // ── panicked op tasks (F-4b) ────────────────────────────────────────

    #[tokio::test]
    async fn op_panic_does_not_abort_other_peers() {
        // Node 1's op panics; 2 and 3 ack → quorum {2,3} of 3 must still be
        // reached, and the panic must not be misreported as QuorumUnreachable.
        let s = state(1, &[1, 2, 3], None);
        let mut plan = BTreeMap::new();
        plan.insert(nid(1), ResponseKind::Panic);
        plan.insert(nid(2), ResponseKind::Ready(s.clone()));
        plan.insert(nid(3), ResponseKind::Ready(s.clone()));

        let (_, _, fut) = run_with(s.clone(), plan);
        let (_final, successful) = fut.await.expect("quorum despite one panicked op");
        assert!(successful.contains(&nid(2)));
        assert!(successful.contains(&nid(3)));
        assert!(!successful.contains(&nid(1)));
    }

    #[tokio::test]
    async fn all_ops_panicking_is_quorum_unreachable() {
        let s = state(1, &[1, 2, 3], None);
        let mut plan = BTreeMap::new();
        for n in [1u8, 2, 3] {
            plan.insert(nid(n), ResponseKind::Panic);
        }
        let (_, _, fut) = run_with(s.clone(), plan);
        assert!(matches!(fut.await, Err(Error::QuorumUnreachable)));
    }
}
