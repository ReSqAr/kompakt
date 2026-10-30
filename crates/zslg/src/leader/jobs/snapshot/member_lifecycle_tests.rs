#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_arguments,
    clippy::result_large_err,
    reason = "test code"
)]
use crate::meta::{Data, ReplicaMembership, ReplicaStatus};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use object_store::memory::InMemory;
use parking_lot::Mutex as SyncMutex;
use tempfile::TempDir;

use crate::api::LocalStoreMut;
use crate::api::ReplicatedRaftState;
use crate::error::Error;
use crate::hlc::{Hlc, HlcGate};
use crate::ids::{LogIndex, NodeId};
use crate::leader::jobs::membership::change::{
    LivenessQuery, MembershipChangeInputs, MembershipChangeManager,
};
use crate::leader::jobs::membership::{MembershipDriver, MembershipObserver, TickOutcome};
use crate::leader::jobs::snapshot::create::{
    SnapshotEntryInputs, SnapshotResult, create_snapshot_entry,
};
use crate::learner::LocalStore;
use crate::learner::StableLog;
use crate::object_store::CompressionLevel;
use crate::object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use crate::raft::RaftNode;
use crate::raft::RaftStateHandle;
use crate::raft::SpareSelectionStrategy;
use crate::tests::fast_raft_config as raft_config;
use crate::tests::make_data;
use crate::tests::upstream;
use crate::tests::wait_for_leader;

async fn integrate_batch(
    store: &LocalStore,
    source: NodeId,
    records: Vec<(LogIndex, Data)>,
) -> Result<(), crate::Error> {
    if records.is_empty() {
        return Ok(());
    }
    let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
    for (idx, rec) in records {
        let _ = tx.send(Ok((source, idx, rec)));
    }
    let (ch, closer) = crossbeam_combinators::Channels::from(rx, "test integrate");
    // Close before the last sender drops, or the consumer forms its
    // verdict as "disconnected, not yet closed" — an abort.
    closer.close();
    drop(tx);
    store.integrate_loop(ch).await
}
use crate::tests::wait_until;

struct LocalObserver {
    states: SyncMutex<BTreeMap<NodeId, RaftStateHandle>>,
    /// Per-node replica-log `max_hlc`, the step-2b drain-bound sample. Absent
    /// (the default) models a replica whose log is empty.
    max_hlc: SyncMutex<BTreeMap<NodeId, Option<Hlc>>>,
}

impl LocalObserver {
    fn from_systems(nodes: &BTreeMap<NodeId, crate::tests::TestNode>) -> Arc<Self> {
        let mut map = BTreeMap::new();
        for (id, sys) in nodes {
            map.insert(*id, sys.raft_state.clone());
        }
        Arc::new(Self {
            states: SyncMutex::new(map),
            max_hlc: SyncMutex::new(BTreeMap::new()),
        })
    }

    fn remove(&self, id: NodeId) {
        self.states.lock().remove(&id);
    }
}

#[async_trait]
impl MembershipObserver for LocalObserver {
    async fn observe(&self, peer: NodeId) -> Result<ReplicaStatus, crate::Error> {
        let states = self.states.lock();
        match states.get(&peer) {
            Some(apply) => Ok(ReplicaStatus {
                node_id: peer,
                frontier: None,
                raft_state: (*apply.snapshot()).clone(),
                effective_cutoff: None,
                // Empty log: contributes no drain bound, but still counts
                // toward the step-2b quorum. Tests that need a bound set it
                // through `max_hlc`.
                max_hlc: self.max_hlc.lock().get(&peer).copied().flatten(),
                peer_names: Vec::new(),
            }),
            None => Err(Error::Raft(format!("peer {peer:?} not registered"))),
        }
    }
}

fn temp_dir(label: &str) -> TempDir {
    tempfile::Builder::new()
        .prefix(&format!("crate-m10b-{label}-"))
        .tempdir()
        .expect("tempdir")
}

struct LeaderWorkspace {
    local_store: Arc<LocalStore>,
    stable_log: Arc<StableLog>,
    hlc_gate: Arc<HlcGate>,
    object_store: Arc<dyn object_store::ObjectStore>,
}

async fn build_workspace(dir: &std::path::Path, id: NodeId) -> LeaderWorkspace {
    LeaderWorkspace {
        local_store: {
            LocalStore::simple_at(dir.join(format!("local-{id}.stable")))
                .await
                .expect("from_db")
        },
        stable_log: crate::tests::stable_log_at(&dir.join(format!("stable-{id}.db"))).await,
        hlc_gate: Arc::new(HlcGate::new()),
        object_store: Arc::new(InMemory::new()),
    }
}

// Multi-thread, not the `#[tokio::test]` default: this test runs five raft
// cores for many phases and asserts n1 holds leadership throughout. On a
// single-threaded runtime a machine-wide CPU stall (a parallel test run's
// processes contending for every core) freezes every node's timers and
// heartbeats together, and whichever task the runtime polls first after the
// stall — a follower's election timeout or the leader's heartbeat — decides
// an election the cluster never meant to have. With worker threads the two
// are serviced concurrently whenever the process gets any CPU, which is the
// production shape and what the leadership assumption actually requires.
#[tokio::test(flavor = "multi_thread")]
async fn full_lifecycle_bootstrap_snapshot_churn_failure() {
    let ids: [NodeId; 5] = std::array::from_fn(|_| NodeId::new_v4());
    let [n1, n2, n3, n4, n5] = ids;

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let leader_raft = nodes.get(&n1).expect("n1").raft.clone();
    leader_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&leader_raft, n1).await;

    let initial_replicas: BTreeSet<NodeId> = [n1, n2, n3].into_iter().collect();
    // Through whoever leads, not pinned to n1: this write lands right after
    // cluster bring-up, when an election can still be settling under load.
    crate::tests::write_state_fenced_via_leader(
        &nodes,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(initial_replicas.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed initial replicas");

    let observer = LocalObserver::from_systems(&nodes);
    wait_until(
        "every node observes initial replica set",
        Duration::from_secs(5),
        || async {
            for id in &ids {
                let s = observer.observe(*id).await.expect("observe");
                if s.raft_state.replica.current().clone() != initial_replicas {
                    return false;
                }
            }
            true
        },
    )
    .await;

    let dir = temp_dir("lifecycle");
    let workspace_n1 = build_workspace(dir.path(), n1).await;
    for i in 1u8..=3 {
        integrate_batch(
            &workspace_n1.local_store,
            upstream(1),
            vec![(
                LogIndex(u64::from(i)),
                make_data(i, u64::from(i) * 1_000, &[i]),
            )],
        )
        .await
        .expect("seed local log");
    }

    let snapshot_store = Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: workspace_n1.object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    ));
    let snapshot_inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: workspace_n1.local_store.as_ref(),
        stable_log: workspace_n1.stable_log.as_ref(),
        snapshot_store: snapshot_store.as_ref(),
        raft_state: &nodes.get(&n1).expect("n1").raft_state,
        compression: CompressionLevel::fast(),
    };
    let result = create_snapshot_entry(&snapshot_inputs, Hlc(10_000))
        .await
        .expect("create_snapshot");
    let new_watermark = match &result {
        SnapshotResult::Committed {
            new_watermark,
            object,
            ..
        } => {
            let current = nodes.get(&n1).expect("n1").raft_state.snapshot();
            let mut manifest = current.manifest_state.manifest.clone();
            if let Some(written) = object {
                manifest.push(written.entry.clone());
            }
            // Through whoever leads: the snapshot commit this write follows can
            // stir an election (a five-node cluster sharing one test runtime,
            // a whole machine's worth of parallel test load), and pinning the
            // write to the pre-snapshot leader asserts that race rather than
            // the write.
            crate::tests::write_state_fenced_via_leader(
                &nodes,
                ReplicatedRaftState {
                    replica: current.replica.clone(),
                    manifest,
                    manifest_watermark: Some(*new_watermark),
                    hlc_cutoff: Some(*new_watermark),
                },
            )
            .await
            .expect("client_write after snapshot");
            *new_watermark
        }
        SnapshotResult::Skipped => panic!("snapshot must include seeded entries"),
    };
    assert_eq!(new_watermark, Hlc(10_000));

    wait_until(
        "every node observes new manifest_watermark",
        Duration::from_secs(5),
        || async {
            for id in &ids {
                let s = observer.observe(*id).await.expect("observe");
                if s.raft_state.manifest_state.manifest_watermark != Some(new_watermark) {
                    return false;
                }
            }
            true
        },
    )
    .await;

    struct StaticLiveness(BTreeSet<NodeId>);
    impl LivenessQuery for StaticLiveness {
        fn candidates(&self) -> std::collections::BTreeMap<NodeId, crate::api::CandidateInfo> {
            self.0
                .iter()
                .map(|&node_id| (node_id, crate::api::CandidateInfo { node_id, rtt: None }))
                .collect()
        }
    }

    let mut _integrator_cancel: Option<tokio_util::sync::CancellationToken> = None;
    let manager = {
        let all_live: BTreeSet<NodeId> = ids.iter().copied().collect();
        let (intents, integrator_cancel) = crate::leader::Integrator::spawn_for_test(
            Arc::new(RaftNode::new(
                leader_raft.clone(),
                crate::network::permissive_gate(),
                std::time::Duration::from_secs(30),
            )),
            Arc::new(nodes.get(&n1).expect("n1").raft_state.clone()),
        );
        _integrator_cancel = Some(integrator_cancel);
        MembershipChangeManager::new(MembershipChangeInputs {
            self_node_id: n1,
            intents,
            leadership: Arc::new(RaftNode::new(
                leader_raft.clone(),
                crate::network::permissive_gate(),
                std::time::Duration::from_secs(30),
            )),
            raft_state: Arc::new(nodes.get(&n1).expect("n1").raft_state.clone()),
            observer: {
                let arc: Arc<dyn MembershipObserver> = Arc::<LocalObserver>::clone(&observer);
                arc
            },
            replica_node_count: 5,
            spare_selection: SpareSelectionStrategy::Random,
            live_nodes: Arc::new(StaticLiveness(all_live)),
        })
    };

    let target_5: BTreeSet<NodeId> = ids.iter().copied().collect();

    manager
        .initiate(target_5.clone())
        .await
        .expect("initiate 3→5");
    {
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(10))
            .expect("deadline");
        loop {
            let cur = nodes.get(&n1).expect("n1").raft_state.snapshot();
            // Nudge the step machine with a fenced write. The watermark must
            // advance from the CURRENT value: apply-time validation rejects
            // regressions, so a restarted counter would be fenced out.
            let next_wm = cur
                .manifest_state
                .manifest_watermark
                .map_or(Hlc(100), |wm| Hlc(wm.raw().saturating_add(100)));
            leader_raft
                .client_write(crate::raft::FencedStateWrite {
                    expected_generation: cur.generation,
                    state: ReplicatedRaftState {
                        replica: cur.replica.clone(),
                        manifest: cur.manifest_state.manifest.clone(),
                        manifest_watermark: Some(next_wm),
                        hlc_cutoff: Some(next_wm),
                    },
                })
                .await
                .ok();
            if matches!(manager.tick().await.expect("tick"), TickOutcome::Complete) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "3→5 membership change did not complete within deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    wait_until(
        "every node observes 5-replica set + joint cleared",
        Duration::from_secs(10),
        || async {
            for id in &ids {
                let s = observer.observe(*id).await.expect("observe");
                if s.raft_state.replica.current().clone() != target_5
                    || s.raft_state.replica.is_joint()
                {
                    return false;
                }
            }
            true
        },
    )
    .await;

    {
        let killed = nodes.remove(&n4).expect("n4 present");
        let _ = killed.raft.shutdown().await;
        observer.remove(n4);
    }

    let current_replicas = leader_raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect::<std::collections::BTreeSet<_>>();
    // Nudge raft after the node failure with a fenced write that keeps the
    // committed manifest/watermark intact (apply-time validation rejects a
    // watermark drop or a replaced manifest that breaks coverage).
    let cur = nodes.get(&n1).expect("n1").raft_state.snapshot();
    let next_wm = cur
        .manifest_state
        .manifest_watermark
        .map_or(Hlc(100), |wm| Hlc(wm.raw().saturating_add(100)));
    let nudged_state = leader_raft
        .client_write(crate::raft::FencedStateWrite {
            expected_generation: cur.generation,
            state: ReplicatedRaftState {
                replica: ReplicaMembership::single(current_replicas),
                manifest: cur.manifest_state.manifest.clone(),
                manifest_watermark: Some(next_wm),
                hlc_cutoff: Some(next_wm),
            },
        })
        .await
        .expect("post-failure client_write");
    let _ = nudged_state;
    assert_eq!(leader_raft.current_leader().await, Some(n1));

    let final_set: BTreeSet<NodeId> = [n1, n2, n3].into_iter().collect();
    manager
        .initiate(final_set.clone())
        .await
        .expect("initiate 5→3");
    {
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(10))
            .expect("deadline");
        loop {
            let cur = nodes.get(&n1).expect("n1").raft_state.snapshot();
            // Monotone nudge — see the 3→5 loop above.
            let next_wm = cur
                .manifest_state
                .manifest_watermark
                .map_or(Hlc(100), |wm| Hlc(wm.raw().saturating_add(100)));
            leader_raft
                .client_write(crate::raft::FencedStateWrite {
                    expected_generation: cur.generation,
                    state: ReplicatedRaftState {
                        replica: cur.replica.clone(),
                        manifest: cur.manifest_state.manifest.clone(),
                        manifest_watermark: Some(next_wm),
                        hlc_cutoff: Some(next_wm),
                    },
                })
                .await
                .ok();
            if matches!(manager.tick().await.expect("tick"), TickOutcome::Complete) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "5→3 membership change did not complete within deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    wait_until(
        "surviving nodes observe shrunk replica set",
        Duration::from_secs(10),
        || async {
            let surviving: Vec<NodeId> = [n1, n2, n3, n5].into_iter().collect();
            for id in &surviving {
                let s = observer.observe(*id).await.expect("observe");
                if s.raft_state.replica.current().clone() != final_set
                    || s.raft_state.replica.is_joint()
                {
                    return false;
                }
            }
            true
        },
    )
    .await;

    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}
