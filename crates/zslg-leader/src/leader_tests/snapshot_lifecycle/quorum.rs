//! The snapshot quorum wait: a wedged replica, and a leader demoted while waiting.

use super::*;

/// A wedged (arbitrarily slow) replica must not block snapshot creation:
/// the cutoff-ACK fan-out needs a quorum, not every replica (EXPERT_REVIEW
/// §10.2 — slow-replica fault mode).
#[tokio::test]
async fn snapshot_quorum_tolerates_one_wedged_replica() {
    use zslg_leader_snapshot::create::{SnapshotInputs, build_snapshot};
    use zslg_log::Replica;
    use zslg_time::HlcParts;

    use zslg_net_fake::{FaultyReplica, MapPeerRouter, ReadyReplica};
    use zslg_time_fake::FixedClock;

    let h = boot().await;
    let _ = h.leader_id;

    // Three-replica set: quorum is 2.
    let replica_ids: [NodeId; 3] = std::array::from_fn(|_| NodeId::new_v4());
    zslg_raft_setup::testing::write_state_fenced(
        &h.raft,
        &h.raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(replica_ids.iter().copied().collect()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed replica set");

    let local_store = Arc::new(h.local_store);
    integrate_batch(
        &local_store,
        upstream(1),
        vec![(LogIndex(0), make_data(1, 100, b"x"))],
    )
    .await
    .expect("integrate");

    let ready = |raft_state| -> Arc<dyn Replica> { Arc::new(ReadyReplica { raft_state }) };
    let (wedged, faults) = FaultyReplica::new(ready(h.raft_state.clone()));
    faults.wedge();

    let router: Arc<dyn Router> = Arc::new(MapPeerRouter {
        replicas: [
            (replica_ids[0], ready(h.raft_state.clone())),
            (replica_ids[1], ready(h.raft_state.clone())),
            (replica_ids[2], wedged as Arc<dyn Replica>),
        ]
        .into_iter()
        .collect(),
    });
    let clock = FixedClock {
        now: Hlc::from_parts(HlcParts {
            physical_ms: 3_000,
            logical: 0,
        }),
    };

    let snapshot_store = Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: h.object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    ));
    let leadership: Arc<dyn zslg_consensus::RaftLeadership> = Arc::new(RaftNode::new(
        h.raft.clone(),
        zslg_consensus_fake::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));
    let inputs = SnapshotInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: local_store.as_ref().clone(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*snapshot_store,
        raft_state: &h.raft_state,
        router,
        leadership,
        self_node_id: h.leader_id,
        quorum_poll_interval: Duration::from_millis(5),
        quorum_wait_timeout: Duration::from_secs(5),
        compression: CompressionLevel::fast(),
    };

    let result = build_snapshot(&inputs, clock.now)
        .await
        .expect("snapshot must succeed with a 2/3 quorum");
    assert!(
        matches!(result, SnapshotResult::Committed { .. }),
        "expected a committed snapshot, got: {result:?}",
    );
}

/// A leader demoted during the quorum wait must not go on to upload. Nothing
/// unsafe follows if it does — the integrator and the apply fence both refuse
/// the commit — but the object is then unreferenced waste for cleanup to
/// collect, and re-checking after the slow part is cheap.
#[tokio::test]
async fn a_leader_demoted_during_the_quorum_wait_writes_no_object() {
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crossbeam_combinators::Channel;
    use zslg_leader_snapshot::create::{SnapshotInputs, build_snapshot};
    use zslg_log::Replica;
    use zslg_log::SnapshotEntry;
    use zslg_net::ServerName;
    use zslg_net_fake::ReadyReplica;
    use zslg_snapshot::SnapshotId;

    /// Replica stub: always drained, no end_index — quorum checks pass instantly.
    struct OnePeerRouter {
        replica: Arc<dyn Replica>,
    }

    impl Router for OnePeerRouter {
        fn register_peer(&self, _node_id: NodeId, _peer_addr: PeerAddr) {}

        fn remote(&self, _node_id: NodeId) -> Arc<dyn zslg_net::ReplicaClient> {
            zslg_net_fake::replica_only(self.replica.clone())
        }

        fn remote_direct(&self, _peer_addr: PeerAddr) -> Arc<dyn zslg_net::ReplicaClient> {
            unimplemented!("OnePeerRouter: direct dial not used")
        }
    }

    /// A [`RaftLeadership`] fake whose current leader is settable mid-test —
    /// unlike the real `RaftNode`, this doesn't need an actual demotion to
    /// drive through raft, just a flag flip before `build_snapshot` runs.
    struct FakeLeadership {
        leader: Mutex<Option<NodeId>>,
    }

    impl FakeLeadership {
        fn new(leader: NodeId) -> Self {
            Self {
                leader: Mutex::new(Some(leader)),
            }
        }

        fn set_leader(&self, leader: Option<NodeId>) {
            *self.leader.lock().expect("lock") = leader;
        }
    }

    #[async_trait::async_trait]
    impl RaftLeadership for FakeLeadership {
        fn current_leader(&self) -> Option<NodeId> {
            *self.leader.lock().expect("lock")
        }

        fn voter_ids(&self) -> BTreeSet<NodeId> {
            BTreeSet::new()
        }

        fn voter_server_names(&self) -> Vec<(NodeId, ServerName)> {
            Vec::new()
        }

        async fn remove_member(
            &self,
            _node_id: zslg_consensus::NodeId,
        ) -> Result<(), zslg_consensus::Error> {
            Ok(())
        }
    }

    /// Counts `write_snapshot` calls by delegating everything else to a real
    /// [`RemoteSnapshotStore`], so a build that gets past the leadership
    /// re-check would show up here.
    struct CountingSnapshotStore {
        inner: Arc<RemoteSnapshotStore>,
        writes: AtomicUsize,
    }

    impl CountingSnapshotStore {
        fn new(inner: Arc<RemoteSnapshotStore>) -> Self {
            Self {
                inner,
                writes: AtomicUsize::new(0),
            }
        }

        fn writes(&self) -> usize {
            self.writes.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl SnapshotStore for CountingSnapshotStore {
        async fn write_snapshot(
            &self,
            id: SnapshotId,
            entries: Channel<
                Result<SnapshotEntry, zslg_snapshot::Error>,
                crossbeam_combinators::Finite,
            >,
            compression: CompressionLevel,
            reason: zslg_snapshot::ObjectStoreReason,
        ) -> Result<SnapshotWrite, zslg_snapshot::Error> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            let _ = reason;
            self.inner
                .write_snapshot(
                    id,
                    entries,
                    compression,
                    zslg_snapshot::ObjectStoreReason::SnapshotCreate,
                )
                .await
        }
        async fn read_snapshot(
            &self,
            id: SnapshotId,
            reason: zslg_snapshot::ObjectStoreReason,
        ) -> Result<
            Channel<Result<SnapshotEntry, zslg_snapshot::Error>, crossbeam_combinators::Finite>,
            zslg_snapshot::Error,
        > {
            let _ = reason;
            self.inner
                .read_snapshot(id, zslg_snapshot::ObjectStoreReason::ArchiveRead)
                .await
        }
        fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
            self.inner.shutdown_token()
        }

        async fn has_bootstrap_fence(&self) -> Result<bool, zslg_snapshot::Error> {
            self.inner.has_bootstrap_fence().await
        }
        async fn write_bootstrap_fence(&self) -> Result<(), zslg_snapshot::Error> {
            self.inner.write_bootstrap_fence().await
        }
        async fn cleanup_snapshots(
            &self,
            live_snapshots: std::collections::BTreeSet<SnapshotId>,
            cleanup_retention: std::time::Duration,
        ) -> Result<CleanupReport, zslg_snapshot::Error> {
            self.inner
                .cleanup_snapshots(live_snapshots, cleanup_retention)
                .await
        }
    }

    let h = boot().await;

    // Seed a one-node replica set so the quorum fan-out has a target, and one
    // row so the window is non-empty (an empty window would take the earlier
    // `NoopEmptyWindow` return and never reach the re-check).
    let replica_id = NodeId::new_v4();
    zslg_raft_setup::testing::write_state_fenced(
        &h.raft,
        &h.raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single([replica_id].into_iter().collect()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed replica set");

    integrate_batch(
        &h.local_store,
        upstream(1),
        vec![(LogIndex(0), make_data(1, 100, b"x"))],
    )
    .await
    .expect("integrate");

    let router: Arc<dyn Router> = Arc::new(OnePeerRouter {
        replica: Arc::new(ReadyReplica {
            raft_state: h.raft_state.clone(),
        }),
    });
    let store = CountingSnapshotStore::new(Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: h.object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    )));

    let leadership = FakeLeadership::new(h.leader_id);
    // Demote before the build starts: `await_quorum_ready` is satisfied by the
    // one-node replica set above, so the re-check after it is the only thing
    // that can stop the upload.
    leadership.set_leader(None);

    let inputs = SnapshotInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: h.local_store.clone(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &store,
        raft_state: &h.raft_state,
        router,
        leadership: Arc::new(leadership),
        self_node_id: h.leader_id,
        quorum_poll_interval: Duration::from_millis(10),
        quorum_wait_timeout: Duration::from_secs(5),
        compression: CompressionLevel::fast(),
    };

    let result = build_snapshot(&inputs, Hlc(500)).await.expect("build");
    assert_eq!(
        result,
        SnapshotResult::Skipped,
        "a demoted node must not build"
    );
    assert_eq!(store.writes(), 0, "and must not write an object");
}
