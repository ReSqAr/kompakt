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
    clippy::type_complexity,
    reason = "test code"
)]
use crate::meta::Data;
use crate::object_store::CompressionLevel;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use crate::api::ReplicatedRaftState;
use crate::api::{
    LivenessProbe, LocalStoreMut, RaftLeadership, Router, SnapshotStore, StableLogSnapshot,
    StateReplicator,
};
use crate::hlc::CutoffApplier;
use crate::hlc::HlcClock;
use crate::hlc::HlcGate;
use crate::hlc::{Hlc, HlcParts};
use crate::ids::{LogIndex, NodeId, PeerAddr, SnapshotId};
use crate::leader::jobs::membership::liveness::LivenessConfig;
use crate::leader::{CompactionTier, LeaderLoopConfig, SnapshotConfig, spawn_leader_loop};
use crate::learner::LocalStore;
use crate::learner::StableLog;
use crate::learner::integrate_manifest_for_test;
use crate::meta::{ReplicaMembership, SnapshotEntry, SnapshotMetadata};
use crate::network::static_peers;
use crate::network::{LocalReplicaClient, PeerRouter, RemoteRouter};
use crate::object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use crate::raft::MembershipConfig;
use crate::raft::RaftNode;
use crate::raft::SpareSelectionStrategy;
use crate::replica::ReplicaLogStore;
use crate::replica::ReplicaService;
use crate::shutdown::Shutdown;

use crate::tests::fast_raft_config as raft_config;
use crate::tests::make_data;
use crate::tests::wait_for_leader;
use crate::tests::wait_until;

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

struct NoOpProbe;

#[async_trait::async_trait]
impl LivenessProbe for NoOpProbe {
    async fn probe(&self) -> Result<(), crate::Error> {
        Ok(())
    }
}

struct PerNode {
    local_store: Arc<LocalStore>,
    stable_log: Arc<StableLog>,
    replica_log: Arc<ReplicaLogStore>,
    hlc_gate: Arc<HlcGate>,
    object_store: Arc<dyn ObjectStore>,
    snapshot_store: Arc<RemoteSnapshotStore>,
    _tempdir: TempDir,
}

async fn build_per_node() -> PerNode {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let local_store = LocalStore::simple_at(tempdir.path().join("local.stable"))
        .await
        .expect("from_db");

    let stable_log = crate::tests::stable_log_at(&tempdir.path().join("stable.db")).await;
    let (replica_log, _flush) = ReplicaLogStore::open_with_flush(
        tempdir.path().join("replica-log"),
        CancellationToken::new(),
    )
    .await
    .expect("open_for_test");
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let snapshot_store = Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    ));
    PerNode {
        local_store,
        stable_log,
        replica_log,
        hlc_gate: Arc::new(HlcGate::new()),
        object_store,
        snapshot_store,
        _tempdir: tempdir,
    }
}

fn make_remote_router(clock: Arc<HlcClock>) -> Arc<RemoteRouter> {
    Arc::new(RemoteRouter::for_test(clock, Duration::from_secs(10)))
}

/// The spawned leader tasks, with the only correct way to stop them.
///
/// `Shutdown::run` is the node *supervisor*: it waits for the first task to
/// exit and reports why. It is not "cancel and drain" — nothing exits until the
/// token fires, so awaiting it without cancelling first hangs forever. Bundling
/// the token with the `Shutdown` makes that impossible to get wrong here.
struct LeaderTasks {
    cancel: CancellationToken,
    shutdown: Shutdown,
}

impl LeaderTasks {
    async fn stop(self) {
        self.cancel.cancel();
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            self.shutdown.run(Duration::from_secs(2)),
        )
        .await;
    }
}

/// Bring up one node's full leader job set through the real
/// [`spawn_leader_loop`] wiring, so these tests exercise the same task graph
/// production does.
///
/// Returns the `Shutdown` that owns every leader task (cancel it to stop them)
/// and the loopback replica client, which the router holds only by `Weak`.
fn spawn_loop(
    self_node_id: NodeId,
    sys: &crate::tests::TestNode,
    storage: &PerNode,
    clock: Arc<HlcClock>,
    remote_router: Arc<RemoteRouter>,
    compaction_interval: Duration,
    compaction_tiers: Vec<CompactionTier>,
    membership_poll_interval: Duration,
) -> (LeaderTasks, Shutdown, Arc<dyn crate::api::ReplicaClient>) {
    let (drained_cutoff_tx, drained_cutoff_rx) =
        tokio::sync::watch::channel::<Option<crate::hlc::Hlc>>(None);
    let applier_cancel = CancellationToken::new();
    tokio::spawn(
        CutoffApplier::new(
            Arc::new(sys.raft_state.clone()),
            storage.hlc_gate.clone(),
            drained_cutoff_tx,
            applier_cancel.clone(),
        )
        .run(),
    );

    let replica = Arc::new(ReplicaService::new(
        storage.replica_log.clone(),
        storage.hlc_gate.clone(),
        self_node_id,
        clock.clone(),
        Arc::new(sys.raft_state.clone()),
        drained_cutoff_rx.clone(),
        crate::unit::nonzero(2048),
        crate::network::static_peers(Vec::new()),
    ));
    let mut server_shutdown = Shutdown::new();
    let self_client: Arc<dyn crate::api::ReplicaClient> = Arc::new(LocalReplicaClient::new(
        replica.clone(),
        Arc::new(RaftNode::new(
            sys.raft.clone(),
            crate::network::permissive_gate(),
            std::time::Duration::from_secs(30),
        )),
        Arc::new(sys.raft_state.clone()),
    ));
    let server_addr = crate::network::bind_server(
        self_client.clone(),
        clock.clone(),
        crate::network::RpcServerConfig {
            listen_addr: "127.0.0.1:0".parse().expect("addr"),
            socket_opts: crate::network::SocketOpts::for_test(),
            node_id: self_node_id,
        },
        &mut server_shutdown,
    )
    .expect("bind server");
    remote_router.register_peer(
        self_node_id,
        PeerAddr {
            addr: server_addr.into(),
            server_name: "localhost".into(),
        },
    );

    let remote: Arc<dyn Router> = remote_router.clone();
    let router = Arc::new(PeerRouter::new(
        self_node_id,
        Arc::downgrade(&self_client),
        remote,
    ));
    let leadership: Arc<dyn RaftLeadership> = Arc::new(RaftNode::new(
        sys.raft.clone(),
        crate::network::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));
    let state_replicator: Arc<dyn StateReplicator> = Arc::new(RaftNode::new(
        sys.raft.clone(),
        crate::network::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));

    let mut leader_shutdown = Shutdown::new();
    spawn_leader_loop(
        &mut leader_shutdown,
        clock,
        storage.local_store.clone(),
        storage.stable_log.clone(),
        storage.snapshot_store.clone(),
        leadership,
        state_replicator,
        Arc::new(sys.raft_state.clone()),
        router,
        LeaderLoopConfig {
            snapshot: SnapshotConfig {
                // Large cap: these tests are not about chunking, so no truncation.
                max_snapshot_entries: std::num::NonZeroUsize::MAX,
                interval: Duration::from_millis(50),
                cutoff_timeout: Duration::from_millis(5),
                cleanup_interval: Duration::from_secs(60),
                cleanup_retention: Duration::from_secs(60),
                compaction_interval,
                compaction_tiers,
                compression: CompressionLevel::fast(),
            },
            membership: MembershipConfig {
                replica_node_count: 3,
                replica_node_max_failures: 5,
                forced_node_removal_timeout: Duration::ZERO,
                poll_interval: membership_poll_interval,
                spare_selection: SpareSelectionStrategy::Random,
            },
            liveness: LivenessConfig {
                // Long enough that these tests are driven by state changes, not
                // by liveness probes.
                probe_interval: Duration::from_secs(60),
                probe_timeout: Duration::from_secs(5),
            },
            quorum_wait_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(20),
            peers_watcher: static_peers(vec![]),
            self_server_name: "test".into(),
            self_node_id,
        },
    );
    (
        LeaderTasks {
            cancel: leader_shutdown.token(),
            shutdown: leader_shutdown,
        },
        server_shutdown,
        self_client,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_loop_only_progresses_on_leader_and_resumes_after_switch() {
    let n1 = NodeId::new_v4();
    let n2 = NodeId::new_v4();
    let ids = [n1, n2];

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let n1_raft = nodes.get(&n1).expect("n1").raft.clone();
    n1_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&n1_raft, n1).await;

    let replicas: std::collections::BTreeSet<NodeId> = [n1].into_iter().collect();
    crate::tests::write_state_fenced(
        &n1_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(replicas.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed");

    for node_id in &ids {
        let apply = nodes.get(node_id).expect("node").raft_state.clone();
        let target = replicas.clone();
        wait_until(
            &format!("{node_id:?} sees seed"),
            Duration::from_secs(5),
            || {
                let apply = apply.clone();
                let target = target.clone();
                async move { apply.snapshot().replica.current().clone() == target }
            },
        )
        .await;
    }

    let storage_n1 = build_per_node().await;
    let storage_n2 = build_per_node().await;
    integrate_batch(
        &storage_n1.local_store,
        n1,
        vec![(LogIndex(0), make_data(1, 100, b"x"))],
    )
    .await
    .expect("seed n1 store");
    integrate_batch(
        &storage_n2.local_store,
        n1,
        vec![(LogIndex(0), make_data(1, 100, b"x"))],
    )
    .await
    .expect("seed n2 store");

    let clock_n1 = Arc::new(HlcClock::new(Duration::from_secs(60)));
    let clock_n2 = Arc::new(HlcClock::new(Duration::from_secs(60)));
    let rpc_n1 = make_remote_router(clock_n1.clone());
    let rpc_n2 = make_remote_router(clock_n2.clone());
    let (cancel_n1, _server_n1, _self_n1) = spawn_loop(
        n1,
        nodes.get(&n1).expect("n1"),
        &storage_n1,
        clock_n1.clone(),
        rpc_n1.clone(),
        Duration::from_secs(60),
        vec![Duration::from_secs(3600).into()],
        Duration::from_millis(50),
    );
    let (cancel_n2, _server_n2, _self_n2) = spawn_loop(
        n2,
        nodes.get(&n2).expect("n2"),
        &storage_n2,
        clock_n2.clone(),
        rpc_n2.clone(),
        Duration::from_secs(60),
        vec![Duration::from_secs(3600).into()],
        Duration::from_millis(50),
    );

    let n1_apply = nodes.get(&n1).expect("n1").raft_state.clone();
    wait_until("n1 manifest grew", Duration::from_secs(10), || {
        let apply = n1_apply.clone();
        async move { !apply.snapshot().manifest_state.manifest.is_empty() }
    })
    .await;

    cancel_n1.stop().await;
    let n1_sys = nodes.remove(&n1).expect("n1");
    let _ = n1_sys.raft.shutdown().await;

    assert!(
        storage_n2.stable_log.end_index().is_none(),
        "n2 (follower) should not have extended its stable log",
    );

    cancel_n2.stop().await;
    for sys in nodes.into_values() {
        let _ = sys.raft.shutdown().await;
    }
    let _ = BTreeMap::<NodeId, ()>::new();
}

/// F-26: the leader loop must actually run compaction. The manifest is
/// seeded with two snapshots sharing a tier bucket whose window closed long
/// ago, so the first compaction tick merges them — visible as a single
/// manifest entry spanning both inputs (`start_hlc < end_hlc`). Seeding
/// instead of creating the snapshots through live rounds keeps the test
/// independent of wall-clock timing: live rounds spread snapshot start HLCs
/// across real time, and on a slow machine they can land in distinct tier
/// buckets where nothing is ever mergeable.
#[tokio::test(flavor = "multi_thread")]
async fn leader_loop_compacts_manifest() {
    let n1 = NodeId::new_v4();
    let ids = [n1];

    let nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let n1_raft = nodes.get(&n1).expect("n1").raft.clone();
    n1_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&n1_raft, n1).await;

    let storage = build_per_node().await;

    // Two snapshots inside one tier bucket that closed ~2 hours ago.
    // base_ms is bucket-aligned so both entries provably share the bucket.
    let tier = CompactionTier::from(Duration::from_secs(4));
    let tier_ms = tier.as_millis();
    let now_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after epoch")
            .as_millis(),
    )
    .expect("now fits u64");
    let base_ms = now_ms
        .checked_sub(7_200_000)
        .expect("clock is past two hours after epoch")
        .checked_div(tier_ms)
        .expect("tier is non-zero")
        .checked_mul(tier_ms)
        .expect("fits");
    let hlc_at = |offset_ms: u64| {
        Hlc::from_parts(HlcParts {
            physical_ms: base_ms.checked_add(offset_ms).expect("fits"),
            logical: 0,
        })
    };
    let metas = [
        SnapshotMetadata {
            start_hlc: hlc_at(0),
            end_hlc: hlc_at(10),
            start_index: LogIndex(0),
            end_index: LogIndex(0),
            snapshot_id: SnapshotId::from_u128(1),
        },
        SnapshotMetadata {
            start_hlc: hlc_at(20),
            end_hlc: hlc_at(30),
            start_index: LogIndex(1),
            end_index: LogIndex(1),
            snapshot_id: SnapshotId::from_u128(2),
        },
    ];
    for (i, meta) in metas.iter().enumerate() {
        let seed = u8::try_from(i.checked_add(1).expect("fits")).expect("fits u8");
        let entry = SnapshotEntry {
            global_index: meta.start_index,
            data: make_data(seed, meta.start_hlc.raw(), b"x"),
        };
        storage
            .snapshot_store
            .write_snapshot(
                meta.snapshot_id,
                crossbeam_combinators::Channels::from_vec(vec![Ok::<_, crate::Error>(entry)]),
                CompressionLevel::fast(),
                crate::metrics::ObjectStoreReason::SnapshotCreate,
            )
            .await
            .expect("write seeded snapshot file");
    }

    let replicas: std::collections::BTreeSet<NodeId> = [n1].into_iter().collect();
    crate::tests::write_state_fenced(
        &n1_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(replicas),
            manifest: metas.to_vec(),
            manifest_watermark: Some(metas[1].end_hlc),
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed manifest");

    let clock = Arc::new(HlcClock::new(Duration::from_secs(60)));
    let rpc = make_remote_router(clock.clone());
    let (cancel, _server, _self_client) = spawn_loop(
        n1,
        nodes.get(&n1).expect("n1"),
        &storage,
        clock.clone(),
        rpc.clone(),
        Duration::from_millis(100),
        vec![tier],
        Duration::from_millis(50),
    );

    let apply = nodes.get(&n1).expect("n1").raft_state.clone();
    wait_until("manifest compacted", Duration::from_secs(20), || {
        let apply = apply.clone();
        async move {
            let manifest = apply.snapshot().manifest_state.manifest.clone();
            manifest.len() == 1 && manifest.iter().all(|m| m.start_hlc < m.end_hlc)
        }
    })
    .await;

    cancel.stop().await;
    for sys in nodes.into_values() {
        let _ = sys.raft.shutdown().await;
    }
}

/// The leader loop must drive an in-flight membership change to completion
/// from raft-state change events alone: the membership ticker is pushed far
/// beyond the test deadline, so only the event branch (nudged by the
/// watermark advances of committed snapshots) can clear the joint config.
#[tokio::test]
async fn leader_loop_completes_membership_change_via_state_events() {
    let n1 = NodeId::new_v4();
    let ids = [n1];

    let nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let n1_raft = nodes.get(&n1).expect("n1").raft.clone();
    n1_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&n1_raft, n1).await;

    let replicas: std::collections::BTreeSet<NodeId> = [n1].into_iter().collect();
    // Establish the single config first: the write point requires a joint
    // config's outgoing set to be the set that was actually in force
    // (`JointEntryWrongOldSet`), so a joint config cannot be seeded onto the
    // empty bootstrap state.
    crate::tests::write_state_fenced(
        &n1_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(replicas.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed single");
    // Then seed a *joint* config: the loop must take it through the generation
    // quorum, step 2b arming, the drain wait, and the final collapse.
    crate::tests::write_state_fenced(
        &n1_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::joint(replicas.clone(), replicas.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed joint");

    let storage = build_per_node().await;
    let clock = Arc::new(HlcClock::new(Duration::from_secs(60)));
    let rpc = make_remote_router(clock.clone());

    let (cancel, _server, _self_client) = spawn_loop(
        n1,
        nodes.get(&n1).expect("n1"),
        &storage,
        clock.clone(),
        rpc.clone(),
        Duration::from_secs(3600),
        vec![Duration::from_secs(3600).into()],
        // Only the event branch may drive the change within the deadline.
        Duration::from_secs(3600),
    );

    let apply = nodes.get(&n1).expect("n1").raft_state.clone();
    // Each round commits one fresh snapshot, i.e. one watermark-advance event.
    // The change needs two nudges (enter generation quorum + pass the
    // post-joint snapshot gate); extra rounds absorb transient probe failures.
    for i in 0u64..3 {
        if apply.snapshot().replica.next().is_none() {
            break;
        }
        let hlc = match apply.snapshot().manifest_state.manifest_watermark {
            Some(wm) => Hlc(wm.raw().checked_add(1).expect("hlc headroom")),
            None => clock.now(),
        };
        let seed = u8::try_from(i.checked_add(1).expect("fits")).expect("fits u8");
        integrate_batch(
            &storage.local_store,
            n1,
            vec![(LogIndex(i), make_data(seed, hlc.raw(), b"x"))],
        )
        .await
        .expect("integrate");
        wait_until("record snapshotted", Duration::from_secs(10), || {
            let apply = apply.clone();
            async move {
                apply
                    .snapshot()
                    .manifest_state
                    .manifest_watermark
                    .is_some_and(|wm| wm >= hlc)
            }
        })
        .await;
        // Catch the stable log up so the next snapshot's index-continuity
        // check passes (no learner load loop runs in this harness).
        integrate_manifest_for_test(
            storage.snapshot_store.clone(),
            storage.stable_log.as_ref(),
            &apply.snapshot().manifest_state,
        )
        .await
        .expect("load manifest");
    }

    wait_until(
        "joint config cleared via state events",
        Duration::from_secs(10),
        || {
            let apply = apply.clone();
            let replicas = replicas.clone();
            async move {
                let state = apply.snapshot();
                state.replica.next().is_none() && *state.replica.current() == replicas
            }
        },
    )
    .await;

    cancel.stop().await;
    for sys in nodes.into_values() {
        let _ = sys.raft.shutdown().await;
    }
}

/// **The regression this whole decomposition exists for.**
///
/// A snapshot build that cannot finish — its replica quorum is wedged, so
/// `await_quorum_ready` sits there until its timeout — must not stop any other
/// leader job. Serializing them would deadlock: the membership drain (§18) is
/// satisfied by snapshot progress.
///
/// The observable proof is that `hlc_cutoff` keeps advancing while
/// `manifest_watermark` cannot: those are two different jobs, and step 1 is no
/// longer the front half of the job that is stuck.
#[tokio::test(flavor = "multi_thread")]
async fn a_wedged_snapshot_build_does_not_block_the_other_jobs() {
    let n1 = NodeId::new_v4();
    let ids = [n1];

    let nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");
    let n1_raft = nodes.get(&n1).expect("n1").raft.clone();
    n1_raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("init");
    wait_for_leader(&n1_raft, n1).await;

    let replicas: std::collections::BTreeSet<NodeId> = [n1].into_iter().collect();
    crate::tests::write_state_fenced(
        &n1_raft,
        &nodes.get(&n1).expect("n1").raft_state,
        ReplicatedRaftState {
            replica: ReplicaMembership::single(replicas.clone()),
            manifest: Vec::new(),
            manifest_watermark: None,
            hlc_cutoff: None,
        },
    )
    .await
    .expect("seed replica set");

    let storage = build_per_node().await;
    let clock = Arc::new(HlcClock::new(Duration::from_secs(60)));

    // Seed one record so the snapshot window is non-empty — otherwise the build
    // would short-circuit instead of reaching the quorum wait.
    integrate_batch(
        &storage.local_store,
        n1,
        vec![(LogIndex(0), make_data(1, clock.now().raw(), b"x"))],
    )
    .await
    .expect("integrate");

    // Wedge the only replica's `fetch_status`, so `await_quorum_ready` can never
    // reach a quorum and the snapshot job parks inside its build.
    let ready = Arc::new(crate::tests::ReadyReplica {
        raft_state: nodes.get(&n1).expect("n1").raft_state.clone(),
    });
    let (map_router, faults) = crate::tests::MapPeerRouter::wrap_all_faulty(
        [(n1, ready as Arc<dyn crate::api::Replica>)]
            .into_iter()
            .collect(),
    );
    let faults = faults.get(&n1).expect("fault handle").clone();
    faults.wedge();
    let router: Arc<dyn Router> = Arc::new(map_router);

    let leadership: Arc<dyn RaftLeadership> = Arc::new(RaftNode::new(
        n1_raft.clone(),
        crate::network::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));
    let state_replicator: Arc<dyn StateReplicator> = Arc::new(RaftNode::new(
        n1_raft.clone(),
        crate::network::permissive_gate(),
        std::time::Duration::from_secs(30),
    ));
    let mut leader_shutdown = Shutdown::new();
    let leader_cancel = leader_shutdown.token();
    spawn_leader_loop(
        &mut leader_shutdown,
        clock,
        storage.local_store.clone(),
        storage.stable_log.clone(),
        storage.snapshot_store.clone(),
        leadership,
        state_replicator,
        Arc::new(nodes.get(&n1).expect("n1").raft_state.clone()),
        router,
        LeaderLoopConfig {
            snapshot: SnapshotConfig {
                // Large cap: these tests are not about chunking, so no truncation.
                max_snapshot_entries: std::num::NonZeroUsize::MAX,
                interval: Duration::from_millis(20),
                cutoff_timeout: Duration::from_millis(1),
                cleanup_interval: Duration::from_secs(60),
                cleanup_retention: Duration::from_secs(60),
                compaction_interval: Duration::from_secs(3600),
                compaction_tiers: vec![Duration::from_secs(3600).into()],
                compression: CompressionLevel::fast(),
            },
            membership: MembershipConfig {
                replica_node_count: 1,
                replica_node_max_failures: 5,
                forced_node_removal_timeout: Duration::ZERO,
                poll_interval: Duration::from_millis(20),
                spare_selection: SpareSelectionStrategy::Random,
            },
            liveness: LivenessConfig {
                probe_interval: Duration::from_secs(60),
                probe_timeout: Duration::from_secs(5),
            },
            // Long enough that the build stays parked for the whole test.
            quorum_wait_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(10),
            peers_watcher: static_peers(vec![]),
            self_server_name: "test".into(),
            self_node_id: n1,
        },
    );

    let apply = nodes.get(&n1).expect("n1").raft_state.clone();
    // The cutoff job is independent of the wedged build, so the cutoff must keep
    // advancing. Requiring several distinct advances rules off a single lucky one.
    let seen: Arc<parking_lot::Mutex<std::collections::BTreeSet<Hlc>>> =
        Arc::new(parking_lot::Mutex::new(std::collections::BTreeSet::new()));
    wait_until(
        "hlc cutoff keeps advancing while the snapshot build is wedged",
        Duration::from_secs(10),
        || {
            let apply = apply.clone();
            let seen = seen.clone();
            async move {
                if let Some(c) = apply.snapshot().manifest_state.hlc_cutoff {
                    seen.lock().insert(c);
                }
                seen.lock().len() >= 3
            }
        },
    )
    .await;

    assert_eq!(
        apply.snapshot().manifest_state.manifest_watermark,
        None,
        "the wedged build must not have published a snapshot — otherwise this \
         test is not observing what it claims to",
    );

    // Releasing the wedge lets the parked build finish, proving it was parked
    // rather than dead.
    faults.unwedge();
    wait_until(
        "snapshot completes once the quorum unwedges",
        Duration::from_secs(20),
        || {
            let apply = apply.clone();
            async move { apply.snapshot().manifest_state.manifest_watermark.is_some() }
        },
    )
    .await;

    LeaderTasks {
        cancel: leader_cancel,
        shutdown: leader_shutdown,
    }
    .stop()
    .await;
    for sys in nodes.into_values() {
        let _ = sys.raft.shutdown().await;
    }
}
