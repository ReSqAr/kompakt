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
pub(crate) use testing_kit::{init_logging, reserve_port, wait_until, wait_until_sync, within};
pub(crate) use zslg_raft_fake::{MemFabric, Partition, test_membership_map};
mod cluster;
pub(crate) use cluster::{Cluster, ClusterConfig, NodePlacement, default_fast_config};
mod invariants;
pub(crate) use invariants::{
    assert_cluster_invariants, assert_cluster_invariants_with_acks, assert_durability_chain_on,
};
mod faults;
pub(crate) use faults::{
    FaultyReplica, MapPeerRouter, ReplicaServiceHarness, SharedClockController, SkewedClock,
    skewed_clock_pair,
};
mod snapshot;
pub(crate) use snapshot::{SnapshotHarness, boot_snapshot_harness, commit_snapshot_result};
mod cutoff_observer;
pub(crate) use cutoff_observer::CutoffObserver;

use std::collections::BTreeMap;
use std::sync::Arc;
pub(crate) use zslg_learner::testing::{local_store_at, stable_log_at, stable_log_engine};
use zslg_net::NetError;
pub(crate) use zslg_net_fake::{ReadyReplica, replica_only};
pub(crate) use zslg_time_fake::FixedClock;

use crate::Error;

use crossbeam_combinators::Channel;
use raft_kit::Config;
use zslg_consensus::NodeId;
use zslg_consensus::RaftState;
use zslg_consensus::RaftStateHandle;
use zslg_log::Data;
use zslg_log::DataId;
use zslg_log::{Replica, StampedWrite, WriteOutcome};
use zslg_raft::Raft;
use zslg_raft_storage::build_raft_for_test;
// ── cluster helpers ───────────────────────────────────────────────────

pub(crate) struct TestNode {
    pub(crate) raft: Raft,
    pub(crate) raft_state: RaftStateHandle,
    pub(crate) _tempdir: tempfile::TempDir,
}

pub(crate) async fn start_cluster(
    ids: &[NodeId],
    cfg: Arc<Config>,
) -> Result<BTreeMap<NodeId, TestNode>, Box<dyn std::error::Error>> {
    let (nodes, _partition) = start_cluster_with_partition(ids, cfg).await?;
    Ok(nodes)
}

/// `start_cluster` plus a [`Partition`] handle for cutting/healing the
/// in-process raft links between the nodes.
pub(crate) async fn start_cluster_with_partition(
    ids: &[NodeId],
    cfg: Arc<Config>,
) -> Result<(BTreeMap<NodeId, TestNode>, Partition), Box<dyn std::error::Error>> {
    let fabric = MemFabric::default();
    let partition = fabric.partition();
    let mut nodes = BTreeMap::new();
    for &id in ids {
        let router = fabric.router(id);
        let tempdir = tempfile::tempdir()?;
        let raft_state = RaftStateHandle::new();
        let raft = build_raft_for_test(
            id,
            cfg.clone(),
            router,
            crate::database::open_for_test(tempdir.path())?,
            raft_state.clone(),
        )
        .await
        .map_err(|e| format!("Raft::new: {e}"))?;
        fabric.register(id, raft.clone());
        nodes.insert(
            id,
            TestNode {
                raft,
                raft_state,
                _tempdir: tempdir,
            },
        );
    }
    Ok((nodes, partition))
}

// ── deadline-bounded wait helpers ─────────────────────────────────────

/// Wait until `raft` observes `expected` as the leader.
pub(crate) async fn wait_for_leader(raft: &Raft, expected: NodeId) {
    wait_until(
        "expected node to become leader",
        std::time::Duration::from_secs(10),
        || async { raft.current_leader().await == Some(expected) },
    )
    .await;
}

/// Wait until some node in the cluster is observed as leader; returns it.
pub(crate) async fn wait_for_any_leader(
    nodes: &BTreeMap<NodeId, TestNode>,
    timeout: std::time::Duration,
) -> NodeId {
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .expect("Instant + timeout does not overflow");
    loop {
        for sys in nodes.values() {
            if let Some(id) = sys.raft.current_leader().await
                && nodes.contains_key(&id)
            {
                return id;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("no leader elected within deadline");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Write one `Data` to a `Replica` and await its outcome, mapped to the
/// pre-stream `write` semantics: `Accepted → Ok(raft_state)`, `Rejected`/`Failed`
/// → `Err(error)`. Opens a single-write [`Replica::write`] for tests that hold one
/// write per call; production callers hold one connection per peer.
pub(crate) async fn replica_write<R: Replica + ?Sized>(
    replica: &R,
    data: Data,
) -> Result<RaftState, Error> {
    let mut outcomes = replica.write(Channel::once(StampedWrite::now(data)));
    // The channel blocks on the replica's pipeline, so it must not be pulled on the
    // reactor.
    let first = tokio::task::spawn_blocking(move || outcomes.recv())
        .await
        .map_err(|e| Error::Net(NetError::Rpc(format!("write stream join: {e}"))))?;
    match first {
        Ok(Ok(WriteOutcome::Accepted { raft_state, .. })) => Ok(raft_state),
        Ok(Ok(WriteOutcome::Rejected { error, .. } | WriteOutcome::Failed { error, .. }))
        | Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(Error::Net(NetError::Rpc(
            "replica dropped write outcome".into(),
        ))),
    }
}

pub(crate) async fn write_state_fenced(
    raft: &Raft,
    raft_state: &RaftStateHandle,
    state: zslg_consensus::ReplicatedRaftState,
) -> Result<(), String> {
    use zslg_raft::{FencedStateWrite, RaftResponse};
    // The transient outcomes keep different clocks, and only one of them is
    // this handle's to wait out:
    //
    // - `RejectedStaleGeneration` is a CAS race against a **live** leader —
    //   someone applied a newer generation between this handle's snapshot and
    //   the write. The leader exists, so retrying is cheap and an attempt
    //   count bounds it (the outer 16).
    // - `ForwardToLeader` with **no** leader id is an election in flight: the
    //   write was not applied, and this node may yet win that election, so
    //   retrying the same handle is the one wait that can succeed here. Under
    //   a parallel test run the whole machine's CPUs are contended and an
    //   election can take seconds, so this side waits on wall-clock at
    //   `wait_for_leader`'s scale, not on an attempt count.
    // - `ForwardToLeader` **with** a leader id means this handle is a follower
    //   that knows its leader: every retry on this handle forwards to the same
    //   node (in the in-memory clusters that address is the `127.0.0.1:0`
    //   placeholder, unreachable by construction), so no wait here can ever
    //   succeed. Return immediately — the caller's move is to re-observe who
    //   leads and write through that node
    //   ([`write_state_fenced_via_leader`]).
    let election_deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(5))
        .expect("Instant + 5 s does not overflow");
    for _ in 0u8..16 {
        let expected_generation = raft_state.snapshot().generation;
        let response = loop {
            match raft
                .client_write(FencedStateWrite {
                    expected_generation,
                    state: state.clone(),
                })
                .await
            {
                Ok(response) => break response,
                Err(e) => {
                    if e.forward_to_leader()
                        .is_some_and(|forward| forward.leader_id.is_none())
                    {
                        if tokio::time::Instant::now() >= election_deadline {
                            return Err(format!(
                                "client_write: no leader stabilized within 5 s (last: {e})"
                            ));
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                    return Err(format!("client_write: {e}"));
                }
            }
        };
        match response.data {
            RaftResponse::Applied => return Ok(()),
            RaftResponse::RejectedStaleGeneration { .. } => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            RaftResponse::RejectedInvalidState { reason } => {
                return Err(format!("fenced state write: invalid raft state: {reason}"));
            }
        }
    }
    Err("fenced state write: lost the generation CAS 16 times".to_owned())
}

/// [`write_state_fenced`] plus the cluster-level patience a leadership flap
/// needs: write through whoever leads **now**, and when that write reports a
/// flap — a forwarded write, an election in flight, a lost CAS — ask again
/// rather than pinning to whichever node the last observation returned.
///
/// This is the loop `raft::tests::partition` has always run after a heal; it
/// lives here because a flap is not partition-specific — any test whose state
/// write lands while a snapshot commit or whole-machine load has stirred an
/// election would otherwise assert the race rather than the write. Every
/// failure class still terminates: at the deadline with the last error intact,
/// or immediately on a non-transient one.
pub(crate) async fn write_state_fenced_via_leader(
    nodes: &BTreeMap<NodeId, TestNode>,
    state: zslg_consensus::ReplicatedRaftState,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(30))
        .expect("Instant + 30 s does not overflow");
    loop {
        let leader_id = wait_for_any_leader(nodes, std::time::Duration::from_secs(10)).await;
        let sys = nodes.get(&leader_id).expect("leader in cluster");
        match write_state_fenced(&sys.raft, &sys.raft_state, state.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

// ── shared test utilities ─────────────────────────────────────────────

/// Build a deterministic [`NodeId`] from a single byte seed.
pub(crate) fn nid(byte: u8) -> zslg_consensus::NodeId {
    let mut bytes = [0u8; 16];
    bytes[15] = byte;
    zslg_consensus::NodeId::from_uuid(uuid::Uuid::from_bytes(bytes))
}

/// Fast raft config suitable for in-memory tests:
/// 50 ms heartbeat, 200–400 ms election timeout.
pub(crate) fn fast_raft_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    )
}

pub(crate) fn make_data(seed: u8, hlc: u64, blob: &[u8]) -> Data {
    Data {
        data_id: make_meta(seed, hlc),
        blob: bytes::Bytes::copy_from_slice(blob),
    }
}

/// Deterministic "upstream" [`NodeId`] with `0xAA` in byte 0 and `seed`
/// in byte 15 — used for testing replication data sources.
pub(crate) fn upstream(seed: u8) -> NodeId {
    let mut b = [0u8; 16];
    b[0] = 0xAA;
    b[15] = seed;
    NodeId::from_uuid(uuid::Uuid::from_bytes(b))
}

/// Build a deterministic [`DataId`]: nil creator node, HLC-seeded, with `seed`
/// as the random tiebreaker. (Formerly returned a `Meta`; the id is now
/// self-describing.) Prefer [`make_data`] when a full [`Data`] with blob is
/// needed.
pub(crate) fn make_meta(seed: u8, hlc: u64) -> DataId {
    DataId::from_parts(
        zslg_time::Hlc(hlc),
        NodeId::from_uuid(uuid::Uuid::nil()),
        u64::from(seed),
    )
}

/// Create a fresh tempdir-backed [`crate::learner::LocalStore`] and return
/// the directory (for cleanup) and the store handle.
pub(crate) async fn fresh_local() -> (
    tempfile::TempDir,
    std::sync::Arc<crate::learner::LocalStore>,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.db");
    let store = crate::tests::local_store_at(path.with_extension("stable"))
        .await
        .expect("from_db");
    (dir, store)
}
