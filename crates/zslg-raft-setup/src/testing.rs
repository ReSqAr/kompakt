//! Test support: an in-process cluster of real Raft nodes (real storage, real
//! state machine) on the in-memory network of `zslg-raft-fake`, with the waits
//! and the fenced-state write the tests around Raft need.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    reason = "test support fails by panicking"
)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use raft_kit::Config;
use rocksdb_kit::{CfSpec, Storage, StorageConfig};
use testing_kit::wait_until;
use zslg_consensus::{NodeId, RaftStateHandle};
use zslg_raft::Raft;
use zslg_raft_fake::MemFabric;
pub use zslg_raft_fake::Partition;
use zslg_raft_storage::{RAFT_LOG_CF, RAFT_META_CF, build_raft_for_test};

/// A storage root carrying the raft column families, uncompressed.
///
/// # Errors
/// When the root cannot be opened.
pub fn open_storage_for_test(root: &Path) -> Result<Arc<Storage>, Box<dyn std::error::Error>> {
    let cfs = [RAFT_LOG_CF, RAFT_META_CF].map(|cf| CfSpec::new(cf, false));
    Ok(Storage::open(
        root,
        StorageConfig::new(bytesize::ByteSize::from_mib(64)),
        &cfs,
    )?)
}

pub struct TestNode {
    pub raft: Raft,
    pub raft_state: RaftStateHandle,
    _tempdir: tempfile::TempDir,
}

pub async fn start_cluster(
    ids: &[NodeId],
    cfg: Arc<Config>,
) -> Result<BTreeMap<NodeId, TestNode>, Box<dyn std::error::Error>> {
    let (nodes, _partition) = start_cluster_with_partition(ids, cfg).await?;
    Ok(nodes)
}

/// `start_cluster` plus a [`Partition`] handle for cutting/healing the
/// in-process raft links between the nodes.
pub async fn start_cluster_with_partition(
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
            open_storage_for_test(tempdir.path())?,
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

/// Wait until `raft` observes `expected` as the leader.
pub async fn wait_for_leader(raft: &Raft, expected: NodeId) {
    wait_until(
        "expected node to become leader",
        std::time::Duration::from_secs(10),
        || async { raft.current_leader().await == Some(expected) },
    )
    .await;
}

/// Wait until some node in the cluster is observed as leader; returns it.
pub async fn wait_for_any_leader(
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

pub async fn write_state_fenced(
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
pub async fn write_state_fenced_via_leader(
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

/// Fast raft config suitable for in-memory tests:
/// 50 ms heartbeat, 200–400 ms election timeout.
#[must_use]
pub fn fast_raft_config() -> Arc<Config> {
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
