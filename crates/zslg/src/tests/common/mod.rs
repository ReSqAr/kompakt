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
pub(crate) use zslg_raft_fake::{Partition, test_membership_map};
pub(crate) use zslg_raft_setup::testing::{
    TestNode, fast_raft_config, start_cluster, start_cluster_with_partition, wait_for_any_leader,
    wait_for_leader, write_state_fenced, write_state_fenced_via_leader,
};
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

pub(crate) use zslg_learner::testing::{local_store_at, stable_log_at, stable_log_engine};
use zslg_net::NetError;
pub(crate) use zslg_net_fake::{ReadyReplica, replica_only};
pub(crate) use zslg_time_fake::FixedClock;

use crate::Error;

use crossbeam_combinators::Channel;
use zslg_consensus::NodeId;
use zslg_consensus::RaftState;
use zslg_log::Data;
use zslg_log::DataId;
use zslg_log::{Replica, StampedWrite, WriteOutcome};
// ── cluster helpers ───────────────────────────────────────────────────

// ── deadline-bounded wait helpers ─────────────────────────────────────

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

// ── shared test utilities ─────────────────────────────────────────────

/// Build a deterministic [`NodeId`] from a single byte seed.
pub(crate) fn nid(byte: u8) -> zslg_consensus::NodeId {
    let mut bytes = [0u8; 16];
    bytes[15] = byte;
    zslg_consensus::NodeId::from_uuid(uuid::Uuid::from_bytes(bytes))
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
