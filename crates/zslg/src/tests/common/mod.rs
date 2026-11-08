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
pub(crate) use faults::{FaultyReplica, MapPeerRouter};
mod snapshot;
pub(crate) use snapshot::{SnapshotHarness, boot_snapshot_harness, commit_snapshot_result};
mod cutoff_observer;
pub(crate) use cutoff_observer::CutoffObserver;

pub(crate) use zslg_learner::testing::{local_store_at, stable_log_at, stable_log_engine};
pub(crate) use zslg_net_fake::{ReadyReplica, replica_only};
pub(crate) use zslg_time_fake::FixedClock;

// ── cluster helpers ───────────────────────────────────────────────────

// ── deadline-bounded wait helpers ─────────────────────────────────────

pub(crate) use zslg_consensus_fake::{nid, upstream};
pub(crate) use zslg_log_fake::make_data;

/// [`zslg_log_fake::replica_write`] with the error widened to the public type.
pub(crate) async fn replica_write<R: zslg_log::Replica + ?Sized>(
    replica: &R,
    data: zslg_log::Data,
) -> Result<zslg_consensus::RaftState, crate::Error> {
    zslg_log_fake::replica_write(replica, data)
        .await
        .map_err(Into::into)
}
