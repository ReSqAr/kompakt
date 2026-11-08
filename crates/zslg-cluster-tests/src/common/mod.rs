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
pub(crate) use testing_kit::{init_logging, reserve_port, wait_until_sync};
mod cluster;
pub(crate) use cluster::{Cluster, ClusterConfig, NodePlacement, default_fast_config};
mod invariants;
pub(crate) use invariants::{
    assert_cluster_invariants, assert_cluster_invariants_with_acks, assert_durability_chain_on,
};
mod cutoff_observer;
pub(crate) use cutoff_observer::CutoffObserver;

// ── cluster helpers ───────────────────────────────────────────────────

// ── deadline-bounded wait helpers ─────────────────────────────────────

pub(crate) use zslg_consensus_fake::nid;

/// [`zslg_log_fake::replica_write`] with the error widened to the public type.
pub(crate) async fn replica_write<R: zslg_log::Replica + ?Sized>(
    replica: &R,
    data: zslg_log::Data,
) -> Result<zslg_consensus::RaftState, zslg::Error> {
    zslg_log_fake::replica_write(replica, data)
        .await
        .map_err(Into::into)
}
