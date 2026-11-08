//! The snapshot job end to end against a real Raft instance and object store.
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

mod commit_guards;
mod quorum;
mod round_trip;

use crossbeam_combinators::testing::ChannelTestExt as _;

use std::sync::Arc;

use std::time::Duration;

use zslg_consensus::RaftLeadership;

use zslg_log::LogError;

use zslg_net::Router;

use zslg_snapshot::{CleanupReport, SnapshotStore, SnapshotWrite};

use bytes::Bytes;

use uuid::Uuid;

use zslg_consensus::NodeId;

use zslg_consensus::RaftStateHandle;

use zslg_consensus::ReplicaMembership;

use zslg_consensus::ReplicatedRaftState;

use zslg_leader_snapshot::create::{SnapshotEntryInputs, SnapshotResult, create_snapshot_entry};

use zslg_learner::LocalStore;

use zslg_learner::testing::integrate_manifest_for_test;

use zslg_log::Data;

use zslg_log::{DataId, LogIndex};

use zslg_log::{LocalStoreMut, StableLogSnapshot};

use zslg_net::PeerAddr;

use zslg_object_store::{RemoteSnapshotStore, SnapshotStoreConfig};

use zslg_raft::Raft;

use zslg_raft_setup::RaftNode;

use zslg_snapshot::CompressionLevel;

use zslg_time::Hlc;

use zslg_consensus_fake::upstream;

use zslg_log_fake::make_data;

pub(super) async fn integrate_batch(
    store: &LocalStore,
    source: NodeId,
    records: Vec<(LogIndex, Data)>,
) -> Result<(), zslg_error::Error> {
    if records.is_empty() {
        return Ok(());
    }
    let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
    for (idx, rec) in records {
        let _ = tx.send(Ok((source, idx, rec)));
    }
    let (ch, closer) = crossbeam_combinators::Channel::from(rx, "test integrate");
    // Close before the last sender drops, or the consumer forms its
    // verdict as "disconnected, not yet closed" — an abort.
    closer.close();
    drop(tx);
    store.integrate_loop(ch).await.map_err(Into::into)
}

use super::snapshot_harness::{
    boot_fake_snapshot_harness as boot_fake, boot_snapshot_harness as boot,
    commit_snapshot_result_via,
};

pub(super) async fn append_entries(local_store: &LocalStore, entries: &[Data]) {
    for (i, d) in entries.iter().enumerate() {
        let idx = LogIndex(u64::try_from(i).expect("fits"));
        integrate_batch(local_store, upstream(1), vec![(idx, d.clone())])
            .await
            .expect("integrate");
    }
}
