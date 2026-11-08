//! Compaction integration tests: a real Raft cluster, local store, stable log
//! and object-store-backed snapshot store, so they live with the assembly that
//! sees all of those.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use std::sync::Arc;

use crossbeam_combinators::testing::ChannelTestExt as _;
use zslg_consensus::NodeId;
use zslg_leader_compaction::compact;
use zslg_log::LogIndex;
use zslg_snapshot::CompactionTier;
use zslg_snapshot::SnapshotStore;
use zslg_time::{Hlc, HlcParts};

use std::collections::BTreeMap;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;

use uuid::Uuid;

use zslg_log::{LocalStoreMut, StableLogSnapshot};

use zslg_leader_snapshot::create::{SnapshotEntryInputs, SnapshotResult, create_snapshot_entry};
use zslg_learner::LocalStore;
use zslg_learner::testing::integrate_manifest_for_test;
use zslg_log::Data;
use zslg_log::DataId;
use zslg_object_store::{RemoteSnapshotStore, SnapshotStoreConfig};
use zslg_snapshot::CompressionLevel;

use super::snapshot_harness::commit_snapshot_result;
use bytes::Bytes;
use zslg_consensus_fake::upstream;
use zslg_raft_setup::testing::fast_raft_config as raft_config;
use zslg_raft_setup::testing::wait_for_leader;

async fn integrate_batch(
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

fn make_data(seed: u32, hlc_ms: u64, blob: &[u8]) -> Data {
    Data {
        data_id: DataId::from_parts(
            Hlc::from_parts(zslg_time::HlcParts {
                physical_ms: hlc_ms,
                logical: 0,
            }),
            NodeId::from_uuid(Uuid::nil()),
            u64::from(seed),
        ),
        blob: Bytes::copy_from_slice(blob),
    }
}

#[tokio::test]
async fn compaction_merges_and_preserves_stable_log_contents() {
    let n1 = NodeId::new_v4();
    let mut nodes = zslg_raft_setup::testing::start_cluster(&[n1], raft_config())
        .await
        .expect("cluster");
    nodes
        .get_mut(&n1)
        .expect("n1")
        .raft
        .initialize(zslg_raft_fake::test_membership_map(&[n1]))
        .await
        .expect("initialize");
    wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;
    let leader = nodes.remove(&n1).expect("n1");

    let dir = tempfile::tempdir().expect("tempdir");
    let local_store = zslg_learner::testing::local_store_at(dir.path().join("local.stable"))
        .await
        .expect("from_db");
    let stable_log = zslg_learner::testing::stable_log_at(&dir.path().join("stable.db")).await;
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ss: Arc<dyn SnapshotStore> = Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    ));

    let cutoffs_ms = [100u64, 200, 300, 400, 500];
    let mut all_entries: Vec<Data> = Vec::new();
    let mut next_local_idx: u64 = 0;
    let mut next_seed: u32 = 1;

    for &cutoff_ms in &cutoffs_ms {
        for offset_ms in [50u64, 0u64] {
            let hlc_ms = cutoff_ms.checked_sub(offset_ms).expect("non-negative");
            let d = make_data(next_seed, hlc_ms, format!("v{next_seed}").as_bytes());
            next_seed = next_seed.checked_add(1).expect("seed overflow");
            integrate_batch(
                &local_store,
                upstream(1),
                vec![(LogIndex(next_local_idx), d.clone())],
            )
            .await
            .expect("integrate");
            next_local_idx = next_local_idx.checked_add(1).expect("idx overflow");
            all_entries.push(d);
        }

        let inputs = SnapshotEntryInputs {
            // Large cap: these tests are not about chunking, so no truncation.
            max_snapshot_entries: std::num::NonZeroUsize::MAX,
            local_store: local_store.as_ref(),
            stable_log: stable_log.as_ref(),
            snapshot_store: ss.as_ref(),
            raft_state: &leader.raft_state,
            compression: CompressionLevel::fast(),
        };
        let cutoff_hlc = Hlc::from_parts(HlcParts {
            physical_ms: cutoff_ms,
            logical: 0,
        });
        let result = create_snapshot_entry(&inputs, cutoff_hlc)
            .await
            .expect("create");
        commit_snapshot_result(&leader.raft, &leader.raft_state, &result).await;
        assert!(matches!(result, SnapshotResult::Committed { .. }));
        let manifest_state = leader.raft_state.snapshot().manifest_state.clone();
        integrate_manifest_for_test(ss.clone(), stable_log.as_ref(), &manifest_state)
            .await
            .expect("load on leader");
    }

    let pre_state = leader.raft_state.snapshot();
    let pre_manifest_state = pre_state.manifest_state.clone();
    let learner_pre_dir = tempfile::tempdir().expect("tempdir");
    let learner_pre =
        zslg_learner::testing::stable_log_engine(learner_pre_dir.path().join("stable.stable"))
            .await
            .expect("open stable log");
    integrate_manifest_for_test(ss.clone(), &learner_pre, &pre_manifest_state)
        .await
        .expect("load pre");
    use futures::TryStreamExt as _;
    let pre_entries: Vec<(LogIndex, Data)> = learner_pre
        .range(
            None,
            LogIndex(u64::try_from(all_entries.len()).expect("fits")),
        )
        .into_test_stream()
        .try_collect()
        .await
        .expect("range pre");
    assert_eq!(pre_entries.len(), all_entries.len());

    let (intents, _integrator_cancel) = crate::Integrator::spawn_for_test(
        Arc::new(zslg_raft_setup::RaftNode::new(
            leader.raft.clone(),
            zslg_consensus_fake::permissive_gate(),
            std::time::Duration::from_secs(30),
        )),
        Arc::new(leader.raft_state.clone()),
    );
    let tiers: [CompactionTier; 1] = [Duration::from_millis(350).into()];
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        compact(
            &ss,
            &leader.raft_state,
            &tiers,
            CompressionLevel::fast(),
            intents.as_ref(),
        ),
    )
    .await
    .expect("compaction test timed out after 30s")
    .expect("compact");
    assert_eq!(outcome.merges_performed, 2);
    assert_eq!(outcome.final_manifest_len, 2);

    let post_state = leader.raft_state.snapshot();
    let post_manifest_state = post_state.manifest_state.clone();
    assert_ne!(
        post_manifest_state.manifest.len(),
        pre_manifest_state.manifest.len()
    );
    assert_eq!(
        post_state.manifest_state.manifest_watermark, pre_state.manifest_state.manifest_watermark,
        "compaction must not move the watermark"
    );

    let post_ids: std::collections::BTreeSet<_> = post_manifest_state
        .manifest
        .iter()
        .map(|m| m.snapshot_id)
        .collect();
    assert_eq!(
        post_ids.len(),
        post_manifest_state.manifest.len(),
        "post-compaction snapshot ids must be unique"
    );
    let pre_ids: std::collections::BTreeSet<_> = pre_manifest_state
        .manifest
        .iter()
        .map(|m| m.snapshot_id)
        .collect();
    let merged_ids: Vec<_> = post_ids.difference(&pre_ids).collect();
    assert_eq!(
        merged_ids.len(),
        2,
        "two freshly-minted merged ids expected"
    );

    let learner_post_dir = tempfile::tempdir().expect("tempdir");
    let learner_post =
        zslg_learner::testing::stable_log_engine(learner_post_dir.path().join("stable.stable"))
            .await
            .expect("from_db");
    integrate_manifest_for_test(ss.clone(), &learner_post, &post_manifest_state)
        .await
        .expect("load post");
    let post_entries: Vec<(LogIndex, Data)> = learner_post
        .range(
            None,
            LogIndex(u64::try_from(all_entries.len()).expect("fits")),
        )
        .into_test_stream()
        .try_collect()
        .await
        .expect("range post");

    assert_eq!(pre_entries.len(), post_entries.len());
    for (pre_kv, post_kv) in pre_entries.iter().zip(post_entries.iter()) {
        assert_eq!(pre_kv.0, post_kv.0, "global index mismatch");
        assert_eq!(pre_kv.1, post_kv.1, "data mismatch");
    }
    assert_eq!(learner_post.end_index(), learner_pre.end_index());
    assert_eq!(learner_post.end_hlc(), learner_pre.end_hlc());

    let _ = BTreeMap::<u64, u64>::new();

    let _ = leader.raft.shutdown().await;
}

#[tokio::test]
async fn compaction_no_op_when_no_groups_can_merge() {
    let n1 = NodeId::new_v4();
    let mut nodes = zslg_raft_setup::testing::start_cluster(&[n1], raft_config())
        .await
        .expect("cluster");
    nodes
        .get_mut(&n1)
        .expect("n1")
        .raft
        .initialize(zslg_raft_fake::test_membership_map(&[n1]))
        .await
        .expect("initialize");
    wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;
    let leader = nodes.remove(&n1).expect("n1");

    let dir = tempfile::tempdir().expect("tempdir");
    let local_store = zslg_learner::testing::local_store_at(dir.path().join("local.stable"))
        .await
        .expect("from_db");
    let stable_log = zslg_learner::testing::stable_log_at(&dir.path().join("stable.db")).await;
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ss: Arc<dyn SnapshotStore> = Arc::new(RemoteSnapshotStore::new(
        SnapshotStoreConfig {
            object_store: object_store.clone(),
        },
        tokio_util::sync::CancellationToken::new(),
    ));

    let d = make_data(1, 100, b"only");
    integrate_batch(&local_store, upstream(1), vec![(LogIndex(0), d)])
        .await
        .expect("integrate");
    let inputs = SnapshotEntryInputs {
        // Large cap: these tests are not about chunking, so no truncation.
        max_snapshot_entries: std::num::NonZeroUsize::MAX,
        local_store: local_store.as_ref(),
        stable_log: stable_log.as_ref(),
        snapshot_store: ss.as_ref(),
        raft_state: &leader.raft_state,
        compression: CompressionLevel::fast(),
    };
    let cutoff_hlc = Hlc::from_parts(HlcParts {
        physical_ms: 100,
        logical: 0,
    });
    let SnapshotResult::Committed { .. } = create_snapshot_entry(&inputs, cutoff_hlc)
        .await
        .expect("create")
    else {
        panic!("expected Committed");
    };
    let pre_manifest = leader.raft_state.snapshot().manifest_state.manifest.clone();

    let (intents, _integrator_cancel) = crate::Integrator::spawn_for_test(
        Arc::new(zslg_raft_setup::RaftNode::new(
            leader.raft.clone(),
            zslg_consensus_fake::permissive_gate(),
            std::time::Duration::from_secs(30),
        )),
        Arc::new(leader.raft_state.clone()),
    );
    let tiers: [CompactionTier; 1] = [Duration::from_secs(10).into()];
    let outcome = compact(
        &ss,
        &leader.raft_state,
        &tiers,
        CompressionLevel::fast(),
        intents.as_ref(),
    )
    .await
    .expect("compact");
    assert_eq!(outcome.merges_performed, 0);
    assert_eq!(outcome.final_manifest_len, pre_manifest.len());

    assert_eq!(
        leader.raft_state.snapshot().manifest_state.manifest,
        pre_manifest
    );

    let _ = leader.raft.shutdown().await;
}
