//! End-to-end replicated-state tests over the in-process cluster: the replica
//! set and the snapshot manifest/watermark propagate to every node's
//! `RaftStateHandle` after a fenced write commits.
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
use std::collections::BTreeSet;
use std::time::Duration;

use crate::api::ReplicatedRaftState;
use crate::ids::SnapshotId;
use crate::meta::{ReplicaMembership, SnapshotMetadata};
use crate::tests::fast_raft_config as raft_config;
use crate::{Hlc, LogIndex, NodeId};

// ── tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn three_node_initialize_and_apply_command() {
    let n1 = NodeId::new_v4();
    let n2 = NodeId::new_v4();
    let n3 = NodeId::new_v4();
    let ids = [n1, n2, n3];

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");

    let leader = nodes.get_mut(&n1).expect("n1 present");
    leader
        .raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("initialize");

    crate::tests::wait_for_leader(&leader.raft, n1).await;

    let mut rx_n1 = nodes.get(&n1).expect("n1").raft_state.subscribe();
    let mut rx_n2 = nodes.get(&n2).expect("n2").raft_state.subscribe();
    let mut rx_n3 = nodes.get(&n3).expect("n3").raft_state.subscribe();

    let target_set = BTreeSet::from([n1, n2, n3]);
    {
        let n1_node = nodes.get(&n1).expect("n1");
        crate::tests::write_state_fenced(
            &n1_node.raft,
            &n1_node.raft_state,
            ReplicatedRaftState {
                replica: ReplicaMembership::single(target_set.clone()),
                manifest: Vec::new(),
                manifest_watermark: None,
                hlc_cutoff: None,
            },
        )
        .await
        .expect("client_write");
    }

    async fn wait_for_set(
        rx: &mut tokio::sync::watch::Receiver<std::sync::Arc<crate::meta::RaftState>>,
        target: &BTreeSet<NodeId>,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if *rx.borrow_and_update().replica.current() == *target {
                return;
            }
            let _ = tokio::time::timeout(Duration::from_millis(200), rx.changed()).await;
            if tokio::time::Instant::now() > deadline {
                panic!("apply state not delivered");
            }
        }
    }
    wait_for_set(&mut rx_n1, &target_set).await;
    wait_for_set(&mut rx_n2, &target_set).await;
    wait_for_set(&mut rx_n3, &target_set).await;

    for id in &ids {
        let snap = nodes.get(id).expect("node").raft_state.snapshot();
        assert_eq!(snap.replica.current().clone(), target_set);
        assert!(snap.generation.raw() >= 1);
    }
    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}

#[tokio::test]
async fn manifest_command_advances_published_watermark() {
    let n1 = NodeId::new_v4();
    let n2 = NodeId::new_v4();
    let n3 = NodeId::new_v4();
    let ids = [n1, n2, n3];

    let mut nodes = crate::tests::start_cluster(&ids, raft_config())
        .await
        .expect("cluster");

    nodes
        .get_mut(&n1)
        .expect("n1")
        .raft
        .initialize(crate::tests::test_membership_map(&ids))
        .await
        .expect("initialize");

    crate::tests::wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;

    let all_nodes = BTreeSet::from([n1, n2, n3]);
    {
        let n1_node = nodes.get(&n1).expect("n1");
        crate::tests::write_state_fenced(
            &n1_node.raft,
            &n1_node.raft_state,
            ReplicatedRaftState {
                replica: ReplicaMembership::single(all_nodes),
                manifest: vec![SnapshotMetadata {
                    start_hlc: Hlc(0),
                    end_hlc: Hlc(7777),
                    start_index: LogIndex(0),
                    end_index: LogIndex(5),
                    snapshot_id: SnapshotId::from_u128(42),
                }],
                manifest_watermark: Some(Hlc(7777)),
                hlc_cutoff: Some(Hlc(7777)),
            },
        )
        .await
        .expect("client_write");
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    'wait: loop {
        for id in &ids {
            let snap = nodes.get(id).expect("node").raft_state.snapshot();
            if snap.manifest_state.manifest_watermark != Some(Hlc(7777)) {
                if tokio::time::Instant::now() > deadline {
                    panic!("watermark did not propagate");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue 'wait;
            }
        }
        break;
    }

    for id in &ids {
        let snap = nodes.get(id).expect("node").raft_state.snapshot();
        assert_eq!(snap.manifest_state.manifest.len(), 1);
        assert_eq!(
            snap.manifest_state.manifest[0].snapshot_id,
            SnapshotId::from_u128(42)
        );
        assert_eq!(snap.manifest_state.manifest_watermark, Some(Hlc(7777)));
    }
    for sys in nodes.values_mut() {
        let _ = sys.raft.shutdown().await;
    }
}
