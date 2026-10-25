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
use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use proptest::prelude::*;

use super::open_test_local_store;
use super::store::LocalStore;
use crate::api::{LocalStoreMut, LocalStoreView};
use crate::ids::{DataId, LogIndex, NodeId};
use crate::meta::Data;

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
    let (ch, closer) = crate::channel::Channels::from(rx, "test integrate");
    // Close before the last sender drops, or the consumer forms its
    // verdict as "disconnected, not yet closed" — an abort.
    closer.close();
    drop(tx);
    store.integrate_loop(ch).await
}

#[derive(Debug, Clone)]
struct Op {
    data_id_slot: u8,
    upstream_slot: u8,
    source_index: u64,
    as_meta_only: bool,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    (0u8..16, 0u8..4, 0u64..1024, any::<bool>()).prop_map(
        |(data_id_slot, upstream_slot, source_index, as_meta_only)| Op {
            data_id_slot,
            upstream_slot,
            source_index,
            as_meta_only,
        },
    )
}

use crate::tests::upstream as upstream_for;

fn data_id_for(slot: u8) -> DataId {
    // Node-independent on purpose: dedup is keyed by the data id alone, so the
    // same slot integrated from different upstreams must collide.
    DataId::test_id(crate::Hlc(0), u64::from(slot))
}

fn record_for(op: &Op) -> Data {
    let data_id = data_id_for(op.data_id_slot);
    if op.as_meta_only {
        Data {
            data_id,
            blob: Bytes::from_static(b""),
        }
    } else {
        Data {
            data_id,
            blob: Bytes::copy_from_slice(&[op.data_id_slot]),
        }
    }
}

fn run_sequence(ops: &[Op]) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("db");

    let mut in_store: BTreeSet<u8> = BTreeSet::new();
    let mut expected_cursor: BTreeMap<u8, u64> = BTreeMap::new();

    rt.block_on(async {
        let (store, worker) = open_test_local_store(&path).await.expect("open");
        for op in ops {
            integrate_batch(
                &store,
                upstream_for(op.upstream_slot),
                vec![(LogIndex(op.source_index), record_for(op))],
            )
            .await
            .expect("integrate");

            in_store.insert(op.data_id_slot);

            let entry = expected_cursor
                .entry(op.upstream_slot)
                .or_insert(op.source_index);
            *entry = (*entry).max(op.source_index);
        }
        drop(store);
        worker.stop().await;
    });

    // Reopen the same root, with no retry ladder: the previous store is
    // dropped and its worker joined above, which is what releases the DB's
    // LOCK. A sleep-and-retry here would hide a real shutdown-ordering bug.
    rt.block_on(async {
        let (store, worker) = open_test_local_store(&path).await.expect("open");

        for (slot, &expected) in &expected_cursor {
            let actual = store
                .subscription_cursor(upstream_for(*slot))
                .await
                .expect("cursor")
                .map(|i| i.raw());
            assert_eq!(
                actual,
                Some(expected),
                "cursor mismatch for upstream {slot}"
            );
        }

        use futures::TryStreamExt as _;
        let all_data_ids: BTreeSet<DataId> = store
            .stream_above_hlc(None)
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .expect("stream")
            .into_iter()
            .map(|d| d.data_id)
            .collect();
        for slot in &in_store {
            let id = data_id_for(*slot);
            assert!(
                all_data_ids.contains(&id),
                "slot {slot} expected in LOCAL_STORE but absent"
            );
        }
        drop(store);
        worker.stop().await;
    });
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn local_store_invariants_hold_across_random_integrations(
        ops in proptest::collection::vec(op_strategy(), 1..64),
    ) {
        run_sequence(&ops);
    }
}
