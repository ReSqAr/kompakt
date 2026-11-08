//! Snapshot-build integration tests: they build real snapshots from a real
//! local store and stable log, which only the leader assembly can see together.

#![allow(
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

use zslg_leader_snapshot::create::{SnapshotEntryInputs, SnapshotResult, create_snapshot_entry};
use zslg_snapshot::CompressionLevel;

use crossbeam_combinators::testing::ChannelTestExt as _;

use bytes::Bytes;
use proptest::prelude::*;
use uuid::Uuid;

use zslg_log::{LocalStoreMut, StableLogSnapshot};

use crate::learner::LocalStore;
use crate::learner::StableLog;
use crate::learner::integrate_manifest_for_test;
use crate::tests::{SnapshotHarness, boot_snapshot_harness, commit_snapshot_result};
use zslg_consensus::NodeId;
use zslg_log::Data;
use zslg_log::{DataId, LogIndex};
use zslg_time::Hlc;

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
    let (ch, closer) = crossbeam_combinators::Channel::from(rx, "test integrate");
    // Close before the last sender drops, or the consumer forms its
    // verdict as "disconnected, not yet closed" — an abort.
    closer.close();
    drop(tx);
    store.integrate_loop(ch).await.map_err(Into::into)
}

#[derive(Debug, Clone)]
struct Case {
    entries: Vec<(u128, u64, u8)>,
    rounds: usize,
}

const HLC_MIN: u64 = 100;
const HLC_MAX: u64 = 160;

fn case_strategy() -> impl Strategy<Value = Case> {
    prop::collection::hash_set(any::<u128>(), 1..40usize)
        .prop_flat_map(|ids| {
            let n = ids.len();
            let ids: Vec<u128> = ids.into_iter().collect();
            (
                Just(ids),
                prop::collection::vec((HLC_MIN..=HLC_MAX, 0u8..3), n),
                1usize..=3,
            )
        })
        .prop_map(|(ids, attrs, rounds)| Case {
            entries: ids
                .into_iter()
                .zip(attrs)
                .map(|(id, (hlc, origin))| (id, hlc, origin))
                .collect(),
            rounds,
        })
}

fn cutoffs_for(rounds: usize) -> Vec<u64> {
    match rounds {
        1 => vec![HLC_MAX],
        2 => vec![130, HLC_MAX],
        _ => vec![120, 140, HLC_MAX],
    }
}

fn round_for(hlc: u64, cutoffs: &[u64]) -> usize {
    cutoffs
        .iter()
        .position(|&c| hlc <= c)
        .expect("last cutoff covers every generated hlc")
}

fn origin_node(slot: u8) -> NodeId {
    let mut b = [0u8; 16];
    b[0] = 0xAA;
    b[15] = slot;
    NodeId::from_uuid(Uuid::from_bytes(b))
}

fn entry_data(id: u128, hlc: u64, origin: u8) -> Data {
    Data {
        data_id: DataId::from_parts(Hlc(hlc), origin_node(origin), id as u64),
        blob: Bytes::copy_from_slice(&id.to_be_bytes()),
    }
}

async fn fresh_stable_log(dir: &std::path::Path, name: &str) -> StableLog {
    crate::tests::stable_log_engine(dir.join(format!("{name}.stable")))
        .await
        .expect("open stable log")
}

async fn collect_log(log: &StableLog) -> Vec<(u64, Hlc, DataId, Bytes)> {
    let entries: Vec<(LogIndex, Data)> = log
        .range(None, LogIndex(1_000))
        .items()
        .filter_map(|r| r.ok())
        .collect();
    entries
        .into_iter()
        .map(|(idx, d)| (idx.raw(), d.data_id.hlc(), d.data_id, d.blob))
        .collect()
}

fn run_case(case: &Case) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");

    rt.block_on(async {
        let h: SnapshotHarness = boot_snapshot_harness().await;
        let learner_dir = tempfile::tempdir().expect("tempdir");
        let learner_a = fresh_stable_log(learner_dir.path(), "a.db").await;
        let learner_b = fresh_stable_log(learner_dir.path(), "b.db").await;

        let cutoffs = cutoffs_for(case.rounds);
        let mut next_index = [0u64; 3];

        for (round, &cutoff) in cutoffs.iter().enumerate() {
            for &(id, hlc, origin) in &case.entries {
                if round_for(hlc, &cutoffs) != round {
                    continue;
                }
                let slot = usize::from(origin);
                let idx = next_index[slot];
                next_index[slot] = idx.checked_add(1).expect("index");
                integrate_batch(
                    &h.local_store,
                    origin_node(origin),
                    vec![(LogIndex(idx), entry_data(id, hlc, origin))],
                )
                .await
                .expect("integrate");
            }

            // With a cap, one build covers at most a chunk, so drive the
            // loop the SnapshotJob's trigger would drive: build while the
            // cutoff is still above the watermark. A case generates at most
            // 40 entries, so at cap 4 a round needs at most 10 chunks plus
            // one no-op empty-window build; 24 is a comfortable ceiling and
            // exhausting it is a bug, not a reason to stop quietly.
            const MAX_CHUNKS_PER_ROUND: usize = 24;
            let mut covered = false;
            for _ in 0..MAX_CHUNKS_PER_ROUND {
                let watermark = h.raft_state.snapshot().manifest_state.manifest_watermark;
                if watermark.is_some_and(|wm| Hlc(cutoff) <= wm) {
                    covered = true;
                    break;
                }
                let inputs = SnapshotEntryInputs {
                    local_store: h.local_store.as_ref(),
                    stable_log: h.stable_log.as_ref(),
                    snapshot_store: &*h.snapshot_store(),
                    raft_state: &h.raft_state,
                    compression: CompressionLevel::fast(),
                    // Small enough that the generated windows chunk, so the
                    // convergence property covers chunk boundaries too.
                    max_snapshot_entries: std::num::NonZeroUsize::new(4).expect("nonzero"),
                };
                let result = create_snapshot_entry(&inputs, Hlc(cutoff))
                    .await
                    .expect("create snapshot");
                commit_snapshot_result(&h.raft, &h.raft_state, &result).await;

                let manifest_state = h.raft_state.snapshot().manifest_state.clone();
                integrate_manifest_for_test(
                    h.snapshot_store(),
                    h.stable_log.as_ref(),
                    &manifest_state,
                )
                .await
                .expect("leader load");
                integrate_manifest_for_test(h.snapshot_store(), &learner_a, &manifest_state)
                    .await
                    .expect("learner A load");
            }
            assert!(
                covered,
                "round {round} (cutoff {cutoff}) did not reach the cutoff within \
                     {MAX_CHUNKS_PER_ROUND} chunks — chunking is not making progress",
            );
        }

        let manifest_state = h.raft_state.snapshot().manifest_state.clone();
        integrate_manifest_for_test(h.snapshot_store(), &learner_b, &manifest_state)
            .await
            .expect("learner B load");

        let leader_log = collect_log(&h.stable_log).await;
        let a_log = collect_log(&learner_a).await;
        let b_log = collect_log(&learner_b).await;

        assert_eq!(leader_log, a_log, "leader and learner A diverge");
        assert_eq!(leader_log, b_log, "leader and learner B diverge");

        for pair in leader_log.windows(2) {
            let (_, prev_hlc, prev_id, _) = &pair[0];
            let (_, hlc, id, _) = &pair[1];
            assert!(
                (prev_hlc, prev_id) < (hlc, id),
                "stable log not strictly (HLC, data_id) ordered: {pair:?}",
            );
        }

        let mut expected: Vec<(Hlc, DataId, Bytes)> = case
            .entries
            .iter()
            .map(|&(id, hlc, origin)| {
                (
                    Hlc(hlc),
                    DataId::from_parts(Hlc(hlc), origin_node(origin), id as u64),
                    Bytes::copy_from_slice(&id.to_be_bytes()),
                )
            })
            .collect();
        expected.sort();
        let actual: Vec<(Hlc, DataId, Bytes)> = leader_log
            .iter()
            .map(|(_, hlc, id, blob)| (*hlc, *id, blob.clone()))
            .collect();
        assert_eq!(actual, expected, "stable content != generated entries");
    });
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn proptest_stable_order_random_ids_identical_across_learners(case in case_strategy()) {
        run_case(&case);
    }
}

/// A window bigger than the cap must advance the watermark only as far as it
/// actually wrote. Advancing to the full cutoff would mark data covered that
/// no snapshot contains — silent data loss for any reader served from the
/// stable prefix.
#[tokio::test]
async fn an_over_cap_window_advances_the_watermark_only_to_what_it_wrote() {
    let h: SnapshotHarness = boot_snapshot_harness().await;
    // 10 entries spread over 10 distinct HLCs.
    for i in 0u64..10 {
        integrate_batch(
            &h.local_store,
            origin_node(0),
            vec![(LogIndex(i), entry_data(u128::from(i), 100 + i, 0))],
        )
        .await
        .expect("integrate");
    }

    let inputs = SnapshotEntryInputs {
        local_store: h.local_store.as_ref(),
        stable_log: h.stable_log.as_ref(),
        snapshot_store: &*h.snapshot_store(),
        raft_state: &h.raft_state,
        compression: CompressionLevel::fast(),
        max_snapshot_entries: std::num::NonZeroUsize::new(3).expect("nonzero"),
    };
    let result = create_snapshot_entry(&inputs, Hlc(200))
        .await
        .expect("create snapshot");

    let SnapshotResult::Committed {
        new_watermark,
        entries,
        ..
    } = result
    else {
        panic!("expected a committed chunk, got {result:?}");
    };
    assert_eq!(
        entries, 3,
        "the chunk stops at the cap (each HLC is distinct)"
    );
    assert_eq!(
        new_watermark,
        Hlc(102),
        "the watermark names the last HLC actually written, not the cutoff",
    );
}

/// The whole backlog must still come out, one chunk per iteration, with no
/// entry lost or duplicated at a chunk boundary.
#[tokio::test]
async fn chunked_builds_cover_the_window_exactly_once() {
    let h: SnapshotHarness = boot_snapshot_harness().await;
    for i in 0u64..10 {
        integrate_batch(
            &h.local_store,
            origin_node(0),
            vec![(LogIndex(i), entry_data(u128::from(i), 100 + i, 0))],
        )
        .await
        .expect("integrate");
    }

    let cap = std::num::NonZeroUsize::new(3).expect("nonzero");
    let mut total = 0usize;
    // Drive the loop the SnapshotJob's trigger would drive: keep building
    // while the cutoff is still above the watermark.
    for _ in 0..10 {
        let watermark = h.raft_state.snapshot().manifest_state.manifest_watermark;
        if watermark.is_some_and(|wm| Hlc(200) <= wm) {
            break;
        }
        let inputs = SnapshotEntryInputs {
            local_store: h.local_store.as_ref(),
            stable_log: h.stable_log.as_ref(),
            snapshot_store: &*h.snapshot_store(),
            raft_state: &h.raft_state,
            compression: CompressionLevel::fast(),
            max_snapshot_entries: cap,
        };
        let result = create_snapshot_entry(&inputs, Hlc(200))
            .await
            .expect("create snapshot");
        if let SnapshotResult::Committed { entries, .. } = &result {
            total = total.checked_add(*entries).expect("count");
        }
        commit_snapshot_result(&h.raft, &h.raft_state, &result).await;
        let manifest_state = h.raft_state.snapshot().manifest_state.clone();
        integrate_manifest_for_test(h.snapshot_store(), h.stable_log.as_ref(), &manifest_state)
            .await
            .expect("leader load");
    }

    assert_eq!(
        total, 10,
        "every entry is written exactly once across chunks"
    );
    assert_eq!(
        h.raft_state.snapshot().manifest_state.manifest_watermark,
        Some(Hlc(200)),
        "the final chunk carries the watermark to the cutoff",
    );
}
