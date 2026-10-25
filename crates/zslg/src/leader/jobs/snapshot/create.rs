//! Building one snapshot object from an open window.
//!
//! ```text
//!   await_quorum_ready(cutoff)   replicas have drained past it
//!   scan (watermark, cutoff]     DataId order == HLC order, so no sort buffer
//!   write object                 streamed, off-reactor encode
//!   CommitSnapshot intent        manifest entry + new watermark
//! ```
//!
//! `max_snapshot_entries` caps a build, and the cap is a **floor**: the scan
//! runs to the end of the HLC it lands in, because the next chunk resumes at an
//! exclusive bound and would otherwise skip the remainder of that HLC. The
//! watermark then advances only to what was written, leaving the window open
//! for the next chunk.
//!
//! Ordering is crash-safety-critical: the cutoff appears in both the opening
//! proposal and the committing one, so a crash between them is safe.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, warn};

use crate::api::{
    LocalStoreView, RaftStateView, Router, SnapshotStore, StableLogSnapshot, WindowItem,
};
use crate::channel::Channels;
use crate::error::Error;
use crate::error::ToStorage;
use crate::hlc::Hlc;
use crate::ids::{LogIndex, SnapshotId};
use crate::learner::ReplicaPoller;
use crate::meta::{SnapshotEntry, SnapshotMetadata};
use crate::metrics::{self, SnapshotState, SnapshotStep};
use crate::object_store::CompressionLevel;
use crate::quorum::{FanOutResponse, fan_out_quorum};

pub(in crate::leader) struct SnapshotEntryInputs<'a> {
    pub(in crate::leader) local_store: &'a dyn LocalStoreView,
    pub(in crate::leader) stable_log: &'a dyn StableLogSnapshot,
    pub(in crate::leader) snapshot_store: &'a dyn SnapshotStore,
    pub(in crate::leader) raft_state: &'a dyn RaftStateView,
    pub(in crate::leader) compression: CompressionLevel,
    /// Soft cap on entries in this chunk — see [`crate::leader::jobs::snapshot::SnapshotConfig::max_snapshot_entries`].
    pub(in crate::leader) max_snapshot_entries: std::num::NonZeroUsize,
}

/// A described change, not a new whole state: the entry plus the manifest it
/// was computed against. The integrator re-checks that premise, so a build that
/// raced is rejected rather than rebased onto indices it no longer continues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnapshotResult {
    Committed {
        /// Absent when the window held no entries: the watermark advances and
        /// no object is written.
        object: Option<WrittenObject>,
        entries: usize,
        new_watermark: Hlc,
        /// Premise: the committed watermark this window started above.
        prev_watermark: Option<Hlc>,
        /// Premise: the manifest end index this build's global indices continue
        /// from.
        prev_end_index: Option<LogIndex>,
    },
    Skipped,
}

/// What a build actually wrote. The three facts arrive together or not at all,
/// so an entry without its object id is unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WrittenObject {
    pub(crate) snapshot_id: SnapshotId,
    pub(crate) entry: SnapshotMetadata,
    pub(crate) end_index: LogIndex,
}

impl SnapshotResult {
    /// The intent that publishes this build, if there is anything to publish.
    #[must_use]
    pub(in crate::leader) fn commit_intent(&self) -> Option<crate::leader::RaftIntent> {
        match self {
            Self::Skipped => None,
            Self::Committed {
                object,
                new_watermark,
                prev_watermark,
                prev_end_index,
                ..
            } => Some(crate::leader::RaftIntent::CommitSnapshot {
                entry: object.as_ref().map(|o| o.entry.clone()),
                new_watermark: *new_watermark,
                prev_watermark: *prev_watermark,
                prev_end_index: *prev_end_index,
            }),
        }
    }
}

#[allow(clippy::expect_used, reason = "channel just created with capacity 1")]
pub(in crate::leader) async fn create_snapshot_entry(
    inputs: &SnapshotEntryInputs<'_>,
    new_cutoff: Hlc,
) -> Result<SnapshotResult, Error> {
    let prev_state = inputs.raft_state.snapshot();
    let lower_bound = prev_state.manifest_state.manifest_watermark;

    if lower_bound.is_some_and(|mw| new_cutoff <= mw) {
        return Ok(SnapshotResult::Skipped);
    }

    let last_manifest_end = prev_state
        .manifest_state
        .manifest
        .last()
        .map(|m| m.end_index);
    let stable_end_index = inputs.stable_log.end_index();

    let next_index_start = match stable_end_index {
        None => 0u64,
        Some(f) => f
            .raw()
            .checked_add(1)
            .ok_or_else(|| Error::Storage("global index overflow".into()))?,
    };

    let snapshot_id = SnapshotId::open(LogIndex(next_index_start));
    let terminal: Arc<std::sync::Mutex<Option<WindowItem>>> = Arc::new(std::sync::Mutex::new(None));
    let end_hlc_raw = Arc::new(AtomicU64::new(0));
    let end_hlc_tracker = end_hlc_raw.clone();

    // Channels built here so `local_store` need not be 'static.
    let window = inputs.local_store.stream_window_ordered(
        lower_bound,
        new_cutoff,
        Some(inputs.max_snapshot_entries),
    );

    type PeekOk = Option<(Channels<Result<SnapshotEntry, Error>>, Hlc)>;

    let (entries, start_hlc) = {
        let end_hlc_tracker = end_hlc_tracker.clone();
        let terminal = terminal.clone();
        let peek_result: PeekOk = tokio::task::spawn_blocking(move || -> Result<PeekOk, Error> {
            let mut data = window;
            let first = match data.recv() {
                Ok(Ok(WindowItem::Entry(d))) => d,
                // A terminal as the first item is an empty window.
                Ok(Ok(WindowItem::Complete | WindowItem::TruncatedAt(_))) => return Ok(None),
                Ok(Err(e)) => return Err(e),
                // No terminal at all: the scan did not run to a conclusion, so
                // the window's extent is unknown.
                Err(_) => {
                    return Err(Error::StreamTruncated {
                        what: "local store window scan".to_owned(),
                    });
                }
            };

            if last_manifest_end.is_some_and(|last| stable_end_index.is_none_or(|f| f < last)) {
                return Err(Error::StableLogBehindManifest {
                    end_index: stable_end_index,
                    manifest_end_index: last_manifest_end.unwrap_or(LogIndex(0)),
                });
            }

            let start_hlc = first.data_id.hlc();
            end_hlc_tracker.store(start_hlc.raw(), Ordering::Relaxed);

            let head = Channels::from_vec(vec![Ok(SnapshotEntry {
                global_index: LogIndex(next_index_start),
                data: first,
            })]);

            let index = Arc::new(AtomicU64::new(next_index_start.saturating_add(1)));
            let seen_terminal = terminal.clone();
            let tail = data.filter_map(move |r| match r {
                Ok(WindowItem::Entry(d)) => {
                    end_hlc_tracker.store(d.data_id.hlc().raw(), Ordering::Relaxed);
                    let idx = LogIndex(index.fetch_add(1, Ordering::Relaxed));
                    Some(Ok(SnapshotEntry {
                        global_index: idx,
                        data: d,
                    }))
                }
                // The terminal is the last item, so recording it here is seen by
                // the time the drain below returns.
                Ok(item) => {
                    *seen_terminal.lock().expect("terminal lock") = Some(item);
                    None
                }
                Err(e) => Some(Err(e)),
            });

            Ok(Some((head.chain(tail), start_hlc)))
        })
        .await
        .to_storage("snapshot stream scan panicked")??;
        match peek_result {
            Some(x) => x,
            None => {
                return Ok(SnapshotResult::Committed {
                    object: None,
                    entries: 0,
                    new_watermark: new_cutoff,
                    prev_watermark: lower_bound,
                    prev_end_index: last_manifest_end,
                });
            }
        }
    };

    let entry_count = inputs
        .snapshot_store
        .write_snapshot(
            snapshot_id,
            entries,
            inputs.compression,
            crate::metrics::ObjectStoreReason::SnapshotCreate,
        )
        .await?;
    let end_hlc = Hlc(end_hlc_raw.load(Ordering::Relaxed));

    let new_watermark = match terminal.lock().expect("terminal lock").take() {
        Some(WindowItem::Complete) => new_cutoff,
        // A capped chunk covers only `(lower_bound, hlc]`; the watermark must
        // name what was written, or the uncovered tail counts as snapshotted.
        Some(WindowItem::TruncatedAt(hlc)) => hlc,
        Some(WindowItem::Entry(_)) | None => {
            return Err(Error::StreamTruncated {
                what: "local store window scan ended without a terminal".to_owned(),
            });
        }
    };

    let count_u64 = u64::try_from(entry_count)
        .map_err(|_| Error::Storage("snapshot entry count overflow".into()))?;
    // The peek guarantees count > 0; saturating_sub keeps zero well-defined.
    let end_offset = count_u64.saturating_sub(1);
    let end_index = LogIndex(
        next_index_start
            .checked_add(end_offset)
            .ok_or_else(|| Error::Storage("global index overflow".into()))?,
    );

    let start_index = LogIndex(next_index_start);

    Ok(SnapshotResult::Committed {
        object: Some(WrittenObject {
            snapshot_id,
            end_index,
            entry: SnapshotMetadata {
                start_hlc,
                end_hlc,
                start_index,
                end_index,
                snapshot_id,
            },
        }),
        entries: entry_count,
        new_watermark,
        prev_watermark: lower_bound,
        prev_end_index: last_manifest_end,
    })
}

/// Everything [`build_snapshot`] needs. No replicator and no clock: it proposes
/// nothing and works from a cutoff it is handed.
pub(in crate::leader) struct SnapshotInputs<'a> {
    pub(in crate::leader) local_store: Arc<dyn LocalStoreView>,
    pub(in crate::leader) stable_log: &'a dyn StableLogSnapshot,
    pub(in crate::leader) snapshot_store: &'a dyn SnapshotStore,
    pub(in crate::leader) raft_state: &'a dyn RaftStateView,
    pub(in crate::leader) router: Arc<dyn Router>,
    /// For the re-check after the quorum wait — see [`build_snapshot`].
    pub(in crate::leader) leadership: Arc<dyn crate::api::RaftLeadership>,
    pub(in crate::leader) self_node_id: crate::ids::NodeId,
    pub(in crate::leader) quorum_poll_interval: Duration,
    pub(in crate::leader) quorum_wait_timeout: Duration,
    pub(in crate::leader) compression: CompressionLevel,
    /// Soft cap on entries in one chunk — see [`crate::leader::jobs::snapshot::SnapshotConfig::max_snapshot_entries`].
    pub(in crate::leader) max_snapshot_entries: std::num::NonZeroUsize,
}

/// Build the snapshot for an already-committed cutoff, proposing nothing.
///
/// Keeping the proposals out (step 1 in `jobs::cutoff`, step 5 in the
/// integrator) is what lets the long waits here run without blocking other
/// leader work — including the membership drain that depends on it (§18).
pub(in crate::leader) async fn build_snapshot(
    inputs: &SnapshotInputs<'_>,
    cutoff: Hlc,
) -> Result<SnapshotResult, Error> {
    let t_start = tokio::time::Instant::now();

    if inputs
        .raft_state
        .snapshot()
        .manifest_state
        .manifest_watermark
        .is_some_and(|wm| cutoff <= wm)
    {
        metrics::snapshot_creation(SnapshotState::NoopEmptyWindow);
        return Ok(SnapshotResult::Skipped);
    }

    await_quorum_ready(inputs, cutoff).await?;
    let t_quorum = t_start.elapsed();

    // Not a safety check — the integrator and apply fence would refuse the
    // commit anyway. This just avoids an upload nobody will reference.
    if !crate::leader::jobs::is_local_leader(inputs.leadership.as_ref(), inputs.self_node_id) {
        debug!(
            ?cutoff,
            "demoted during the quorum wait; abandoning the build"
        );
        return Ok(SnapshotResult::Skipped);
    }

    let entry_inputs = SnapshotEntryInputs {
        local_store: inputs.local_store.as_ref(),
        stable_log: inputs.stable_log,
        snapshot_store: inputs.snapshot_store,
        raft_state: inputs.raft_state,
        compression: inputs.compression,
        max_snapshot_entries: inputs.max_snapshot_entries,
    };
    let result = create_snapshot_entry(&entry_inputs, cutoff).await?;
    let t_total = t_start.elapsed();
    let t_entry_step = t_total.saturating_sub(t_quorum);

    tracing::debug!(
        quorum_ms = t_quorum.as_millis(),
        entry_ms = t_entry_step.as_millis(),
        total_ms = t_total.as_millis(),
        "snapshot build timing",
    );

    metrics::snapshot_creation_step(SnapshotStep::AwaitQuorum, t_quorum);
    metrics::snapshot_creation_step(SnapshotStep::WriteObject, t_entry_step);
    metrics::snapshot_creation_step(SnapshotStep::Total, t_total);

    match &result {
        SnapshotResult::Committed { entries, .. } => {
            metrics::snapshot_creation(SnapshotState::Success);
            metrics::snapshot_entries(*entries);
        }
        SnapshotResult::Skipped => {
            metrics::snapshot_creation(SnapshotState::NoopEmptyWindow);
        }
    }

    Ok(result)
}

/// Poll one replica until it reports having drained `cutoff`.
///
/// Unbounded on purpose — `await_quorum_ready` owns the deadline, and a quorum
/// only needs *some* replicas to answer, so a peer that never does must not cap
/// how long the others get.
async fn await_cutoff_drained(
    transport: &Arc<dyn crate::api::ReplicaClient>,
    peer: crate::ids::NodeId,
    cutoff: Hlc,
    poll: Duration,
) {
    let mut delay = poll;
    loop {
        match transport.fetch_status().await.map(|s| s.effective_cutoff) {
            Ok(Some(c)) if c >= cutoff => return,
            Ok(_) => debug!(?peer, ?cutoff, "cutoff not yet drained"),
            Err(e) => debug!(?peer, error = %e, "fetch_status probe failed"),
        }
        tokio::time::sleep(delay).await;
        delay = delay.saturating_mul(2).min(poll.saturating_mul(16));
    }
}

async fn await_quorum_ready(inputs: &SnapshotInputs<'_>, new_cutoff: Hlc) -> Result<(), Error> {
    let current_state = (*inputs.raft_state.snapshot()).clone();
    let router = inputs.router.clone();
    let local_store = inputs.local_store.clone();
    let cutoff = new_cutoff;
    let poll = inputs.quorum_poll_interval;

    let quorum_result = tokio::time::timeout(
        inputs.quorum_wait_timeout,
        fan_out_quorum(current_state, move |peer| {
            let transport = router.remote(peer);
            let local_store = local_store.clone();
            async move {
                await_cutoff_drained(&transport, peer, cutoff, poll).await;
                match ReplicaPoller::check(transport, local_store, peer).await {
                    Ok(Some(state)) => Ok(FanOutResponse::Ready(state)),
                    Ok(None) => Ok(FanOutResponse::Failed),
                    Err(e) => Err(e),
                }
            }
        }),
    )
    .await;

    match quorum_result {
        Ok(Ok((_state, _successful))) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_elapsed) => {
            warn!(
                timeout = ?inputs.quorum_wait_timeout,
                "replica quorum not reached within timeout",
            );
            Err(Error::QuorumUnreachable)
        }
    }
}

#[cfg(test)]
#[allow(
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
mod tests {
    use super::*;

    use bytes::Bytes;
    use proptest::prelude::*;
    use uuid::Uuid;

    use crate::api::{LocalStoreMut, StableLogSnapshot};

    use crate::hlc::Hlc;
    use crate::ids::{DataId, LogIndex, NodeId};
    use crate::learner::LocalStore;
    use crate::learner::StableLog;
    use crate::learner::integrate_manifest_for_test;
    use crate::meta::Data;
    use crate::tests::{SnapshotHarness, boot_snapshot_harness, commit_snapshot_result};

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
        StableLog::open_engine(dir.join(format!("{name}.stable")))
            .await
            .expect("open stable log")
    }

    async fn collect_log(log: &StableLog) -> Vec<(u64, Hlc, DataId, Bytes)> {
        let entries: Vec<(LogIndex, Data)> = log
            .range(LogIndex(0), LogIndex(1_000))
            .into_iter()
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
}
