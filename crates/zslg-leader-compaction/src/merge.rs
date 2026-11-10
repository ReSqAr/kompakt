//! Compaction — roll old snapshots up into fewer, larger objects.
//!
//! Resolution decays with age: the further back an entry is, the wider the
//! window it is folded into.
//!
//! Each tier slices time into fixed buckets. The bucket holding `now` is
//! **open** — more entries can still land in it; every earlier bucket is
//! **closed**. An entry folds into its widest closed bucket, and everything
//! sharing that bucket becomes one object. Open buckets are never touched, so a
//! merge can never target a moving window.
//!
//! ```text
//!   tiers = [1h, 1d]                                    now = Tue 14:30
//!
//!   Mon 09:15  ┐  day closed
//!   Mon 09:40  ├───────────────────> Mon 00:00    fold by day
//!   Mon 22:05  ┘
//!
//!   Tue 11:00  ┐  day open, hour closed
//!   Tue 11:50  ┴───────────────────> Tue 11:00    fold by hour
//!
//!   Tue 13:20  ────────────────────> Tue 13:00    fold by hour
//!
//!   Tue 14:05     all buckets open ─> itself      nothing closed yet
//! ```
//!
//! Each group is published as a manifest splice, so a snapshot committed
//! mid-merge keeps its place. That needs every group to be a contiguous run of
//! the manifest, which holds because a bucket, once closed, stays closed — so
//! keys are monotone in age and cannot interleave.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use zslg_consensus::RaftStateView;
use zslg_log::LogError;
use zslg_snapshot::SnapshotStore;

use crate::Error;
use crate::metrics;
use crossbeam_combinators::{Cancellation, Channel};
use metrics_kit::OpState;
use zslg_consensus::{IntentOutcome, IntentSubmitter};
use zslg_snapshot::SnapshotId;
use zslg_snapshot::SnapshotMetadata;
use zslg_snapshot::{CompactionTier, CompressionLevel};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionOutcome {
    pub merges_performed: usize,
    pub final_manifest_len: usize,
}

/// Merge eligible groups, publishing each splice as it completes.
pub async fn compact<'a>(
    snapshot_store: &'a Arc<dyn SnapshotStore>,
    raft_state: &'a dyn RaftStateView,
    compaction_tiers: &'a [CompactionTier],
    compression: CompressionLevel,
    intents: &'a dyn IntentSubmitter,
) -> Result<CompactionOutcome, Error> {
    let result = compact_inner(
        snapshot_store,
        raft_state,
        compaction_tiers,
        compression,
        intents,
    )
    .await;
    metrics::compaction_run(OpState::from_ok(result.is_ok()));
    result
}

async fn compact_inner<'a>(
    snapshot_store: &'a Arc<dyn SnapshotStore>,
    raft_state: &'a dyn RaftStateView,
    compaction_tiers: &'a [CompactionTier],
    compression: CompressionLevel,
    intents: &'a dyn IntentSubmitter,
) -> Result<CompactionOutcome, Error> {
    let prev_state = raft_state.snapshot();
    let current_manifest = prev_state.manifest_state.manifest.clone();
    if current_manifest.is_empty() {
        return Ok(CompactionOutcome {
            merges_performed: 0,
            final_manifest_len: 0,
        });
    }

    let mut total_merges: usize = 0;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();

    let buckets = group_by_tiers(&current_manifest, compaction_tiers, now);

    for (_, entries) in buckets {
        if entries.len() < 2 {
            continue;
        }
        let replaced: BTreeSet<SnapshotId> = entries.iter().map(|m| m.snapshot_id).collect();
        let merged = merge_group(snapshot_store, entries, compression).await?;
        match intents
            .submit(zslg_consensus::RaftIntent::CompactManifest {
                replaced,
                replacement: vec![merged],
            })
            .await?
        {
            IntentOutcome::Applied { .. } => {
                total_merges = total_merges
                    .checked_add(1)
                    .ok_or_else(|| Error::Log(LogError::Storage("merge count overflow".into())))?;
            }
            // Another compaction already merged these; the object we just
            // wrote is left for `jobs::cleanup`.
            IntentOutcome::AlreadySatisfied | IntentOutcome::Superseded { .. } => {}
        }
    }

    Ok(CompactionOutcome {
        merges_performed: total_merges,
        final_manifest_len: raft_state.snapshot().manifest_state.manifest.len(),
    })
}

fn bucket_start(ms: u64, tier_ms: u64) -> u64 {
    if tier_ms == 0 {
        return ms;
    }
    ms.checked_div(tier_ms)
        .and_then(|q| q.checked_mul(tier_ms))
        .unwrap_or(ms)
}

/// A bucket is closed once `now` has moved out of it — no further entry can
/// land there.
fn bucket_is_closed(entry_ms: u64, now_ms: u64, tier_ms: u64) -> bool {
    bucket_start(entry_ms, tier_ms) != bucket_start(now_ms, tier_ms)
}

/// The widest tier at which this entry's bucket is closed, or `None` while
/// every one of its buckets is still open.
fn widest_closed_tier(entry_ms: u64, now_ms: u64, tiers: &[CompactionTier]) -> Option<u64> {
    tiers
        .iter()
        .filter(|t| bucket_is_closed(entry_ms, now_ms, t.as_millis()))
        .max()
        .map(|t| t.as_millis())
}

/// The bucket an entry compacts into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BucketKey {
    /// No tier of this entry has closed yet, so it keys on its own start and
    /// can group with nothing.
    Open { start_ms: u64 },
    /// The widest closed bucket the entry falls in. The tier is part of the
    /// identity: buckets of different widths beginning at the same instant are
    /// different buckets, and merging across them would splice a
    /// non-contiguous run of the manifest.
    Closed { tier_ms: u64, start_ms: u64 },
}

/// Group the manifest by bucket. Entries sharing a key merge into one object.
fn group_by_tiers(
    manifest: &[SnapshotMetadata],
    tiers: &[CompactionTier],
    now: Duration,
) -> BTreeMap<BucketKey, BTreeSet<SnapshotMetadata>> {
    let now_ms = zslg_time::duration_as_ms_clamped(now);

    let mut buckets: BTreeMap<BucketKey, BTreeSet<SnapshotMetadata>> = BTreeMap::new();
    for meta in manifest {
        let ms = meta.start_hlc.parts().physical_ms;
        let key = match widest_closed_tier(ms, now_ms, tiers) {
            Some(tier_ms) => BucketKey::Closed {
                tier_ms,
                start_ms: bucket_start(ms, tier_ms),
            },
            None => BucketKey::Open { start_ms: ms },
        };
        buckets.entry(key).or_default().insert(meta.clone());
    }
    buckets
}

#[allow(
    clippy::expect_used,
    reason = "group is non-empty (caller checks len >= 2)"
)]
async fn merge_group(
    snapshot_store: &Arc<dyn SnapshotStore>,
    group: BTreeSet<SnapshotMetadata>,
    compression: CompressionLevel,
) -> Result<SnapshotMetadata, Error> {
    let mut group: Vec<_> = group.into_iter().collect();
    group.sort_by_key(|m| m.start_hlc);

    let ids: Vec<SnapshotId> = group.iter().map(|m| m.snapshot_id).collect();

    let first = group.first().expect("non-empty group");
    let last = group.last().expect("non-empty group");
    let new_id = SnapshotId::closed(first.start_index, last.end_index);
    let merged_meta = SnapshotMetadata {
        start_hlc: first.start_hlc,
        end_hlc: last.end_hlc,
        start_index: first.start_index,
        end_index: last.end_index,
        snapshot_id: new_id,
    };

    // The sources in manifest order, each opened only when the one before it
    // is drained. An open error is that phase's first item and an aborted
    // source aborts the whole stream, so `write_snapshot` — the one place that
    // surfaces them — refuses to finalize a short merge.
    let runtime = tokio::runtime::Handle::current();
    let cancel = Cancellation::from_token(&snapshot_store.shutdown_token());
    let entries_ch = Channel::chain_all(ids.into_iter().map(|id| {
        let store = Arc::clone(snapshot_store);
        Channel::defer_async(&runtime, move || async move {
            store
                .read_snapshot(id, zslg_snapshot::ObjectStoreReason::Compaction)
                .await
                .unwrap_or_else(|e| Channel::once(Err(e)))
        })
    }))
    .until(&cancel);

    let write_result = snapshot_store
        .write_snapshot(
            new_id,
            entries_ch,
            compression,
            zslg_snapshot::ObjectStoreReason::Compaction,
        )
        .await;
    write_result?;
    Ok(merged_meta)
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
    use zslg_log::LogIndex;
    use zslg_time::Hlc;
    use zslg_time::HlcParts;

    fn meta(start_ms: u64, end_ms: u64, id: u64) -> SnapshotMetadata {
        SnapshotMetadata {
            start_hlc: Hlc::from_parts(HlcParts {
                physical_ms: start_ms,
                logical: 0,
            }),
            end_hlc: Hlc::from_parts(HlcParts {
                physical_ms: end_ms,
                logical: 0,
            }),
            start_index: LogIndex(id),
            end_index: LogIndex(id),
            snapshot_id: SnapshotId::from_u128(u128::from(id)),
        }
    }

    #[test]
    fn entries_in_a_closed_bucket_merge() {
        let tiers: [CompactionTier; 1] = [Duration::from_millis(100).into()];
        let manifest = vec![meta(10, 40, 1), meta(50, 90, 2)];
        let buckets = group_by_tiers(&manifest, &tiers, Duration::from_millis(200));
        assert_eq!(buckets.len(), 1, "closed bucket with 2 entries → 1 group");
        assert_eq!(buckets.values().next().unwrap().len(), 2);
    }

    #[test]
    fn entries_in_an_open_bucket_do_not_merge() {
        let tiers: [CompactionTier; 1] = [Duration::from_millis(100).into()];
        let manifest = vec![meta(10, 40, 1), meta(50, 90, 2)];
        let buckets = group_by_tiers(&manifest, &tiers, Duration::from_millis(80));
        assert_eq!(buckets.len(), 2, "open bucket → each entry stays separate");
        for entries in buckets.values() {
            assert_eq!(entries.len(), 1);
        }
    }

    #[test]
    fn different_buckets_never_merged() {
        let tiers: [CompactionTier; 1] = [Duration::from_millis(100).into()];
        let manifest = vec![meta(10, 80, 1), meta(120, 190, 2)];
        let buckets = group_by_tiers(&manifest, &tiers, Duration::from_millis(300));
        assert_eq!(buckets.len(), 2, "different buckets stay separate");
        for entries in buckets.values() {
            assert_eq!(entries.len(), 1);
        }
    }

    #[test]
    fn empty_manifest() {
        let tiers: [CompactionTier; 1] = [Duration::from_millis(1000).into()];
        let buckets = group_by_tiers(&[], &tiers, Duration::ZERO);
        assert!(buckets.is_empty());
    }

    #[test]
    fn single_entry_never_merged() {
        let tiers: [CompactionTier; 1] = [Duration::from_millis(100).into()];
        let manifest = vec![meta(10, 80, 1)];
        let buckets = group_by_tiers(&manifest, &tiers, Duration::from_millis(200));
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets.values().next().unwrap().len(), 1);
    }

    #[test]
    fn zero_tier_does_not_divide_by_zero() {
        let tiers: [CompactionTier; 1] = [Duration::ZERO.into()];
        let manifest = vec![meta(10, 40, 1), meta(10, 40, 2)];
        let buckets = group_by_tiers(&manifest, &tiers, Duration::from_millis(1000));
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets.values().next().unwrap().len(), 2);
    }

    #[test]
    fn multiple_closed_buckets_each_merge() {
        let tiers: [CompactionTier; 1] = [Duration::from_millis(100).into()];
        let manifest = vec![
            meta(10, 40, 1),
            meta(50, 90, 2),
            meta(110, 140, 3),
            meta(150, 190, 4),
        ];
        let buckets = group_by_tiers(&manifest, &tiers, Duration::from_millis(1000));
        assert_eq!(buckets.len(), 2);
        for entries in buckets.values() {
            assert_eq!(entries.len(), 2);
        }
    }

    use proptest::prelude::*;

    /// Build a manifest whose entries are strictly increasing in both HLC and
    /// index, which is what the real manifest guarantees.
    fn manifest_strategy() -> impl Strategy<Value = Vec<SnapshotMetadata>> {
        prop::collection::vec(1u64..5_000_000u64, 1..25usize).prop_map(|gaps| {
            let mut out = Vec::with_capacity(gaps.len());
            let mut hlc_ms = 1_000u64;
            let mut index = 0u64;
            for (seed, gap) in gaps.into_iter().enumerate() {
                hlc_ms = hlc_ms.saturating_add(gap);
                let start = Hlc::from_parts(zslg_time::HlcParts {
                    physical_ms: hlc_ms,
                    logical: 0,
                });
                out.push(SnapshotMetadata {
                    start_hlc: start,
                    end_hlc: start,
                    start_index: LogIndex(index),
                    end_index: LogIndex(index),
                    snapshot_id: SnapshotId::from_parts(
                        LogIndex(index),
                        None,
                        u64::try_from(seed).unwrap_or(0),
                    ),
                });
                index = index.saturating_add(1);
            }
            out
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// **The property the splice depends on.** `CompactManifest` replaces a
        /// group in place; if the group is not a contiguous run of the manifest,
        /// the splice leaves an index gap, `validate_replicated_state` rejects
        /// it, and a full merge upload is wasted — silently, every tick.
        ///
        /// Contiguity is not enforced anywhere; it *follows* from the bucketing
        /// scheme, which is why it is worth pinning here rather than trusting.
        #[test]
        fn every_tier_group_is_a_contiguous_run_of_the_manifest(
            manifest in manifest_strategy(),
            now_ms in 1_000u64..100_000_000u64,
        ) {
            let tiers: Vec<CompactionTier> = vec![
                Duration::from_secs(60).into(),
                Duration::from_secs(60 * 15).into(),
                Duration::from_secs(60 * 60).into(),
                Duration::from_secs(60 * 60 * 24).into(),
            ];
            let groups = group_by_tiers(&manifest, &tiers, Duration::from_millis(now_ms));

            for (key, group) in groups {
                let positions: Vec<usize> = group
                    .iter()
                    .map(|m| {
                        manifest
                            .iter()
                            .position(|c| c.snapshot_id == m.snapshot_id)
                            .expect("group members come from the manifest")
                    })
                    .collect();
                let mut sorted = positions.clone();
                sorted.sort_unstable();
                for pair in sorted.windows(2) {
                    prop_assert_eq!(
                        pair[1],
                        pair[0].saturating_add(1),
                        "bucket {:?} is not a contiguous manifest run: positions {:?}",
                        key,
                        sorted,
                    );
                }
            }
        }
    }
}
