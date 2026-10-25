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

use crate::api::{RaftStateView, SnapshotStore};
use crate::channel::Channels;
use crate::error::Error;
use crate::ids::SnapshotId;
use crate::leader::integrator::{IntentOutcome, IntentSubmitter};
use crate::meta::{SnapshotEntry, SnapshotMetadata};
use crate::metrics::{self, OpState};
use crate::object_store::CompressionLevel;

/// A time window; snapshots sharing a bucket at this width may merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompactionTier(Duration);

impl CompactionTier {
    pub(in crate::leader) fn as_millis(&self) -> u64 {
        crate::hlc::duration_as_ms_clamped(self.0)
    }

    pub(in crate::leader) fn as_duration(&self) -> Duration {
        self.0
    }
}

impl From<Duration> for CompactionTier {
    fn from(d: Duration) -> Self {
        Self(d)
    }
}

impl std::ops::Add<Duration> for CompactionTier {
    type Output = Self;
    fn add(self, rhs: Duration) -> Self {
        Self(self.0.saturating_add(rhs))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::leader) struct CompactionOutcome {
    pub(in crate::leader) merges_performed: usize,
    pub(in crate::leader) final_manifest_len: usize,
}

/// Merge eligible groups, publishing each splice as it completes.
pub(in crate::leader) async fn compact<'a>(
    snapshot_store: &'a Arc<dyn SnapshotStore>,
    raft_state: &'a dyn RaftStateView,
    compaction_tiers: &'a [CompactionTier],
    compression: CompressionLevel,
    intents: &'a IntentSubmitter,
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
    intents: &'a IntentSubmitter,
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
            .submit(crate::leader::RaftIntent::CompactManifest {
                replaced,
                replacement: vec![merged],
            })
            .await?
        {
            IntentOutcome::Applied { .. } => {
                total_merges = total_merges
                    .checked_add(1)
                    .ok_or_else(|| Error::Storage("merge count overflow".into()))?;
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
    let now_ms = crate::hlc::duration_as_ms_clamped(now);

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

/// Concatenate `ids` into `tx` in manifest order, returning once the merge is
/// over for any reason. Errors are forwarded as items, so `write_snapshot` is
/// the single place that surfaces them.
async fn feed_sources(
    store: &Arc<dyn SnapshotStore>,
    ids: Vec<SnapshotId>,
    tx: &crossbeam_channel::Sender<Result<SnapshotEntry, Error>>,
    cancel: &tokio_util::sync::CancellationToken,
) {
    for id in ids {
        if cancel.is_cancelled() {
            return;
        }
        let ch = match store
            .read_snapshot(id, crate::metrics::ObjectStoreReason::Compaction)
            .await
        {
            Ok(ch) => ch,
            Err(e) => {
                let _ = tx.send(Err(e));
                return;
            }
        };
        // Awaited one source at a time, so entries never interleave and index
        // order is preserved.
        let send_tx = tx.clone();
        let fed = tokio::task::spawn_blocking(move || {
            for item in ch {
                let is_err = item.is_err();
                if send_tx.send(item).is_err() {
                    return Feed::ConsumerGone;
                }
                if is_err {
                    return Feed::ErrorForwarded;
                }
            }
            Feed::Done
        })
        .await;
        match fed {
            Ok(Feed::Done) => {}
            // The merge is over; opening the remaining sources would read them
            // from the object store only to discard the bytes.
            Ok(Feed::ConsumerGone | Feed::ErrorForwarded) => return,
            Err(join_error) => {
                let _ = tx.send(Err(Error::Storage(format!(
                    "merge_group spawn_blocking panicked: {join_error}"
                ))));
                return;
            }
        }
    }
}

/// Outcome of feeding one source snapshot into the merge.
enum Feed {
    /// Every entry forwarded; move to the next source.
    Done,
    /// `write_snapshot` is gone — it failed or was aborted.
    ConsumerGone,
    /// A read or decode error went into the channel for the writer to surface.
    ErrorForwarded,
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

    // The feeder concatenates the sources into `tx` in manifest order. Errors
    // go into `tx` too, so `write_snapshot` is the one place that surfaces them.
    let (tx, rx) =
        crossbeam_channel::bounded(crate::leader::jobs::snapshot::SNAPSHOT_WRITE_FORWARDER_CAP);
    let (entries_ch, entries_closer) = Channels::from(rx, "merge_group feeder");
    let store = snapshot_store.clone();
    let cancel = snapshot_store.shutdown_token();
    let feeder = tokio::spawn(async move {
        feed_sources(&store, ids, &tx, &cancel).await;
        // `tx` is still owned here, so it drops after the close — the ordering
        // `Closer::close` requires. Only a panic skips this, which is exactly
        // when the writer should see `End::Aborted` (CLAUDE.md §20).
        entries_closer.close();
    });

    let write_result = snapshot_store
        .write_snapshot(
            new_id,
            entries_ch,
            compression,
            crate::metrics::ObjectStoreReason::Compaction,
        )
        .await;
    // Drain the feeder after the write completes; ignore its unit result.
    let _ = feeder.await;
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
    use crate::Hlc;
    use crate::hlc::HlcParts;
    use crate::ids::{LogIndex, NodeId};

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
                let start = Hlc::from_parts(crate::hlc::HlcParts {
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

    use std::collections::BTreeMap;
    use std::time::Duration;

    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use uuid::Uuid;

    use crate::api::{LocalStoreMut, SnapshotStore, StableLogSnapshot};

    use crate::ids::DataId;
    use crate::leader::jobs::snapshot::create::{
        SnapshotEntryInputs, SnapshotResult, create_snapshot_entry,
    };
    use crate::learner::LocalStore;
    use crate::learner::StableLog;
    use crate::learner::integrate_manifest_for_test;
    use crate::meta::Data;
    use crate::object_store::CompressionLevel;
    use crate::object_store::{RemoteSnapshotStore, SnapshotStoreConfig};

    use crate::tests::commit_snapshot_result;
    use crate::tests::fast_raft_config as raft_config;
    use crate::tests::{upstream, wait_for_leader};
    use bytes::Bytes;

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

    fn make_data(seed: u32, hlc_ms: u64, blob: &[u8]) -> Data {
        Data {
            data_id: DataId::from_parts(
                Hlc::from_parts(crate::hlc::HlcParts {
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
        let mut nodes = crate::tests::start_cluster(&[n1], raft_config())
            .await
            .expect("cluster");
        nodes
            .get_mut(&n1)
            .expect("n1")
            .raft
            .initialize(crate::tests::test_membership_map(&[n1]))
            .await
            .expect("initialize");
        wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;
        let leader = nodes.remove(&n1).expect("n1");

        let dir = tempfile::tempdir().expect("tempdir");
        let local_store = LocalStore::simple_at(dir.path().join("local.stable"))
            .await
            .expect("from_db");
        let stable_log = crate::tests::stable_log_at(&dir.path().join("stable.db")).await;
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
        let learner_pre = StableLog::open_engine(learner_pre_dir.path().join("stable.stable"))
            .await
            .expect("open stable log");
        integrate_manifest_for_test(ss.clone(), &learner_pre, &pre_manifest_state)
            .await
            .expect("load pre");
        use futures::TryStreamExt as _;
        let pre_entries: Vec<(LogIndex, Data)> = learner_pre
            .range(
                LogIndex(0),
                LogIndex(u64::try_from(all_entries.len()).expect("fits")),
            )
            .into_stream()
            .try_collect()
            .await
            .expect("range pre");
        assert_eq!(pre_entries.len(), all_entries.len());

        let (intents, _integrator_cancel) = crate::leader::Integrator::spawn_for_test(
            Arc::new(crate::raft::RaftNode::new(
                leader.raft.clone(),
                crate::network::permissive_gate(),
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
                &intents,
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
            post_state.manifest_state.manifest_watermark,
            pre_state.manifest_state.manifest_watermark,
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
        let learner_post = StableLog::open_engine(learner_post_dir.path().join("stable.stable"))
            .await
            .expect("from_db");
        integrate_manifest_for_test(ss.clone(), &learner_post, &post_manifest_state)
            .await
            .expect("load post");
        let post_entries: Vec<(LogIndex, Data)> = learner_post
            .range(
                LogIndex(0),
                LogIndex(u64::try_from(all_entries.len()).expect("fits")),
            )
            .into_stream()
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
        let mut nodes = crate::tests::start_cluster(&[n1], raft_config())
            .await
            .expect("cluster");
        nodes
            .get_mut(&n1)
            .expect("n1")
            .raft
            .initialize(crate::tests::test_membership_map(&[n1]))
            .await
            .expect("initialize");
        wait_for_leader(&nodes.get(&n1).expect("n1").raft, n1).await;
        let leader = nodes.remove(&n1).expect("n1");

        let dir = tempfile::tempdir().expect("tempdir");
        let local_store = LocalStore::simple_at(dir.path().join("local.stable"))
            .await
            .expect("from_db");
        let stable_log = crate::tests::stable_log_at(&dir.path().join("stable.db")).await;
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

        let (intents, _integrator_cancel) = crate::leader::Integrator::spawn_for_test(
            Arc::new(crate::raft::RaftNode::new(
                leader.raft.clone(),
                crate::network::permissive_gate(),
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
            &intents,
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
}
