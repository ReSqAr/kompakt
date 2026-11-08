//! The object-store-backed [`StableArchiveReader`]: it turns a raft manifest +
//! the snapshot store into a bounded stream of stable-log entries. This is the
//! single place that knows how to select and tile the manifest snapshots that
//! cover an index/HLC window; both the snapshot loader (populating the local
//! stable log) and the reader (serving a purged prefix just-in-time) go through
//! it, seeing only [`ArchiveBounds`].
//!
//! The raft manifest tiles the entire stable history `[0, end_index]`
//! contiguously with snapshots that are never garbage-collected, so any index
//! range that was ever stable is recoverable here.

use std::sync::Arc;
use zslg_consensus::RaftStateView;
use zslg_snapshot::SnapshotStore;

use crossbeam_combinators::{Channel, Contiguous, Finite, NonDecreasing, OrderedChannel};
use zslg_log::ArchiveStream;
use zslg_log::LogIndex;
use zslg_log::SnapshotEntry;
use zslg_log::{ArchiveBounds, ArchiveUpperBound, StableArchiveReader};
use zslg_snapshot::SnapshotMetadata;
use zslg_time::Hlc;

/// Reads the archived (purged) prefix of the stable log from object-store
/// snapshots. Holds the snapshot store (the object-store read point), the raft
/// state (the source of truth for which snapshots tile the history), and the
/// runtime the snapshot opens run on.
pub struct ObjectStoreArchiveReader {
    snapshot_store: Arc<dyn SnapshotStore>,
    raft_state: Arc<dyn RaftStateView>,
    runtime: tokio::runtime::Handle,
}

impl ObjectStoreArchiveReader {
    /// Built inside a runtime: its handle is where the snapshot opens run,
    /// whichever thread later reads the stream.
    #[must_use]
    pub fn new(snapshot_store: Arc<dyn SnapshotStore>, raft_state: Arc<dyn RaftStateView>) -> Self {
        Self {
            snapshot_store,
            raft_state,
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

/// The manifest snapshots covering `bounds`, in ascending index order. The one
/// definition of "which snapshots hold this window", shared by the loader and
/// the reader.
///
/// A snapshot must hold an entry above `after` (`end_index > after`); the
/// upper bound prunes snapshots wholly at or above the cap (`start_index <
/// until` for an index bound). Because index order == HLC order in the stable
/// log, the index bound is exact.
#[must_use]
fn covering_snapshots(
    manifest: &[SnapshotMetadata],
    bounds: ArchiveBounds,
) -> Vec<SnapshotMetadata> {
    let mut snaps: Vec<SnapshotMetadata> = manifest
        .iter()
        .filter(|m| {
            bounds.after.is_none_or(|after| m.end_index > after)
                && match bounds.until {
                    ArchiveUpperBound::Unbounded => true,
                    ArchiveUpperBound::IndexExclusive(until) => m.start_index < until,
                }
        })
        .cloned()
        .collect();
    snaps.sort_by_key(|m| m.start_index);
    snaps
}

/// Whether an item is still inside the window's upper bound. `Err` items pass:
/// they end the consumer's read themselves.
fn not_above(until: ArchiveUpperBound) -> impl FnMut(&Result<SnapshotEntry, crate::Error>) -> bool {
    move |item| match (item, until) {
        (Ok(entry), ArchiveUpperBound::IndexExclusive(until)) => entry.global_index < until,
        _ => true,
    }
}

impl StableArchiveReader for ObjectStoreArchiveReader {
    /// ```text
    ///   covering snapshots ─ defer_async(read_snapshot) each ─ chain_all
    ///     ─ take_while(below until) ─ filter_above(after)
    ///     ─ Contiguous from after ─ covers(..until) ─ also NonDecreasing<Hlc>
    /// ```
    fn stream(&self, bounds: ArchiveBounds) -> ArchiveStream {
        let run =
            || -> OrderedChannel<Result<SnapshotEntry, Error>, Contiguous<LogIndex>, Finite> {
                let covering =
                    covering_snapshots(&self.raft_state.snapshot().manifest_state.manifest, bounds);
                let phases = covering.into_iter().map(|meta| {
                    let store = Arc::clone(&self.snapshot_store);
                    Channel::defer_async(&self.runtime, move || async move {
                        store
                            .read_snapshot(
                                meta.snapshot_id,
                                zslg_snapshot::ObjectStoreReason::ArchiveRead,
                            )
                            .await
                            .map(Channel::err_into)
                            .unwrap_or_else(|e| Channel::once(Err(Error::from(e))))
                    })
                });
                let ch = Channel::chain_all(phases)
                    .take_while(not_above(bounds.until))
                    .filter_above::<LogIndex>(bounds.after)
                    .ordered::<Contiguous<LogIndex>>(bounds.after, "archive tiling");
                let ch = match bounds.until {
                    ArchiveUpperBound::IndexExclusive(until) => {
                        ch.covers(..until, "archive window")
                    }
                    ArchiveUpperBound::Unbounded => ch,
                };
                ch.also::<NonDecreasing<Hlc>>(None, "archive HLC")
            };
        let result: OrderedChannel<Result<SnapshotEntry, Error>, Contiguous<LogIndex>, Finite> =
            run();
        result.err_into()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests {
    use super::*;
    use zslg_snapshot::SnapshotId;

    fn meta(start: u64, end: u64) -> SnapshotMetadata {
        SnapshotMetadata {
            start_hlc: Hlc(start),
            end_hlc: Hlc(end),
            start_index: LogIndex(start),
            end_index: LogIndex(end),
            snapshot_id: SnapshotId::from_u128(u128::from(start)),
        }
    }

    fn ids(snaps: &[SnapshotMetadata]) -> Vec<(u64, u64)> {
        snaps
            .iter()
            .map(|m| (m.start_index.raw(), m.end_index.raw()))
            .collect()
    }

    fn bounds(after: Option<LogIndex>, until: ArchiveUpperBound) -> ArchiveBounds {
        ArchiveBounds { after, until }
    }

    /// A contiguous manifest given out of order is returned sorted by start
    /// index; the lower bound prunes snapshots wholly below `from`.
    #[test]
    fn covering_selects_in_index_order_from_lower_bound() {
        // Deliberately unsorted input.
        let manifest = vec![meta(10, 14), meta(0, 4), meta(5, 9)];

        // Unbounded from earliest → all three, sorted.
        assert_eq!(
            ids(&covering_snapshots(
                &manifest,
                bounds(None, ArchiveUpperBound::Unbounded),
            )),
            vec![(0, 4), (5, 9), (10, 14)],
        );

        // After 4: the snapshot ending at 4 holds nothing above it and is
        // pruned; the one starting at 5 is kept.
        assert_eq!(
            ids(&covering_snapshots(
                &manifest,
                bounds(Some(LogIndex(4)), ArchiveUpperBound::Unbounded,),
            )),
            vec![(5, 9), (10, 14)],
        );

        // After the last index any snapshot holds → nothing.
        assert!(
            covering_snapshots(
                &manifest,
                bounds(Some(LogIndex(14)), ArchiveUpperBound::Unbounded,),
            )
            .is_empty()
        );
        // Empty manifest → nothing.
        assert!(covering_snapshots(&[], bounds(None, ArchiveUpperBound::Unbounded),).is_empty());
    }

    /// An `IndexExclusive` upper bound prunes snapshots whose first index is
    /// already at or above the cap.
    #[test]
    fn covering_index_exclusive_selects_prefix() {
        let manifest = vec![meta(0, 4), meta(5, 9), meta(10, 14)];

        // Cap 7 keeps [0,4] (end 4 < 7) and [5,9] (start 5 < 7, includes snap
        // straddling the boundary); [10,14] (start 10 >= 7) is pruned whole.
        assert_eq!(
            ids(&covering_snapshots(
                &manifest,
                bounds(None, ArchiveUpperBound::IndexExclusive(LogIndex(7)),),
            )),
            vec![(0, 4), (5, 9)],
        );

        // Cap at snapshot boundary: `until == 5` excludes [5,9] (start 5 >= 5).
        assert_eq!(
            ids(&covering_snapshots(
                &manifest,
                bounds(None, ArchiveUpperBound::IndexExclusive(LogIndex(5)),),
            )),
            vec![(0, 4)],
        );
    }
}

error_union::error_union! {
    /// What this crate can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(zslg_lifecycle_types::LifecycleError),
        Config(zslg_lifecycle_types::ConfigError),
        Clock(zslg_time::ClockError),
        Log(zslg_log::LogError),
        Snapshot(zslg_snapshot::SnapshotError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    bridge zslg_log::Error
    bridge zslg_snapshot::Error
    bridge zslg_raft::Error
    bridge zslg_quorum_read::Error
    bridge zslg_error::Error
}
