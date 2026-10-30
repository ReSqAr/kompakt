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

use crate::api::{
    ArchiveBounds, ArchiveLowerBound, ArchiveUpperBound, RaftStateView, SnapshotStore,
    StableArchiveReader,
};
use crate::error::Error;
use crate::hlc::Hlc;
use crate::ids::LogIndex;
use crate::meta::SnapshotMetadata;
use crossbeam_combinators::Channels;

/// Depth of the channel handed back by [`stream`](ObjectStoreArchiveReader::stream).
/// Matches the snapshot-store read channel: the consumer drains on its own
/// blocking thread, and a full channel back-pressures the forwarding task.
const ARCHIVE_STREAM_CHANNEL_CAP: usize = 256;

/// Reads the archived (purged) prefix of the stable log from object-store
/// snapshots. Holds the snapshot store (the object-store read point) and the
/// raft state (the source of truth for which snapshots tile the history).
pub(in crate::learner) struct ObjectStoreArchiveReader {
    snapshot_store: Arc<dyn SnapshotStore>,
    raft_state: Arc<dyn RaftStateView>,
}

impl ObjectStoreArchiveReader {
    #[must_use]
    pub(in crate::learner) fn new(
        snapshot_store: Arc<dyn SnapshotStore>,
        raft_state: Arc<dyn RaftStateView>,
    ) -> Self {
        Self {
            snapshot_store,
            raft_state,
        }
    }
}

/// The lower bound resolved to a concrete inclusive index.
fn lower_index(from: ArchiveLowerBound) -> LogIndex {
    match from {
        ArchiveLowerBound::Earliest => LogIndex::ZERO,
        ArchiveLowerBound::FromIndex(idx) => idx,
    }
}

/// The manifest snapshots covering `bounds`, in ascending index order. The one
/// definition of "which snapshots hold this window", shared by the loader and
/// the reader.
///
/// A snapshot must hold an entry at or above the lower bound (`end_index >=
/// from`); the upper bound prunes snapshots wholly at or above the cap
/// (`start_index < until` for an index bound). Because index order == HLC order
/// in the stable log, the index bound is exact.
#[must_use]
fn covering_snapshots(
    manifest: &[SnapshotMetadata],
    bounds: ArchiveBounds,
) -> Vec<SnapshotMetadata> {
    let from = lower_index(bounds.from);
    let mut snaps: Vec<SnapshotMetadata> = manifest
        .iter()
        .filter(|m| {
            m.end_index >= from
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

/// Where an entry sits relative to the requested window.
enum Placement {
    /// Below the lower bound — already served elsewhere; skip it.
    Below,
    /// Inside the window — forward it.
    Within,
    /// At or above the upper bound — the window is done (entries are ordered).
    Above,
}

fn place(index: LogIndex, bounds: ArchiveBounds) -> Placement {
    if index < lower_index(bounds.from) {
        return Placement::Below;
    }
    match bounds.until {
        ArchiveUpperBound::Unbounded => Placement::Within,
        ArchiveUpperBound::IndexExclusive(until) => {
            if index < until {
                Placement::Within
            } else {
                Placement::Above
            }
        }
    }
}

#[async_trait::async_trait]
impl StableArchiveReader for ObjectStoreArchiveReader {
    async fn stream(
        &self,
        bounds: ArchiveBounds,
    ) -> Result<Channels<Result<crate::meta::SnapshotEntry, Error>>, Error> {
        let covering =
            covering_snapshots(&self.raft_state.snapshot().manifest_state.manifest, bounds);
        let from_idx = lower_index(bounds.from);
        let snapshot_store = self.snapshot_store.clone();
        let (tx, rx) = crossbeam_channel::bounded(ARCHIVE_STREAM_CHANNEL_CAP);
        let (ch_out, closer) = Channels::from(rx, "stable_archive::stream");

        // Every exit is a `break`, so the single `closer.close()` below is
        // unconditional and `tx` — still owned by this scope — drops after it.
        // A panic is the only way past the close, which is exactly when the
        // consumer should see `End::Aborted` rather than a clean end.
        tokio::spawn(async move {
            let mut expected: Option<LogIndex> = None;
            let mut last_hlc: Option<Hlc> = None;
            // A failed open has already sent a terminal error; without this the
            // tiling check below would send a second one behind it.
            let mut open_failed = false;
            for meta in covering {
                let mut ch = match snapshot_store
                    .read_snapshot(
                        meta.snapshot_id,
                        crate::metrics::ObjectStoreReason::ArchiveRead,
                    )
                    .await
                {
                    Ok(ch) => ch,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        open_failed = true;
                        break;
                    }
                };
                let inner_tx = tx.clone();
                let spawn_res = tokio::task::spawn_blocking(move || {
                    let mut exp = expected;
                    let mut hlc = last_hlc;
                    let mut above = false;
                    loop {
                        match ch.recv() {
                            Ok(Ok(entry)) => {
                                match place(entry.global_index, bounds) {
                                    Placement::Above => {
                                        above = true;
                                        break;
                                    }
                                    Placement::Below => continue,
                                    Placement::Within => {
                                        let idx = entry.global_index;
                                        let want = exp.unwrap_or(from_idx);
                                        if idx != want {
                                            let _ = inner_tx.send(Err(Error::StableLogGap(
                                                format!(
                                                    "archive stream: expected index {} but got {}",
                                                    want.raw(),
                                                    idx.raw(),
                                                ),
                                            )));
                                            return (exp, hlc, true);
                                        }
                                        let entry_hlc = entry.data.data_id.hlc();
                                        assert!(
                                            hlc.is_none_or(|prev| entry_hlc >= prev),
                                            "archive stream: HLC regressed to {entry_hlc:?} at index {idx:?} after {hlc:?}"
                                        );
                                        exp = Some(LogIndex::next_after(Some(idx)));
                                        hlc = Some(entry_hlc);
                                        if inner_tx.send(Ok(entry)).is_err() {
                                            return (exp, hlc, true);
                                        }
                                    }
                                }
                            }
                            Ok(Err(e)) => {
                                let _ = inner_tx.send(Err(e));
                                return (exp, hlc, true);
                            }
                            Err(_disconnected) => break,
                        }
                    }
                    (exp, hlc, above)
                })
                .await;
                let (next_expected, next_hlc, above_seen) = match spawn_res {
                    Ok(r) => r,
                    Err(join_error) => {
                        let _ = tx.send(Err(Error::Storage(format!(
                            "archive stream reader panicked: {join_error}"
                        ))));
                        (expected, last_hlc, true)
                    }
                };
                expected = next_expected;
                last_hlc = next_hlc;
                if above_seen {
                    break;
                }
            }
            if !open_failed
                && let ArchiveUpperBound::IndexExclusive(until) = bounds.until
                && expected != Some(until)
            {
                let _ = tx.send(Err(Error::StableLogGap(format!(
                    "archive stream: window below index {} not fully tiled (reached {:?})",
                    until.raw(),
                    expected.map(LogIndex::raw),
                ))));
            }
            closer.close();
        });

        Ok(ch_out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::hlc::Hlc;
    use crate::ids::SnapshotId;

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

    fn bounds(from: ArchiveLowerBound, until: ArchiveUpperBound) -> ArchiveBounds {
        ArchiveBounds { from, until }
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
                bounds(ArchiveLowerBound::Earliest, ArchiveUpperBound::Unbounded),
            )),
            vec![(0, 4), (5, 9), (10, 14)],
        );

        // `from` at a snapshot's end_index still includes it (inclusive end);
        // the earlier snapshot (end 4 < 5) is pruned.
        assert_eq!(
            ids(&covering_snapshots(
                &manifest,
                bounds(
                    ArchiveLowerBound::FromIndex(LogIndex(5)),
                    ArchiveUpperBound::Unbounded,
                ),
            )),
            vec![(5, 9), (10, 14)],
        );

        // A `from` beyond every snapshot → nothing.
        assert!(
            covering_snapshots(
                &manifest,
                bounds(
                    ArchiveLowerBound::FromIndex(LogIndex(15)),
                    ArchiveUpperBound::Unbounded,
                ),
            )
            .is_empty()
        );
        // Empty manifest → nothing.
        assert!(
            covering_snapshots(
                &[],
                bounds(ArchiveLowerBound::Earliest, ArchiveUpperBound::Unbounded),
            )
            .is_empty()
        );
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
                bounds(
                    ArchiveLowerBound::Earliest,
                    ArchiveUpperBound::IndexExclusive(LogIndex(7)),
                ),
            )),
            vec![(0, 4), (5, 9)],
        );

        // Cap at snapshot boundary: `until == 5` excludes [5,9] (start 5 >= 5).
        assert_eq!(
            ids(&covering_snapshots(
                &manifest,
                bounds(
                    ArchiveLowerBound::Earliest,
                    ArchiveUpperBound::IndexExclusive(LogIndex(5)),
                ),
            )),
            vec![(0, 4)],
        );
    }
}
