//! Retiring the replica log's covered prefix.
//!
//! ```text
//!   on raft state change, at most every PURGE_MIN_INTERVAL:
//!       threshold = manifest_watermark - grace
//!       hand it to the log, which range-deletes the covered prefix
//! ```
//!
//! **The watermark, never the `hlc_cutoff`.** The cutoff is advanced by a
//! pure-Raft proposal *before* any snapshot object exists, so during a
//! snapshot-creation stall it runs ahead of what is actually recoverable;
//! purging there would drop committed data no snapshot covers. The watermark
//! advances only once the snapshot object is durable, which makes everything at
//! or below it recoverable by construction. Since `manifest_watermark <=
//! hlc_cutoff` always holds, this is the conservative choice of the two (design
//! doc §140).
//!
//! The threshold is a *request*: the store clamps it strictly below its frontier
//! entry (CLAUDE.md §8), so the newest entry always survives and the durable
//! record of `next_index` stays recoverable at restart. Deletion is a durable
//! `DeleteRange` over the covered key-order prefix, applied synchronously by
//! `ReplicaLogStore::set_purge_threshold` — nothing survives a reopen, and there
//! is no compaction to wait on.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::api::RaftStateView;
use crate::error::Error;
use crate::hlc::{Hlc, hlc_subtract};
use crate::replica::ReplicaLogStore;

/// Don't hand over a new purge threshold more than once per interval even if
/// the watermark advances faster. The handoff walks the newly covered prefix
/// and commits a range delete, so it is cheap but not free.
const PURGE_MIN_INTERVAL: Duration = Duration::from_secs(30);

pub(in crate::replica) async fn run_purge_worker(
    raft_state: Arc<dyn RaftStateView>,
    replica_log: Arc<ReplicaLogStore>,
    grace: Duration,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let mut rx = raft_state.subscribe();
    let mut last: Option<Hlc> = None;
    let mut last_purge = Instant::now();

    // Once at startup, before any raft event arrives.
    {
        let watermark = raft_state.snapshot().manifest_state.manifest_watermark;
        last = purge_once_blocking(&replica_log, watermark, grace, last).await;
    }

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            changed = rx.changed() => if changed.is_err() {
                return Ok(());
            },
        }
        if last_purge.elapsed() >= PURGE_MIN_INTERVAL {
            let watermark = raft_state.snapshot().manifest_state.manifest_watermark;
            let new_last = purge_once_blocking(&replica_log, watermark, grace, last).await;
            if new_last != last {
                last_purge = Instant::now();
            }
            last = new_last;
        }
    }
}

/// [`purge_once`] run on the blocking pool: `set_purge_threshold` does a scan
/// plus a `Storage::write` plus a blocking `Storage::fence` (CLAUDE.md §19),
/// and this worker's loop is an async task — calling it inline would block a
/// runtime worker thread on disk I/O.
async fn purge_once_blocking(
    replica_log: &Arc<ReplicaLogStore>,
    watermark: Option<Hlc>,
    grace: Duration,
    last: Option<Hlc>,
) -> Option<Hlc> {
    let replica_log = Arc::clone(replica_log);
    match tokio::task::spawn_blocking(move || purge_once(&replica_log, watermark, grace, last))
        .await
    {
        Ok(new_last) => new_last,
        // The blocking task panicked — leave `last` unchanged so the next
        // tick retries rather than silently losing the threshold.
        Err(e) => {
            warn!(error = %e, "replica purge task panicked; retrying on the next tick");
            last
        }
    }
}

/// `None` when nothing is purgeable yet — no watermark at all, or the only
/// snapshot is younger than the grace window.
///
/// A bare `Hlc`: the log's retirement rank is a running max HLC, so the
/// threshold is directly comparable. This used to be a `DataId` padded to the
/// top of its `(node, random)` sub-space, which is what expressing an HLC
/// threshold in a `DataId`-ranked keyspace required.
fn replica_purge_threshold(watermark: Option<Hlc>, grace: Duration) -> Option<Hlc> {
    hlc_subtract(watermark?, grace)
}

fn purge_once(
    replica_log: &ReplicaLogStore,
    watermark: Option<Hlc>,
    grace: Duration,
    last: Option<Hlc>,
) -> Option<Hlc> {
    let threshold = replica_purge_threshold(watermark, grace)?;

    if last == Some(threshold) {
        return last;
    }

    // A failed purge is degraded-but-functional (CLAUDE.md §6): nothing reads
    // below the floor, and the next tick retries with a fresher threshold.
    if let Err(e) = replica_log.set_purge_threshold(threshold) {
        warn!(error = %e, ?threshold, "replica purge failed; retrying on the next tick");
        return last;
    }
    debug!(?threshold, "replica log purged up to the threshold");
    Some(threshold)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::hlc::HlcParts;

    /// Build an HLC with the given physical milliseconds and a zero logical
    /// component — `hlc_subtract` always zeroes the logical part, so this makes
    /// round-tripping through it an identity at zero grace.
    fn hlc_ms(physical_ms: u64) -> Hlc {
        Hlc::from_parts(HlcParts {
            physical_ms,
            logical: 0,
        })
    }

    /// B-1′ regression: the replica purge threshold derives from the manifest
    /// watermark, never the hlc_cutoff. The cutoff isn't even an input here —
    /// during a snapshot-creation stall the cutoff runs ahead of the watermark,
    /// and purging at the cutoff would drop committed data no snapshot covers.
    #[test]
    fn invariant_replica_purge_uses_watermark_not_cutoff() {
        // No committed snapshot yet → nothing is purgeable, whatever the cutoff
        // says. Purging here would leave a learner skipping entries with no
        // snapshot to recover them from.
        assert_eq!(replica_purge_threshold(None, Duration::ZERO), None);

        // With zero grace the threshold is exactly the watermark — proving it is
        // the watermark that drives purge, not anything else.
        let watermark = hlc_ms(100);
        assert_eq!(
            replica_purge_threshold(Some(watermark), Duration::ZERO),
            Some(watermark),
        );

        // The grace window is subtracted from the watermark (in physical ms).
        assert_eq!(
            replica_purge_threshold(Some(hlc_ms(100_000)), Duration::from_secs(60)),
            Some(hlc_ms(40_000)),
            "threshold must be watermark − grace",
        );

        // A snapshot younger than the grace window → underflow → nothing purged
        // yet (data stays fetchable for the grace period after being snapshotted).
        assert_eq!(
            replica_purge_threshold(Some(hlc_ms(10)), Duration::from_secs(60)),
            None,
        );
    }

    /// The covered boundary is inclusive: an entry whose running-max secondary
    /// equals `watermark − grace` is retired, and everything below it with it.
    #[test]
    fn replica_purge_threshold_boundary_is_inclusive() {
        let watermark = hlc_ms(100);
        let t = replica_purge_threshold(Some(watermark), Duration::ZERO).expect("threshold");
        assert_eq!(t, watermark, "the boundary HLC is itself covered");
        assert!(Hlc(watermark.raw().saturating_sub(1)) < t);
    }
}
