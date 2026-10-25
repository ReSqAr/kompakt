//! Optional TTL purge for the stable log. Mirrors the replica log's purge
//! worker (`crate::replica::purge`): event-driven off the raft manifest
//! watermark, it hands the stable log a retire threshold of `watermark − ttl`.
//! Everything at or below the threshold is gone the moment the durable
//! `DeleteRange` (`StableLog::set_purge_threshold`) returns — reclaiming the
//! space it occupied on disk is a separate, later step left to RocksDB's
//! `periodic_compaction_seconds`; a subsequent read of the purged prefix is
//! served just-in-time from object-store snapshots (`learner::read`), so the
//! caller never notices (design doc §794).

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::api::{RaftStateView, StableLogSnapshot};
use crate::error::Error;
use crate::hlc::{Hlc, hlc_subtract};
use crate::metrics;

/// Don't hand over a new purge threshold more than once per interval even if
/// the watermark advances faster. The handoff is a scan to find the boundary
/// entry plus a `WriteBatch` plus an fsync (`set_purge_threshold`) — cheap
/// next to a snapshot, but not free — so there is no point re-running it on
/// every watermark event.
const PURGE_MIN_INTERVAL: Duration = Duration::from_secs(30);

pub(in crate::learner) async fn run_stable_purge_worker(
    raft_state: Arc<dyn RaftStateView>,
    stable_log: Arc<dyn StableLogSnapshot>,
    ttl: Duration,
    cancel: CancellationToken,
) -> Result<(), Error> {
    // Key off the manifest watermark, mirroring the replica purge worker. For
    // the stable log this is doubly safe: every stable entry is by construction
    // already covered by a committed snapshot in the manifest (the stable log is
    // built *from* those snapshots), and the manifest's snapshots are never
    // garbage-collected — so a purged read is always recoverable from object
    // store regardless of the threshold.
    let mut rx = raft_state.subscribe();
    let mut last: Option<Hlc> = None;
    let mut last_purge = Instant::now();

    // Initial purge — always run once at startup.
    {
        let watermark = raft_state.snapshot().manifest_state.manifest_watermark;
        last = purge_once(&stable_log, watermark, ttl, last).await;
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
            let new_last = purge_once(&stable_log, watermark, ttl, last).await;
            if new_last != last {
                last_purge = Instant::now();
            }
            last = new_last;
        }
    }
}

/// The HLC up to which the stable log may be purged: the manifest watermark
/// minus the TTL, or `None` when there is no watermark yet or the watermark is
/// younger than the TTL (nothing old enough to purge).
fn stable_purge_threshold(watermark: Option<Hlc>, ttl: Duration) -> Option<Hlc> {
    hlc_subtract(watermark?, ttl)
}

/// [`purge_once_blocking`] run on the blocking pool: `StableLogSnapshot::set_purge_threshold`
/// does a scan plus a `Storage::write` plus a blocking `Storage::fence`
/// (CLAUDE.md §19), and this worker's loop is an async task — calling it
/// inline would block a runtime worker thread on disk I/O.
async fn purge_once(
    stable_log: &Arc<dyn StableLogSnapshot>,
    watermark: Option<Hlc>,
    ttl: Duration,
    last: Option<Hlc>,
) -> Option<Hlc> {
    let stable_log = Arc::clone(stable_log);
    match tokio::task::spawn_blocking(move || {
        purge_once_blocking(stable_log.as_ref(), watermark, ttl, last)
    })
    .await
    {
        Ok(new_last) => new_last,
        // The blocking task panicked — leave `last` unchanged so the next
        // tick retries rather than silently losing the threshold.
        Err(e) => {
            warn!(error = %e, "stable log purge task panicked; retrying on the next tick");
            last
        }
    }
}

fn purge_once_blocking(
    stable_log: &dyn StableLogSnapshot,
    watermark: Option<Hlc>,
    ttl: Duration,
    last: Option<Hlc>,
) -> Option<Hlc> {
    let threshold = stable_purge_threshold(watermark, ttl)?;
    if last == Some(threshold) {
        return last;
    }
    // The log clamps the threshold strictly below its frontier entry's HLC
    // (`StableLog::set_purge_threshold`), so the recovery anchor is never
    // dropped. The range delete is durable and fenced before this returns.
    if let Err(e) = stable_log.set_purge_threshold(threshold) {
        // Degraded-but-functional (CLAUDE.md §6): nothing reads below the
        // floor without the object-store fallback, so a failed purge is not
        // an invariant violation. Leave `last` unchanged so the next tick
        // retries the same threshold instead of silently giving up on it.
        warn!(?threshold, error = %e, "stable log purge threshold write failed");
        return last;
    }
    // Publish the purge horizon for operators. This is the pre-clamp value we
    // asked for (`watermark − ttl`) — the operator-facing "how far back are we
    // purging" — not the effective value `set_purge_threshold` may clamp below
    // the frontier. Covers both the worker's handoff and the initial-purge
    // call at startup (`run_stable_purge_worker`).
    metrics::stable_log_purge_threshold_physical_ms(threshold);
    debug!(
        ?threshold,
        "stable log purge threshold handed to the engine"
    );
    Some(threshold)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::HlcParts;

    fn hlc_ms(physical_ms: u64) -> Hlc {
        Hlc::from_parts(HlcParts {
            physical_ms,
            logical: 0,
        })
    }

    /// The threshold is the manifest watermark minus the TTL — the same
    /// watermark-derived rule the replica log uses.
    #[test]
    fn stable_purge_threshold_is_watermark_minus_ttl() {
        // No watermark → nothing purgeable.
        assert_eq!(stable_purge_threshold(None, Duration::ZERO), None);

        // Zero TTL → threshold is exactly the watermark.
        let watermark = hlc_ms(100);
        assert_eq!(
            stable_purge_threshold(Some(watermark), Duration::ZERO),
            Some(watermark),
        );

        // The TTL is subtracted in physical ms.
        assert_eq!(
            stable_purge_threshold(Some(hlc_ms(100_000)), Duration::from_secs(60)),
            Some(hlc_ms(40_000)),
            "threshold must be watermark − ttl",
        );

        // A watermark younger than the TTL → underflow → nothing purged yet.
        assert_eq!(
            stable_purge_threshold(Some(hlc_ms(10)), Duration::from_secs(60)),
            None,
        );
    }
}
