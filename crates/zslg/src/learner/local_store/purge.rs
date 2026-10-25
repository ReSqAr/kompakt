//! Retiring local-store entries the stable log has taken over.
//!
//! The threshold trails the stable watermark by `purge_grace_period` rather
//! than tracking it exactly, so a reader mid-stream at the boundary is not cut
//! off by the handover. Driven by watermark events, not a timer, but rate-limited
//! to [`PURGE_MIN_INTERVAL`] on top — arming a threshold is cheap, the
//! compaction it eventually provokes is not.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::api::{LocalStoreMut, StableLogSnapshot};
use crate::error::Error;
use crate::hlc::{Hlc, hlc_subtract};
use crate::shutdown::Shutdown;

/// Floor on the gap between two threshold advances, however fast `end_hlc`
/// moves.
const PURGE_MIN_INTERVAL: Duration = Duration::from_secs(30);

pub(in crate::learner) fn spawn_local_store_purge_worker(
    shutdown: &mut Shutdown,
    local_store: Arc<dyn LocalStoreMut>,
    stable_log: Arc<dyn StableLogSnapshot>,
    purge_grace_period: Duration,
) {
    let cancel = shutdown.token();
    shutdown.spawn(
        "local_store:purge",
        run(local_store, stable_log, purge_grace_period, cancel),
    );
}

async fn run(
    local_store: Arc<dyn LocalStoreMut>,
    stable_log: Arc<dyn StableLogSnapshot>,
    grace: Duration,
    cancel: CancellationToken,
) -> Result<(), Error> {
    // The watermark's *state*, not the event stream: this loop only ever asked
    // "did the watermark move", which a watch answers without a payload and
    // without a lag to skip past.
    let mut watermark = stable_log.watch_watermark();
    let mut last: Option<Hlc> = None;
    let mut last_purge = Instant::now();

    // Initial purge — always run once at startup.
    last = purge_once(local_store.as_ref(), stable_log.as_ref(), grace, last).await;

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            changed = watermark.changed() => {
                if changed.is_err() {
                    return Ok(()); // the log is gone
                }
            }
        }
        if last_purge.elapsed() >= PURGE_MIN_INTERVAL {
            let new_last = purge_once(local_store.as_ref(), stable_log.as_ref(), grace, last).await;
            if new_last != last {
                last_purge = Instant::now();
            }
            last = new_last;
        }
    }
}

async fn purge_once(
    local_store: &dyn LocalStoreMut,
    stable_log: &dyn StableLogSnapshot,
    grace: Duration,
    last: Option<Hlc>,
) -> Option<Hlc> {
    let loaded = stable_log.end_hlc()?;
    let threshold = hlc_subtract(loaded, grace)?;
    if last.is_some_and(|t| t >= threshold) {
        return last;
    }
    match local_store.purge_below_hlc(threshold).await {
        Ok(()) => debug!(?threshold, "local-store purge ok"),
        Err(e) => warn!(error = %e, "local-store purge failed"),
    }
    Some(threshold)
}
