use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::info;

use super::peer::ReplicationSubscriber;
use crate::api::{LocalStoreMut, RaftStateView, Router};
use crate::error::Error;
use crate::hlc::HlcClock;
use crate::ids::NodeId;
use crate::learner::LocalStore;
use crate::warn_throttle::ThrottledWarn;

const REPLICATION_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const REPLICATION_MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Bound the subscriber-retry `warn!` flood (every failed attempt under a shed
/// loop) to one line per interval, plus a summary with the suppressed count.
const SUBSCRIBER_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// Cadence of the learner's store-gauge emissions (counts are cache-served).
const STORE_GAUGE_TICK_INTERVAL: Duration = Duration::from_secs(1);

/// Runs one subscriber task per effective replica, starting and cancelling them
/// as the replica set changes. Holds the concrete `LocalStore` rather than
/// `dyn LocalStoreView` so store gauges can be emitted from here without
/// widening the public trait — see [`Self::emit_store_gauges`].
pub(in crate::learner) struct ReplicationManager {
    clock: Arc<HlcClock>,
    raft_state: Arc<dyn RaftStateView>,
    local_store: Arc<LocalStore>,
    router: Arc<dyn Router>,
}

impl ReplicationManager {
    #[must_use]
    pub(in crate::learner) fn new(
        clock: Arc<HlcClock>,
        raft_state: Arc<dyn RaftStateView>,
        local_store: Arc<LocalStore>,
        router: Arc<dyn Router>,
    ) -> Self {
        Self {
            clock,
            raft_state,
            local_store,
            router,
        }
    }

    /// Emits nothing — there is no learner-level store gauge at present. Kept
    /// with its ticker as the seam for one, since this is the only place
    /// holding the concrete `LocalStore`.
    fn emit_store_gauges(&self) {}

    pub(in crate::learner) async fn run(self, cancel: CancellationToken) -> Result<(), Error> {
        info!("replication manager: starting");
        fn effective_replicas(
            state: &crate::meta::RaftState,
        ) -> std::collections::BTreeSet<NodeId> {
            let mut peers = state.replica.current().clone();
            if let Some(next) = state.replica.next() {
                peers.extend(next.iter().copied());
            }
            peers
        }

        let mut cancels: HashMap<NodeId, CancellationToken> = HashMap::new();
        let mut tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        let mut apply_rx = self.raft_state.subscribe();
        let mut gauge_ticker = tokio::time::interval(STORE_GAUGE_TICK_INTERVAL);
        gauge_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        {
            let state = self.raft_state.snapshot();
            for peer_id in effective_replicas(&state) {
                info!(?peer_id, "starting replication subscriber");
                let tok = CancellationToken::new();
                cancels.insert(peer_id, tok.clone());
                tasks.spawn(subscriber_loop(
                    peer_id,
                    self.clock.clone(),
                    self.local_store.clone(),
                    self.router.clone(),
                    tok,
                ));
            }
        }

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    info!("replication manager: shutdown");
                    break;
                }
                _ = gauge_ticker.tick() => {
                    self.emit_store_gauges();
                }
                changed = apply_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let state = self.raft_state.snapshot();
                    let wanted = effective_replicas(&state);

                    for peer_id in &wanted {
                        if !cancels.contains_key(peer_id) {
                            let tok = CancellationToken::new();
                            cancels.insert(*peer_id, tok.clone());
                            tasks.spawn(subscriber_loop(
                                *peer_id,
                                self.clock.clone(),
                                self.local_store.clone(),
                                self.router.clone(),
                                tok,
                            ));
                        }
                    }

                    let removed: Vec<NodeId> = cancels
                        .keys()
                        .filter(|id| !wanted.contains(*id))
                        .copied()
                        .collect();
                    for id in removed {
                        if let Some(tok) = cancels.remove(&id) {
                            tok.cancel();
                        }
                    }
                }
                Some(res) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(e) = res {
                        tracing::error!(error = ?e, "replication subscriber panicked");
                    }
                }
            }
        }

        for (_, tok) in cancels.drain() {
            tok.cancel();
        }
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
}

/// Backoff after a failed attempt, judged by **durable progress** rather than
/// by how long the attempt lived.
///
/// A connection that made records durable was useful however briefly it lived;
/// one that made none was not, however long it lived. Only the second deserves
/// to be backed off. Judging by duration inverts this under a shed loop, where
/// every attempt is short *and* productive: the ladder pins at its 10 s ceiling
/// and throttles the very catch-up it is watching.
#[must_use]
fn next_backoff(current: Duration, records_retired: u64) -> Duration {
    if records_retired > 0 {
        REPLICATION_INITIAL_BACKOFF
    } else {
        current
            .saturating_mul(2)
            .min(REPLICATION_MAX_BACKOFF)
            .max(REPLICATION_INITIAL_BACKOFF)
    }
}

/// Classify why an attempt ended, using only what the learner can actually
/// know. A shed on the serving replica's fan-out reaches us as an ordinary
/// stream end, so `Ended` deliberately covers "socket dropped", "replica
/// restarted" and "someone shed" alike — `zslg_log_subscription_shed_total` on
/// either node is what disambiguates. Claiming more precision here would be
/// guessing.
fn end_reason(e: &Error) -> crate::metrics::SubscriptionEndReason {
    use crate::metrics::SubscriptionEndReason as R;
    match e {
        Error::UnexpectedNodeId { .. } | Error::UnknownPeer { .. } => R::PeerGone,
        Error::SubscriptionEnded { .. } => R::Ended,
        _ => R::Failed,
    }
}

async fn subscriber_loop(
    peer_id: NodeId,
    clock: Arc<HlcClock>,
    local_store: Arc<dyn LocalStoreMut>,
    router: Arc<dyn Router>,
    task_cancel: CancellationToken,
) {
    let mut backoff = REPLICATION_INITIAL_BACKOFF;
    let mut first_connect = true;
    let mut warn = ThrottledWarn::new(SUBSCRIBER_WARN_INTERVAL);
    loop {
        if !first_connect {
            crate::metrics::subscription_reconnect();
        }
        first_connect = false;

        let start = tokio::time::Instant::now();
        let upstream = router.remote(peer_id);
        let sub = ReplicationSubscriber::new(
            peer_id,
            clock.clone(),
            upstream.clone(),
            local_store.clone(),
        );
        let outcome = tokio::select! {
            r = sub.run() => r,
            () = task_cancel.cancelled() => break,
        };
        let elapsed = start.elapsed();
        // Per-attempt shape, emitted for every attempt including successful
        // ones: lifetime and durable progress together are what separate a
        // healthy long-lived subscription from a reconnect loop that re-pays
        // the replica's catch-up scan for a bufferful of records each time.
        crate::metrics::subscription_connection_seconds(elapsed);
        crate::metrics::subscription_records_retired(outcome.records_retired);
        if let Err(e) = outcome.result {
            crate::metrics::subscription_ended(end_reason(&e));
            backoff = next_backoff(backoff, outcome.records_retired);
            let is_expected_transient = matches!(e, Error::UnexpectedNodeId { .. })
                || matches!(&e, Error::UnknownPeer { .. });
            if is_expected_transient {
                tracing::debug!(
                    error = %e,
                    peer_id = ?peer_id,
                    ?backoff,
                    "replication subscriber: peer gone or replaced; retrying until membership converges",
                );
            } else if let Some(suppressed) = warn.gate() {
                if suppressed > 0 {
                    tracing::warn!(
                        suppressed,
                        peer_id = ?peer_id,
                        "replication subscriber errors: {} suppressed in the last {}s (see zslg_log_subscription_ended_total for the exact count)",
                        suppressed,
                        SUBSCRIBER_WARN_INTERVAL.as_secs(),
                    );
                }
                // `records_retired` and `connection_seconds` are what make
                // this line diagnostic rather than decorative: retiring ~0
                // records over a sub-second connection, repeatedly, is a
                // shed loop, not a flaky peer.
                tracing::warn!(
                    error = %e,
                    peer_id = ?peer_id,
                    ?backoff,
                    records_retired = outcome.records_retired,
                    connection_seconds = elapsed.as_secs_f64(),
                    "replication subscriber error; retrying",
                );
            }
        }

        let jittered = crate::jitter(backoff);
        tokio::select! {
            () = task_cancel.cancelled() => break,
            () = tokio::time::sleep(jittered) => {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    /// The shed loop: every attempt lives ~10 ms and retires a bufferful of
    /// records. A duration test (`elapsed > backoff` → reset) sees only a short
    /// connection and doubles, every time, until it sits at 10 s — throttling a
    /// learner that is successfully draining a backlog to ~200 records/s.
    /// Progress, not duration, decides.
    #[test]
    fn progress_resets_the_ladder_however_briefly_the_connection_lived() {
        assert_eq!(
            next_backoff(REPLICATION_MAX_BACKOFF, 768),
            REPLICATION_INITIAL_BACKOFF,
            "an attempt that made records durable was useful, however short",
        );
    }

    #[test]
    fn no_progress_escalates_toward_the_ceiling() {
        let mut b = REPLICATION_INITIAL_BACKOFF;
        for _ in 0..20 {
            b = next_backoff(b, 0);
        }
        assert_eq!(b, REPLICATION_MAX_BACKOFF, "must saturate, not overflow");
    }

    /// A peer that is genuinely gone must still be backed off — the ladder has
    /// to climb, or a dead peer is retried every 250 ms forever.
    #[test]
    fn a_dead_peer_still_climbs() {
        let first = next_backoff(REPLICATION_INITIAL_BACKOFF, 0);
        assert!(
            first > REPLICATION_INITIAL_BACKOFF,
            "zero progress must escalate on the very first failure",
        );
        assert!(first <= REPLICATION_MAX_BACKOFF);
    }
}
