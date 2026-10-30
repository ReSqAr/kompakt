//! `sync_quorum`: the freshness fence a reader opts into before a read that
//! must observe its own writes.
//!
//! Being caught up means two things, and the second is easy to forget:
//!
//! ```text
//!   1. per replica   local cursor >= that replica's frontier   ReplicaPoller
//!      (quorum of)   — covers entries still IN the replica logs
//!
//!   2. cluster-wide  stable log loaded >= quorum's manifest    wait_for_
//!                    watermark — covers entries the leader     stable_caught_up
//!                    already snapshotted and the replicas purged
//! ```
//!
//! Check 1 alone has a hole: data promoted into a snapshot is purged from the
//! replicas, and a lagging subscriber resuming across the purged range skips it
//! (its cursor jumps to the max surviving index). Such data lives only in the
//! stable log, so the cursor can satisfy check 1 while the entry is nowhere on
//! this node. Check 2 closes it by gating on the manifest the quorum itself
//! reports — the highest generation `fan_out_quorum` saw.
//!
//! Both are bounded by the debouncer's `check_timeout`, so a partition or a
//! permanently-missing upstream degrades to `Error::QuorumReadTimeout` rather
//! than wedging this and every later quorum read.
//!
//! [`QuorumReadDebouncer`] is the cost control: a quorum check is a cluster-wide
//! round trip, so concurrent callers coalesce into one batch and batches are
//! spaced by `deadtime`. Every waiter of a batch gets that batch's result.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::oneshot;

use crate::api::QuorumCheck;
use crate::api::{LocalStoreView, RaftStateView, Replica, Router, StableLogSnapshot};
use crate::error::Error;
use crate::ids::NodeId;
use crate::meta::{RaftManifestState, RaftState};
use crate::metrics::OpState;
use crate::quorum::{FanOutResponse, fan_out_quorum};

// ── Multi-replica quorum check ──────────────────────────────────────────────

pub(in crate::learner) struct MultiReplicaQuorumCheck {
    router: Arc<dyn Router>,
    local_store: Arc<dyn LocalStoreView>,
    raft_state: Arc<dyn RaftStateView>,
    stable_log: Arc<dyn StableLogSnapshot>,
}

impl MultiReplicaQuorumCheck {
    #[must_use]
    pub(in crate::learner) fn new(
        router: Arc<dyn Router>,
        local_store: Arc<dyn LocalStoreView>,
        raft_state: Arc<dyn RaftStateView>,
        stable_log: Arc<dyn StableLogSnapshot>,
    ) -> Self {
        Self {
            router,
            local_store,
            raft_state,
            stable_log,
        }
    }
}

#[async_trait]
impl QuorumCheck for MultiReplicaQuorumCheck {
    async fn check(&self) -> Result<(), Error> {
        let router = self.router.clone();
        let local_store = self.local_store.clone();
        let (final_state, _successful) =
            fan_out_quorum((*self.raft_state.snapshot()).clone(), move |peer| {
                let transport = router.remote(peer);
                let local_store = local_store.clone();
                async move {
                    match ReplicaPoller::check(transport, local_store, peer).await {
                        Ok(Some(state)) => Ok(FanOutResponse::Ready(state)),
                        Ok(None) => Ok(FanOutResponse::Failed),
                        Err(e) => Err(e),
                    }
                }
            })
            .await?;

        // Check 2 (module header): the cursor comparison above cannot see data
        // the leader already snapshotted and the replicas purged.
        wait_for_stable_caught_up(self.stable_log.as_ref(), &final_state.manifest_state).await
    }
}

/// Wait until this node's stable log has loaded up to `quorum_manifest`'s
/// watermark. Returns immediately when the quorum reports no watermark at all.
///
/// Subscribes to the event stream before reading `end_hlc`, so an advance
/// landing between the two is not lost.
async fn wait_for_stable_caught_up(
    stable_log: &dyn StableLogSnapshot,
    quorum_manifest: &RaftManifestState,
) -> Result<(), Error> {
    let Some(target) = quorum_manifest.manifest_watermark else {
        tracing::debug!(
            "quorum read: the quorum reports no manifest watermark; nothing to wait for"
        );
        return Ok(());
    };

    // `integrate_manifest` advances `end_hlc` for every load, including
    // watermark-only advances with no snapshot files, so an empty window needs
    // no special case here.
    //
    // The watch is level-triggered and delivers its current value on the first
    // poll, so there is no subscribe-then-read gap to close by hand and no lag
    // to tolerate: this waits on the watermark's *state*, not on a stream of
    // events about it.
    // Only a wait that actually waits is worth a line; the fast path is every
    // read on a caught-up node.
    let local = stable_log.end_hlc();
    let waiting = local.is_none_or(|hlc| hlc < target);
    if waiting {
        tracing::debug!(
            ?target,
            ?local,
            "quorum read: waiting for the stable log to reach the quorum's watermark"
        );
    }
    let started = tokio::time::Instant::now();
    stable_log
        .watch_watermark()
        .wait_for(|wm| wm.is_some_and(|hlc| hlc >= target))
        .await
        .map_err(|_| Error::Shutdown)?;
    if waiting {
        tracing::debug!(
            ?target,
            waited_ms = started.elapsed().as_millis(),
            "quorum read: stable log caught up"
        );
    }
    Ok(())
}

// ── Per-replica poller ──────────────────────────────────────────────────────

/// Checks whether the local store is caught up to a single upstream replica.
pub(crate) struct ReplicaPoller;

impl ReplicaPoller {
    /// Fetch the replica's status, then wait for the local store's
    /// subscription cursor to reach its frontier. Returns the replica's
    /// `RaftState` on success, `None` if the replica is unreachable.
    pub(crate) async fn check(
        transport: Arc<dyn Replica>,
        local_store: Arc<dyn LocalStoreView>,
        target: NodeId,
    ) -> Result<Option<RaftState>, Error> {
        // Nothing below runs until this returns, and it has no deadline of its
        // own — the caller's `check_timeout` is the only bound. Logged either
        // side so a quorum read parked here is distinguishable from one that
        // never started.
        tracing::debug!(?target, "quorum read: fetching replica status");
        let (end_index, config) = match transport.fetch_status().await {
            Ok(status) => (status.frontier, status.raft_state),
            Err(e) => {
                tracing::debug!(?target, error = %e, "quorum read: replica status fetch failed");
                return Ok(None);
            }
        };
        tracing::debug!(
            ?target,
            frontier = ?end_index.map(crate::ids::LogIndex::raw),
            "quorum read: replica status fetched"
        );

        let Some(end_index) = end_index else {
            tracing::debug!(
                ?target,
                "quorum read: replica log is empty; nothing to catch up to"
            );
            return Ok(Some(config));
        };

        // Subscribe before the first check, so an integration racing it is a
        // version bump this receiver has not seen — `changed()` then returns
        // immediately rather than waiting for the next one.
        let mut integrated = local_store.subscribe_integrated();
        // The cursor is in the replica's own index space and advances only as
        // this node integrates that replica's stream, so a frontier it never
        // delivers parks this loop until the caller's deadline.
        let mut waited = false;
        loop {
            if Self::upstream_caught_up(local_store.as_ref(), target, end_index).await? {
                if waited {
                    tracing::debug!(
                        ?target,
                        end_index = end_index.raw(),
                        "quorum read: upstream caught up"
                    );
                }
                return Ok(Some(config));
            }
            if !waited {
                waited = true;
                let cursor = local_store.subscription_cursor(target).await?;
                tracing::debug!(
                    ?target,
                    end_index = end_index.raw(),
                    ?cursor,
                    "quorum read: waiting for the local store to reach this replica's frontier"
                );
            }
            integrated.changed().await.map_err(|_| {
                Error::Storage(format!(
                    "quorum check: local store closed before {target:?} caught up"
                ))
            })?;
        }
    }

    async fn upstream_caught_up(
        local_store: &dyn LocalStoreView,
        upstream: NodeId,
        end_index: crate::ids::LogIndex,
    ) -> Result<bool, Error> {
        let cursor = local_store.subscription_cursor(upstream).await?;
        Ok(cursor.is_some_and(|c| c >= end_index))
    }
}

// ── Read debouncer ──────────────────────────────────────────────────────────

#[derive(Clone)]
pub(in crate::learner) struct QuorumReadDebouncer {
    state: Arc<SyncMutex<Option<DebounceState>>>,
    deadtime: Duration,
    /// Deadline for one batched check; on expiry every waiter of the batch
    /// receives `Error::QuorumReadTimeout` instead of hanging (F-21).
    check_timeout: Duration,
    quorum_check: Arc<dyn QuorumCheck>,
    last_check: Arc<SyncMutex<Option<tokio::time::Instant>>>,
}

struct DebounceState {
    waiters: Vec<oneshot::Sender<Result<(), Error>>>,
}

/// The debouncer's tunables, produced by
/// [`LearnerConfig::quorum_read_debouncer_config`](crate::learner::LearnerConfig::quorum_read_debouncer_config).
pub(in crate::learner) struct QuorumReadDebouncerConfig {
    pub(in crate::learner) deadtime: Duration,
    pub(in crate::learner) check_timeout: Duration,
}

impl QuorumReadDebouncer {
    #[must_use]
    pub(in crate::learner) fn new(
        quorum_check: Arc<dyn QuorumCheck>,
        config: &QuorumReadDebouncerConfig,
    ) -> Self {
        Self {
            state: Arc::new(SyncMutex::new(None)),
            deadtime: config.deadtime,
            check_timeout: config.check_timeout,
            quorum_check,
            last_check: Arc::new(SyncMutex::new(None)),
        }
    }

    /// Test convenience: build from loose parts without threading a config.
    #[cfg(test)]
    pub(in crate::learner) fn for_test(
        quorum_check: Arc<dyn QuorumCheck>,
        deadtime: Duration,
        check_timeout: Duration,
    ) -> Self {
        Self::new(
            quorum_check,
            &QuorumReadDebouncerConfig {
                deadtime,
                check_timeout,
            },
        )
    }

    /// Register a waiter for the next quorum check, batch-coalescing
    /// concurrent callers. The first caller arms the timer; subsequent callers
    /// ride the same batch. Bounded by `check_timeout` — a
    /// partition/permanently-missing upstream degrades to `QuorumReadTimeout`.
    pub(in crate::learner) async fn sync_quorum(&self) -> Result<(), Error> {
        let batch = DebounceBatch::from_debouncer(self);
        batch.execute().await
    }
}

/// One batched quorum-check cycle: collects waiters under the debouncer's
/// mutex, arms the debounce timer, runs the check (with a deadline), and fans
/// the result out to every registered waiter.
struct DebounceBatch<'a> {
    state: &'a Arc<SyncMutex<Option<DebounceState>>>,
    quorum_check: &'a Arc<dyn QuorumCheck>,
    deadtime: Duration,
    check_timeout: Duration,
    last_check: &'a Arc<SyncMutex<Option<tokio::time::Instant>>>,
}

impl<'a> DebounceBatch<'a> {
    fn from_debouncer(d: &'a QuorumReadDebouncer) -> Self {
        Self {
            state: &d.state,
            quorum_check: &d.quorum_check,
            deadtime: d.deadtime,
            check_timeout: d.check_timeout,
            last_check: &d.last_check,
        }
    }

    async fn execute(self) -> Result<(), Error> {
        let (rx, arm_timer) = {
            let mut state = self.state.lock();
            let (tx, rx) = oneshot::channel();
            let arm = match state.as_mut() {
                Some(existing) => {
                    existing.waiters.push(tx);
                    false
                }
                None => {
                    *state = Some(DebounceState { waiters: vec![tx] });
                    true
                }
            };
            (rx, arm)
        };

        tracing::debug!(
            batch = if arm_timer { "armed" } else { "joined" },
            "quorum read: registered with a debounce batch"
        );
        if arm_timer {
            let state = self.state.clone();
            let quorum_check = self.quorum_check.clone();
            let deadtime = self.deadtime;
            let check_timeout = self.check_timeout;
            let last_check = self.last_check.clone();
            tokio::spawn(async move {
                let sleep_dur = last_check
                    .lock()
                    .and_then(|t| deadtime.checked_sub(t.elapsed()))
                    .unwrap_or(Duration::ZERO);
                tokio::time::sleep(sleep_dur).await;

                let waiters = {
                    let mut state = state.lock();
                    state.take().map(|s| s.waiters).unwrap_or_default()
                };

                // Deadline-bound: a partition or a permanently-missing
                // upstream must degrade to a typed error, not wedge this and
                // every subsequent quorum read (F-21). Dropping the check
                // future cancels its fan-out tasks.
                let result = match tokio::time::timeout(check_timeout, quorum_check.check()).await {
                    Ok(result) => result,
                    Err(_) => {
                        crate::metrics::read_token_wait_timeout();
                        Err(Error::QuorumReadTimeout {
                            timeout: check_timeout,
                        })
                    }
                };
                *last_check.lock() = Some(tokio::time::Instant::now());
                for waiter in waiters {
                    let _ = waiter.send(result.clone());
                }
            });
        }

        let probe = crate::metrics::read_token_wait_probe();
        let result = rx.await.map_err(|_| Error::Cancelled);
        probe.finish(OpState::from_ok(result.is_ok()));
        result?
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::hlc::Hlc;
    use crate::ids::{LogIndex, NodeId};
    use crate::meta::Data;
    use crate::tests::upstream;
    use crossbeam_combinators::Channels;

    use super::*;
    use crate::api::WindowItem;

    use crate::api::QuorumCheck;

    /// Fake `QuorumCheck` that returns a pre-set result.
    struct ScriptedQuorum {
        result: std::sync::Mutex<Result<(), Error>>,
    }

    impl ScriptedQuorum {
        fn pass() -> Self {
            Self {
                result: std::sync::Mutex::new(Ok(())),
            }
        }
    }

    #[async_trait]
    impl QuorumCheck for ScriptedQuorum {
        async fn check(&self) -> Result<(), Error> {
            self.result.lock().expect("lock").clone()
        }
    }

    #[tokio::test]
    async fn single_sync_quorum_delegates_to_quorum_check() {
        let check = Arc::new(ScriptedQuorum::pass());
        let debouncer =
            QuorumReadDebouncer::for_test(check, Duration::ZERO, Duration::from_secs(5));
        debouncer.sync_quorum().await.expect("ok");
    }

    #[tokio::test]
    async fn concurrent_sync_quorum_coalesces_into_one_check() {
        use std::sync::atomic::{AtomicU32, Ordering};

        struct CountingCheck {
            calls: AtomicU32,
        }

        #[async_trait]
        impl QuorumCheck for CountingCheck {
            async fn check(&self) -> Result<(), Error> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                // Let the batch gather — the first caller arms the timer,
                // subsequent waiters ride.
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }
        }

        let check = Arc::new(CountingCheck {
            calls: AtomicU32::new(0),
        });
        let debouncer = QuorumReadDebouncer::for_test(
            check.clone(),
            Duration::from_millis(10),
            Duration::from_secs(5),
        );

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let d = debouncer.clone();
                tokio::spawn(async move { d.sync_quorum().await })
            })
            .collect();
        for h in handles {
            h.await.expect("join").expect("sync_quorum");
        }
        assert_eq!(check.calls.load(Ordering::SeqCst), 1);
    }

    struct StubLocalStore {
        inner: Arc<std::sync::Mutex<StubLocalInner>>,
    }

    struct StubLocalInner {
        cursors: BTreeMap<NodeId, LogIndex>,
    }

    impl StubLocalStore {
        fn new() -> Self {
            Self {
                inner: Arc::new(std::sync::Mutex::new(StubLocalInner {
                    cursors: BTreeMap::new(),
                })),
            }
        }

        fn set_cursor(&self, node: NodeId, idx: LogIndex) {
            self.inner.lock().expect("lock").cursors.insert(node, idx);
        }
    }

    #[async_trait]
    impl LocalStoreView for StubLocalStore {
        fn stream_above_hlc(&self, _threshold: Option<Hlc>) -> Channels<Result<Data, Error>> {
            unimplemented!()
        }
        fn stream_window_ordered(
            &self,
            _above: Option<Hlc>,
            _upto: Hlc,
            _max_entries: Option<std::num::NonZeroUsize>,
        ) -> Channels<Result<WindowItem, Error>> {
            unimplemented!()
        }
        fn subscribe_integrated(&self) -> tokio::sync::watch::Receiver<()> {
            let (tx, rx) = tokio::sync::watch::channel(());
            drop(tx);
            rx
        }
        fn subscribe_unstable(&self) -> crossbeam_combinators::Channels<crate::meta::Data> {
            crossbeam_combinators::Channels::from_infinite(
                crossbeam_channel::never(),
                "stub unstable",
            )
        }
        async fn subscription_cursor(&self, upstream: NodeId) -> Result<Option<LogIndex>, Error> {
            Ok(self
                .inner
                .lock()
                .expect("lock")
                .cursors
                .get(&upstream)
                .copied())
        }
    }

    #[tokio::test]
    async fn upstream_locally_caught_up_requires_cursor_at_or_past_frontier() {
        let store = StubLocalStore::new();
        // Cursor at 5, frontier 5 → caught up.
        store.set_cursor(upstream(1), LogIndex(5));
        assert!(
            ReplicaPoller::upstream_caught_up(&store, upstream(1), LogIndex(5))
                .await
                .expect("ok")
        );
        // Cursor at 4, frontier 5 → not caught up.
        store.set_cursor(upstream(1), LogIndex(4));
        assert!(
            !ReplicaPoller::upstream_caught_up(&store, upstream(1), LogIndex(5))
                .await
                .expect("ok")
        );
    }
}
