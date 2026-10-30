//! One subscription attempt against one replica.
//!
//! ```text
//!   cursor ──> subscribe_from ──> reader (spawn_blocking) ──> integrate_loop
//!   (durable)                     tags peer, meters gaps      (own thread,
//!        ^                                                     commits batches)
//!        └──────────── re-read after the attempt ends ─────────────┘
//! ```
//!
//! Reading and committing are separate threads so the stream keeps producing
//! while a commit is in flight; the channel between them is the backpressure.
//!
//! Progress is measured by re-reading the durable cursor afterwards, not by
//! counting records handed to the channel. Only what the integrate loop
//! committed counts — a record buffered when the stream died was not — and that
//! number, paired with the error, is what lets the manager tell a livelocked
//! reconnect from a peer that is merely flaky. Neither half is enough alone.

use crate::Error;
use crate::api::{LocalStoreMut, Replica};
use crate::hlc::HlcClock;
use crate::ids::{LogIndex, NodeId};
use crate::meta::Data;
use std::sync::Arc;

/// Sample `zslg_hlc_skew_seconds` for roughly every Nth replicated item — a
/// per-peer drift signal, not a per-item cost.
const SKEW_SAMPLE_STRIDE: usize = 8;

/// What one subscription attempt did and why it stopped. See
/// [`ReplicationSubscriber::run`].
pub(in crate::learner) struct AttemptOutcome {
    /// Records this attempt made **durable** — the advance of the peer's
    /// persisted subscription cursor. Zero means the attempt accomplished
    /// nothing, whatever the error says.
    pub(in crate::learner) records_retired: u64,
    pub(in crate::learner) result: Result<(), Error>,
}

/// Internal: carries the resume cursor back out so the progress delta can be
/// computed against it after the attempt ends, on both the ok and error paths.
struct InnerOutcome {
    resume_from: Option<LogIndex>,
    result: Result<(), Error>,
}

/// Records between two cursor positions. `None` is "before index 0", so a fresh
/// learner that reaches index `n` retired `n + 1` records.
fn cursor_delta(from: Option<LogIndex>, to: Option<LogIndex>) -> u64 {
    match (from, to) {
        (_, None) => 0,
        (None, Some(to)) => to.raw().saturating_add(1),
        (Some(from), Some(to)) => to.raw().saturating_sub(from.raw()),
    }
}

pub(in crate::learner) struct ReplicationSubscriber {
    peer_id: NodeId,
    clock: Arc<HlcClock>,
    upstream: Arc<dyn Replica>,
    local_store: Arc<dyn LocalStoreMut>,
}

impl ReplicationSubscriber {
    #[must_use]
    pub(in crate::learner) fn new(
        peer_id: NodeId,
        clock: Arc<HlcClock>,
        upstream: Arc<dyn Replica>,
        local_store: Arc<dyn LocalStoreMut>,
    ) -> Self {
        Self {
            peer_id,
            clock,
            upstream,
            local_store,
        }
    }

    /// Run one subscription attempt to completion.
    pub(in crate::learner) async fn run(self) -> AttemptOutcome {
        let peer_id = self.peer_id;
        let local_store = Arc::clone(&self.local_store);
        let result = self.run_inner().await;
        // Re-read the durable cursor rather than counting records in flight:
        // only what the integrate loop committed counts as progress, and a
        // record buffered when the stream died was not committed.
        let records_retired = match local_store.subscription_cursor(peer_id).await {
            Ok(end) => cursor_delta(result.resume_from, end),
            Err(_) => 0,
        };
        AttemptOutcome {
            records_retired,
            result: result.result,
        }
    }

    async fn run_inner(self) -> InnerOutcome {
        let resume_from = match self.local_store.subscription_cursor(self.peer_id).await {
            Ok(c) => c,
            Err(e) => {
                return InnerOutcome {
                    resume_from: None,
                    result: Err(e),
                };
            }
        };
        InnerOutcome {
            resume_from,
            result: self.stream_from(resume_from).await,
        }
    }

    async fn stream_from(self, resume_from: Option<LogIndex>) -> Result<(), Error> {
        let peer_id = self.peer_id;
        let clock = Arc::clone(&self.clock);
        // `filter_map` runs at pull time on the integrator's forwarding thread,
        // so tagging and metering cost no thread and no queue of their own —
        // the socket is already decoupled by the client's own reader loop.
        // `Fn`, not `FnMut`, hence the atomic for the gap cursor.
        let expected_next =
            std::sync::atomic::AtomicU64::new(LogIndex::next_after(resume_from).raw());
        // Decided once, not per item, so skew sampling stays "every Nth item".
        let stride = u64::try_from(SKEW_SAMPLE_STRIDE).unwrap_or(8);

        let stream = self.upstream.subscribe_from(resume_from).filter_map(
            move |item: Result<(LogIndex, Data), Error>| {
                let (idx, data) = match item {
                    Ok(v) => v,
                    // In-band: `integrate_loop` stops on it and returns it.
                    Err(e) => return Some(Err(e)),
                };
                let prev = LogIndex(
                    expected_next.load(std::sync::atomic::Ordering::Relaxed),
                );
                let (next_expected, gapped) = detect_purge_gap(idx, prev);
                expected_next
                    .store(next_expected.raw(), std::sync::atomic::Ordering::Relaxed);
                if gapped {
                    crate::metrics::subscription_purge_gaps(1);
                    tracing::debug!(
                        peer_id = ?peer_id,
                        from = ?prev,
                        to = ?idx,
                        "subscription skipped replica-purged indices (benign; recovered via snapshot load)",
                    );
                }
                if idx.raw().checked_rem(stride).unwrap_or(1) == 0 {
                    let skew_s = clock.skew_against(data.data_id.hlc()).as_secs_f64();
                    crate::metrics::hlc_skew(peer_id, skew_s);
                }
                crate::metrics::subscription_messages(1);
                Some(Ok((peer_id, idx, data)))
            },
        );

        // A subscription is infinite by contract: the replica's append stream
        // never completes, so even a *declared* close is the upstream going
        // away — it only ever comes from a stub with nothing to serve. Both
        // must be errors, because `manager` escalates its backoff ladder and
        // logs only on `Err`; returning `Ok` here re-subscribed every 250 ms
        // in silence, never climbing toward REPLICATION_MAX_BACKOFF.
        match self.local_store.integrate_loop(stream).await {
            Ok(()) => Err(Error::subscription_ended(
                peer_id,
                "upstream declared the stream complete, but a \
                 subscription cannot complete",
            )),
            Err(Error::SubscriptionStreamEnded { reason }) => {
                Err(Error::subscription_ended(peer_id, reason))
            }
            Err(other) => Err(other),
        }
    }
}

/// Classify one delivered index against the expected next contiguous one. The
/// replica log is gapless, so an index strictly greater than expected means the
/// replica purged the entries in between (the learner resumed across a purged
/// range). Returns the advanced expectation and whether this index jumped.
///
/// Per item, not per batch: the stream arrives one record at a time through a
/// `filter_map`, and the batch form obliged the only caller to clone the record
/// — a `Bytes` refcount bump per record on the subscription hot path — purely
/// to build a one-element slice. The payload was never read.
///
/// Pure (no I/O), so it is directly testable. Duplicates from the subscribe
/// overlap window (index `<` expected) are ignored and do not regress the
/// expectation.
fn detect_purge_gap(idx: LogIndex, expected_next: LogIndex) -> (LogIndex, bool) {
    let gapped = idx > expected_next;
    let expected = if idx >= expected_next {
        LogIndex::next_after(Some(idx))
    } else {
        expected_next
    };
    (expected, gapped)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;

    use futures::stream::BoxStream;

    use super::*;
    use crate::api::{LocalStoreView, Replica, StampedWrite, WriteOutcome};
    use crate::hlc::HlcClock;
    use crate::ids::LogIndex;
    use crate::meta::{Data, ReplicaStatus};
    use crate::tests::{fresh_local, make_meta, upstream as upstream_id};
    use crossbeam_combinators::Channels;

    /// Drive a subscriber whose fake upstream declares completion. Even a
    /// *declared* close is an error (a subscription cannot complete), so these
    /// assert the error rather than success — the records sent before it are
    /// still integrated, because `run` awaits the integrate loop before
    /// propagating the reader's error.
    async fn run_expecting_end(sub: ReplicationSubscriber) {
        let err = sub
            .run()
            .await
            .result
            .expect_err("a subscription cannot complete");
        assert!(
            matches!(err, Error::SubscriptionEnded { .. }),
            "expected SubscriptionEnded, got: {err:?}"
        );
    }

    /// A [`Replica`] stub for subscriber tests. Returns pre-configured
    /// items from an in-memory vec.
    struct FakeUpstreamReplica {
        items: Mutex<Option<Vec<(LogIndex, Data)>>>,
    }

    impl FakeUpstreamReplica {
        fn new(items: Vec<(LogIndex, Data)>) -> Self {
            Self {
                items: Mutex::new(Some(items)),
            }
        }
    }

    #[async_trait]
    impl Replica for FakeUpstreamReplica {
        fn write(&self, _: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
            unimplemented!()
        }
        async fn fetch_status(&self) -> Result<ReplicaStatus, Error> {
            unimplemented!()
        }
        fn subscribe_from(
            &self,
            _from: Option<LogIndex>,
        ) -> Channels<Result<(LogIndex, Data), Error>> {
            let items = self.items.lock().expect("lock").take().unwrap_or_default();
            Channels::from_vec(
                items
                    .into_iter()
                    .map(|(idx, data)| Ok((idx, data)))
                    .collect(),
            )
        }
        async fn subscribe_status(
            &self,
        ) -> Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> {
            unimplemented!()
        }
    }

    /// A [`Replica`] stub whose subscription is a real *infinite* stream that
    /// then dies — a peer going away mid-stream.
    struct DyingUpstreamReplica;

    #[async_trait]
    impl Replica for DyingUpstreamReplica {
        fn write(&self, _: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>> {
            unimplemented!()
        }
        async fn fetch_status(&self) -> Result<ReplicaStatus, Error> {
            unimplemented!()
        }
        fn subscribe_from(
            &self,
            _from: Option<LogIndex>,
        ) -> Channels<Result<(LogIndex, Data), Error>> {
            let (tx, rx) = crossbeam_channel::bounded::<Result<(LogIndex, Data), Error>>(1);
            drop(tx); // the peer went away
            Channels::from_infinite(rx, "client subscribe stream")
        }
        async fn subscribe_status(
            &self,
        ) -> Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> {
            unimplemented!()
        }
    }

    /// A subscription that ends must surface as `Err`, because
    /// `subscriber::manager` escalates its reconnect backoff only on `Err`. An
    /// `Ok` here would retry a permanently-gone peer every
    /// `REPLICATION_INITIAL_BACKOFF` (250 ms) forever, never climbing toward
    /// `REPLICATION_MAX_BACKOFF`.
    #[tokio::test]
    async fn subscription_loss_is_an_error_so_backoff_can_escalate() {
        let (_dir, local) = fresh_local().await;
        let upstream: Arc<dyn Replica> = Arc::new(DyingUpstreamReplica);
        let clock = Arc::new(HlcClock::new(Duration::from_millis(200)));
        let sub = ReplicationSubscriber::new(upstream_id(9), clock, upstream, local);

        let err = sub
            .run()
            .await
            .result
            .expect_err("a dead subscription must not look like a clean finish");
        assert!(
            matches!(err, Error::SubscriptionEnded { .. }),
            "expected SubscriptionEnded, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn subscriber_integrates_inline_records() {
        let (_dir, local) = fresh_local().await;

        let m1 = make_meta(1, 300);
        let m2 = make_meta(2, 300);
        let items: Vec<(LogIndex, Data)> = vec![
            (
                LogIndex(0),
                Data {
                    data_id: m1,
                    blob: Bytes::from_static(b"a"),
                },
            ),
            (
                LogIndex(1),
                Data {
                    data_id: m2,
                    blob: Bytes::from_static(b"b"),
                },
            ),
        ];
        let peer = upstream_id(1);
        let upstream: Arc<dyn Replica> = Arc::new(FakeUpstreamReplica::new(items));
        let clock = Arc::new(HlcClock::new(Duration::from_millis(200)));
        let sub = ReplicationSubscriber::new(peer, clock, upstream, local.clone());

        run_expecting_end(sub).await;

        use futures::TryStreamExt as _;
        let all: Vec<crate::meta::Data> = local
            .stream_above_hlc(None)
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .expect("stream");
        assert!(all.iter().any(|d| d.data_id == m1));
        assert!(all.iter().any(|d| d.data_id == m2));
        assert_eq!(
            local.subscription_cursor(peer).await.expect("ok"),
            Some(LogIndex(1))
        );
    }

    #[tokio::test]
    async fn subscriber_integrates_full_data() {
        let (_dir, local) = fresh_local().await;

        let m = make_meta(7, 300);
        let body = Data {
            data_id: m,
            blob: Bytes::from_static(b"the body"),
        };
        let items: Vec<(LogIndex, Data)> = vec![(LogIndex(0), body.clone())];
        let peer = upstream_id(2);
        let upstream: Arc<dyn Replica> = Arc::new(FakeUpstreamReplica::new(items));
        let clock = Arc::new(HlcClock::new(Duration::from_millis(200)));
        let sub = ReplicationSubscriber::new(peer, clock, upstream, local.clone());
        run_expecting_end(sub).await;

        use futures::TryStreamExt as _;
        let all: Vec<Data> = local
            .stream_above_hlc(None)
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .expect("stream");
        assert!(all.iter().any(|d| d == &body));
        assert_eq!(
            local.subscription_cursor(peer).await.expect("ok"),
            Some(LogIndex(0))
        );
    }

    // ── per-attempt progress accounting ──────────────────────────────────

    /// The progress measure must count only *durable* advance, and must report
    /// exactly zero when an attempt retired nothing — that zero is the signal
    /// that separates a shed loop from a flaky peer.
    #[test]
    fn cursor_delta_measures_durable_progress() {
        assert_eq!(cursor_delta(None, None), 0, "no cursor yet = no progress");
        assert_eq!(
            cursor_delta(Some(LogIndex(7)), Some(LogIndex(7))),
            0,
            "an attempt that retired nothing must report zero, not one",
        );
        assert_eq!(
            cursor_delta(None, Some(LogIndex(0))),
            1,
            "a fresh learner reaching index 0 retired one record",
        );
        assert_eq!(cursor_delta(Some(LogIndex(4)), Some(LogIndex(9))), 5);
    }

    // ── B-1 purge-gap detection ──────────────────────────────────────────

    /// Fold a delivered sequence the way `stream_from` does — one item at a
    /// time, carrying the expectation forward — so the tests exercise the same
    /// path production takes rather than a batch helper that no caller uses.
    fn fold_gaps(indices: &[u64], expected_next: LogIndex) -> (LogIndex, u64) {
        let mut expected = expected_next;
        let mut gaps: u64 = 0;
        for i in indices {
            let (next, gapped) = detect_purge_gap(LogIndex(*i), expected);
            expected = next;
            if gapped {
                gaps = gaps.saturating_add(1);
            }
        }
        (expected, gaps)
    }

    #[test]
    fn detect_purge_gap_finds_leading_gap() {
        // Fresh learner (expected next = 0); first surviving index is 5, so
        // entries 0..=4 were purged on the replica.
        let (next, gaps) = fold_gaps(&[5, 6, 7], LogIndex::next_after(None));
        assert_eq!(gaps, 1, "one leading gap (indices 0..=4 purged)");
        assert_eq!(next, LogIndex(8));
    }

    #[test]
    fn detect_purge_gap_none_when_contiguous() {
        let (next, gaps) = fold_gaps(&[3, 4], LogIndex::next_after(Some(LogIndex(2))));
        assert_eq!(gaps, 0, "contiguous delivery must report no gaps");
        assert_eq!(next, LogIndex(5));
    }

    #[test]
    fn detect_purge_gap_mid_stream_and_ignores_duplicates() {
        // expected 3; a duplicate index 2 from the subscribe overlap arrives
        // first (ignored, no regress), then contiguous 3, then a jump to 7 →
        // one gap (4..=6).
        let (next, gaps) = fold_gaps(&[2, 3, 7], LogIndex(3));
        assert_eq!(
            gaps, 1,
            "one mid-stream gap (4..=6 purged); the duplicate 2 is ignored"
        );
        assert_eq!(next, LogIndex(8));
    }

    /// A duplicate must never pull the expectation backwards: the next real
    /// index would then read as a gap and inflate the purge counter.
    #[test]
    fn detect_purge_gap_duplicate_does_not_regress_the_expectation() {
        let (next, gapped) = detect_purge_gap(LogIndex(2), LogIndex(5));
        assert_eq!(next, LogIndex(5), "expectation held");
        assert!(!gapped, "a replay is not a gap");
    }

    /// End-to-end: a fresh learner whose upstream delivers only the survivors
    /// of a purge (indices 5,6,7; 0..=4 purged) must still integrate them and
    /// advance the cursor past the gap — the gap is surfaced, not fatal.
    #[tokio::test]
    async fn subscriber_integrates_across_purge_gap_and_advances_cursor() {
        let (_dir, local) = fresh_local().await;
        let m5 = make_meta(5, 300);
        let m7 = make_meta(7, 300);
        let items: Vec<(LogIndex, Data)> = vec![
            (
                LogIndex(5),
                Data {
                    data_id: m5,
                    blob: Bytes::from_static(b"e5"),
                },
            ),
            (
                LogIndex(6),
                Data {
                    data_id: make_meta(6, 300),
                    blob: Bytes::from_static(b"e6"),
                },
            ),
            (
                LogIndex(7),
                Data {
                    data_id: m7,
                    blob: Bytes::from_static(b"e7"),
                },
            ),
        ];
        let peer = upstream_id(1);
        let upstream: Arc<dyn Replica> = Arc::new(FakeUpstreamReplica::new(items));
        let clock = Arc::new(HlcClock::new(Duration::from_millis(200)));
        let sub = ReplicationSubscriber::new(peer, clock, upstream, local.clone());

        run_expecting_end(sub).await;

        use futures::TryStreamExt as _;
        let data_ids: Vec<crate::ids::DataId> = local
            .stream_above_hlc(None)
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .expect("stream")
            .into_iter()
            .map(|d| d.data_id)
            .collect();
        assert!(data_ids.contains(&m5));
        assert!(data_ids.contains(&m7));
        assert_eq!(
            local.subscription_cursor(peer).await.expect("ok"),
            Some(LogIndex(7)),
            "cursor must advance past the purge gap to the max surviving index",
        );
    }
}
