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
use std::sync::Arc;
use zslg_consensus::ConsensusError;
use zslg_consensus::NodeId;
use zslg_log::Data;
use zslg_log::LogError;
use zslg_log::LogIndex;
use zslg_log::{LocalStoreMut, Replica};
use zslg_time::Clock;

/// Sample `zslg_hlc_skew_seconds` for roughly every Nth replicated item — a
/// per-peer drift signal, not a per-item cost.
const SKEW_SAMPLE_STRIDE: usize = 8;

/// What one subscription attempt did and why it stopped. See
/// [`ReplicationSubscriber::run`].
pub(crate) struct AttemptOutcome {
    /// Records this attempt made **durable** — the advance of the peer's
    /// persisted subscription cursor. Zero means the attempt accomplished
    /// nothing, whatever the error says.
    pub records_retired: u64,
    pub result: Result<(), Error>,
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

pub(crate) struct ReplicationSubscriber {
    peer_id: NodeId,
    clock: Arc<dyn Clock>,
    upstream: Arc<dyn Replica>,
    local_store: Arc<dyn LocalStoreMut>,
}

impl ReplicationSubscriber {
    #[must_use]
    pub(crate) fn new(
        peer_id: NodeId,
        clock: Arc<dyn Clock>,
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
    pub(crate) async fn run(self) -> AttemptOutcome {
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
                    result: Err(e.into()),
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
        // Decided once, not per item, so skew sampling stays "every Nth item".
        let stride = u64::try_from(SKEW_SAMPLE_STRIDE).unwrap_or(8);

        // The upstream is already `StrictlyIncreasing`: forward purge gaps are
        // allowed and duplicate/backwards indices abort inside the channel.
        // This filter only tags, samples and meters the accepted items.
        let stream = self
            .upstream
            .subscribe_from(resume_from)
            .into_channel()
            .filter_map(move |item: Result<(LogIndex, Data), zslg_log::Error>| {
                let (idx, data) = match item {
                    Ok(v) => v,
                    // In-band: `integrate_loop` stops on it and returns it.
                    Err(e) => return Some(Err(e)),
                };
                if idx.raw().checked_rem(stride).unwrap_or(1) == 0 {
                    let skew_s = clock.skew_against(data.data_id.hlc()).as_secs_f64();
                    crate::metrics::hlc_skew(peer_id, skew_s);
                }
                crate::metrics::subscription_messages(1);
                Some(Ok((peer_id, idx, data)))
            });

        // A subscription is infinite by contract: the replica's append stream
        // never completes, so even a *declared* close is the upstream going
        // away — it only ever comes from a stub with nothing to serve. Both
        // must be errors, because `manager` escalates its backoff ladder and
        // logs only on `Err`; returning `Ok` here re-subscribed every 250 ms
        // in silence, never climbing toward REPLICATION_MAX_BACKOFF.
        match self.local_store.integrate_loop(stream.into_finite()).await {
            Ok(()) => Err(ConsensusError::subscription_ended(
                peer_id,
                "upstream declared the stream complete, but a \
                 subscription cannot complete",
            )
            .into()),
            Err(zslg_log::Error::Log(LogError::SubscriptionStreamEnded { reason })) => {
                Err(ConsensusError::subscription_ended(peer_id, reason).into())
            }
            Err(other) => Err(other.into()),
        }
    }
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
    use crossbeam_combinators::testing::ChannelTestExt as _;
    use std::sync::Arc;
    use std::sync::Mutex;
    use zslg_consensus::ConsensusError;

    use async_trait::async_trait;
    use bytes::Bytes;

    use futures::stream::BoxStream;

    use super::*;
    use crossbeam_combinators::{Channel, Finite, Infinite};
    use zslg_consensus::ReplicaStatus;
    use zslg_consensus_fake::upstream as upstream_id;
    use zslg_log::Data;
    use zslg_log::LogIndex;
    use zslg_log::{LocalStoreView, Replica, StampedWrite, WriteOutcome};
    use zslg_log_fake::{FakeLocalStore, make_meta};
    use zslg_time_fake::CountingClock;

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
            matches!(
                err,
                Error::Consensus(ConsensusError::SubscriptionEnded { .. })
            ),
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
        fn write(
            &self,
            _: Channel<StampedWrite, Finite>,
        ) -> Channel<Result<WriteOutcome, zslg_log::Error>, Infinite> {
            let run = || -> Channel<Result<WriteOutcome, Error>, Infinite> { unimplemented!() };
            let result: Channel<Result<WriteOutcome, Error>, Infinite> = run();
            result.err_into()
        }
        async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
            let result: Result<ReplicaStatus, Error> = async { unimplemented!() }.await;
            result.map_err(Into::into)
        }
        fn subscribe_from(&self, _from: Option<LogIndex>) -> zslg_log::ReplicaStream {
            let items = self.items.lock().expect("lock").take().unwrap_or_default();
            Channel::from_vec(
                items
                    .into_iter()
                    .map(|(idx, data)| Ok((idx, data)))
                    .collect(),
            )
            .chain(Channel::aborted("test replica"))
            .ordered(None, "replica subscription")
        }
        async fn subscribe_status(
            &self,
        ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error>
        {
            let result: Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> =
                async { unimplemented!() }.await;
            result
                .map(|s| {
                    futures::StreamExt::boxed(futures::StreamExt::map(s, |r| r.map_err(Into::into)))
                })
                .map_err(Into::into)
        }
    }

    /// A [`Replica`] stub whose subscription is a real *infinite* stream that
    /// then dies — a peer going away mid-stream.
    struct DyingUpstreamReplica;

    #[async_trait]
    impl Replica for DyingUpstreamReplica {
        fn write(
            &self,
            _: Channel<StampedWrite, Finite>,
        ) -> Channel<Result<WriteOutcome, zslg_log::Error>, Infinite> {
            let run = || -> Channel<Result<WriteOutcome, Error>, Infinite> { unimplemented!() };
            let result: Channel<Result<WriteOutcome, Error>, Infinite> = run();
            result.err_into()
        }
        async fn fetch_status(&self) -> Result<ReplicaStatus, zslg_log::Error> {
            let result: Result<ReplicaStatus, Error> = async { unimplemented!() }.await;
            result.map_err(Into::into)
        }
        fn subscribe_from(&self, _from: Option<LogIndex>) -> zslg_log::ReplicaStream {
            let (tx, rx) = crossbeam_channel::bounded::<Result<(LogIndex, Data), Error>>(1);
            drop(tx); // the peer went away
            (Channel::from_infinite(rx, "client subscribe stream")
                .ordered(None, "replica subscription"))
            .err_into()
        }
        async fn subscribe_status(
            &self,
        ) -> Result<BoxStream<'static, Result<ReplicaStatus, zslg_log::Error>>, zslg_log::Error>
        {
            let result: Result<BoxStream<'static, Result<ReplicaStatus, Error>>, Error> =
                async { unimplemented!() }.await;
            result
                .map(|s| {
                    futures::StreamExt::boxed(futures::StreamExt::map(s, |r| r.map_err(Into::into)))
                })
                .map_err(Into::into)
        }
    }

    /// A subscription that ends must surface as `Err`, because
    /// `subscriber::manager` escalates its reconnect backoff only on `Err`. An
    /// `Ok` here would retry a permanently-gone peer every
    /// `REPLICATION_INITIAL_BACKOFF` (250 ms) forever, never climbing toward
    /// `REPLICATION_MAX_BACKOFF`.
    #[tokio::test]
    async fn subscription_loss_is_an_error_so_backoff_can_escalate() {
        let local = Arc::new(FakeLocalStore::new());
        let upstream: Arc<dyn Replica> = Arc::new(DyingUpstreamReplica);
        let clock = Arc::new(CountingClock::new());
        let sub = ReplicationSubscriber::new(upstream_id(9), clock, upstream, local);

        let err = sub
            .run()
            .await
            .result
            .expect_err("a dead subscription must not look like a clean finish");
        assert!(
            matches!(
                err,
                Error::Consensus(ConsensusError::SubscriptionEnded { .. })
            ),
            "expected SubscriptionEnded, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn subscriber_integrates_inline_records() {
        let local = Arc::new(FakeLocalStore::new());

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
        let clock = Arc::new(CountingClock::new());
        let sub = ReplicationSubscriber::new(peer, clock, upstream, local.clone());

        run_expecting_end(sub).await;

        use futures::TryStreamExt as _;
        let all: Vec<zslg_log::Data> = local
            .stream_above_hlc(None)
            .into_test_stream()
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
        let local = Arc::new(FakeLocalStore::new());

        let m = make_meta(7, 300);
        let body = Data {
            data_id: m,
            blob: Bytes::from_static(b"the body"),
        };
        let items: Vec<(LogIndex, Data)> = vec![(LogIndex(0), body.clone())];
        let peer = upstream_id(2);
        let upstream: Arc<dyn Replica> = Arc::new(FakeUpstreamReplica::new(items));
        let clock = Arc::new(CountingClock::new());
        let sub = ReplicationSubscriber::new(peer, clock, upstream, local.clone());
        run_expecting_end(sub).await;

        use futures::TryStreamExt as _;
        let all: Vec<Data> = local
            .stream_above_hlc(None)
            .into_test_stream()
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

    /// End-to-end: a fresh learner whose upstream delivers only the survivors
    /// of a purge (indices 5,6,7; 0..=4 purged) must still integrate them and
    /// advance the cursor past the gap — the gap is surfaced, not fatal.
    #[tokio::test]
    async fn subscriber_integrates_across_purge_gap_and_advances_cursor() {
        let local = Arc::new(FakeLocalStore::new());
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
        let clock = Arc::new(CountingClock::new());
        let sub = ReplicationSubscriber::new(peer, clock, upstream, local.clone());

        run_expecting_end(sub).await;

        use futures::TryStreamExt as _;
        let data_ids: Vec<zslg_log::DataId> = local
            .stream_above_hlc(None)
            .into_test_stream()
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
