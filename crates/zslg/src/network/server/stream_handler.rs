//! The server end of the two persistent streams: [`run_write`] (writer→replica)
//! and [`run_subscribe`] (log tailing). Both are handed a whole connection after
//! its handshake and own it until it dies.
//!
//! Tokio is kept out of the writer→replica hot path. Once adopted, the socket is
//! a blocking `std::net::TcpStream` driven by two loops, and the replica write is
//! opened **once** for the connection — so a write costs a channel send, not an
//! async dispatch plus per-write setup:
//!
//! ```text
//!               ┌── reader loop ──> data_ch ──┐
//!   socket ─────┤                             ├──> replica.write() ──┐
//!               └── drift ────────> drift_ch ─┘                      │
//!                                    (merged)                        │
//!               ┌── writer loop <── outcomes, out of order, by DataId┘
//!   socket <────┘  coalesces everything ready now into one flush
//! ```
//!
//! Drift is the reason the two ingress channels merge rather than one path
//! handling both: a frame whose HLC is out of skew must fail *only itself*, so it
//! bypasses the replica and joins the outcome stream directly. Tearing the
//! connection down instead would punish every concurrent write on it.
//!
//! Both loops run on `spawn_blocking` rather than dedicated threads: the parked
//! count is bounded by cluster fan-in (≈2 per inbound connection), far under the
//! pool. They also drop their `ReplicaClient` as soon as the streams are open —
//! a loop parked in `read()` must not pin the replica, or a shutdown followed by
//! a fast relaunch hits "dir already open".

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::api::{ReplicaClient, StampedWrite};
use crate::error::Error;
use crate::ids::LogIndex;
use crate::network::framing::{
    AsyncFrameSink, AsyncFrameStream, TcpFrameSink, TcpFrameStream, adopt_blocking,
};
use crate::network::messages::{RpcReplicaRequest, RpcRequest, WriteOutcome, WriteStreamResponse};
use crossbeam_combinators::{AbortReason, Channels, End};

/// In-flight window between the socket reader and the replica's write pipeline.
/// Backpressure, not buffering: a peer that outruns the replica's flush loop
/// blocks in the reader rather than growing an unbounded queue. Sized to cover a
/// batch burst without throttling the steady state.
const WRITE_STREAM_CAPACITY: usize = 4096;

/// Maximum items coalesced before forcing a flush. Large enough to amortize
/// the frame overhead without building excessive TCP buffers.
const MAX_BATCH: usize = 512;

/// Server end of a persistent write stream — see the header for the shape.
pub(in crate::network) async fn run_write(
    client: Arc<dyn ReplicaClient>,
    frame_recv: AsyncFrameStream,
    sink: AsyncFrameSink,
    cancel: CancellationToken,
) {
    // Adopt the connection as a blocking std socket (off the reactor).
    let (blk_sink, blk_recv, raw) = match adopt_blocking(sink, frame_recv) {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "write stream: could not adopt blocking socket");
            return;
        }
    };

    let (data_tx, data_rx) = crossbeam_channel::bounded::<StampedWrite>(WRITE_STREAM_CAPACITY);
    let (drift_tx, drift_rx) = crossbeam_channel::bounded::<Result<crate::api::WriteOutcome, Error>>(
        WRITE_STREAM_CAPACITY,
    );
    // Connection-scoped: these end only when the connection dies, which for the
    // peer is an error it retries — never a declared completion.
    let data_ch = Channels::from_infinite(data_rx, "write stream ingress");
    let drift_ch = Channels::from_infinite(drift_rx, "write stream drift");
    let outcomes = client.write(data_ch).merge(drift_ch);
    // `outcomes` owns everything now (§ header: a parked loop must not pin it).
    drop(client);

    let reader = tokio::task::spawn_blocking(move || reader_loop(blk_recv, &data_tx, &drift_tx));
    let writer = tokio::task::spawn_blocking(move || writer_loop(blk_sink, outcomes));

    // Join both off the reactor; on cancel, force both socket ops to return so the
    // blocking loops exit.
    let join = async move {
        let _ = reader.await;
        let _ = writer.await;
    };
    tokio::pin!(join);
    tokio::select! {
        () = cancel.cancelled() => {
            let _ = raw.shutdown(std::net::Shutdown::Both);
            join.await;
        }
        () = &mut join => {}
    }
    drop(raw);
}

/// Blocking reader loop. Anything that is not a `Write` frame — a decode error,
/// an unexpected frame, or stream end — stops the read side and drops `data_tx`;
/// the replica then drains its in-flight writes and ends the outcome stream,
/// which shuts the writer down in turn. That cascade is the only teardown path.
fn reader_loop(
    mut recv: TcpFrameStream,
    data_tx: &crossbeam_channel::Sender<StampedWrite>,
    drift_tx: &crossbeam_channel::Sender<Result<crate::api::WriteOutcome, Error>>,
) {
    loop {
        match recv.next_frame_raw_blocking::<RpcRequest>() {
            Some(Ok((RpcRequest::Replica(RpcReplicaRequest::Write { data }), drift))) => {
                let fed = match drift {
                    Some(drift_err) => drift_tx
                        .send(Ok(crate::api::WriteOutcome::Failed {
                            data_id: data.data_id,
                            error: drift_err,
                        }))
                        .is_ok(),
                    None => data_tx.send(StampedWrite::now(*data)).is_ok(),
                };
                if !fed {
                    return; // replica stream or writer gone
                }
            }
            // A transport/decode error, a non-Write frame, or stream end.
            Some(Ok(_)) | Some(Err(_)) | None => return,
        }
    }
}

/// Blocking writer thread: drain completed [`WriteOutcome`]s, frame each as a
/// `WriteStreamResponse`, and write them — coalescing every outcome ready *right
/// now* into one flush per burst. Exits when the channel closes (reader done +
/// all in-flight committed), on a terminal stream error, or when the socket
/// breaks.
fn writer_loop(
    mut sink: TcpFrameSink,
    mut outcomes: Channels<Result<crate::api::WriteOutcome, Error>>,
) {
    loop {
        let Some(batch) = outcomes.ready_batch(MAX_BATCH) else {
            return; // disconnected
        };
        for item in batch {
            let outcome = match item {
                Ok(outcome) => outcome,
                Err(_) => return,
            };
            if sink.feed_blocking(outcome_to_response(outcome)).is_err() {
                return;
            }
        }
        if sink.flush_blocking().is_err() {
            return;
        }
    }
}

/// Map a replica [`WriteOutcome`](crate::api::WriteOutcome) onto its wire
/// `WriteStreamResponse` (1:1).
fn outcome_to_response(outcome: crate::api::WriteOutcome) -> WriteStreamResponse {
    match outcome {
        crate::api::WriteOutcome::Accepted {
            data_id,
            raft_state,
        } => WriteStreamResponse {
            data_id,
            outcome: WriteOutcome::Accepted { raft_state },
        },
        crate::api::WriteOutcome::Rejected {
            data_id,
            error,
            raft_state,
        } => WriteStreamResponse {
            data_id,
            outcome: WriteOutcome::Rejected { error, raft_state },
        },
        crate::api::WriteOutcome::Failed { data_id, error } => WriteStreamResponse {
            data_id,
            outcome: WriteOutcome::Failed { error },
        },
    }
}

pub(in crate::network) async fn run_subscribe(
    client: Arc<dyn ReplicaClient>,
    from: Option<LogIndex>,
    frame_recv: AsyncFrameStream,
    sink: AsyncFrameSink,
    cancel: CancellationToken,
) {
    let (blk_sink, _blk_recv, raw) = match adopt_blocking(sink, frame_recv) {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "subscribe stream: could not adopt");
            return;
        }
    };
    let items = client.subscribe_from(from);
    drop(client);
    let writer = tokio::task::spawn_blocking(move || subscribe_writer_loop(blk_sink, items));
    let join = async move {
        let _ = writer.await;
    };
    tokio::pin!(join);
    tokio::select! {
        () = cancel.cancelled() => { let _ = raw.shutdown(std::net::Shutdown::Both); join.await; }
        () = &mut join => {}
    }
    drop(raw);
}

/// Blocking writer for a subscription stream, with a per-item ordering check.
///
/// A *forward* jump is benign — entries purged between the cursor and the
/// surviving range — and is only counted. An index going **backwards** means the
/// serving replica's log is corrupt (it is gapless, single-writer, and read in
/// index order, so a reorder or duplicate cannot happen in correct operation):
/// log it, count it, and close the stream. The subscriber reconnects with
/// exponential backoff (`learner/subscriber/manager.rs`), so closing cannot hot
/// loop, and `zslg_log_subscription_reorders_total` is the operator signal.
///
/// Deliberately not a `panic!`: its blast radius depends on the final binary's
/// panic strategy, which a library does not control — under `unwind` it killed
/// only this thread anyway (`run_subscribe` swallows the `JoinError`), so it
/// bought nothing over closing the connection.
fn subscribe_writer_loop(mut sink: TcpFrameSink, mut items: crate::ordered::ReplicaStream) {
    loop {
        let Some(batch) = items.ready_batch(MAX_BATCH) else {
            if let End::Aborted {
                reason: AbortReason::OrderViolation { what },
            } = items.end_reason()
            {
                crate::metrics::subscription_reorder();
                tracing::error!(
                    what,
                    "subscription stream closed: replica log index violated strict order"
                );
            }
            return; // disconnected or order violation
        };
        for item in batch {
            let (idx, record) = match item {
                Ok(pair) => pair,
                Err(_) => return,
            };
            if sink
                .feed_blocking(crate::network::messages::SubscribeStreamItem {
                    source_index: idx,
                    record,
                })
                .is_err()
            {
                return;
            }
        }
        if sink.flush_blocking().is_err() {
            return;
        }
    }
}
