//! Per-connection write stream — the remote half of
//! [`Replica::write`](crate::api::Replica::write).
//!
//! **One call, one TCP connection, no reopen.** The stream drives writes off
//! `input` and yields their outcomes; when the connection breaks it simply
//! disconnects the output — the **caller** (the writer coordinator) owns reopen and
//! knows which writes are still un-acked, so it resends them on the next call. That
//! is why nothing here tracks in-flight writes: there is nothing to fail on break.
//!
//! Responses are keyed by `DataId` on the wire (already unique — 64 random bits +
//! HLC), so the server may reply in any order.

use std::sync::Weak;

use crossbeam_channel::Sender;

use crate::api::{StampedWrite, WriteOutcome};
use crate::channel::Channels;
use crate::error::Error;
use crate::meta::Data;
use crate::metrics;
use crate::network::client::{RpcClient, Target};
use crate::network::framing::{TcpFrameSink, TcpFrameStream, adopt_blocking};
use crate::network::messages::WriteOutcome as WireWriteOutcome;
use crate::network::messages::{RpcReplicaRequest, RpcRequest, WriteStreamResponse};

/// Open a write stream to `target` over **one** connection. Drives writes off
/// `input`, yielding their outcomes. See [`Replica::write`](crate::api::Replica::write).
///
/// Only the connect is async; once dialed, the socket is adopted as a blocking std
/// stream and driven **fully synchronously** by two `spawn_blocking` loops — the
/// writer pulls `input` straight onto the wire (no intermediate channel), the reader
/// decodes responses into the output. Symmetric with the server (`run_write`).
#[must_use]
pub(in crate::network) fn open_write(
    client: Weak<RpcClient>,
    target: Target,
    input: Channels<StampedWrite>,
    cap: usize,
) -> Channels<Result<WriteOutcome, Error>> {
    // cap ≥ the in-flight window (the writer's permit pool), so the reader's outcome
    // sends never fill.
    let (out_tx, out_rx) = crossbeam_channel::bounded::<Result<WriteOutcome, Error>>(cap);

    let ch = Channels::from_infinite(out_rx, "client write stream");
    tokio::spawn(async move {
        let Some(rpc) = client.upgrade() else {
            let _ = out_tx.send(Err(Error::Shutdown));
            return; // node shutting down
        };
        let dialed = rpc.dial_write_stream(&target).await;
        drop(rpc); // don't pin the RpcClient while the connection runs
        let (sink, stream) = match dialed {
            Ok(pair) => pair,
            // Terminal for this connection: report + disconnect; the caller reopens.
            Err(e) => {
                let _ = out_tx.send(Err(e));
                return;
            }
        };
        let (blk_sink, blk_stream, closer) = match adopt_blocking(sink, stream) {
            Ok(t) => t,
            Err(e) => {
                let _ = out_tx.send(Err(e));
                return;
            }
        };

        // Writer owns `input` and writes straight to the socket (no bridge); reader
        // owns `out_tx` and decodes responses. `spawn_blocking` — one connection.
        //
        // The writer force-closes the socket **itself**, the moment its loop ends,
        // from inside its own blocking task — that close is what returns the
        // reader's blocking `read()`. It must not live out here: an async task can
        // stop being polled (a runtime shutting down drops it), and the reader
        // would then park in `read()` forever, which in turn blocks the blocking
        // pool's own shutdown indefinitely. Nothing that unblocks a parked
        // blocking loop may depend on async cleanup running.
        //
        // Both exit orders end up there. Writer first: it closes, the reader hits
        // EOF. Reader first (peer closed / decode error): its `out_tx` drops, the
        // output disconnects, the coordinator — which sees `End::Aborted`, reopens
        // with backoff and resends everything un-acked — stops feeding `input`, and
        // the writer ends in turn.
        tokio::task::spawn_blocking(move || {
            writer_loop(input, blk_sink);
            let _ = closer.shutdown(std::net::Shutdown::Both);
        });
        tokio::task::spawn_blocking(move || reader_loop(blk_stream, out_tx));
    });

    ch
}

/// Blocking writer loop: pull writes off `input` and frame them onto the socket,
/// batched (one flush per burst). Ends when `input` drains or the socket breaks.
fn writer_loop(mut input: Channels<StampedWrite>, mut sink: TcpFrameSink) {
    const MAX_BATCH: usize = 512;
    loop {
        let Some(batch) = input.ready_batch(MAX_BATCH) else {
            return; // input drained
        };
        let send_t0 = std::time::Instant::now();
        for item in batch {
            metrics::write_stream_input_wait(item.enqueued.elapsed());
            if sink.feed_blocking(write_req(item.data)).is_err() {
                return;
            }
        }
        if sink.flush_blocking().is_err() {
            return;
        }
        metrics::write_stream_send(send_t0.elapsed());
    }
}

fn write_req(data: Data) -> RpcRequest {
    RpcRequest::Replica(RpcReplicaRequest::Write {
        data: Box::new(data),
    })
}

/// Blocking reader loop: decode responses (with the per-frame HLC-drift verdict) and
/// resolve each into `out`. Ends on decode error / EOF, or when the consumer drops.
fn reader_loop(mut stream: TcpFrameStream, out: Sender<Result<WriteOutcome, Error>>) {
    // The drift verdict lets a skewed frame fail as its own write instead of tearing
    // the connection down.
    while let Some(Ok((resp, drift))) = stream.next_frame_raw_blocking::<WriteStreamResponse>() {
        if resolve_response(&out, resp, drift).is_err() {
            return; // consumer dropped the output
        }
    }
}

/// Map one decoded response onto a [`WriteOutcome`] and hand it to `out`. A
/// drift frame (its HLC exceeded the skew limit) fails only that write. Returns
/// `Err` if the consumer has dropped the output.
fn resolve_response(
    out: &crossbeam_channel::Sender<Result<WriteOutcome, Error>>,
    resp: WriteStreamResponse,
    drift: Option<Error>,
) -> Result<(), ()> {
    let WriteStreamResponse { data_id, outcome } = resp;
    let write_outcome = if let Some(drift_err) = drift {
        WriteOutcome::Failed {
            data_id,
            error: drift_err,
        }
    } else {
        match outcome {
            WireWriteOutcome::Accepted { raft_state } => WriteOutcome::Accepted {
                data_id,
                raft_state,
            },
            WireWriteOutcome::Rejected { error, raft_state } => WriteOutcome::Rejected {
                data_id,
                error,
                raft_state,
            },
            WireWriteOutcome::Failed { error } => WriteOutcome::Failed { data_id, error },
        }
    };
    // `try_send`: cap ≥ the in-flight window, so a Full would mean the consumer has
    // stalled a full window deep (dead connection) — never block the reactor.
    match out.try_send(Ok(write_outcome)) {
        Ok(()) => Ok(()),
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => Err(()),
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            // Unreachable by the sizing invariant (cap ≥ in-flight window); if it
            // ever fires the connection is effectively dead and the write is resent
            // on reopen. Drop rather than block the reactor.
            metrics::writer_outcome_dropped();
            tracing::warn!("write-stream outcome channel full; dropping (consumer stalled)");
            Ok(())
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::hlc::Hlc;
    use crate::ids::DataId;

    fn data_id(tag: u8) -> DataId {
        DataId([tag; 32])
    }

    #[test]
    fn drift_response_fails_only_its_own_write() {
        let (out, rx) = crossbeam_channel::unbounded();
        let drift = Some(Error::HlcDriftTooLarge {
            peer: Hlc(0),
            local: Hlc(0),
        });
        resolve_response(
            &out,
            WriteStreamResponse {
                data_id: data_id(1),
                outcome: WireWriteOutcome::Failed {
                    error: Error::Rpc("unused on drift path".into()),
                },
            },
            drift,
        )
        .unwrap();
        assert!(matches!(
            rx.try_recv(),
            Ok(Ok(WriteOutcome::Failed {
                error: Error::HlcDriftTooLarge { .. },
                ..
            }))
        ));
    }

    #[test]
    fn answered_response_maps_to_outcome() {
        let (out, rx) = crossbeam_channel::unbounded();
        resolve_response(
            &out,
            WriteStreamResponse {
                data_id: data_id(1),
                outcome: WireWriteOutcome::Failed {
                    error: Error::Rpc("boom".into()),
                },
            },
            None,
        )
        .unwrap();
        assert!(matches!(
            rx.try_recv(),
            Ok(Ok(WriteOutcome::Failed {
                error: Error::Rpc(_),
                ..
            }))
        ));
    }

    #[test]
    fn resolving_into_a_dropped_consumer_reports_shutdown() {
        let (out, rx) = crossbeam_channel::unbounded();
        drop(rx);
        let res = resolve_response(
            &out,
            WriteStreamResponse {
                data_id: data_id(1),
                outcome: WireWriteOutcome::Failed {
                    error: Error::Rpc("boom".into()),
                },
            },
            None,
        );
        assert!(res.is_err());
    }
}
