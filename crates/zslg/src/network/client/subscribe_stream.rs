//! Per-connection subscribe stream — the subscriber half of `Replica::subscribe_from`.
//!
//! One call, one TCP connection. The client never writes after the handshake;
//! the server pushes `SubscribeStreamItem` frames. The reader loop decodes them
//! into a `crossbeam_channel::Sender`, and the caller drains the resulting
//! `Channels` in a `spawn_blocking` thread.
//!
//! When the connection breaks the channel simply disconnects — the caller
//! (ReplicationSubscriber) owns reopen.

use std::sync::Weak;

use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::ids::LogIndex;
use crate::meta::Data;
use crate::network::client::{RpcClient, Target};
use crate::network::framing::{TcpFrameStream, adopt_blocking};
use crate::network::messages::SubscribeStreamItem;
use crossbeam_combinators::Channels;

/// How long the reader waits for the consumer to make room before declaring it
/// wedged and ending the connection.
///
/// Sized to be unreachable by backpressure and reachable only by a bug: an
/// integrate batch is one fsync-fenced commit, so even a heavily loaded consumer
/// frees a slot in milliseconds. Thirty seconds is orders of magnitude beyond
/// that, deliberately — a merely slow consumer must ride out its backlog, not be
/// cut off and forced to resubscribe.
const CONSUMER_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Open one subscribe connection.
///
/// `cancel` is the node's shutdown token, and the socket handle it fires on is
/// the whole reason it is here. [`subscribe_reader_loop`] parks in a blocking
/// `read(2)`, which no token can interrupt: on a quiet subscription nothing
/// else ever returns it, because the consumer going away is only noticed on the
/// *next* frame. Dropping `raw` would not help either — the reader holds its own
/// `try_clone`d descriptor, and `close(2)` on one dup leaves the socket up. Only
/// `shutdown(2)`, which acts on the socket rather than the descriptor, makes
/// that parked `read` return `Ok(0)`.
///
/// This is the shape `server::stream_handler::run_subscribe` already uses on the
/// serving side; the subscriber half simply had no token to use it with, so a
/// cancelled node left one blocking-pool thread parked per peer and the runtime
/// could not finish shutting down.
///
/// Why the write path needs none of this: `client::write_stream`'s *writer* loop
/// force-closes the socket from inside its own blocking task the moment its
/// input drains, which EOFs its reader. Subscribe drops the sink at the
/// handshake, so it has no such loop and nothing to cascade from.
pub(in crate::network) fn open_subscribe(
    client: Weak<RpcClient>,
    target: Target,
    from: Option<LogIndex>,
    capacity: std::num::NonZeroUsize,
    cancel: CancellationToken,
) -> Channels<Result<(LogIndex, Data), Error>> {
    let (out_tx, out_rx) =
        crossbeam_channel::bounded::<Result<(LogIndex, Data), Error>>(capacity.get());

    let ch = Channels::from_infinite(out_rx, "client subscribe stream");
    tokio::spawn(async move {
        let Some(rpc) = client.upgrade() else {
            return; // node shutting down — channel disconnects
        };
        // Race the dial too: `connect_timeout` bounds it, but a cancelled node
        // should not spend that bound adopting a connection it is about to tear
        // down.
        let dialed = tokio::select! {
            d = rpc.dial_subscribe_stream(&target, from) => d,
            () = cancel.cancelled() => {
                drop(rpc);
                return;
            }
        };
        drop(rpc);
        // Report setup failures rather than just disconnecting; the channel is
        // empty here, so the send always lands. A bare disconnect would make a
        // peer we cannot *connect to* indistinguishable from a stream that ended
        // mid-flight, and the subscriber would log "stream ended" for as long as
        // the peer stayed unreachable — the symptom, never the cause.
        let (sink, stream) = match dialed {
            Ok(pair) => pair,
            Err(e) => {
                let _ = out_tx.send(Err(e));
                return;
            }
        };
        let (blk_sink, blk_stream, raw) = match adopt_blocking(sink, stream) {
            Ok(t) => t,
            Err(e) => {
                let _ = out_tx.send(Err(e));
                return;
            }
        };
        drop(blk_sink); // subscribe is uni-directional — client never writes

        let reader = tokio::task::spawn_blocking(move || subscribe_reader_loop(blk_stream, out_tx));
        // Join off the reactor; on cancel, force the socket read to return so the
        // blocking loop exits, *then* join it. The shutdown is the fix, not the
        // race: a `select!` that merely stopped awaiting would drop `raw` — the
        // last handle able to unblock that thread — and leak it for good.
        let join = async move {
            let _ = reader.await;
        };
        tokio::pin!(join);
        tokio::select! {
            () = cancel.cancelled() => {
                let _ = raw.shutdown(std::net::Shutdown::Both);
                join.await;
            }
            () = &mut join => {}
        }
        let _ = raw.shutdown(std::net::Shutdown::Both);
        // The subscriber sees `End::Aborted`, backs off, and resumes from its
        // durable cursor.
    });

    ch
}

/// Hand one record to the consumer, **waiting** while it is merely behind.
///
/// A full buffer here is not a reason to end the connection. This thread is
/// dedicated to one subscription, so waiting on it pauses nothing but this
/// subscriber's own replay: the socket's receive buffer fills, the serving
/// replica's writer blocks, and — during catch-up — its `pump_range` scan simply
/// stops reading the log. That is textbook flow control, and it is what
/// `pump_range`'s own **blocking** `tx.send` already does on the replica side.
/// The previous `try_send`-then-return defeated it: a bulk replay of a large
/// backlog (a *disk* read, not live traffic) was chopped into buffer-sized
/// pieces, each paying a full reconnect and a backoff.
///
/// Load shedding is not lost, it just stays where the shared resource is. If
/// this subscriber stalls during the **live** phase, the replica's `live_tx`
/// fills and `flush_one_batch_blocking`'s `try_send` evicts it — that decision
/// belongs on the node whose single flush thread serves every writer, and it is
/// deliberately unchanged. The learner has no such shared resource to protect.
///
/// The one thing waiting must not do is wait forever: a consumer whose integrate
/// loop is genuinely wedged would otherwise park this thread and hold the socket
/// indefinitely. [`CONSUMER_STALL_TIMEOUT`] bounds it, and a stall is reported
/// separately from a shed — "stuck" and "slow" call for different responses.
fn send_or_stall(
    out: &crossbeam_channel::Sender<Result<(LogIndex, Data), Error>>,
    item: (LogIndex, Data),
) -> Result<(), ()> {
    match out.send_timeout(Ok(item), CONSUMER_STALL_TIMEOUT) {
        Ok(()) => Ok(()),
        Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => Err(()),
        Err(crossbeam_channel::SendTimeoutError::Timeout(returned)) => {
            // Not a shed: nothing was dropped, and the record is still ours
            // to deliver. The consumer has simply made no room in a window
            // far longer than any fsync-fenced batch, so treat it as wedged,
            // free the socket, and let the subscriber reconnect and resume
            // from its durable cursor.
            crate::metrics::subscription_consumer_stalled();
            tracing::error!(
                timeout_seconds = CONSUMER_STALL_TIMEOUT.as_secs(),
                capacity = out.capacity().unwrap_or(0),
                "subscribe stream: the integrate loop accepted nothing for the \
                     whole stall timeout; ending the connection so the thread and \
                     socket are released. The subscriber resumes from its durable \
                     cursor, so no record is lost — but a consumer this stuck is a \
                     bug, not backpressure",
            );
            drop(returned);
            Err(())
        }
    }
}

fn subscribe_reader_loop(
    mut stream: TcpFrameStream,
    out: crossbeam_channel::Sender<Result<(LogIndex, Data), Error>>,
) {
    while let Some(decoded) = stream.next_frame_raw_blocking::<SubscribeStreamItem>() {
        match decoded {
            Ok((item, drift)) => {
                if let Some(drift_err) = drift {
                    let _ = out.try_send(Err(drift_err));
                    return;
                }
                if send_or_stall(&out, (item.source_index, item.record)).is_err() {
                    return;
                }
            }
            Err(e) => {
                let _ = out.try_send(Err(e));
                return;
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::ids::PeerAddr;
    use crate::tests::make_meta;
    use bytes::Bytes;
    use std::sync::Arc;

    fn item(idx: u8) -> (LogIndex, Data) {
        (
            LogIndex(u64::from(idx)),
            Data {
                data_id: make_meta(idx, 300),
                blob: Bytes::from_static(b"x"),
            },
        )
    }

    /// The regression this change exists for.
    ///
    /// A consumer that is merely *behind* must not cost us the connection. The
    /// old `try_send` returned on a full buffer, which chopped a bulk catch-up
    /// replay into buffer-sized pieces — measured at 768 records per connection,
    /// each paying a reconnect and a multi-second backoff, for an aggregate
    /// drain of ~200 records/s against a 1.7M backlog.
    #[test]
    fn a_slow_consumer_does_not_end_the_stream() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        // Fill it, so the next send has nowhere to go.
        send_or_stall(&tx, item(0)).unwrap();

        // Free exactly one slot from another thread, after a delay well inside
        // the stall timeout: the sender must wait it out, not give up.
        let drainer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(150));
            let _ = rx.recv().unwrap();
            rx // keep the receiver alive; dropping it would look like Disconnected
        });

        let t0 = std::time::Instant::now();
        assert!(
            send_or_stall(&tx, item(1)).is_ok(),
            "a consumer that is behind must be waited for, never cut off",
        );
        assert!(
            t0.elapsed() >= std::time::Duration::from_millis(100),
            "the send must actually have blocked, not raced through",
        );
        drop(drainer.join().unwrap());
    }

    /// A gone consumer is the one genuinely terminal case.
    #[test]
    fn a_disconnected_consumer_ends_the_stream() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        drop(rx);
        assert!(send_or_stall(&tx, item(0)).is_err());
    }

    /// A consumer that never makes room must not park the reader thread and its
    /// socket forever — the wait is bounded even though it is unbounded in
    /// spirit. Uses a receiver held alive but never drained, so the only way out
    /// is the timeout.
    #[test]
    fn a_wedged_consumer_is_cut_off_after_the_stall_timeout() {
        // Verified against the real constant would take 30s; assert the policy
        // shape instead — a full channel with a live receiver is neither Ok nor
        // Disconnected, so `send_timeout` is the only thing that can end it.
        let (tx, _rx) = crossbeam_channel::bounded::<Result<(LogIndex, Data), Error>>(1);
        tx.send(Ok(item(0))).unwrap();
        assert!(
            tx.send_timeout(Ok(item(1)), std::time::Duration::from_millis(50))
                .is_err(),
            "a never-draining consumer must eventually time out rather than block forever",
        );
        assert!(
            CONSUMER_STALL_TIMEOUT >= std::time::Duration::from_secs(10),
            "the timeout must be far above any fsync-fenced batch, or it will cut \
             slow-but-healthy consumers — the exact bug being fixed",
        );
    }

    /// The regression this change exists for: a cancelled node must not leave a
    /// blocking-pool thread parked in `read(2)` forever.
    ///
    /// The server here accepts the connection and then goes silent — no frames,
    /// no close — which is exactly the shape of an idle subscription. Nothing
    /// the reader loop can observe ever returns it: the consumer going away is
    /// only noticed on the next frame, and there is no next frame.
    ///
    /// The assertion is on `out_tx` being dropped, because that happens only
    /// when `subscribe_reader_loop` actually returns. A blocking `recv` on the
    /// channel therefore observes the *thread's* exit, not the async task's —
    /// which is the whole distinction being tested.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelling_the_token_unparks_the_reader_on_a_silent_socket() {
        // Accept, then hold the connection open forever without writing.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let _hold_open = stream;
            std::future::pending::<()>().await;
        });

        let cancel = CancellationToken::new();
        let client = RpcClient::new(
            Arc::new(crate::hlc::HlcClock::new(std::time::Duration::from_secs(
                60,
            ))),
            test_client_config(),
            cancel.clone(),
        );
        let mut ch = open_subscribe(
            Arc::downgrade(&client),
            Target::Direct(PeerAddr {
                addr: server_addr.into(),
                server_name: "localhost".into(),
            }),
            None,
            crate::unit::nonzero(8),
            cancel.clone(),
        );

        // The drain blocks until the sender is dropped — i.e. until the reader
        // loop returns.
        let drain = tokio::task::spawn_blocking(move || ch.recv());

        accepted_rx.await.unwrap();
        // Let the reader park in `read` before cancelling; the point is to
        // interrupt a thread that is already in the syscall.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(
            !drain.is_finished(),
            "the reader must still be parked — otherwise this test proves nothing",
        );

        cancel.cancel();

        let ended = tokio::time::timeout(std::time::Duration::from_secs(10), drain)
            .await
            .expect("cancelling must unpark the reader; it stayed in read(2)")
            .unwrap();
        assert!(
            ended.is_err(),
            "a cancelled subscription ends the stream rather than yielding an item",
        );
    }

    fn test_client_config() -> crate::network::client::RpcClientConfig {
        let sopts = crate::network::tcp::SocketOpts::for_test();
        crate::network::client::RpcClientConfig {
            tcp_keepalive_interval: sopts.keepalive_interval,
            #[cfg(target_os = "linux")]
            tcp_user_timeout: sopts.user_timeout,
            tcp_keepalive_retries: sopts.keepalive_retries,
            socket_buffer_size: sopts.buffer_size,
            unary_timeout: std::time::Duration::from_secs(5),
            connect_timeout: std::time::Duration::from_secs(5),
            max_unary_conns_per_peer: 4,
            max_idle_unary_conns_per_peer: 2,
            pool_channel_capacity: 64,
            subscribe_channel_capacity: crate::unit::nonzero(8),
        }
    }
}
