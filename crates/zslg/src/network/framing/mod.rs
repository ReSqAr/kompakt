//! The RPC wire format, and the two ways to drive it.
//!
//! ```text
//!   ┌── len: u32 BE ──┬───── postcard(RpcEnvelope<T>) ─────┬── crc32(4) ──┐
//!   │  counts body    │  hlc | node_id | inner             │  over the    │
//!   │  INCLUDING crc  │                                    │  envelope    │
//!   └─────────────────┴────────────────────────────────────┴──────────────┘
//! ```
//!
//! Every RPC — data plane and raft alike — is exactly one such frame, so there
//! is one integrity check and one HLC exchange point rather than per-message
//! wrapping. The CRC is corruption detection, not confidentiality: the cluster
//! runs on a trusted network with no TLS (design doc §10).
//!
//! Written by hand rather than delegated to `LengthDelimitedCodec` because that
//! encoder takes an owned `Bytes` and copies it — an allocation per frame that
//! no scratch buffer can remove. Both ends of every connection are this code, so
//! the layout is ours to define; `frame_layout_is_length_prefix_body_crc` pins
//! it against its parts, since a self-roundtrip would still pass if both halves
//! drifted together.
//!
//! **Async and blocking are the same wire.** `FrameSink`/`FrameStream` are
//! generic over the byte stream and each has both an async and a blocking face,
//! sharing one encoder (`encode_into_pending`) and one frame-boundary decoder
//! (`try_take_frame`) — so a blocking sender and an async receiver interoperate
//! byte-for-byte. A connection starts async for the handshake and is then
//! [`adopt_blocking`]ed onto a dedicated thread where the hot path wants it.
//!
//! Buffers live on the sink/stream, never inside a future. That is what makes
//! `next_frame_raw` cancel-safe (it is awaited in `select!` arms) and what keeps
//! the steady state allocation-free; the constants below bound what those
//! long-lived buffers may retain.

/// Width of the big-endian length prefix.
const LEN_PREFIX: usize = 4;

/// Largest body accepted or emitted. On the read side this is checked *before*
/// reserving, so a corrupt or hostile prefix cannot drive an unbounded
/// allocation — 4 bytes on the wire must not be able to demand 4 GiB of memory.
const MAX_FRAME_LEN: usize = 8 * 1024 * 1024;

/// `feed` flushes on its own once the pending buffer reaches this size, so a
/// `feed`-in-a-loop caller cannot buffer without bound.
const BACKPRESSURE_BOUNDARY: usize = 16 * 1024;

/// Steady-state cap for the long-lived per-connection buffers. One oversized
/// frame can grow a buffer past this; it is released once the buffer next drains
/// (see `shrink_drained`) so a single large frame cannot pin a big allocation for
/// the life of the connection. Same policy as `object_store::framing`'s scratch.
const BUF_SHRINK_CAP: usize = 16 * 1024;

/// Spare capacity requested before each `read_buf`. Also the read buffer's
/// steady-state size.
const READ_CHUNK: usize = 16 * 1024;

/// Release a drained buffer's capacity if a large frame grew it past
/// [`BUF_SHRINK_CAP`]. Only runs when the buffer holds nothing, so no data
/// moves. Same policy as `object_store::framing::reset_scratch`.
fn shrink_drained(scratch: &mut Vec<u8>) {
    if scratch.is_empty() && scratch.capacity() > BUF_SHRINK_CAP {
        scratch.shrink_to(BUF_SHRINK_CAP);
    }
}

mod adopt;
mod sink;
mod stream;

pub(in crate::network) use adopt::adopt_blocking;
pub(in crate::network) use sink::FrameSink;
pub(in crate::network) use stream::FrameStream;

pub(in crate::network) type TcpFrameSink = FrameSink<std::net::TcpStream>;
pub(in crate::network) type TcpFrameStream = FrameStream<std::net::TcpStream>;
pub(in crate::network) type AsyncFrameSink = FrameSink<tokio::net::tcp::OwnedWriteHalf>;
pub(in crate::network) type AsyncFrameStream = FrameStream<tokio::net::tcp::OwnedReadHalf>;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use std::sync::Arc;

    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, duplex};

    use super::*;
    use crate::api::Clock;
    use crate::error::Error;
    use crate::framing::verify_frame_crc32;
    use crate::hlc::Hlc;
    use crate::ids::NodeId;
    use crate::network::messages::RpcEnvelope;

    #[test]
    fn shrink_drained_when_empty_and_over_capacity() {
        let mut v = Vec::with_capacity(32 * 1024);
        shrink_drained(&mut v);
        assert!(v.capacity() <= BUF_SHRINK_CAP);
    }

    #[test]
    fn shrink_drained_when_non_empty_leaves_capacity() {
        let mut v = Vec::with_capacity(32 * 1024);
        v.push(1);
        shrink_drained(&mut v);
        assert!(v.capacity() > BUF_SHRINK_CAP);
    }

    #[test]
    fn shrink_drained_when_already_under_cap() {
        let mut v = Vec::with_capacity(1024);
        shrink_drained(&mut v);
        assert!(v.capacity() <= BUF_SHRINK_CAP);
    }

    /// Clock pinned to a fixed HLC; `integrate` echoes the peer (no drift).
    struct FixedClock;
    impl Clock for FixedClock {
        fn now(&self) -> Hlc {
            Hlc(7)
        }
        fn integrate(&self, peer: Hlc) -> Result<Hlc, Error> {
            Ok(peer)
        }
    }

    fn clock() -> Arc<dyn Clock> {
        Arc::new(FixedClock)
    }

    /// A frame written by `FrameSink` carries a trailing CRC that `FrameStream`
    /// verifies transparently — an untouched value round-trips.
    #[tokio::test]
    async fn framed_envelope_roundtrips() {
        let (wr, rd) = duplex(4096);
        let mut sink = FrameSink::new(wr, clock(), None);
        let mut stream = FrameStream::new(rd, clock(), None);
        sink.send(0xDEAD_BEEFu64).await.expect("send");
        let got: u64 = stream.next_frame().await.expect("some").expect("ok");
        assert_eq!(got, 0xDEAD_BEEF);
    }

    /// Collect the raw wire bytes a `FrameSink` emits for `value`.
    async fn wire_bytes<T: Serialize + DeserializeOwned + Send + 'static>(value: T) -> Vec<u8> {
        let (wr, mut rd) = duplex(64 * 1024);
        let mut sink = FrameSink::new(wr, clock(), None);
        sink.send(value).await.expect("send");
        drop(sink); // drop the write half → FIN, so read_to_end returns
        let mut wire = Vec::new();
        rd.read_to_end(&mut wire).await.expect("read wire");
        wire
    }

    /// The wire format, pinned against its parts: a 4-byte big-endian length
    /// prefix, the postcard envelope, then the CRC32 of that envelope — with the
    /// prefix counting the CRC. We write *and* parse this format, so a
    /// self-roundtrip alone would still pass if both halves drifted together.
    #[tokio::test]
    async fn frame_layout_is_length_prefix_body_crc() {
        let wire = wire_bytes(0xDEAD_BEEFu64).await;
        let (prefix, body) = wire.split_at(LEN_PREFIX);
        let prefix: [u8; LEN_PREFIX] = prefix.try_into().unwrap();

        assert_eq!(
            usize::try_from(u32::from_be_bytes(prefix)).unwrap(),
            body.len(),
            "length prefix must count the body including its CRC"
        );
        let envelope = verify_frame_crc32(body).expect("trailing crc32 over the envelope");
        let expected = postcard::to_allocvec(&RpcEnvelope {
            hlc: Hlc(7),
            node_id: None::<NodeId>,
            inner: 0xDEAD_BEEFu64,
        })
        .unwrap();
        assert_eq!(envelope, expected, "body must be the postcard envelope");
    }

    /// `feed` × N + one `flush` delivers every frame, in order.
    #[tokio::test]
    async fn fed_frames_arrive_in_order_after_one_flush() {
        let (wr, rd) = duplex(64 * 1024);
        let mut sink = FrameSink::new(wr, clock(), None);
        for i in 0u64..32 {
            sink.feed(i).await.expect("feed");
        }
        sink.flush().await.expect("flush");

        let mut stream = FrameStream::new(rd, clock(), None);
        for i in 0u64..32 {
            let got: u64 = stream.next_frame().await.expect("some").expect("ok");
            assert_eq!(got, i);
        }
    }

    /// A `feed` loop with no interleaved `flush` must not buffer without bound:
    /// past the backpressure boundary `feed` flushes on its own, so the bytes
    /// reach the peer and the sink's buffer stays small.
    #[tokio::test]
    async fn feed_flushes_itself_at_the_backpressure_boundary() {
        let (wr, mut rd) = duplex(1024 * 1024);
        let mut sink = FrameSink::new(wr, clock(), None);
        // Each frame is ~256 B, so this is many times the 8 KiB boundary.
        let payload = vec![7u8; 256];
        for _ in 0..256 {
            sink.feed(payload.clone()).await.expect("feed");
            assert!(
                sink.pending.len() < BACKPRESSURE_BOUNDARY,
                "pending buffer grew past the boundary: {}",
                sink.pending.len()
            );
        }
        let mut sniff = [0u8; 1];
        rd.read_exact(&mut sniff).await.expect("bytes on the wire");
    }

    /// A frame larger than the peer will accept is refused at encode time rather
    /// than written and rejected on the far side — and the rejected frame leaves
    /// no partial bytes behind in the sink.
    #[tokio::test]
    async fn oversized_frame_is_refused_and_leaves_no_debris() {
        let (wr, rd) = duplex(64 * 1024);
        let mut sink = FrameSink::new(wr, clock(), None);
        let huge = vec![0u8; MAX_FRAME_LEN.saturating_add(1)];
        let err = sink.feed(huge).await.expect_err("oversized frame refused");
        assert!(matches!(err, Error::Codec(_)), "unexpected error: {err:?}");
        assert!(sink.pending.is_empty(), "partial frame left in the sink");

        // The sink is still usable, and the next frame is the first thing the
        // peer sees.
        sink.send(1u64).await.expect("send");
        let mut stream = FrameStream::new(rd, clock(), None);
        assert_eq!(
            stream.next_frame::<u64>().await.expect("some").expect("ok"),
            1
        );
    }

    /// A corrupt or hostile length prefix is rejected against the frame limit
    /// *before* any buffer is reserved for it — otherwise 4 bytes on the wire
    /// could demand a 4 GiB allocation.
    #[tokio::test]
    async fn absurd_length_prefix_is_rejected_without_allocating() {
        let wire = [0xFFu8, 0xFF, 0xFF, 0xFF];
        let mut stream = FrameStream::new(&wire[..], clock(), None);
        let err = stream
            .next_frame::<u64>()
            .await
            .expect("some")
            .expect_err("absurd length must be rejected");
        assert!(matches!(err, Error::Codec(_)), "unexpected error: {err:?}");
        assert!(
            stream.scratch.capacity() <= READ_CHUNK,
            "reserved for an unvalidated length: {}",
            stream.scratch.capacity()
        );
    }

    /// EOF part-way through a frame is a truncated stream, not a clean close —
    /// it must surface as an error, never as a normal end-of-stream.
    #[tokio::test]
    async fn eof_mid_frame_is_an_error_not_a_clean_close() {
        let wire = wire_bytes(0xDEAD_BEEFu64).await;
        let truncated = &wire[..wire.len().saturating_sub(1)];
        let mut stream = FrameStream::new(truncated, clock(), None);
        let err = stream
            .next_frame::<u64>()
            .await
            .expect("some")
            .expect_err("truncated frame must be rejected");
        assert!(matches!(err, Error::Rpc(_)), "unexpected error: {err:?}");
    }

    /// Frames are reassembled across arbitrary read boundaries: a reader that
    /// yields one byte at a time still produces whole frames, in order.
    #[tokio::test]
    async fn frames_reassemble_across_partial_reads() {
        let mut wire = wire_bytes(0xDEAD_BEEFu64).await;
        wire.extend(wire_bytes(0x1234_5678u64).await);

        /// Read half that hands back at most one byte per call.
        struct ByteAtATime(std::io::Cursor<Vec<u8>>);
        impl tokio::io::AsyncRead for ByteAtATime {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                let mut one = [0u8; 1];
                let n = std::io::Read::read(&mut self.0, &mut one).unwrap();
                buf.put_slice(&one[..n]);
                std::task::Poll::Ready(Ok(()))
            }
        }

        let mut stream = FrameStream::new(ByteAtATime(std::io::Cursor::new(wire)), clock(), None);
        assert_eq!(
            stream.next_frame::<u64>().await.expect("some").expect("ok"),
            0xDEAD_BEEF
        );
        assert_eq!(
            stream.next_frame::<u64>().await.expect("some").expect("ok"),
            0x1234_5678
        );
        assert!(stream.next_frame::<u64>().await.is_none(), "clean eof");
    }

    /// `next_frame_raw` is awaited inside `select!` arms, so losing that race
    /// must not drop or reorder buffered bytes. Cancel it repeatedly against a
    /// timer that always wins, then read: every frame must still be there.
    /// A cancel-unsafe implementation (partial reads parked in the future)
    /// corrupts the stream here.
    #[tokio::test(start_paused = true)]
    async fn next_frame_is_cancel_safe() {
        // Trickle the frames out one byte at a time, so a read is genuinely
        // pending — and therefore cancelled — most times round the loop.
        let mut wire = Vec::new();
        for i in 0u64..16 {
            wire.extend(wire_bytes(i).await);
        }
        let (mut wr, rd) = duplex(64 * 1024);
        let writer = tokio::spawn(async move {
            for byte in wire {
                wr.write_all(&[byte]).await.expect("write byte");
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });

        let mut stream = FrameStream::new(rd, clock(), None);
        let mut cancels = 0u32;
        for i in 0u64..16 {
            let got = loop {
                tokio::select! {
                    biased;
                    () = tokio::time::sleep(std::time::Duration::from_micros(500)) => {
                        // Timer won: `next_frame_raw` is dropped mid-flight,
                        // usually with a partial frame already buffered.
                        cancels = cancels.saturating_add(1);
                    }
                    frame = stream.next_frame::<u64>() => {
                        break frame.expect("some").expect("ok");
                    }
                }
            };
            assert_eq!(got, i, "frame lost or reordered by cancellation");
        }
        assert!(cancels > 0, "test never actually cancelled a read");
        writer.await.expect("writer");
    }

    /// A one-bit flip anywhere in a framed envelope is caught by the frame CRC
    /// before decode — in-flight corruption on the RPC path is rejected.
    #[tokio::test]
    async fn framed_envelope_detects_corruption() {
        // Capture the raw wire bytes `FrameSink` emits (length prefix + body + CRC).
        let (wr, mut rd) = duplex(4096);
        let mut sink = FrameSink::new(wr, clock(), None);
        sink.send(0xDEAD_BEEFu64).await.expect("send");
        drop(sink); // FIN so the read side sees EOF after the frame
        let mut wire = Vec::new();
        rd.read_to_end(&mut wire).await.expect("read wire");

        // Flip a byte inside the body (past the 4-byte length prefix).
        let mid = wire.len() / 2;
        wire[mid] ^= 0x01;

        let mut stream = FrameStream::new(&wire[..], clock(), None);
        let err = stream
            .next_frame::<u64>()
            .await
            .expect("some")
            .expect_err("corruption must be rejected");
        assert!(matches!(err, Error::Codec(_)), "unexpected error: {err:?}");
    }

    /// Decode one value off a blocking stream, asserting no HLC drift.
    fn blocking_next<R: std::io::Read, T: Serialize + DeserializeOwned>(
        stream: &mut FrameStream<R>,
    ) -> Option<T> {
        match stream.next_frame_raw_blocking::<T>() {
            Some(Ok((value, drift))) => {
                assert!(drift.is_none(), "unexpected drift");
                Some(value)
            }
            Some(Err(e)) => panic!("blocking read error: {e:?}"),
            None => None,
        }
    }

    /// The blocking sink emits the **exact same bytes** as the async sink — both
    /// go through `encode_into_pending`, so a blocking sender and an async
    /// receiver (and vice-versa) interoperate byte-for-byte.
    #[tokio::test]
    async fn blocking_sink_wire_matches_async_sink() {
        let mut buf = Vec::new();
        {
            let mut sink = FrameSink::new(&mut buf, clock(), None);
            sink.feed_blocking(0xDEAD_BEEFu64).expect("feed_blocking");
            sink.flush_blocking().expect("flush_blocking");
        }
        assert_eq!(buf, wire_bytes(0xDEAD_BEEFu64).await);
    }

    /// A blocking round-trip: `feed_blocking` + `flush_blocking` then
    /// `next_frame_raw_blocking` over the same bytes returns the value untouched,
    /// reassembling across reads.
    #[test]
    fn blocking_framed_envelope_roundtrips() {
        let mut buf = Vec::new();
        {
            let mut sink = FrameSink::new(&mut buf, clock(), None);
            for i in 0u64..8 {
                sink.feed_blocking(i).expect("feed_blocking");
            }
            sink.flush_blocking().expect("flush_blocking");
        }
        let mut stream = FrameStream::new(std::io::Cursor::new(buf), clock(), None);
        for i in 0u64..8 {
            assert_eq!(blocking_next::<_, u64>(&mut stream), Some(i));
        }
        assert!(
            blocking_next::<_, u64>(&mut stream).is_none(),
            "clean eof after the last frame"
        );
    }

    /// EOF mid-frame is an error on the blocking path too, never a clean close.
    #[test]
    fn blocking_eof_mid_frame_is_an_error() {
        let mut buf = Vec::new();
        {
            let mut sink = FrameSink::new(&mut buf, clock(), None);
            sink.feed_blocking(0xDEAD_BEEFu64).expect("feed_blocking");
            sink.flush_blocking().expect("flush_blocking");
        }
        buf.truncate(buf.len().saturating_sub(1));
        let mut stream = FrameStream::new(std::io::Cursor::new(buf), clock(), None);
        let err = stream
            .next_frame_raw_blocking::<u64>()
            .expect("some")
            .expect_err("truncated frame must be rejected");
        assert!(matches!(err, Error::Rpc(_)), "unexpected error: {err:?}");
    }

    /// The handshake handoff: read-ahead bytes buffered past a frame on the async
    /// stream survive `into_raw_parts` → `new_with_prefill` onto a blocking
    /// stream, so the frame that was already read is not lost.
    #[tokio::test]
    async fn buffered_remainder_hands_off_to_blocking_stream() {
        let mut wire = wire_bytes(0x1111_1111u64).await;
        wire.extend(wire_bytes(0x2222_2222u64).await);

        // One `read_buf` pulls the whole slice in; frame 1 decodes, frame 2 stays
        // buffered.
        let mut async_stream = FrameStream::new(&wire[..], clock(), None);
        let first: u64 = async_stream.next_frame().await.expect("some").expect("ok");
        assert_eq!(first, 0x1111_1111);

        let (_reader, remainder, clk, node) = async_stream.into_raw_parts();
        assert!(!remainder.is_empty(), "frame 2 should still be buffered");

        // The blocking stream reads frame 2 straight out of the prefill (its
        // inner reader is empty).
        let mut blocking =
            FrameStream::new_with_prefill(std::io::Cursor::new(Vec::new()), clk, node, remainder);
        assert_eq!(blocking_next::<_, u64>(&mut blocking), Some(0x2222_2222));
    }
}
