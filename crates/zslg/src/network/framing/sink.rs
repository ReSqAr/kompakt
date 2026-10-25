//! The write half: encode a value into an envelope and put the frame on the
//! wire. Layout and the async/blocking split are in the module header.
//!
//! One buffer, appended to and written whole. `feed` × N + one `flush` amortizes
//! the per-frame syscall and wakeup — the transport analog of the storage
//! write-batch pattern — and `feed` self-flushes at [`BACKPRESSURE_BOUNDARY`] so
//! a caller that never flushes still cannot buffer without bound.
//!
//! A frame that fails to encode leaves nothing behind: the partial body is
//! truncated away, because a length prefix promising bytes that never arrive
//! would desynchronize the peer's stream permanently.

use std::sync::Arc;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

use crate::api::Clock;
use crate::error::{Error, ToCodec, ToRpc};
use crate::framing::append_crc32_from;
use crate::ids::NodeId;
use crate::network::messages::RpcEnvelope;

use super::{BACKPRESSURE_BOUNDARY, LEN_PREFIX, MAX_FRAME_LEN, shrink_drained};

/// postcard sink that serializes straight into the sink's pending buffer.
///
/// The point is `try_extend`: postcard's own `ExtendFlavor` goes through
/// `Extend<u8>`, one byte at a time. `extend_from_slice` is a `memcpy`, which is
/// what a payload blob wants.
struct VecFlavor<'a>(&'a mut Vec<u8>);

impl postcard::ser_flavors::Flavor for VecFlavor<'_> {
    type Output = ();

    #[inline(always)]
    fn try_push(&mut self, data: u8) -> postcard::Result<()> {
        self.0.push(data);
        Ok(())
    }

    #[inline(always)]
    fn try_extend(&mut self, b: &[u8]) -> postcard::Result<()> {
        self.0.extend_from_slice(b);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<()> {
        Ok(())
    }
}

/// Generic over the byte-stream write half so the framing (and everything
/// above it) is transport-agnostic; the default type parameter names the
/// transport actually in use (a plain-TCP write half), so call sites just say
/// `FrameSink`.
pub(in crate::network) struct FrameSink<W> {
    write: W,
    /// Frames fed but not yet written to `write`. Reused across frames, so the
    /// steady state allocates nothing per frame: this side only appends, then
    /// writes the lot and clears.
    pub(super) pending: Vec<u8>,
    clock: Arc<dyn Clock>,
    /// Node ID embedded in every outgoing envelope.
    /// `None` for anonymous (direct-by-address) connects during join.
    node_id: Option<NodeId>,
}

impl<W> FrameSink<W> {
    #[must_use]
    pub(in crate::network) fn new(send: W, clock: Arc<dyn Clock>, node_id: Option<NodeId>) -> Self {
        Self {
            write: send,
            pending: Vec::with_capacity(BACKPRESSURE_BOUNDARY),
            clock,
            node_id,
        }
    }

    /// Encode one frame into the pending buffer (no I/O). `Ok(true)` = the
    /// buffer reached the backpressure boundary and the caller should flush.
    /// The one encoder, shared by the async and blocking paths.
    fn encode_into_pending<T: Serialize>(&mut self, value: T) -> Result<bool, Error> {
        let env = RpcEnvelope {
            hlc: self.clock.now(),
            node_id: self.node_id,
            inner: value,
        };
        // Placeholder, envelope, CRC over what was just written, then backfill
        // the length — so nothing allocates once the buffer is at steady state.
        let start = self.pending.len();
        self.pending.extend_from_slice(&[0u8; LEN_PREFIX]);
        // In-bounds: `start` is a length of the same buffer we just extended.
        let body_start = start.saturating_add(LEN_PREFIX);

        if let Err(e) =
            postcard::serialize_with_flavor::<_, _, ()>(&env, VecFlavor(&mut self.pending))
        {
            // Drop the half-written frame — a partial body must never reach the
            // wire behind a length prefix that promises more.
            self.pending.truncate(start);
            return Err(Error::Codec(format!("encode envelope: {e}")));
        }

        append_crc32_from(&mut self.pending, body_start);

        // Both sides are lengths of this buffer, and `body_start <= len`.
        let body_len = self.pending.len().saturating_sub(body_start);
        if body_len > MAX_FRAME_LEN {
            self.pending.truncate(start);
            shrink_drained(&mut self.pending);
            return Err(Error::Codec(format!(
                "rpc frame of {body_len} bytes exceeds the {MAX_FRAME_LEN}-byte limit"
            )));
        }
        let prefix = u32::try_from(body_len).to_codec("frame length")?;
        self.pending[start..body_start].copy_from_slice(&prefix.to_be_bytes());

        // Bound the pending buffer: `feed` × N with no interleaved `flush` (the
        // write-stream loops) would otherwise grow it without limit.
        Ok(self.pending.len() >= BACKPRESSURE_BOUNDARY)
    }

    /// Deconstruct into `(inner writer, clock, node_id)` so the writer half can
    /// be handed to another transport (e.g. reuniting the split halves of a TCP
    /// connection into a blocking `std::net::TcpStream` after the handshake).
    /// Any buffered `pending` frames are dropped — call only on a flushed sink.
    pub(in crate::network) fn into_raw_parts(self) -> (W, Arc<dyn Clock>, Option<NodeId>) {
        let Self {
            write,
            pending: _,
            clock,
            node_id,
        } = self;
        (write, clock, node_id)
    }
}

impl<W: AsyncWrite + Unpin + Send> FrameSink<W> {
    /// Buffer one frame without flushing. Use [`send`](Self::send) when a single
    /// frame must reach the peer immediately.
    pub(in crate::network) async fn feed<T: Serialize + DeserializeOwned>(
        &mut self,
        value: T,
    ) -> Result<(), Error> {
        if self.encode_into_pending(value)? {
            self.flush().await?;
        }
        Ok(())
    }

    /// Flush all buffered (`feed`) frames to the underlying stream.
    pub(in crate::network) async fn flush(&mut self) -> Result<(), Error> {
        if !self.pending.is_empty() {
            self.write
                .write_all(&self.pending)
                .await
                .to_rpc("stream write")?;
            self.pending.clear();
            shrink_drained(&mut self.pending);
        }
        self.write.flush().await.to_rpc("stream flush")
    }

    /// Feed one frame and flush it immediately.
    ///
    /// Boxed at the *definition*, not the call site (CLAUDE.md §2): `send` is
    /// awaited from many places, and the dispatchers that await it
    /// (`server/handlers.rs`) already sit near the `large_stack_frames` bar with
    /// an `RpcFrame` live across the await. Inlining this future pushes three of
    /// them over it.
    pub(in crate::network) fn send<'a, T: Serialize + DeserializeOwned + Send + 'a>(
        &'a mut self,
        value: T,
    ) -> BoxFuture<'a, Result<(), Error>> {
        async move {
            self.feed(value).await?;
            self.flush().await
        }
        .boxed()
    }
}

/// Blocking counterpart, for connections driven by a dedicated OS thread over
/// `std::net::TcpStream` — the writer→replica hot path.
impl<W: std::io::Write> FrameSink<W> {
    /// Blocking [`feed`](FrameSink::feed).
    pub(in crate::network) fn feed_blocking<T: Serialize>(
        &mut self,
        value: T,
    ) -> Result<(), Error> {
        if self.encode_into_pending(value)? {
            self.flush_blocking()?;
        }
        Ok(())
    }

    /// Blocking [`flush`](FrameSink::flush).
    pub(in crate::network) fn flush_blocking(&mut self) -> Result<(), Error> {
        if !self.pending.is_empty() {
            self.write.write_all(&self.pending).to_rpc("stream write")?;
            self.pending.clear();
            shrink_drained(&mut self.pending);
        }
        self.write.flush().to_rpc("stream flush")
    }
}
