//! The read half: reassemble frames off a byte stream and decode them. Layout
//! and the async/blocking split are in the module header.
//!
//! One buffer, consumed by a moving cursor rather than by removal — a single
//! read often carries many frames, and dropping each from the front would
//! re-copy everything behind it. The decoded prefix is reclaimed at most once
//! per read, and a frame decodes straight out of the buffer, so a frame costs no
//! copy at all.
//!
//! Two rules the callers depend on:
//!
//! - **Cancel safety.** Every byte read lands in `scratch` before returning, and
//!   the only await is `read_buf` (cancel-safe: on cancellation nothing was
//!   read). `next_frame_raw` is awaited inside `select!` arms, so a partial read
//!   parked in the future would corrupt the stream.
//! - **EOF is only clean on a frame boundary.** Bytes left buffered at EOF mean
//!   a truncated stream, which must surface as an error and never as a normal
//!   end — see CLAUDE.md §20 on why a consumer cannot trust termination alone.
//!
//! HLC drift is reported, not enforced, by [`FrameStream::next_frame_raw`]: it
//! hands back the value *and* the verdict so a caller able to correlate the
//! frame can fail just that one. [`FrameStream::next_frame`] is the fail-fast
//! default.

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::api::Clock;
use crate::error::{Error, ToCodec};
use crate::framing::verify_frame_crc32;
use crate::ids::NodeId;
use crate::network::messages::RpcEnvelope;

use super::{LEN_PREFIX, MAX_FRAME_LEN, READ_CHUNK, shrink_drained};

/// Generic over the byte-stream read half — see
/// [`FrameSink`](super::FrameSink).
pub(in crate::network) struct FrameStream<R> {
    read: R,
    /// Bytes read from `read`. Lives on the struct, not in the `next_frame_raw`
    /// future — that is what keeps `next_frame_raw` cancel-safe (see its docs).
    /// The undecoded remainder is `scratch[consumed..]`; use [`pending`].
    ///
    /// [`pending`]: Self::pending
    pub(super) scratch: Vec<u8>,
    /// How much of `scratch` has already been decoded. Frames are dropped by moving
    /// this forward, never by shifting the buffer: one read can deliver many
    /// frames, and removing each from the front would re-copy all the ones behind
    /// it. The prefix is reclaimed at most once per read (see `reclaim`).
    consumed: usize,
    clock: Arc<dyn Clock>,
    /// Expected node ID in incoming envelopes.
    /// `None` skips the check (used by server side when responding,
    /// or by a direct-by-address connect when the target identity is not yet known).
    expected_node_id: Option<NodeId>,
}

impl<R> FrameStream<R> {
    #[must_use]
    pub(in crate::network) fn new(
        recv: R,
        clock: Arc<dyn Clock>,
        expected_node_id: Option<NodeId>,
    ) -> Self {
        Self {
            read: recv,
            scratch: Vec::with_capacity(READ_CHUNK),
            consumed: 0,
            clock,
            expected_node_id,
        }
    }

    /// Like [`new`](Self::new) but seeds the buffer with bytes a *different*
    /// transport already read. The async stream routinely reads past the
    /// handshake frame, so those bytes (from
    /// [`into_raw_parts`](Self::into_raw_parts)) must be replayed here or the
    /// first real frame is lost.
    #[must_use]
    pub(in crate::network) fn new_with_prefill(
        recv: R,
        clock: Arc<dyn Clock>,
        expected_node_id: Option<NodeId>,
        prefill: Vec<u8>,
    ) -> Self {
        Self {
            read: recv,
            scratch: prefill,
            consumed: 0,
            clock,
            expected_node_id,
        }
    }

    /// Deconstruct into `(inner reader, undecoded remainder, clock,
    /// expected_node_id)`. The remainder is the bytes read past the last returned
    /// frame — replay them via [`new_with_prefill`](Self::new_with_prefill) when
    /// handing a partially-read stream to another transport, or they are lost.
    pub(in crate::network) fn into_raw_parts(self) -> (R, Vec<u8>, Arc<dyn Clock>, Option<NodeId>) {
        let remainder = self
            .scratch
            .get(self.consumed..)
            .unwrap_or_default()
            .to_vec();
        let Self {
            read,
            clock,
            expected_node_id,
            ..
        } = self;
        (read, remainder, clock, expected_node_id)
    }

    /// The bytes read but not yet decoded — where the next frame starts.
    fn pending(&self) -> &[u8] {
        self.scratch.get(self.consumed..).unwrap_or_default()
    }

    /// Drop the already-decoded prefix, making room for the next read. Once the
    /// buffer is fully drained this is a `clear` (no copying), which is the
    /// common case: a read usually ends on a frame boundary.
    fn reclaim(&mut self) {
        if self.consumed == self.scratch.len() {
            self.scratch.clear();
        } else if self.consumed > 0 {
            // Partial frame at the tail: shift it to the front. At most one
            // memmove per read, never one per frame.
            self.scratch.drain(..self.consumed);
        }
        self.consumed = 0;
        shrink_drained(&mut self.scratch);
    }

    /// Body length of the next frame, once its length prefix is buffered.
    /// `Ok(None)` means "prefix not here yet"; the body itself may still be
    /// short, which the caller checks. `Err` on a prefix above
    /// [`MAX_FRAME_LEN`] — rejected before any reserve, so a corrupt or hostile
    /// prefix cannot drive a huge allocation.
    fn peek_body_len(&self) -> Result<Option<usize>, Error> {
        let Some(prefix) = self.pending().get(..LEN_PREFIX) else {
            return Ok(None);
        };
        let prefix: [u8; LEN_PREFIX] = prefix
            .try_into()
            .map_err(|_| Error::Codec("frame length prefix".into()))?;
        let body_len = usize::try_from(u32::from_be_bytes(prefix)).to_codec("frame length")?;
        if body_len > MAX_FRAME_LEN {
            return Err(Error::Codec(format!(
                "rpc frame of {body_len} bytes exceeds the {MAX_FRAME_LEN}-byte limit"
            )));
        }
        Ok(Some(body_len))
    }

    /// Try to pull one fully-buffered frame out of `scratch` without any I/O.
    /// `Some(_)` = a frame boundary is buffered (inner may be a decode `Err`);
    /// `None` = more bytes are needed. Shared by the async and blocking read
    /// loops so the frame-boundary logic has one source of truth.
    fn try_take_frame<T: Serialize + DeserializeOwned>(
        &mut self,
    ) -> Option<Result<(T, Option<Error>), Error>> {
        let body_len = match self.peek_body_len() {
            Ok(len) => len,
            Err(e) => return Some(Err(e)),
        };
        // `body_len <= MAX_FRAME_LEN` and `LEN_PREFIX` is 4, so no overflow.
        if let Some(end) = body_len.map(|n| n.saturating_add(LEN_PREFIX))
            && self.pending().len() >= end
        {
            // Decode straight out of the read buffer — no per-frame copy.
            // `decode_envelope` returns owned values, so the borrow ends here
            // and the frame's bytes can be dropped.
            let decoded = self.decode_envelope::<T>(&self.pending()[LEN_PREFIX..end]);
            self.consumed = self.consumed.saturating_add(end);
            return Some(decoded);
        }
        None
    }

    /// Decode one envelope's `data` bytes: enforce the expected node id, absorb
    /// the peer HLC (returning the drift verdict rather than failing), and hand
    /// back the inner value.
    fn decode_envelope<T: Serialize + DeserializeOwned>(
        &self,
        data: &[u8],
    ) -> Result<(T, Option<Error>), Error> {
        // Verify the trailing CRC32 against the bytes *as received*, before
        // decoding — catches in-flight corruption on any frame.
        let body = verify_frame_crc32(data).to_codec("verify envelope")?;
        let env: RpcEnvelope<T> = postcard::from_bytes(body).to_codec("decode envelope")?;
        if let (Some(expected), Some(actual)) = (self.expected_node_id, env.node_id)
            && actual != expected
        {
            return Err(Error::unexpected_node_id(expected, actual));
        }
        // Absorb the peer HLC. On success the clock advances; on drift the error
        // is surfaced (clock not advanced) but the frame is still handed back.
        let drift = self.clock.integrate(env.hlc).err();
        Ok((env.inner, drift))
    }
}

impl<R: AsyncRead + Unpin + Send> FrameStream<R> {
    /// Read and decode the next frame, returning the decoded value **together
    /// with the HLC-drift verdict** rather than failing on drift:
    /// - `Some(Ok((value, None)))` — within the skew limit (clock integrated).
    /// - `Some(Ok((value, Some(err))))` — drift exceeded; `err` is
    ///   `HlcDriftTooLarge` and the clock was **not** advanced. The value is still
    ///   returned so a caller that can correlate the frame (e.g. the write pool,
    ///   by `DataId`) can fail just that frame instead of tearing the stream down.
    /// - `Some(Err(_))` / `None` — transport / decode / node-id break.
    ///
    /// **Cancel safe** (`client/write_stream.rs`, `server/stream_handler.rs`
    /// await it in `select!` arms). Do not add a second await point, and do not
    /// park decoded state in a local.
    pub(in crate::network) async fn next_frame_raw<T: Serialize + DeserializeOwned>(
        &mut self,
    ) -> Option<Result<(T, Option<Error>), Error>> {
        loop {
            if let Some(frame) = self.try_take_frame::<T>() {
                return Some(frame);
            }

            // Drop what has been decoded, and always leave room: `read_buf` with
            // no spare capacity reports `Ok(0)`, indistinguishable from EOF.
            self.reclaim();
            self.scratch.reserve(READ_CHUNK);
            match self.read.read_buf(&mut self.scratch).await {
                // Clean close only if no frame was in flight; a stranded partial
                // frame is a truncated stream, not a normal end.
                Ok(0) if self.scratch.is_empty() => return None,
                Ok(0) => {
                    return Some(Err(Error::Rpc(format!(
                        "stream read: eof mid-frame with {} buffered bytes",
                        self.scratch.len()
                    ))));
                }
                Ok(_) => {}
                Err(e) => return Some(Err(Error::Rpc(format!("stream read: {e}")))),
            }
        }
    }

    /// Read the next frame, failing on HLC drift (`HlcDriftTooLarge`). Thin
    /// wrapper over [`next_frame_raw`] — the default for streams that treat drift
    /// as fatal (subscribe, unary, server ingress).
    ///
    /// [`next_frame_raw`]: Self::next_frame_raw
    pub(in crate::network) async fn next_frame<T: Serialize + DeserializeOwned>(
        &mut self,
    ) -> Option<Result<T, Error>> {
        match self.next_frame_raw::<T>().await {
            Some(Ok((value, None))) => Some(Ok(value)),
            Some(Ok((_, Some(err)))) | Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

/// Blocking counterpart, for connections driven by a dedicated OS thread over
/// `std::net::TcpStream`.
impl<R: std::io::Read> FrameStream<R> {
    /// Blocking [`next_frame_raw`](FrameStream::next_frame_raw). Nothing to be
    /// cancel-safe against — one thread owns this stream and blocks in `read`.
    pub(in crate::network) fn next_frame_raw_blocking<T: Serialize + DeserializeOwned>(
        &mut self,
    ) -> Option<Result<(T, Option<Error>), Error>> {
        loop {
            if let Some(frame) = self.try_take_frame::<T>() {
                return Some(frame);
            }
            self.reclaim();
            // Grow with zeroed room and read into it, then truncate to what was
            // read: `std::io::Read` needs an initialized target, and
            // `forbid(unsafe_code)` rules out reading into uninit spare capacity.
            let base = self.scratch.len();
            self.scratch.resize(base.saturating_add(READ_CHUNK), 0);
            match self
                .read
                .read(self.scratch.get_mut(base..).unwrap_or_default())
            {
                Ok(0) => {
                    self.scratch.truncate(base);
                    if self.scratch.is_empty() {
                        return None;
                    }
                    return Some(Err(Error::Rpc(format!(
                        "stream read: eof mid-frame with {} buffered bytes",
                        self.scratch.len()
                    ))));
                }
                Ok(n) => self.scratch.truncate(base.saturating_add(n)),
                Err(e) => {
                    self.scratch.truncate(base);
                    return Some(Err(Error::Rpc(format!("stream read: {e}"))));
                }
            }
        }
    }
}
