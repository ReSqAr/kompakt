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

pub use adopt::adopt_blocking;
pub use sink::FrameSink;
pub use stream::FrameStream;

pub type TcpFrameSink = FrameSink<std::net::TcpStream>;
pub type TcpFrameStream = FrameStream<std::net::TcpStream>;
pub type AsyncFrameSink = FrameSink<tokio::net::tcp::OwnedWriteHalf>;
pub type AsyncFrameStream = FrameStream<tokio::net::tcp::OwnedReadHalf>;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests;
