//! The async→blocking hand-off, in one place because both ends of a write
//! connection make it: the client at `open_write`, the server at `run_write`.
//! Doing it twice would risk the two halves disagreeing about the read-ahead.

use crate::error::{Error, ToRpc};

use super::{AsyncFrameSink, AsyncFrameStream, TcpFrameSink, TcpFrameStream};

/// Reunite the split halves into one `std::net::TcpStream` off the reactor, then
/// rebuild blocking framing over independent clones. Returns
/// `(sink, stream, force-close handle)` — the third is a spare handle held only
/// to shut both directions down at teardown.
///
/// Whatever the async reader buffered past the handshake is replayed into the
/// blocking stream; dropping it would lose the first real frame.
pub(in crate::network) fn adopt_blocking(
    sink: AsyncFrameSink,
    stream: AsyncFrameStream,
) -> Result<(TcpFrameSink, TcpFrameStream, std::net::TcpStream), Error> {
    let (write_half, sink_clock, node_id) = sink.into_raw_parts();
    let (read_half, remainder, clock, expected) = stream.into_raw_parts();
    let tcp = read_half
        .reunite(write_half)
        .map_err(|e| Error::Rpc(format!("reunite tcp halves: {e}")))?;
    let raw = tcp.into_std().to_rpc("tcp into_std")?;
    raw.set_nonblocking(false)
        .to_rpc("tcp set_nonblocking(false)")?;
    // Independent handles: one per blocking loop, plus `raw` to force-close.
    let read_sock = raw.try_clone().to_rpc("clone tcp read half")?;
    let write_sock = raw.try_clone().to_rpc("clone tcp write half")?;
    let blk_stream = TcpFrameStream::new_with_prefill(read_sock, clock, expected, remainder);
    let blk_sink = TcpFrameSink::new(write_sock, sink_clock, node_id);
    Ok((blk_sink, blk_stream, raw))
}
