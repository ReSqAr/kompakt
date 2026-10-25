//! The snapshot object's bytes.
//!
//! ```text
//!   ┌────────┬───────────────────────────────────────────────────────┐
//!   │ magic  │  4B len │ postcard(record) │ crc32 │  4B len │ ...    │
//!   │  4B    │         └──── len covers both ─────┘                  │
//!   └────────┴───────────────────────────────────────────────────────┘
//!    "SNZC"   └─ one zstd stream ──────────────────────────────────── ┘
//! ```
//!
//! Compression is not a format choice: the body is always one streaming zstd
//! frame, and only its level varies. The magic sits *outside* that frame, since
//! the reader must build the decoder before it can read anything else; the CRC
//! sits *inside* it, so it covers the record itself and catches at-rest bit-rot
//! rather than merely transport damage.
//!
//! Both directions stream, with exactly one frame resident and one reused
//! scratch buffer, so a hundreds-of-MB snapshot never lands in memory whole.
//! Both run inside `spawn_blocking` (docs/PERFORMANCE.md entry 23) — zstd and
//! postcard are CPU-bound and must not touch a reactor worker.
//!
//! End-of-stream is a clean EOF *at a length prefix*, and nowhere else: a zero
//! read anywhere else in a frame is truncation, not an ending. That asymmetry is
//! what makes a short object an error rather than a shorter snapshot.

use std::io;
use std::io::Read as _;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, ToCodec};
use crate::framing::{append_crc32, verify_frame_crc32};
use crate::object_store::CompressionLevel;

const MAGIC: [u8; 4] = *b"SNZC";

/// Steady-state cap for the reused scratch buffer. Reuse is what keeps a
/// several-hundred-thousand-record stream from allocating per frame; shrinking
/// back to this is what stops one oversized record from pinning its allocation
/// for the rest of the stream.
const SCRATCH_SHRINK_CAP: usize = 16 * 1024;

fn reset_scratch(scratch: &mut Vec<u8>) {
    scratch.clear();
    if scratch.capacity() > SCRATCH_SHRINK_CAP {
        scratch.shrink_to(SCRATCH_SHRINK_CAP);
    }
}

/// Frames only — the magic is the caller's, since it must land outside the zstd
/// stream this may be writing into. Returns `(record_count, body_bytes)`.
///
/// On `Err` the caller must **not** finalize the upload: the object is a partial
/// prefix, and the format has no way to say so.
fn encode_frames<T: Serialize, W: io::Write>(
    writer: &mut W,
    mut next: impl FnMut() -> Option<Result<T, Error>>,
) -> Result<(usize, u64), Error> {
    let mut count: usize = 0;
    let mut uncompressed_bytes: u64 = 0;
    let mut scratch: Vec<u8> = Vec::new();
    while let Some(item) = next() {
        let item = item?;
        reset_scratch(&mut scratch);
        scratch = postcard::to_extend(&item, scratch).to_codec("encode snapshot entry")?;
        append_crc32(&mut scratch);
        let body_len = u32::try_from(scratch.len())
            .map_err(|_| Error::Storage("snapshot frame length overflow".into()))?;
        writer
            .write_all(&body_len.to_be_bytes())
            .map_err(|e| Error::Compression(format!("write frame length: {e}")))?;
        writer
            .write_all(&scratch)
            .map_err(|e| Error::Compression(format!("write frame body: {e}")))?;

        // Both counters can only grow by a bounded per-record amount; overflow
        // is a corruption signal, so propagate rather than wrap/saturate.
        uncompressed_bytes = uncompressed_bytes
            .checked_add(u64::from(body_len))
            .ok_or_else(|| Error::Storage("snapshot byte count overflow".into()))?;
        count = count
            .checked_add(1)
            .ok_or_else(|| Error::Storage("record count overflow".into()))?;
    }
    Ok((count, uncompressed_bytes))
}

/// `next` returning `None` ends the stream; `Some(Err)` aborts it.
///
/// **The writer comes back on the error path too**, which is the whole reason for
/// the tuple return: the caller needs it to `abort()` the multipart upload rather
/// than orphan its uploaded parts. `None` means genuinely unrecoverable — `zstd`'s
/// encoder consumed it before failing, or `finish()` itself errored — and the
/// caller falls back to the object-store cleanup pass.
#[must_use = "recover the writer to finish() or abort() the multipart upload, and check the encode Result"]
pub(in crate::object_store) fn encode_framed_records_streaming<T: Serialize, W: io::Write>(
    mut writer: W,
    compression: CompressionLevel,
    next: impl FnMut() -> Option<Result<T, Error>>,
) -> (Option<W>, Result<(usize, u64), Error>) {
    if let Err(e) = writer
        .write_all(&MAGIC)
        .map_err(|e| Error::Compression(format!("write magic: {e}")))
    {
        return (Some(writer), Err(e));
    }

    let mut enc = match zstd::stream::write::Encoder::new(writer, compression.get())
        .map_err(|e| Error::Compression(format!("zstd encoder init: {e}")))
    {
        Ok(enc) => enc,
        // `new` consumed `writer` before failing — it is unrecoverable.
        Err(e) => return (None, Err(e)),
    };
    let frames = encode_frames(&mut enc, next);
    // Unconditional: on the error branch the flushed end-frame is irrelevant,
    // but `finish` is the only way to get `W` back to abort.
    match enc.finish() {
        Ok(writer) => (Some(writer), frames),
        Err(fe) => {
            // Prefer the original framing error; fall back to the finish error.
            let err = frames
                .err()
                .unwrap_or_else(|| Error::Compression(format!("zstd finish: {fe}")));
            (None, Err(err))
        }
    }
}

/// `Ok(false)` on a clean EOF — zero bytes at the *start* of the read — and
/// `Ok(true)` when filled. Anything between is truncation, and an error.
fn read_full_or_eof<R: io::Read>(reader: &mut R, buf: &mut [u8]) -> Result<bool, Error> {
    let mut filled = 0usize;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => {
                if filled == 0 {
                    return Ok(false);
                }
                return Err(Error::Compression(format!(
                    "truncated frame length prefix: {filled} of {} bytes",
                    buf.len()
                )));
            }
            Ok(n) => {
                filled = filled
                    .checked_add(n)
                    .ok_or_else(|| Error::Storage("frame read offset overflow".into()))?;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(Error::Compression(format!("read frame length: {e}"))),
        }
    }
    Ok(true)
}

/// Invokes `sink` once per record, over a sync `reader` — in production a
/// [`tokio_util::io::SyncIoBridge`] over the object's byte stream.
///
/// `sink` is the backpressure point: while it blocks, the decode is paused and
/// no further frame is read. That is what bounds memory, so a `sink` that
/// buffers instead of blocking removes the bound.
pub(in crate::object_store) fn decode_framed_records_streaming<T: DeserializeOwned, R: io::Read>(
    mut reader: R,
    mut sink: impl FnMut(T) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut magic = [0u8; 4];
    reader
        .read_exact(&mut magic)
        .map_err(|e| Error::Compression(format!("read magic: {e}")))?;

    if magic != MAGIC {
        return Err(Error::Compression(format!(
            "unknown magic bytes: {magic:02x?}"
        )));
    }
    let mut inner = zstd::stream::read::Decoder::new(reader)
        .map_err(|e| Error::Compression(format!("zstd decoder init: {e}")))?;

    let mut frame: Vec<u8> = Vec::new();
    loop {
        let mut len_buf = [0u8; 4];
        if !read_full_or_eof(&mut inner, &mut len_buf)? {
            break;
        }
        let frame_len = usize::try_from(u32::from_be_bytes(len_buf))
            .map_err(|_| Error::Storage("snapshot frame length overflow".into()))?;
        // `read_exact`, not `read_full_or_eof`: past the length prefix an EOF is
        // truncation, however clean it looks.
        reset_scratch(&mut frame);
        frame.resize(frame_len, 0);
        inner
            .read_exact(&mut frame)
            .map_err(|e| Error::Compression(format!("read frame body: {e}")))?;
        let inner_bytes = verify_frame_crc32(&frame).to_codec("verify snapshot entry")?;
        let value = postcard::from_bytes::<T>(inner_bytes).to_codec("decode snapshot entry")?;
        sink(value)?;
    }
    Ok(())
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
    use super::*;

    use super::super::test_entry as entry;
    use crate::meta::SnapshotEntry;
    use crate::object_store::CompressionLevel;

    /// Encode `records` into an in-memory buffer with `encode_framed_records_streaming`
    /// (pulling from a `Vec::into_iter`), then decode the buffer back with the sync
    /// streaming decoder — the same two functions the production write/read paths run
    /// inside `spawn_blocking`, here over plain in-memory buffers (no object store).
    fn roundtrip(records: Vec<SnapshotEntry>, level: CompressionLevel) -> Vec<SnapshotEntry> {
        let bytes = encode_to_vec(records, level).expect("encode");
        decode_all(&bytes).expect("decode")
    }

    fn encode_to_vec(
        records: Vec<SnapshotEntry>,
        level: CompressionLevel,
    ) -> Result<Vec<u8>, Error> {
        let mut iter = records.into_iter();
        let (buf, res) =
            encode_framed_records_streaming::<SnapshotEntry, _>(Vec::new(), level, || {
                iter.next().map(Ok)
            });
        res?;
        Ok(buf.expect("encoder returns the writer on success"))
    }

    fn decode_all(bytes: &[u8]) -> Result<Vec<SnapshotEntry>, Error> {
        let mut out = Vec::new();
        decode_framed_records_streaming::<SnapshotEntry, _>(
            std::io::Cursor::new(bytes.to_vec()),
            |e| {
                out.push(e);
                Ok(())
            },
        )?;
        Ok(out)
    }

    #[test]
    fn roundtrip_single_record() {
        let records = vec![entry(0)];
        assert_eq!(
            roundtrip(records.clone(), CompressionLevel::standard()),
            records
        );
    }

    #[test]
    fn roundtrip_multiple_records_preserves_order() {
        let records: Vec<SnapshotEntry> = (0..10).map(entry).collect();
        assert_eq!(
            roundtrip(records.clone(), CompressionLevel::fast()),
            records
        );
    }

    #[test]
    fn roundtrip_empty() {
        let records: Vec<SnapshotEntry> = vec![];
        assert!(roundtrip(records, CompressionLevel::standard()).is_empty());
    }

    #[test]
    fn decode_unknown_magic_errors() {
        let result = decode_all(b"XXXXhello");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .to_lowercase()
                .contains("magic"),
            "error should mention magic"
        );
    }

    /// The uncompressed variant of this format is gone, and its magic is not
    /// grandfathered: an object written by a build that still emitted `"SNAP"`
    /// is refused rather than silently mis-decoded.
    #[test]
    fn decode_rejects_legacy_uncompressed_magic() {
        let result = decode_all(b"SNAPhello");
        assert!(
            result
                .expect_err("legacy magic must be refused")
                .to_string()
                .to_lowercase()
                .contains("magic")
        );
    }

    /// Every level in the accepted range must produce something the decoder
    /// reads back, including the `0` that zstd reinterprets as its default.
    #[test]
    fn roundtrip_at_range_bounds() {
        let records = vec![entry(1), entry(2)];
        for level in [0, 1, CompressionLevel::MAX] {
            let level = CompressionLevel::new(level).expect("level in range");
            assert_eq!(
                roundtrip(records.clone(), level),
                records,
                "level {level:?}"
            );
        }
    }

    #[test]
    fn level_above_max_is_rejected() {
        assert!(CompressionLevel::new(23).is_err());
        assert!(CompressionLevel::new(u8::MAX).is_err());
    }

    #[test]
    fn decode_truncated_errors() {
        // Fewer than 4 bytes — can't read the magic header.
        assert!(decode_all(b"SN").is_err());
    }

    #[test]
    fn decode_failure_surfaces_as_error() {
        // Encode a `0u64` record (8 bytes) then try to decode it as a SnapshotEntry
        // → per-record decode error.
        let mut iter = std::iter::once(0u64);
        let (bytes, res) = encode_framed_records_streaming::<u64, _>(
            Vec::new(),
            CompressionLevel::standard(),
            || iter.next().map(Ok),
        );
        res.expect("encode u64");
        assert!(decode_all(&bytes.expect("writer")).is_err());
    }

    /// The CRC lives *inside* the zstd stream, so it covers the record bytes
    /// themselves rather than merely their compressed image.
    #[test]
    fn decode_detects_record_corruption() {
        let bytes =
            encode_to_vec(vec![entry(7)], CompressionLevel::standard()).expect("encode compressed");
        // Corrupt a byte in the compressed body (after the 4-byte magic); zstd
        // decompression yields altered record bytes the CRC then rejects (or zstd
        // itself errors first — either way, not silent acceptance).
        let mut corrupt = bytes.clone();
        let mid = corrupt.len() / 2;
        corrupt[mid] ^= 0x01;
        assert!(decode_all(&corrupt).is_err());
    }

    /// A large run exercises the streaming path (many frames through one zstd
    /// stream) and verifies count + order are preserved end-to-end.
    #[test]
    fn roundtrip_large_run() {
        let records: Vec<SnapshotEntry> = (0..2_000).map(entry).collect();
        let mut iter = records.clone().into_iter();
        let (bytes, res) = encode_framed_records_streaming::<SnapshotEntry, _>(
            Vec::new(),
            CompressionLevel::fast(),
            || iter.next().map(Ok),
        );
        let (count, _uncompressed) = res.expect("encode large");
        assert_eq!(count, records.len());
        assert_eq!(
            decode_all(&bytes.expect("writer")).expect("decode large"),
            records
        );
    }
    /// Early termination — an upload that died mid-object, a stream cut short —
    /// is caught at *every* offset, not merely mid-frame. zstd's own end marker
    /// is what buys this: a cut landing exactly on a frame boundary used to look
    /// like a legitimately shorter snapshot on the uncompressed path, and now
    /// fails because the decoder never sees the frame's end.
    #[test]
    fn decode_detects_truncation_at_any_offset() {
        let records: Vec<SnapshotEntry> = (0..50).map(entry).collect();
        let bytes = encode_to_vec(records, CompressionLevel::standard()).expect("encode");
        let len = bytes.len();
        for cut in 4..len {
            assert!(
                decode_all(&bytes[..cut]).is_err(),
                "truncation to {cut} of {len} bytes must be rejected"
            );
        }
        assert!(
            decode_all(&bytes).is_ok(),
            "the intact object still decodes"
        );
    }
}
