//! Frame integrity: `[record | crc32(4)]`.
//!
//! Corruption detection, not authentication. The whole encoded record is hashed
//! — identity metadata as well as payload — so a flipped `data_id` or log index
//! is caught, not just a flipped blob byte.
//!
//! Used by the two surfaces RocksDB's own block checksums do not reach: RPC
//! envelopes (`network::framing`) and object-store snapshot records
//! (`object_store::framing`). Everything inside the node's DB is covered by
//! RocksDB itself.

use crate::error::Error;

/// CRC32 (IEEE) of `bytes`, as the 4 big-endian bytes written on the wire.
/// Guaranteed detection of 1–2 bit errors and bursts <= 32 bits — corruption
/// detection, not security. Returning the wire bytes (not a host-endian `u32`)
/// keeps the byte-order choice in one place, so every frame codec writes and
/// compares raw bytes with no per-site `to_be_bytes`/`from_be_bytes`.
fn crc32(bytes: &[u8]) -> [u8; 4] {
    crc32fast::hash(bytes).to_be_bytes()
}

/// Append the 4-byte CRC32 of `buf`'s current contents to `buf`, producing a
/// `[record | crc32(4)]` frame. Pair with [`verify_frame_crc32`].
///
/// `reserve_exact(4)` first: `postcard::to_allocvec` hands back a tightly-sized
/// `Vec`, so a bare `extend_from_slice` would grow-and-copy the whole buffer.
pub(crate) fn append_crc32(buf: &mut Vec<u8>) {
    append_crc32_from(buf, 0);
}

/// [`append_crc32`] over `buf[start..]` only: checksums the record that starts
/// at `start` and appends its CRC to the end. For a caller assembling frames in
/// one long-lived buffer, where the bytes before `start` are earlier frames (or
/// this frame's own header) and must not be covered — the RPC sink, see
/// `network::framing`. A `start` past the end checksums nothing, which
/// [`verify_frame_crc32`] then rejects as a short frame.
pub(crate) fn append_crc32_from(buf: &mut Vec<u8>, start: usize) {
    let sum = crc32(buf.get(start..).unwrap_or_default());
    buf.reserve_exact(4);
    buf.extend_from_slice(&sum);
}

/// Check the 4 wire bytes `checksum` against the CRC32 of `body`. `Err` on
/// mismatch. Internal primitive behind [`verify_frame_crc32`].
fn verify_crc32(body: &[u8], checksum: &[u8; 4]) -> Result<(), Error> {
    if *checksum != crc32(body) {
        return Err(Error::Codec("crc32 checksum mismatch".into()));
    }
    Ok(())
}

/// Split a contiguous `[record | crc32(4)]` frame, verifying the trailing
/// checksum against the record bytes. Returns the record slice on success;
/// `Error::Codec` on a short buffer or a mismatch. Verify **before** decoding.
pub(crate) fn verify_frame_crc32(frame: &[u8]) -> Result<&[u8], Error> {
    let body_len = frame
        .len()
        .checked_sub(4)
        .ok_or_else(|| Error::Codec("crc32 frame shorter than checksum".into()))?;
    let (body, sum) = frame.split_at(body_len);
    let sum: &[u8; 4] = sum
        .try_into()
        .map_err(|_| Error::Codec("crc32 checksum slice".into()))?;
    verify_crc32(body, sum)?;
    Ok(body)
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
    use crate::error::Error;

    // ── CRC32 frame integrity ───────────────────────────────────────────

    #[test]
    fn crc32_roundtrip() {
        let mut buf = b"the whole encoded record".to_vec();
        let record_len = buf.len();
        append_crc32(&mut buf);
        assert_eq!(buf.len(), record_len + 4);
        assert_eq!(
            verify_frame_crc32(&buf).expect("verify"),
            &buf[..record_len]
        );
    }

    #[test]
    fn crc32_detects_body_flip() {
        let mut buf = b"record identity + blob".to_vec();
        append_crc32(&mut buf);
        buf[0] ^= 0x01; // flip a bit in the record (e.g. a data_id byte)
        assert!(matches!(verify_frame_crc32(&buf), Err(Error::Codec(_))));
    }

    #[test]
    fn crc32_detects_checksum_flip() {
        let mut buf = b"record".to_vec();
        append_crc32(&mut buf);
        let last = buf.len() - 1;
        buf[last] ^= 0x01; // flip a bit in the trailing CRC
        assert!(matches!(verify_frame_crc32(&buf), Err(Error::Codec(_))));
    }

    #[test]
    fn crc32_rejects_short_frame() {
        // Fewer than 4 bytes cannot carry a checksum.
        assert!(matches!(
            verify_frame_crc32(&[0u8; 3]),
            Err(Error::Codec(_))
        ));
    }

    #[test]
    fn crc32_verifies_empty_record() {
        // A zero-length record still round-trips: the frame is exactly the CRC.
        let mut buf = Vec::new();
        append_crc32(&mut buf);
        assert_eq!(buf.len(), 4);
        assert!(verify_frame_crc32(&buf).expect("verify empty").is_empty());
    }

    #[test]
    fn crc32_body_and_checksum_split_form() {
        // The low-level form: body and checksum held separately (the streaming
        // decoder's shape). A matching pair verifies; a flipped body does not.
        let body = b"separately-read record";
        let sum = crc32(body);
        assert!(verify_crc32(body, &sum).is_ok());
        assert!(matches!(
            verify_crc32(b"different record", &sum),
            Err(Error::Codec(_))
        ));
    }
}
