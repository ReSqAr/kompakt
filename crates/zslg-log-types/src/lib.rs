//! The replicated log's vocabulary: records (Data), positions (DataId, LogIndex), events, and the errors of reading and writing them.

// carved from zslg
use bytes::Bytes;
use crossbeam_combinators::{Key, Successor};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;
use zslg_node_types::NodeId;
use zslg_time_types::{HasHlc, Hlc};

/// Unique data identifier: 32 bytes = 64-bit HLC (big-endian) ‖ 128-bit NodeId
/// (big-endian) ‖ 64-bit random (big-endian). Self-describing — the originating
/// node and creation HLC are both recoverable, so no separate `Meta` header is
/// needed.
///
/// Derives `Ord` so lexicographic sort by the raw bytes produces
/// `(hlc, node_id, random)` ordering — the leading 8 HLC bytes dominate, which
/// is what the HLC-ordered unstable tail relies on. The 64-bit random word makes
/// same-`(hlc, node)` collisions effectively impossible (2⁻⁶⁴ per pair).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DataId(pub [u8; 32]);

impl DataId {
    /// Assemble from an HLC, the originating node, and a random tiebreaker.
    /// Argument order matches the byte layout: HLC, then NodeId, then random.
    #[must_use]
    pub fn from_parts(hlc: Hlc, node_id: NodeId, random: u64) -> Self {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&hlc.raw().to_be_bytes());
        bytes[8..24].copy_from_slice(&node_id.0.as_u128().to_be_bytes());
        bytes[24..].copy_from_slice(&random.to_be_bytes());
        Self(bytes)
    }

    /// The creation HLC, extracted from bytes 0..8.
    #[must_use]
    #[allow(
        clippy::expect_used,
        reason = "try_into on exact [u8; 8] is infallible"
    )]
    pub fn hlc(self) -> Hlc {
        Hlc(u64::from_be_bytes(self.0[..8].try_into().expect("8 bytes")))
    }

    /// The originating node, extracted from bytes 8..24.
    #[must_use]
    #[allow(
        clippy::expect_used,
        reason = "try_into on exact [u8; 16] is infallible"
    )]
    pub fn source_node(self) -> NodeId {
        NodeId(Uuid::from_u128(u128::from_be_bytes(
            self.0[8..24].try_into().expect("16 bytes"),
        )))
    }

    /// Test helper: a deterministic id with a nil node and `seed` as the random
    /// word. Keeps the HLC orderable while giving distinct ids per `seed`.
    #[must_use]
    pub fn test_id(hlc: Hlc, seed: u64) -> Self {
        Self::from_parts(hlc, NodeId(Uuid::nil()), seed)
    }

    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Contiguous, monotonic index into a replica or local log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogIndex(pub u64);

impl LogIndex {
    /// The first index of any log. A subscription starts *after* a position
    /// (`Reader::subscribe_stable_log(after)`), and `None` means from the
    /// beginning; this is the first index that start yields.
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub fn raw(self) -> u64 {
        self.0
    }

    /// Return the index that follows `prev`, or `ZERO` when `prev` is `None`.
    ///
    /// # Panics
    /// Panics on `u64::MAX` — saturating would silently produce a *duplicate*
    /// index, violating the gapless-and-unique log invariant. The overflow is
    /// unreachable in practice (2^64 appends).
    #[must_use]
    #[allow(
        clippy::expect_used,
        reason = "2^64 log entries is unreachable in practice"
    )]
    pub fn next_after(prev: Option<LogIndex>) -> LogIndex {
        match prev {
            None => LogIndex::ZERO,
            Some(i) => LogIndex(
                i.raw()
                    .checked_add(1)
                    .expect("LogIndex overflow: log index space exhausted"),
            ),
        }
    }

    /// The exclusive start that names this index as the first one wanted:
    /// the index before it, or `None` for the very first. The inverse of
    /// [`next_after`](Self::next_after) — `next_after(i.before()) == i`.
    #[must_use]
    pub fn before(self) -> Option<LogIndex> {
        self.0.checked_sub(1).map(LogIndex)
    }

    #[must_use]
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    #[must_use]
    pub fn into_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

/// Self-describing data id + opaque user blob. The id carries the creation HLC
/// (`data_id.hlc()`) and the originating node (`data_id.source_node()`), so no
/// separate metadata header is needed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Data {
    pub data_id: DataId,
    #[serde(with = "serde_bytes_compat")]
    pub blob: Bytes,
}

/// Events emitted on the stable-log broadcast channel in causal order:
/// `Appended` entries appear before any `Watermark` that covers them, so when a
/// consumer sees `Watermark(hlc)`, every entry with `data_id.hlc() <= hlc` has
/// already been emitted.
#[derive(Debug, Clone)]
pub enum StableLogEvent {
    Appended { index: LogIndex, data: Data },
    Watermark(Hlc),
}

/// Events of the unstable-tail subscription, in arrival (integration) order.
/// `Appended` may repeat; consumers dedup by `data_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnstableEvent {
    Appended(Data),
    /// Every entry with `data_id.hlc() <= hlc` is settled: the stable log
    /// delivers it, or it never converged and is gone. Drop held entries at or
    /// below it.
    Watermark(Hlc),
}

mod serde_bytes_compat {
    use bytes::Bytes;
    use serde::de::{Error, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub(super) fn serialize<S: Serializer>(bytes: &Bytes, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_bytes(bytes.as_ref())
    }

    /// Bulk byte-buffer deserialize. The previous `<Vec<u8>>::deserialize` went
    /// through serde's generic `Vec<T>` path = one `SeqAccess::next_element`
    /// visitor call **per byte**, which dominated CPU on the hot decode path
    /// (`Data::decode` was ~40% of all samples at inflight=128; see
    /// docs/PERFORMANCE.md entry 9). `deserialize_bytes` hands us the whole slice
    /// in one shot (postcard reads a varint length, then the raw bytes).
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Bytes, D::Error> {
        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Bytes;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte buffer")
            }

            fn visit_bytes<E: Error>(self, v: &[u8]) -> Result<Bytes, E> {
                Ok(Bytes::copy_from_slice(v))
            }

            fn visit_borrowed_bytes<E: Error>(self, v: &'de [u8]) -> Result<Bytes, E> {
                Ok(Bytes::copy_from_slice(v))
            }

            fn visit_byte_buf<E: Error>(self, v: Vec<u8>) -> Result<Bytes, E> {
                Ok(Bytes::from(v))
            }
        }

        de.deserialize_bytes(BytesVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub global_index: LogIndex,
    pub data: Data,
}

impl Successor for LogIndex {
    fn next_after(prev: Option<Self>) -> Self {
        Self::next_after(prev)
    }
}

impl<E> Key<Result<(LogIndex, Data), E>> for LogIndex {
    fn key(item: &Result<(LogIndex, Data), E>) -> Option<Self> {
        item.as_ref().ok().map(|(index, _)| *index)
    }
}

impl<E> Key<Result<StableLogEvent, E>> for LogIndex {
    fn key(item: &Result<StableLogEvent, E>) -> Option<Self> {
        match item {
            Ok(StableLogEvent::Appended { index, .. }) => Some(*index),
            Ok(StableLogEvent::Watermark(_)) | Err(_) => None,
        }
    }
}

impl<E> Key<Result<SnapshotEntry, E>> for LogIndex {
    fn key(item: &Result<SnapshotEntry, E>) -> Option<Self> {
        item.as_ref().ok().map(|entry| entry.global_index)
    }
}

impl<E> Key<Result<Data, E>> for DataId {
    fn key(item: &Result<Data, E>) -> Option<Self> {
        item.as_ref().ok().map(|data| data.data_id)
    }
}

impl HasHlc for Data {
    fn hlc(&self) -> Option<Hlc> {
        Some(self.data_id.hlc())
    }
}

impl HasHlc for SnapshotEntry {
    fn hlc(&self) -> Option<Hlc> {
        Some(self.data.data_id.hlc())
    }
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

    fn sample_data_id() -> DataId {
        DataId::from_parts(Hlc(100), NodeId::from_uuid(Uuid::from_u128(1)), 2)
    }

    // ── DataId ──────────────────────────────────────────────────────────

    #[test]
    fn data_id_test_id_is_unique_per_seed() {
        assert_ne!(DataId::test_id(Hlc(0), 1), DataId::test_id(Hlc(0), 2));
    }

    #[test]
    fn data_id_parts_roundtrip() {
        let hlc = Hlc(0x1234_5678_9abc_def0);
        let node = NodeId::from_uuid(Uuid::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff));
        let random = 0xcafe_babe_dead_beef;
        let id = DataId::from_parts(hlc, node, random);
        assert_eq!(id.hlc(), hlc);
        assert_eq!(id.source_node(), node);
        assert_eq!(u64::from_be_bytes(id.0[24..].try_into().unwrap()), random);
    }

    #[test]
    fn data_id_hlc_extracts_first_8_bytes() {
        let hlc = Hlc(0xaaaa_bbbb_cccc_dddd);
        let id = DataId::test_id(hlc, 0);
        assert_eq!(id.hlc(), hlc);
    }

    #[test]
    fn data_id_bytes_roundtrip() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xde;
        bytes[31] = 0xef;
        let id = DataId::from_bytes(bytes);
        assert_eq!(id.into_bytes(), bytes);
    }

    #[test]
    fn data_id_ordering_follows_hlc() {
        // HLC dominates over both node and random.
        let a = DataId::from_parts(Hlc(1), NodeId::from_uuid(Uuid::from_u128(u128::MAX)), 999);
        let b = DataId::test_id(Hlc(2), 0);
        assert!(a < b);
    }

    #[test]
    fn data_id_ordering_then_node_then_random() {
        // Same HLC: NodeId is the next discriminator, then random.
        let a = DataId::from_parts(Hlc(1), NodeId::from_uuid(Uuid::from_u128(1)), u64::MAX);
        let b = DataId::from_parts(Hlc(1), NodeId::from_uuid(Uuid::from_u128(2)), 0);
        assert!(a < b);

        let c = DataId::from_parts(Hlc(1), NodeId::from_uuid(Uuid::from_u128(1)), 0);
        let d = DataId::from_parts(Hlc(1), NodeId::from_uuid(Uuid::from_u128(1)), 1);
        assert!(c < d);
    }

    // ── LogIndex ────────────────────────────────────────────────────────

    #[test]
    fn log_index_zero_and_raw() {
        assert_eq!(LogIndex::ZERO, LogIndex(0));
        assert_eq!(LogIndex::ZERO.raw(), 0);
        assert_eq!(LogIndex(42).raw(), 42);
    }

    #[test]
    fn log_index_next_after_none_is_zero() {
        assert_eq!(LogIndex::next_after(None), LogIndex::ZERO);
    }

    #[test]
    fn log_index_next_after_some_increments() {
        assert_eq!(LogIndex::next_after(Some(LogIndex(5))), LogIndex(6));
    }

    /// F-5: saturating at `u64::MAX` would silently hand out a *duplicate*
    /// index; overflow must panic instead (unreachable in practice).
    #[test]
    #[should_panic(expected = "LogIndex overflow")]
    fn log_index_next_after_panics_at_max() {
        let _ = LogIndex::next_after(Some(LogIndex(u64::MAX)));
    }

    #[test]
    fn log_index_ordering() {
        assert!(LogIndex(1) < LogIndex(2));
        assert_eq!(LogIndex(7), LogIndex(7));
    }

    // ── serde roundtrips ────────────────────────────────────────────────

    #[test]
    fn data_serde_roundtrip() {
        let d = Data {
            data_id: sample_data_id(),
            blob: Bytes::from_static(b"hello world"),
        };
        let bytes = postcard::to_allocvec(&d).expect("encode");
        let decoded: Data = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, d);
    }

    #[test]
    fn snapshot_entry_serde_roundtrip() {
        let e = SnapshotEntry {
            global_index: LogIndex(7),
            data: Data {
                data_id: sample_data_id(),
                blob: Bytes::from_static(b"x"),
            },
        };
        let bytes = postcard::to_allocvec(&e).expect("encode");
        assert_eq!(
            postcard::from_bytes::<SnapshotEntry>(&bytes).expect("decode"),
            e
        );
    }

    #[test]
    fn empty_blob_roundtrips() {
        let d = Data {
            data_id: sample_data_id(),
            blob: Bytes::new(),
        };
        let bytes = postcard::to_allocvec(&d).expect("encode");
        let decoded: Data = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, d);
        assert!(decoded.blob.is_empty());
    }
}

/// The replicated and local logs and the storage under them: storage, codec, gaps, truncation, subscriptions.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum LogError {
    /// Storage I/O failure.
    #[error("storage error: {0}")]
    Storage(String),

    /// The leader's local stable log has not yet caught up with the committed
    /// manifest, so a snapshot's global index assignment cannot continue from
    /// it. Backpressure, not a fault: the leader re-ingests its own snapshots
    /// through the object store, so this clears on its own. Distinct from
    /// `Storage` precisely so it does not report as a storage failure
    /// (`SnapshotState::WaitingForStableLog`).
    #[error(
        "stable log behind manifest: end_index={end_index:?}, manifest_end_index={manifest_end_index:?}"
    )]
    StableLogBehindManifest {
        end_index: Option<LogIndex>,
        manifest_end_index: LogIndex,
    },

    /// A gap or ordering violation in the stable log's recoverable history:
    /// the never-GC'd snapshots that must tile an index window skipped an
    /// index, delivered one out of HLC order, or stopped short of the window;
    /// or a served stable read would emit a non-contiguous / HLC-regressing
    /// entry. The stable history is meant to be a gapless, HLC-monotonic,
    /// index-ordered stream fully tiled by object-store snapshots (design doc
    /// §794; CLAUDE.md §8) — a violation is a serious invariant breach,
    /// surfaced loudly instead of forwarding a gap to a reader. Distinct from a
    /// transient `Storage`/`ObjectStore` fault so callers and metrics can page
    /// on it specifically.
    #[error("stable log gap: {0}")]
    StableLogGap(String),

    /// postcard serialisation/deserialisation failure.
    #[error("codec error: {0}")]
    Codec(String),

    /// A finite stream ended without its producer declaring completion (see
    /// `channel::End::Aborted`): the producer panicked, or was discarded before
    /// it ran. Items may be missing, so a consumer that needs the whole stream
    /// must fail rather than treat it as a clean end.
    #[error("stream {what} ended without closing; it may be truncated")]
    StreamTruncated { what: String },

    /// A subscription stream ended. Subscriptions are infinite by contract — a
    /// replica's append stream never completes — so any end means the upstream
    /// went away: a dropped connection, or a peer that is shutting down or not
    /// routable. A *declared* close is no different: it only ever comes from a
    /// stub with nothing to serve. The subscriber backs off and resumes from its
    /// durable cursor.

    /// A subscription stream ended without naming its peer — produced inside
    /// `integrate_loop`, which serves any upstream and so cannot attribute it.
    /// The per-peer caller re-wraps it as [`Error::SubscriptionEnded`].
    #[error("subscription stream ended: {reason}")]
    SubscriptionStreamEnded { reason: String },

    /// A client write exceeded `WriterConfig::write_timeout` (default 10s),
    /// covering the admission-acquire + fan-out phases, so a leaked permit or a
    /// stalled self/loopback target surfaces as a bounded error instead of a
    /// silent hang.
    #[error("write timed out after {elapsed:?}")]
    WriteTimeout { elapsed: std::time::Duration },

    /// A quorum read could not confirm replica catch-up within the deadline.
    /// The system may be partitioned or an upstream may be unrecoverable;
    /// callers can retry or fall back to a weaker read level.
    #[error("quorum read timed out after {timeout:?}")]
    QuorumReadTimeout { timeout: std::time::Duration },
}

/// Adapt any displayable error into [`LogError::Codec`], prefixed with what was being done.
pub trait ToCodec<T> {
    /// # Errors
    /// The original error, rendered into a [`LogError::Codec`] with `context`.
    fn to_codec(self, context: &str) -> Result<T, LogError>;
}

impl<T, E: std::fmt::Display> ToCodec<T> for Result<T, E> {
    fn to_codec(self, context: &str) -> Result<T, LogError> {
        self.map_err(|e| LogError::Codec(format!("{context}: {e}")))
    }
}

/// Adapt any displayable error into [`LogError::Storage`], prefixed with what was being done.
pub trait ToStorage<T> {
    /// # Errors
    /// The original error, rendered into a [`LogError::Storage`] with `context`.
    fn to_storage(self, context: &str) -> Result<T, LogError>;
}

impl<T, E: std::fmt::Display> ToStorage<T> for Result<T, E> {
    fn to_storage(self, context: &str) -> Result<T, LogError> {
        self.map_err(|e| LogError::Storage(format!("{context}: {e}")))
    }
}
