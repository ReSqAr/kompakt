//! Strongly-typed ids — newtypes over what would otherwise be interchangeable
//! integers and uuids.
//!
//! The point is that `LogIndex`, `Hlc` and `RaftGeneration` are all `u64` and
//! mean entirely different things; a signature taking three of them is a bug
//! waiting to be written. Nothing here has logic beyond construction and
//! projection.
//!
//! [`DataId`] is the exception worth reading: it is not opaque. It packs the
//! creation HLC, the originating node and 64 random bits, so a `Data` needs no
//! metadata header and no two writes can collide (CLAUDE.md §8).

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::hlc::Hlc;

/// Unique node identifier (UUID-backed, persisted across restarts).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct NodeId(pub(crate) Uuid);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl NodeId {
    /// All-`0xFF` node id — the lexicographic maximum. Used as a range
    /// bound when scanning `DataId`-keyed keyspaces by HLC window (P3).
    pub(crate) const MAX: Self = Self(Uuid::from_u128(u128::MAX));

    #[must_use]
    pub(crate) fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    /// Only tests build a `NodeId` from a chosen UUID; production always mints
    /// a fresh one (`new_v4`) or decodes a stored/wire value.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::ids) fn into_uuid(self) -> Uuid {
        self.0
    }
}

/// Stable identity string for a peer (e.g. its Kubernetes hostname). A newtype
/// wrapper around `String` used consistently through config, RPC messages, and
/// the connection pool. It is the peer's logical name — the membership gate
/// admits a joiner only if its `server_name` appears in `config.peers`
/// (`crate::api::MembershipGate`, enforced by `crate::network::PeersMembershipGate`) — not a TLS SNI value; the transport is
/// plain TCP on a trusted network.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServerName(pub(in crate::ids) String);

impl std::fmt::Display for ServerName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for ServerName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ServerName {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl std::ops::Deref for ServerName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// A peer's dial target as an unresolved `host:port` string (e.g.
/// `"replica-1:9000"` or `"127.0.0.1:5400"`). Kept unresolved so it can be
/// re-resolved via DNS at every (re)connect — a container that returns at a new
/// IP is then reached without restarting its peers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HostPort(String);

impl HostPort {
    /// Build a `host:port` from parts.
    #[must_use]
    pub fn new(host: &str, port: u16) -> Self {
        Self(format!("{host}:{port}"))
    }

    /// Resolve to a concrete socket address via DNS. Called immediately before
    /// each dial so address changes are picked up. Returns the first address.
    pub async fn resolve(&self) -> Result<SocketAddr, crate::Error> {
        tokio::net::lookup_host(self.0.as_str())
            .await
            .map_err(|e| crate::Error::Rpc(format!("resolve {}: {e}", self.0)))?
            .next()
            .ok_or_else(|| crate::Error::Rpc(format!("resolve {}: no addresses", self.0)))
    }
}

impl From<&str> for HostPort {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<SocketAddr> for HostPort {
    fn from(addr: SocketAddr) -> Self {
        Self(addr.to_string())
    }
}

impl std::fmt::Display for HostPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Network address of a peer: dial `host:port` plus its `server_name` identity
/// (used for bootstrap-node selection and membership; not a TLS SNI value).
/// Used as the key in the connection pool, so `NodeId`-registered peers and
/// direct-dial peers share the same pooled TCP connections.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerAddr {
    pub addr: HostPort,
    pub server_name: ServerName,
}

impl std::fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

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
pub struct DataId(pub(crate) [u8; 32]);

impl DataId {
    /// Assemble from an HLC, the originating node, and a random tiebreaker.
    /// Argument order matches the byte layout: HLC, then NodeId, then random.
    #[must_use]
    pub(crate) fn from_parts(hlc: Hlc, node_id: NodeId, random: u64) -> Self {
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

/// Gapless, monotonic index into a replica or local log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogIndex(pub(crate) u64);

impl LogIndex {
    /// The first index of any log — and the only start point a caller with no
    /// prior cursor can name, which is why it is public: `Reader::stable_tail`
    /// and `Reader::subscribe_stable_log` require an explicit `from`.
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

    #[must_use]
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    #[must_use]
    pub fn into_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

/// Snapshot identifier: the first global index the snapshot covers, the last
/// one when it was known at mint time, and a 64-bit random suffix.
///
/// The indices are there for whoever browses the bucket — the object name
/// renders them (`object_store::snapshot_store::snapshot_name`), so the store
/// reads as a tiling of the stable log. A freshly created snapshot has no
/// `end_index`: its id is minted before the window is streamed, so its length
/// is not yet known. A compaction-merged one does, since it replaces a known run
/// of manifest entries. Either way the manifest's [`crate::meta::SnapshotMetadata`]
/// is the authority on what a snapshot covers; the id only names it.
///
/// The random suffix is what makes every minted id globally unique: two
/// leaders racing over the same window (a crashed or deposed one included)
/// mint the same start index, and the no-CAS, immutable, never-overwrite object
/// store (CLAUDE.md §11) must not let the loser clobber the committed object.
///
/// Ordering is `(start_index, end_index, random)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SnapshotId {
    start_index: LogIndex,
    end_index: Option<LogIndex>,
    random: u64,
}

impl SnapshotId {
    /// Assemble from explicit parts.
    #[must_use]
    pub(crate) fn from_parts(
        start_index: LogIndex,
        end_index: Option<LogIndex>,
        random: u64,
    ) -> Self {
        Self {
            start_index,
            end_index,
            random,
        }
    }

    /// A fresh id for a snapshot whose extent is known only from its start —
    /// the snapshot-creation path, which mints before streaming the window.
    #[must_use]
    pub(crate) fn open(start_index: LogIndex) -> Self {
        Self::from_parts(start_index, None, rand::random())
    }

    /// A fresh id for a snapshot covering exactly `start_index..=end_index` —
    /// the compaction-merge path.
    #[must_use]
    pub(crate) fn closed(start_index: LogIndex, end_index: LogIndex) -> Self {
        Self::from_parts(start_index, Some(end_index), rand::random())
    }

    /// The first global index the snapshot covers.
    #[must_use]
    pub(crate) fn start_index(self) -> LogIndex {
        self.start_index
    }

    /// The last global index the snapshot covers, if known when it was minted.
    #[must_use]
    pub(crate) fn end_index(self) -> Option<LogIndex> {
        self.end_index
    }

    /// The random suffix.
    #[must_use]
    pub(crate) fn random(self) -> u64 {
        self.random
    }

    /// Test shorthand: the high 64 bits become the start index, the low 64 the
    /// random suffix, with no end index.
    #[must_use]
    #[cfg(test)]
    #[allow(clippy::expect_used, reason = "each half of a u128 fits u64")]
    pub(crate) fn from_u128(val: u128) -> Self {
        let high = u64::try_from(val >> 64).expect("high half fits u64");
        let low = u64::try_from(val & u128::from(u64::MAX)).expect("low half fits u64");
        Self::from_parts(LogIndex(high), None, low)
    }
}

/// Monotonic Raft membership generation (quorum trusts the highest).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct RaftGeneration(pub(crate) u64);

impl RaftGeneration {
    pub(crate) const ZERO: Self = Self(0);

    /// The raw counter. Public because `RaftState::generation` is: a caller
    /// that can read the generation must be able to compare or print it.
    #[must_use]
    pub fn raw(self) -> u64 {
        self.0
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

    // ── NodeId ──────────────────────────────────────────────────────────

    #[test]
    fn node_id_new_v4_is_unique() {
        assert_ne!(NodeId::new_v4(), NodeId::new_v4());
    }

    #[test]
    fn node_id_uuid_roundtrip() {
        let u = Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
        let id = NodeId::from_uuid(u);
        assert_eq!(id.into_uuid(), u);
        assert_eq!(id.0, u);
    }

    #[test]
    fn node_id_display_matches_uuid() {
        let u = Uuid::from_u128(42);
        assert_eq!(NodeId::from_uuid(u).to_string(), u.to_string());
    }

    #[test]
    fn node_id_default_is_nil() {
        assert_eq!(NodeId::default().into_uuid(), Uuid::nil());
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

    // ── SnapshotId ──────────────────────────────────────────────────────

    #[test]
    fn snapshot_id_fresh_ids_are_unique() {
        // The random suffix means two ids minted over the *same* range differ —
        // this is what keeps two leaders racing over one window (or one merge
        // group) from ever writing the same object (the no-CAS overwrite hazard).
        assert_ne!(SnapshotId::open(LogIndex(0)), SnapshotId::open(LogIndex(0)));
        assert_ne!(
            SnapshotId::closed(LogIndex(4), LogIndex(9)),
            SnapshotId::closed(LogIndex(4), LogIndex(9))
        );
    }

    #[test]
    fn snapshot_id_parts_roundtrip() {
        let id = SnapshotId::from_parts(LogIndex(7), Some(LogIndex(11)), 0xcafe_babe_dead_beef);
        assert_eq!(id.start_index(), LogIndex(7));
        assert_eq!(id.end_index(), Some(LogIndex(11)));
        assert_eq!(id.random(), 0xcafe_babe_dead_beef);
        let open = SnapshotId::open(LogIndex(3));
        assert_eq!(open.start_index(), LogIndex(3));
        assert_eq!(open.end_index(), None);
    }

    #[test]
    fn snapshot_id_ordering_follows_start_index() {
        let a = SnapshotId::from_parts(LogIndex(1), Some(LogIndex(u64::MAX)), u64::MAX);
        let b = SnapshotId::from_parts(LogIndex(2), None, 0);
        assert!(a < b);
    }

    // ── RaftGeneration ──────────────────────────────────────────────────

    #[test]
    fn raft_generation_zero_and_raw() {
        assert_eq!(RaftGeneration::ZERO, RaftGeneration(0));
        assert_eq!(RaftGeneration::ZERO.raw(), 0);
        assert_eq!(RaftGeneration(9).raw(), 9);
    }

    #[test]
    fn raft_generation_ordering() {
        assert!(RaftGeneration(1) < RaftGeneration(2));
    }

    // ── serde ───────────────────────────────────────────────────────────

    #[test]
    fn ids_serde_roundtrip() {
        let n = NodeId::new_v4();
        let d = DataId::test_id(Hlc(42), 7);
        let l = LogIndex(123);
        let s = SnapshotId::from_u128(456);
        let g = RaftGeneration(789);
        assert_eq!(
            postcard::from_bytes::<NodeId>(&postcard::to_allocvec(&n).unwrap()).unwrap(),
            n
        );
        assert_eq!(
            postcard::from_bytes::<DataId>(&postcard::to_allocvec(&d).unwrap()).unwrap(),
            d
        );
        assert_eq!(
            postcard::from_bytes::<LogIndex>(&postcard::to_allocvec(&l).unwrap()).unwrap(),
            l
        );
        assert_eq!(
            postcard::from_bytes::<SnapshotId>(&postcard::to_allocvec(&s).unwrap()).unwrap(),
            s
        );
        assert_eq!(
            postcard::from_bytes::<RaftGeneration>(&postcard::to_allocvec(&g).unwrap()).unwrap(),
            g
        );
    }

    // ── HostPort ────────────────────────────────────────────────────────

    #[test]
    fn host_port_new_display_and_from_str() {
        let hp = HostPort::new("replica-1", 9000);
        assert_eq!(hp.to_string(), "replica-1:9000");
        assert_eq!(HostPort::from("replica-1:9000"), hp);
    }

    #[test]
    fn host_port_from_socket_addr() {
        let sa: SocketAddr = "127.0.0.1:5400".parse().unwrap();
        assert_eq!(HostPort::from(sa).to_string(), "127.0.0.1:5400");
    }

    #[test]
    fn host_port_serde_is_transparent() {
        let hp = HostPort::from("replica-2:7000");
        let bytes = postcard::to_allocvec(&hp).unwrap();
        assert_eq!(postcard::from_bytes::<HostPort>(&bytes).unwrap(), hp);
        // `#[serde(transparent)]`: encodes identically to the inner String.
        assert_eq!(
            bytes,
            postcard::to_allocvec(&"replica-2:7000".to_owned()).unwrap()
        );
    }

    #[tokio::test]
    async fn host_port_resolves_ip_literal() {
        let got = HostPort::from("127.0.0.1:5400").resolve().await.unwrap();
        assert_eq!(got, "127.0.0.1:5400".parse::<SocketAddr>().unwrap());
    }

    #[tokio::test]
    async fn host_port_resolves_localhost_to_loopback() {
        let got = HostPort::new("localhost", 5400).resolve().await.unwrap();
        assert!(got.ip().is_loopback());
        assert_eq!(got.port(), 5400);
    }

    #[tokio::test]
    async fn host_port_resolve_failure_is_typed() {
        let err = HostPort::from("not-a-host-port")
            .resolve()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("resolve"));
    }
}
