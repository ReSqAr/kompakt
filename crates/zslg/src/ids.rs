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

use crate::error::NetError;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use zslg_log_types::LogIndex;

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
            .map_err(|e| crate::Error::Net(NetError::Rpc(format!("resolve {}: {e}", self.0))))?
            .next()
            .ok_or_else(|| {
                crate::Error::Net(NetError::Rpc(format!("resolve {}: no addresses", self.0)))
            })
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
    use zslg_log_types::DataId;
    use zslg_node_types::NodeId;
    use zslg_time_types::Hlc;

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
