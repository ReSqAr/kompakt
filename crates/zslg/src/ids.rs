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
    use zslg_log_types::LogIndex;
    use zslg_node_types::NodeId;
    use zslg_snapshot_types::SnapshotId;
    use zslg_time_types::Hlc;

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
