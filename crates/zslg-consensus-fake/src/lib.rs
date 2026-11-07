//! In-memory stand-ins for the consensus ports.

use std::sync::Arc;
use zslg_consensus::{MembershipGate, NodeId};
use zslg_net::PeerAddr;

/// Admits everything. For tests where membership policy is not under test.
#[must_use]
pub fn permissive_gate() -> Arc<dyn MembershipGate> {
    Arc::new(PermissiveGate)
}

struct PermissiveGate;

impl MembershipGate for PermissiveGate {
    fn admit(&self, _node_id: NodeId, _peer_addr: &PeerAddr) -> Result<(), zslg_consensus::Error> {
        Ok(())
    }
}

/// A deterministic [`NodeId`] with `byte` in the last position.
#[must_use]
pub fn nid(byte: u8) -> NodeId {
    let mut b = [0u8; 16];
    b[15] = byte;
    NodeId::from_uuid(uuid::Uuid::from_bytes(b))
}

/// A deterministic "upstream" [`NodeId`] (`0xAA` in byte 0, `seed` in byte 15),
/// for replication data sources.
#[must_use]
pub fn upstream(seed: u8) -> NodeId {
    let mut b = [0u8; 16];
    b[0] = 0xAA;
    b[15] = seed;
    NodeId::from_uuid(uuid::Uuid::from_bytes(b))
}
