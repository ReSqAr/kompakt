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
