//! A node's identity: the UUID every log, vote and subscription is attributed to.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique node identifier (UUID-backed, persisted across restarts).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct NodeId(#[doc(hidden)] pub Uuid);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl NodeId {
    /// All-`0xFF` node id — the lexicographic maximum. Used as a range
    /// bound when scanning `DataId`-keyed keyspaces by HLC window (P3).
    #[doc(hidden)]
    pub const MAX: Self = Self(Uuid::from_u128(u128::MAX));

    #[must_use]
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    /// Only tests build a `NodeId` from a chosen UUID; production always mints
    /// a fresh one (`new_v4`) or decodes a stored/wire value.
    #[must_use]
    #[doc(hidden)]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    #[must_use]
    #[doc(hidden)]
    pub fn into_uuid(self) -> Uuid {
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
}
