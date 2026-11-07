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

use serde::{Deserialize, Serialize};

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
}
