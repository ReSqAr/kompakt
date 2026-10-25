//! The local store's two key codecs — the `data` family and the `cursors`
//! family — ported verbatim from the `Key` impls this store used before it
//! opened column families of the node's one DB.
//!
//! ```text
//!   data     DataId (32)                32 bytes, dropped by a `DeleteRange`
//!                                      below the purge threshold
//!   cursors  NodeId (16)               16 bytes, overwritten in place
//! ```
//!
//! The key is big-endian, so a **byte** compare is the `Ord` compare. That is
//! what lets `purge_below_hlc` express "hlc <= threshold" as a key-order
//! prefix with no lookup: `DataId` is itself HLC-major (`ids.rs`), so the key
//! alone already sorts in HLC order. (An older layout appended an `Hlc`
//! secondary; it was redundant with the primary and is gone.)

use crate::ids::{DataId, NodeId};

/// `DataId` is 32 raw bytes, already HLC-big-endian-major (`ids.rs`).
pub(in crate::learner::local_store) const DATA_PRIMARY_WIDTH: usize = 32;

/// A cursor key is the upstream `NodeId` alone — one overwritten slot per peer,
/// zero-width secondary, never retired.
pub(in crate::learner::local_store) const CURSOR_KEY_WIDTH: usize = 16;

#[must_use]
pub(in crate::learner::local_store) fn encode_data_key(
    data_id: DataId,
) -> [u8; DATA_PRIMARY_WIDTH] {
    data_id.0
}

#[must_use]
pub(in crate::learner::local_store) fn decode_data_key(bytes: &[u8]) -> Option<DataId> {
    let primary: [u8; DATA_PRIMARY_WIDTH] = bytes.try_into().ok()?;
    Some(DataId(primary))
}

#[must_use]
pub(in crate::learner::local_store) fn encode_cursor_key(node: NodeId) -> [u8; CURSOR_KEY_WIDTH] {
    node.0.as_u128().to_be_bytes()
}

// There is no `decode_cursor_key`: the cursors family is only ever point-read
// by a known upstream and overwritten, never scanned, so a decoder would have
// no caller but its own test.

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::hlc::Hlc;
    use proptest::prelude::*;

    #[test]
    fn data_key_roundtrips() {
        let data_id = DataId::from_parts(
            Hlc(0xCAFE),
            NodeId::from_uuid(uuid::Uuid::from_u128(0xBEEF)),
            0x1234,
        );
        let key = encode_data_key(data_id);
        assert_eq!(key.len(), 32);
        assert_eq!(decode_data_key(&key), Some(data_id));
    }

    #[test]
    fn data_key_decode_rejects_wrong_length() {
        assert_eq!(decode_data_key(&[0u8; 31]), None);
        assert_eq!(decode_data_key(&[0u8; 40]), None);
        assert_eq!(decode_data_key(&[]), None);
    }

    #[test]
    fn cursor_key_is_the_node_id_big_endian() {
        let node = NodeId::from_uuid(uuid::Uuid::from_u128(0xABCD_EF01));
        let key = encode_cursor_key(node);
        assert_eq!(key.len(), 16);
        assert_eq!(key, 0xABCD_EF01u128.to_be_bytes());
    }

    /// The property the retirement filter rests on: a byte compare of two
    /// encoded keys IS the `DataId` compare.
    #[test]
    fn key_order_matches_tuple_order() {
        proptest!(|(
            (p1, p2) in (any::<[u8; 32]>(), any::<[u8; 32]>()),
        )| {
            let k1 = encode_data_key(DataId(p1));
            let k2 = encode_data_key(DataId(p2));
            prop_assert_eq!(k1.cmp(&k2), DataId(p1).cmp(&DataId(p2)));
        });
    }
}
