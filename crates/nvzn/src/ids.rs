use serde::{Deserialize, Serialize};

pub(crate) const PROJECTION_TYPE_ID_LEN: usize = 4;

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub struct ProjectionTypeId([u8; PROJECTION_TYPE_ID_LEN]);

impl ProjectionTypeId {
    #[must_use]
    pub const fn from_slice(u: [u8; PROJECTION_TYPE_ID_LEN]) -> Self {
        Self(u)
    }

    #[must_use]
    pub const fn as_slice(&self) -> [u8; PROJECTION_TYPE_ID_LEN] {
        self.0
    }

    #[must_use]
    pub const fn from_u32(u: u32) -> Self {
        Self(u.to_be_bytes())
    }

    #[must_use]
    pub const fn as_u32(&self) -> u32 {
        u32::from_be_bytes(self.0)
    }

    #[must_use]
    pub const fn from_string_hash(s: &str, version: u32) -> Self {
        let h = const_fnv1a_hash::fnv1a_hash_str_32(s);
        Self::from_u32(h ^ version.wrapping_mul(0x9E37_79B9))
    }
}

pub(crate) const PROJECTION_ID_LEN: usize = 16;

#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub struct ProjectionId([u8; PROJECTION_ID_LEN]);

impl ProjectionId {
    pub const MIN: Self = Self([u8::MIN; PROJECTION_ID_LEN]);
    pub const MAX: Self = Self([u8::MAX; PROJECTION_ID_LEN]);

    #[must_use]
    pub const fn from_slice(u: [u8; PROJECTION_ID_LEN]) -> Self {
        Self(u)
    }

    #[must_use]
    pub const fn as_slice(&self) -> [u8; PROJECTION_ID_LEN] {
        self.0
    }

    #[must_use]
    pub const fn from_u128(u: u128) -> Self {
        Self(u.to_be_bytes())
    }

    #[must_use]
    pub const fn as_u128(&self) -> u128 {
        u128::from_be_bytes(self.0)
    }

    #[must_use]
    pub fn from_uuid(u: uuid::Uuid) -> Self {
        Self::from_u128(u.as_u128())
    }

    #[must_use]
    pub fn as_uuid(&self) -> uuid::Uuid {
        uuid::Uuid::from_u128(self.as_u128())
    }
}

pub(crate) const INDEX_ID_LEN: usize = 4;

#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub struct IndexId([u8; INDEX_ID_LEN]);

impl IndexId {
    #[must_use]
    pub const fn from_slice(u: [u8; INDEX_ID_LEN]) -> Self {
        Self(u)
    }

    #[must_use]
    pub const fn as_slice(&self) -> [u8; INDEX_ID_LEN] {
        self.0
    }

    #[must_use]
    pub const fn from_u32(u: u32) -> Self {
        Self(u.to_be_bytes())
    }

    #[must_use]
    pub const fn as_u32(&self) -> u32 {
        u32::from_be_bytes(self.0)
    }

    #[must_use]
    pub const fn from_string_hash(s: &str, version: u32) -> Self {
        let h = const_fnv1a_hash::fnv1a_hash_str_32(s);
        Self::from_u32(h ^ version.wrapping_mul(0x9E37_79B9))
    }
}

/// A tenant's identity: the [`TenantId`] every key carries, and the name every
/// metric is labelled with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tenant {
    id: TenantId,
    name: &'static str,
}

impl Tenant {
    #[must_use]
    pub const fn new(name: &'static str, version: u64) -> Self {
        Self {
            id: TenantId::from_string_hash(name, version),
            name,
        }
    }

    #[must_use]
    pub const fn id(&self) -> TenantId {
        self.id
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

pub(crate) const APP_ID_LEN: usize = 16;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Debug)]
pub struct TenantId([u8; APP_ID_LEN]);

impl TenantId {
    #[must_use]
    pub const fn from_slice(u: [u8; APP_ID_LEN]) -> Self {
        Self(u)
    }

    #[must_use]
    pub const fn as_slice(&self) -> [u8; APP_ID_LEN] {
        self.0
    }

    #[must_use]
    pub const fn from_u128(u: u128) -> Self {
        Self(u.to_be_bytes())
    }

    #[must_use]
    pub const fn as_u128(&self) -> u128 {
        u128::from_be_bytes(self.0)
    }

    #[expect(
        clippy::as_conversions,
        reason = "widening u8 -> u128 is lossless; From is not const"
    )]
    #[must_use]
    pub const fn from_string_hash(s: &str, version: u64) -> Self {
        let h = const_fnv1a_hash::fnv1a_hash_str_128(s);
        Self::from_u128(
            h ^ (version as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835),
        )
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn projection_id_u128_roundtrip(raw in any::<u128>()) {
            prop_assert_eq!(ProjectionId::from_u128(raw).as_u128(), raw);
        }

        #[test]
        fn projection_id_uuid_roundtrip(raw in any::<u128>()) {
            let uuid = uuid::Uuid::from_u128(raw);
            prop_assert_eq!(ProjectionId::from_uuid(uuid).as_uuid(), uuid);
        }

        #[test]
        fn projection_id_order_matches_u128(a in any::<u128>(), b in any::<u128>()) {
            prop_assert_eq!(
                a.cmp(&b),
                ProjectionId::from_u128(a).cmp(&ProjectionId::from_u128(b))
            );
        }

        #[test]
        fn index_id_u32_roundtrip(raw in any::<u32>()) {
            prop_assert_eq!(IndexId::from_u32(raw).as_u32(), raw);
        }

        #[test]
        fn index_id_order_matches_u32(a in any::<u32>(), b in any::<u32>()) {
            prop_assert_eq!(a.cmp(&b), IndexId::from_u32(a).cmp(&IndexId::from_u32(b)));
        }

        #[test]
        fn projection_type_id_roundtrip(raw in any::<u32>()) {
            prop_assert_eq!(ProjectionTypeId::from_u32(raw).as_u32(), raw);
        }

        #[test]
        fn tenant_id_u128_roundtrip(raw in any::<u128>()) {
            prop_assert_eq!(TenantId::from_u128(raw).as_u128(), raw);
        }
    }

    #[test]
    fn tenant_id_hash_is_deterministic() {
        assert_eq!(
            TenantId::from_string_hash("todo", 0),
            TenantId::from_string_hash("todo", 0)
        );
        assert_ne!(
            TenantId::from_string_hash("todo", 0),
            TenantId::from_string_hash("todo", 1)
        );
        assert_ne!(
            TenantId::from_string_hash("todo", 0),
            TenantId::from_string_hash("user", 0)
        );
    }

    #[test]
    fn projection_id_min_max_ordering() {
        let mid = ProjectionId::from_u128(1);
        assert_eq!(ProjectionId::MIN, ProjectionId::from_slice([u8::MIN; 16]));
        assert_eq!(ProjectionId::MAX, ProjectionId::from_slice([u8::MAX; 16]));
        assert_eq!(mid.cmp(&ProjectionId::MIN), std::cmp::Ordering::Greater);
        assert_eq!(mid.cmp(&ProjectionId::MAX), std::cmp::Ordering::Less);
    }
}
