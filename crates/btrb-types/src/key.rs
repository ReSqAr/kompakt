//! The process-wide signing secret.

/// A 32-byte secret shared by everything this process signs with.
///
/// Not `Debug`-derived, not comparable, not `Copy`: the derives are how a
/// secret ends up in a log line or in a timing-variable comparison. Reading the
/// bytes is deliberately a named call.
#[derive(Clone)]
pub struct SigningKey([u8; SigningKey::BYTES]);

impl SigningKey {
    pub const BYTES: usize = 32;

    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::BYTES]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn expose(&self) -> &[u8; Self::BYTES] {
        &self.0
    }
}

impl core::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("SigningKey(<redacted>)")
    }
}
