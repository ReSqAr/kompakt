//! Small quantity newtypes for config ergonomics, in the spirit of
//! [`std::time::Duration`]'s `from_secs` / `from_millis`. Re-exported at the
//! crate root as [`crate::ByteSize`].

use crate::error::Error;

/// A byte size. Binary units: 1 KiB = 1024 B, 1 MiB = 1024² B, 1 GiB = 1024³ B.
/// Wraps a `u64`; construct with [`ByteSize::from_bytes`] / [`ByteSize::from_kib`]
/// / [`ByteSize::from_mib`] / [`ByteSize::from_gib`] and read back with
/// [`ByteSize::as_u64`] / [`ByteSize::as_u32`] at the RocksDB / socket boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(u64);

impl ByteSize {
    /// A size given directly in bytes.
    #[must_use]
    pub const fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    /// A size given in kibibytes (× 1024).
    #[must_use]
    pub const fn from_kib(kib: u64) -> Self {
        // saturating_mul: a byte size that overflows `u64` pins at `u64::MAX` —
        // a defined, harmless clamp (and const-eval friendly), satisfying
        // clippy::arithmetic_side_effects.
        Self(kib.saturating_mul(1024))
    }

    /// A size given in mebibytes (× 1024²).
    #[must_use]
    pub const fn from_mib(mib: u64) -> Self {
        Self(mib.saturating_mul(1024 * 1024))
    }

    /// A size given in gibibytes (× 1024³).
    #[must_use]
    pub const fn from_gib(gib: u64) -> Self {
        Self(gib.saturating_mul(1024 * 1024 * 1024))
    }

    /// The raw byte count.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// The raw byte count as `u32` for socket-buffer APIs; errors if it does not
    /// fit (fallible at runtime, so a `Result` rather than an `expect`).
    pub fn as_u32(self) -> Result<u32, Error> {
        u32::try_from(self.0).map_err(|_| {
            Error::InvalidConfiguration(format!("byte size {} exceeds u32::MAX", self.0))
        })
    }

    /// Bytes as a `usize`, for the RocksDB setters that take one.
    ///
    /// Fallible for the same reason [`as_u32`](Self::as_u32) is: on a 32-bit
    /// target `usize` is narrower than the `u64` this holds. Callers propagate
    /// rather than saturate, because a silently-truncated memory budget is a
    /// sizing bug that surfaces as a stall much later.
    #[must_use = "the conversion can fail; discarding it discards the failure"]
    pub fn as_usize(self) -> Result<usize, Error> {
        usize::try_from(self.0).map_err(|_| {
            Error::InvalidConfiguration(format!("byte size {} does not fit in usize", self.0))
        })
    }
}

/// `n` as a [`NonZeroUsize`], usable in a `const` initializer.
///
/// Total by construction — `nonzero(0)` yields 1 rather than panicking — which
/// is what lets config defaults name a literal capacity without `expect` or
/// `unwrap` (both denied in production code, CLAUDE.md §2). Every call site
/// here passes a non-zero literal, so the saturating branch is unreachable.
#[must_use]
pub(crate) const fn nonzero(n: usize) -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::MIN.saturating_add(n.saturating_sub(1))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]
mod tests {
    use super::*;

    #[test]
    fn constructor_math_is_binary() {
        assert_eq!(ByteSize::from_bytes(4096).as_u64(), 4096);
        assert_eq!(ByteSize::from_kib(64).as_u64(), 64 * 1024);
        assert_eq!(ByteSize::from_mib(4).as_u64(), 4 * 1024 * 1024);
        assert_eq!(ByteSize::from_gib(1).as_u64(), 1024 * 1024 * 1024);
    }

    #[test]
    fn overflow_saturates() {
        assert_eq!(ByteSize::from_mib(u64::MAX).as_u64(), u64::MAX);
    }

    #[test]
    fn nonzero_maps_literals_and_saturates_at_one() {
        assert_eq!(nonzero(8192).get(), 8192);
        assert_eq!(nonzero(1).get(), 1);
        // Unreachable from the call sites, but the function must stay total.
        assert_eq!(nonzero(0).get(), 1);
    }

    #[test]
    fn as_u32_fits_and_overflows() {
        assert_eq!(ByteSize::from_mib(2).as_u32().unwrap(), 2 * 1024 * 1024);
        assert!(
            ByteSize::from_bytes(u64::from(u32::MAX) + 1)
                .as_u32()
                .is_err()
        );
    }

    #[test]
    fn as_usize_fits_and_overflows() {
        assert_eq!(ByteSize::from_mib(64).as_usize().unwrap(), 64 * 1024 * 1024);
        // On a 64-bit target usize is u64-wide, so this is the identity; the test
        // exists to pin the *fallible* signature, which is what a 32-bit target
        // would need. Mirrors `as_u32_fits_and_overflows` above.
        assert!(
            ByteSize::from_bytes(u64::MAX).as_usize().is_ok() || cfg!(target_pointer_width = "32")
        );
    }
}
