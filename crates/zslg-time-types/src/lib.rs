//! Hybrid logical clocks: the value type, its order, and the errors of crossing
//! a bound. The clock that mints them and the gate that admits writes by them
//! live in `zslg`; this crate is only what every other crate may name.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hlc(#[doc(hidden)] pub u64);

const HLC_LOGICAL_BITS: u32 = 16;
const HLC_LOGICAL_MASK: u64 = 0xFFFF;
const HLC_PHYSICAL_MASK: u64 = !HLC_LOGICAL_MASK;
#[doc(hidden)]
pub const HLC_MAX_PHYSICAL_MS: u64 = 0x0000_FFFF_FFFF_FFFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[doc(hidden)]
pub struct HlcParts {
    pub physical_ms: u64,
    pub logical: u16,
}

impl Hlc {
    #[doc(hidden)]
    pub const ZERO: Self = Self(0);

    #[must_use]
    #[doc(hidden)]
    pub fn raw(self) -> u64 {
        self.0
    }

    /// The physical-millisecond part (the top 48 bits).
    #[must_use]
    pub fn physical_ms(self) -> u64 {
        self.parts().physical_ms
    }

    #[must_use]
    #[allow(
        clippy::expect_used,
        reason = "logical mask is constrained to 16 bits by HLC_LOGICAL_MASK"
    )]
    #[doc(hidden)]
    pub fn parts(self) -> HlcParts {
        let physical_ms = (self.0 & HLC_PHYSICAL_MASK) >> HLC_LOGICAL_BITS;
        let logical_u64 = self.0 & HLC_LOGICAL_MASK;
        let logical = u16::try_from(logical_u64).expect("logical mask <= u16::MAX");
        HlcParts {
            physical_ms,
            logical,
        }
    }

    #[must_use]
    #[doc(hidden)]
    pub fn from_parts(parts: HlcParts) -> Self {
        debug_assert!(
            parts.physical_ms <= HLC_MAX_PHYSICAL_MS,
            "physical_ms {} exceeds 48-bit HLC range",
            parts.physical_ms
        );
        let physical_part = (parts.physical_ms & HLC_MAX_PHYSICAL_MS) << HLC_LOGICAL_BITS;
        let logical_part = u64::from(parts.logical);
        Self(physical_part | logical_part)
    }

    #[must_use]
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    #[must_use]
    pub fn into_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

/// `None` if `d` reaches below zero. The logical counter is dropped, not
/// carried: the result is an instant, and comparing it against write HLCs must
/// not depend on some node's event count within a millisecond.
#[doc(hidden)]
#[must_use]
pub fn hlc_subtract(t: Hlc, d: Duration) -> Option<Hlc> {
    let HlcParts { physical_ms, .. } = t.parts();
    let physical_ms = physical_ms.checked_sub(duration_as_ms_clamped(d))?;
    Some(Hlc::from_parts(HlcParts {
        physical_ms,
        logical: 0,
    }))
}

#[doc(hidden)]
#[must_use]
pub fn duration_as_ms_clamped(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
pub enum CutoffError {
    #[error("hlc {hlc:?} is not strictly after cutoff {cutoff:?}")]
    AtOrBeforeCutoff { hlc: Hlc, cutoff: Hlc },
}

/// A hybrid-logical-clock violation: a cutoff, drift or skew bound was crossed.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum ClockError {
    /// HLC below the current cutoff.
    #[error("hlc cutoff violation: {0}")]
    HlcCutoff(#[from] CutoffError),

    /// Peer's HLC drifted beyond the configured skew.
    #[error("hlc drift too large: peer={peer:?} local={local:?}")]
    HlcDriftTooLarge { peer: Hlc, local: Hlc },

    /// Writer-replica wall-clock skew over max_clock_skew.
    #[error("clock skew too large: writer={writer:?} replica={replica:?}")]
    ClockSkewTooLarge { writer: Hlc, replica: Hlc },
}

impl ClockError {
    /// A stable, low-cardinality label naming the variant — for metric labels
    /// and structured logging. Keep these strings stable; they are a metric
    /// label dimension.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::HlcCutoff(_) => "hlc_cutoff",
            Self::HlcDriftTooLarge { .. } => "hlc_drift_too_large",
            Self::ClockSkewTooLarge { .. } => "clock_skew_too_large",
        }
    }
}

/// Anything that may sit at an HLC: items of a stream ordered by HLC. `None`
/// means the item is not positional and passes through without advancing the
/// order (the contract of `Key`).
///
/// One blanket `Key` impl below turns every `HasHlc` item into an
/// `OrderedChannel` position by HLC, over any error type, so the owning
/// crates implement this trait for their item types instead of each writing
/// a `Key` impl (which the orphan rule would not let them place anywhere).
pub trait HasHlc {
    fn hlc(&self) -> Option<Hlc>;
}

impl<A, T: HasHlc> HasHlc for (A, T) {
    fn hlc(&self) -> Option<Hlc> {
        self.1.hlc()
    }
}

impl<T: HasHlc, E> crossbeam_combinators::Key<Result<T, E>> for Hlc {
    fn key(item: &Result<T, E>) -> Option<Self> {
        item.as_ref().ok().and_then(HasHlc::hlc)
    }
}

/// What admits a write: held until the write is durable, then dropped to release
/// the admission. Opaque on purpose — the gate that issues it and the log that
/// holds it need not know each other's types.
pub type Admission = Box<dyn std::any::Any + Send>;
