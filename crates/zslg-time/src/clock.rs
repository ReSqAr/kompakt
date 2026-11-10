//! The node's hybrid logical clock, as seen by everything that stamps or
//! validates an HLC.
//!
//! Both methods advance one strictly monotonic counter, so a peer's clock can
//! only ever pull this node's forward — never back. `integrate` is therefore
//! the seam through which a wrong clock would spread, and it refuses a peer
//! further than `max_clock_skew` away (`ClockError::HlcDriftTooLarge`) rather than
//! believing it. Rules: `HlcClock` (in zslg).
//!
//! Substitutable so tests can drive time; the production impl is `HlcClock`.

use zslg_time_types::Hlc;

pub trait Clock: Send + Sync + 'static {
    #[must_use]
    fn now(&self) -> Hlc;

    fn integrate(&self, peer: Hlc) -> Result<Hlc, crate::Error>;

    /// Absolute physical-ms difference between this clock and a peer's HLC,
    /// without advancing or mutating anything. Lets a caller watch a peer's
    /// drift toward the rejection threshold before `integrate` refuses it.
    #[must_use]
    fn skew_against(&self, peer: Hlc) -> std::time::Duration;
}
