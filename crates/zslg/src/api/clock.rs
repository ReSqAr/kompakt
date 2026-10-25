//! The node's hybrid logical clock, as seen by everything that stamps or
//! validates an HLC.
//!
//! Both methods advance one strictly monotonic counter, so a peer's clock can
//! only ever pull this node's forward — never back. `integrate` is therefore
//! the seam through which a wrong clock would spread, and it refuses a peer
//! further than `max_clock_skew` away (`Error::HlcDriftTooLarge`) rather than
//! believing it. Rules: [`crate::hlc::HlcClock`].
//!
//! Substitutable so tests can drive time; the production impl is `HlcClock`.

use crate::hlc::Hlc;

pub(crate) trait Clock: Send + Sync + 'static {
    #[must_use]
    fn now(&self) -> Hlc;

    fn integrate(&self, peer: Hlc) -> Result<Hlc, crate::Error>;
}
