//! Write admission against the HLC cutoff: which writes may still be accepted.

use zslg_time_types::{Admission, CutoffError, Hlc};

pub trait WriteGate: Send + Sync + 'static {
    /// Admit a write stamped `hlc`, or refuse it if it is at or below the
    /// cutoff. The returned [`Admission`] must be held until the write is
    /// durable: while any is outstanding the cutoff cannot advance past it.
    ///
    /// # Errors
    /// [`CutoffError`] if `hlc` is at or below the current cutoff.
    fn check_hlc(&self, hlc: Hlc) -> Result<Admission, CutoffError>;
}
