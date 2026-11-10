//! The gate that admits writes — a generation barrier over the HLC cutoff.
//!
//! Every cutoff is a *generation*. `check_hlc` hands out an [`HlcGuard`] belonging
//! to the generation current at that moment; installing a new cutoff waits for the
//! previous generation to empty.
//!
//! ```text
//!   check_hlc(h)    h > cutoff ─> HlcGuard(gen N)      else Err
//!
//!   set_cutoff(c)   1. register waiter on gen N   ─┐  this order is the
//!                   2. swap in gen N+1, cutoff c   │  whole trick
//!                   3. await every gen-N guard    ─┘
//! ```
//!
//! Steps 1 and 2 cannot be swapped: a guard dropping in between would fire its
//! wakeup before anyone was listening, and the drain would wait forever.
//!
//! That drain is the drain-before-ACK invariant (CLAUDE.md §8) — when
//! `set_cutoff` returns, nothing with `hlc <= c` can still be in flight, so a
//! snapshot covering the window cannot miss a write that later commits. It is
//! deliberately conservative: rather than track each open write's HLC, it waits
//! out *all* of the previous generation.
//!
//! The cutoff never regresses, and this type is the sole arbiter of what counts
//! as an advance — callers act on `set_cutoff`'s answer instead of comparing for
//! themselves. `set_lock` serialises concurrent callers so that answer is stable.

use parking_lot::Mutex as SyncMutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::metrics;
use zslg_time::{CutoffError, Hlc};

/// Cadence for warning that a `set_cutoff` drain is still waiting on in-flight
/// writes. A single stuck `HlcGuard` blocks the drain — and with it the leader's
/// snapshot progress — so surface it rather than hang silently.
const SET_CUTOFF_DRAIN_WARN_INTERVAL: Duration = Duration::from_secs(5);

struct TrackedHlcCutoff {
    cutoff: Option<Hlc>,
    drained: Arc<Notify>,
}

impl Drop for TrackedHlcCutoff {
    fn drop(&mut self) {
        // Fires before drained is released; set_cutoff holds its own
        // clone of the Arc so Notify outlives this drop.
        self.drained.notify_waiters();
    }
}

pub struct HlcGate {
    current: SyncMutex<Arc<TrackedHlcCutoff>>,
    set_lock: AsyncMutex<()>,
}

impl std::fmt::Debug for HlcGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HlcGate")
            .field("current", &self.current.lock().cutoff)
            .finish()
    }
}

impl Default for HlcGate {
    fn default() -> Self {
        Self::new()
    }
}

pub struct HlcGuard {
    _tracked: Arc<TrackedHlcCutoff>,
}

impl Drop for HlcGuard {
    fn drop(&mut self) {
        // Event-driven `zslg_hlc_guards_in_flight` gauge; a no-op without a
        // recorder installed, so safe in unit/loom tests.
        metrics::hlc_guards_dec();
    }
}

impl std::fmt::Debug for HlcGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HlcGuard")
            .field("cutoff", &self._tracked.cutoff)
            .finish()
    }
}

impl HlcGate {
    #[must_use]
    pub fn new() -> Self {
        Self {
            current: SyncMutex::new(Arc::new(TrackedHlcCutoff {
                cutoff: None,
                drained: Arc::new(Notify::new()),
            })),
            set_lock: AsyncMutex::new(()),
        }
    }

    #[must_use]
    #[cfg(test)]
    fn current(&self) -> Option<Hlc> {
        self.current.lock().cutoff
    }

    /// Strictly above the cutoff, so `==` is rejected: the snapshot covering
    /// `(wm, cutoff]` is built from a catch-up that has already run, and every
    /// later snapshot starts strictly above `cutoff`. A write admitted at
    /// exactly the cutoff would be ACKed into no snapshot at all.
    pub fn check_hlc(&self, hlc: Hlc) -> Result<HlcGuard, CutoffError> {
        let current = self.current.lock();
        if let Some(cutoff) = current.cutoff
            && hlc <= cutoff
        {
            return Err(CutoffError::AtOrBeforeCutoff { hlc, cutoff });
        }
        metrics::hlc_guards_inc();
        Ok(HlcGuard {
            _tracked: current.clone(),
        })
    }

    /// `true` once `new` is installed *and* drained; `false` if it was at or
    /// below the current cutoff and so ignored.
    ///
    /// Ignoring skips the drain safely, because guards under the current cutoff
    /// are for HLCs strictly above it and cannot be what `new` needs to wait
    /// for. It is still not a fast path: the check sits behind `set_lock`, so a
    /// concurrent drain of the same value is awaited before answering `false`.
    #[must_use]
    pub async fn set_cutoff(&self, new: Hlc) -> bool {
        let _lock = self.set_lock.lock().await;

        let mut notified = {
            let mut current = self.current.lock();
            if current.cutoff.is_some_and(|c| new <= c) {
                return false;
            }

            // Step 1 before step 2 — see the header.
            let notified = current.drained.clone().notified_owned();
            let mut notified = Box::pin(notified);
            notified.as_mut().enable();

            *current = Arc::new(TrackedHlcCutoff {
                cutoff: Some(new),
                drained: Arc::new(Notify::new()),
            });

            notified
        };

        let _probe = metrics::hlc_cutoff_drain_probe();
        // Race the drain against a warn-ticker so a stuck in-flight write (an
        // `HlcGuard` that never drops) is visible instead of hanging silently.
        let started = tokio::time::Instant::now();
        let mut warn_tick = tokio::time::interval(SET_CUTOFF_DRAIN_WARN_INTERVAL);
        warn_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        warn_tick.tick().await; // consume the immediate first tick
        loop {
            tokio::select! {
                () = &mut notified => break,
                _ = warn_tick.tick() => tracing::warn!(
                    cutoff = ?new,
                    elapsed_s = started.elapsed().as_secs(),
                    "hlc cutoff: still draining in-flight writes for set_cutoff \
                     (a stuck write blocks the leader's snapshot progress)"
                ),
            }
        }
        true
    }
}

impl zslg_time::WriteGate for HlcGate {
    fn check_hlc(&self, hlc: Hlc) -> Result<zslg_time::Admission, CutoffError> {
        Self::check_hlc(self, hlc).map(|guard| Box::new(guard) as zslg_time::Admission)
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
mod tests;
