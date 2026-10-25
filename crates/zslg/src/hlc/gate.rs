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
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::hlc::Hlc;
use crate::metrics;

/// Cadence for warning that a `set_cutoff` drain is still waiting on in-flight
/// writes. A single stuck `HlcGuard` blocks the drain — and with it the leader's
/// snapshot progress — so surface it rather than hang silently.
const SET_CUTOFF_DRAIN_WARN_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
pub enum CutoffError {
    #[error("hlc {hlc:?} is not strictly after cutoff {cutoff:?}")]
    AtOrBeforeCutoff { hlc: Hlc, cutoff: Hlc },
}

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

pub(crate) struct HlcGate {
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

pub(crate) struct HlcGuard {
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
    pub(crate) fn new() -> Self {
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
    pub(crate) fn check_hlc(&self, hlc: Hlc) -> Result<HlcGuard, CutoffError> {
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
    pub(crate) async fn set_cutoff(&self, new: Hlc) -> bool {
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    reason = "test code"
)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    // ── no cutoff installed ─────────────────────────────────────────────

    #[test]
    fn no_cutoff_admits_any_hlc() {
        let cutoff = HlcGate::new();
        assert!(cutoff.current().is_none());
        let _g = cutoff.check_hlc(Hlc(0)).expect("admits 0");
        let _g = cutoff.check_hlc(Hlc(u64::MAX)).expect("admits max");
    }

    // ── rejection at or below cutoff ────────────────────────────────────

    #[tokio::test]
    async fn check_rejects_at_or_below_cutoff() {
        let cutoff = HlcGate::new();
        assert!(cutoff.set_cutoff(Hlc(100)).await, "advance from None");

        let err = cutoff.check_hlc(Hlc(50)).expect_err("must reject");
        assert!(matches!(
            err,
            CutoffError::AtOrBeforeCutoff {
                hlc: Hlc(50),
                cutoff: Hlc(100)
            }
        ));

        // Only strictly above the cutoff is admitted.
        let _g = cutoff.check_hlc(Hlc(101)).expect("> cutoff is admitted");
    }

    /// F-3 invariant: a write at exactly the cutoff must be rejected — the
    /// covering snapshot's catch-up has already run and every later snapshot
    /// starts strictly above the cutoff, so an admitted boundary write would
    /// never reach any snapshot despite being ACKed.
    #[tokio::test]
    async fn invariant_write_at_cutoff_rejected() {
        let cutoff = HlcGate::new();
        assert!(cutoff.set_cutoff(Hlc(100)).await, "advance from None");
        let err = cutoff.check_hlc(Hlc(100)).expect_err("== cutoff rejected");
        assert!(matches!(
            err,
            CutoffError::AtOrBeforeCutoff {
                hlc: Hlc(100),
                cutoff: Hlc(100)
            }
        ));
    }

    // ── set_cutoff idempotency ──────────────────────────────────────────

    /// Guards issued *under* the current cutoff are for HLCs strictly above
    /// it, so a same-value re-set may skip the drain phase entirely.
    #[tokio::test]
    async fn set_cutoff_same_value_skips_drain_of_current_guards() {
        let cutoff = HlcGate::new();
        assert!(cutoff.set_cutoff(Hlc(42)).await, "advance from None");
        let _guard = cutoff.check_hlc(Hlc(50)).expect("guard above cutoff");
        // The guard is for hlc > 42 — re-declaring 42 needs no drain.
        tokio::time::timeout(Duration::from_millis(100), cutoff.set_cutoff(Hlc(42)))
            .await
            .expect("same-value set_cutoff must not wait for guards above the cutoff");
    }

    /// F-2 invariant: a same-value `set_cutoff` must honour a concurrent,
    /// still-draining `set_cutoff` of that value — it may only fast-return
    /// once that drain has completed (serialised via `set_lock`).
    #[tokio::test(start_paused = true)]
    async fn invariant_same_value_set_cutoff_honours_concurrent_drain() {
        let cutoff = Arc::new(HlcGate::new());
        // Guard issued under the initial (None) generation; hlc <= 100, so
        // installing 100 must drain it.
        let guard = cutoff.check_hlc(Hlc(10)).expect("guard");

        let a = cutoff.clone();
        let mut first = tokio::spawn(async move { a.set_cutoff(Hlc(100)).await });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!first.is_finished(), "first set_cutoff must be draining");

        // Same value, while the first call is still draining: must wait for
        // that drain, not fast-return.
        let b = cutoff.clone();
        let mut second = tokio::spawn(async move { b.set_cutoff(Hlc(100)).await });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !second.is_finished(),
            "same-value set_cutoff must wait for the in-flight drain"
        );

        drop(guard);
        let first_advanced = tokio::time::timeout(Duration::from_secs(1), &mut first)
            .await
            .expect("first timeout")
            .expect("first join");
        assert!(first_advanced, "first call advanced from None");
        let second_advanced = tokio::time::timeout(Duration::from_secs(1), &mut second)
            .await
            .expect("second timeout")
            .expect("second join");
        assert!(
            !second_advanced,
            "same-value second call is a no-op after the concurrent drain"
        );
        assert_eq!(cutoff.current(), Some(Hlc(100)));
    }

    #[tokio::test]
    async fn set_cutoff_same_value_returns_promptly_without_guards() {
        let cutoff = HlcGate::new();
        assert!(cutoff.set_cutoff(Hlc(42)).await, "advance from None");
        assert_eq!(cutoff.current(), Some(Hlc(42)));
        // With no outstanding guards a same-value re-set drains instantly.
        tokio::time::timeout(Duration::from_millis(100), cutoff.set_cutoff(Hlc(42)))
            .await
            .expect("re-setting same cutoff with no guards must return promptly");
        assert_eq!(cutoff.current(), Some(Hlc(42)));
    }

    // ── no-regress ──────────────────────────────────────────────────────

    /// A `set_cutoff` below the current cutoff is a no-op: it returns `false`,
    /// leaves the cutoff untouched, returns promptly, and does NOT drain guards
    /// issued above the (higher) current cutoff.
    #[tokio::test]
    async fn set_cutoff_ignores_regression() {
        let cutoff = HlcGate::new();
        assert!(cutoff.set_cutoff(Hlc(100)).await, "advance from None");
        // A live guard for hlc > 100 — a regressed set must not wait on it.
        let _guard = cutoff.check_hlc(Hlc(150)).expect("guard above cutoff");
        let advanced = tokio::time::timeout(Duration::from_millis(100), cutoff.set_cutoff(Hlc(50)))
            .await
            .expect("regressed set_cutoff must not drain guards above the cutoff");
        assert!(!advanced, "regression is a no-op");
        assert_eq!(cutoff.current(), Some(Hlc(100)), "cutoff must not regress");
    }

    /// `set_cutoff` returns `true` only when the cutoff advances; `false` for a
    /// same-value re-set and for a regression.
    #[tokio::test]
    async fn set_cutoff_reports_advance_vs_noop() {
        let cutoff = HlcGate::new();
        assert!(cutoff.set_cutoff(Hlc(100)).await, "None -> 100 advances");
        assert!(cutoff.set_cutoff(Hlc(200)).await, "100 -> 200 advances");
        assert!(!cutoff.set_cutoff(Hlc(200)).await, "200 -> 200 is a no-op");
        assert!(
            !cutoff.set_cutoff(Hlc(150)).await,
            "200 -> 150 is a regression"
        );
        assert_eq!(cutoff.current(), Some(Hlc(200)));
    }

    // ── set_cutoff does not wait without guards ─────────────────────────

    #[tokio::test]
    async fn set_returns_immediately_with_no_guards() {
        let cutoff = HlcGate::new();
        tokio::time::timeout(Duration::from_secs(1), cutoff.set_cutoff(Hlc(50)))
            .await
            .expect("must not hang");
        assert_eq!(cutoff.current(), Some(Hlc(50)));
    }

    // ── set waits for outstanding guards ────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn set_waits_for_single_guard_to_drop() {
        let cutoff = Arc::new(HlcGate::new());
        let guard = cutoff.check_hlc(Hlc(10)).expect("guard");

        let c = cutoff.clone();
        let mut handle = tokio::spawn(async move { c.set_cutoff(Hlc(50)).await });

        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !handle.is_finished(),
            "set_cutoff must block while guard is alive"
        );

        // While blocked, the new cutoff is already visible to check_hlc.
        assert!(matches!(
            cutoff.check_hlc(Hlc(0)),
            Err(CutoffError::AtOrBeforeCutoff { .. })
        ));

        drop(guard);
        tokio::time::timeout(Duration::from_secs(1), &mut handle)
            .await
            .expect("did not complete after guard drop")
            .expect("join");
    }

    #[tokio::test(start_paused = true)]
    async fn set_waits_for_all_guards_to_drop() {
        let cutoff = Arc::new(HlcGate::new());
        let g1 = cutoff.check_hlc(Hlc(10)).expect("g1");
        let g2 = cutoff.check_hlc(Hlc(20)).expect("g2");
        let g3 = cutoff.check_hlc(Hlc(30)).expect("g3");

        let c = cutoff.clone();
        let mut handle = tokio::spawn(async move { c.set_cutoff(Hlc(100)).await });

        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(!handle.is_finished());

        drop(g1);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(!handle.is_finished(), "still waiting on g2 + g3");

        drop(g2);
        drop(g3);
        tokio::time::timeout(Duration::from_secs(1), &mut handle)
            .await
            .expect("did not complete after all guards dropped")
            .expect("join");
    }

    // ── concurrent set_cutoff ───────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn concurrent_set_cutoff_serialises() {
        let cutoff = Arc::new(HlcGate::new());
        let guard = cutoff.check_hlc(Hlc(10)).expect("guard");

        let c1 = cutoff.clone();
        let mut h1 = tokio::spawn(async move { c1.set_cutoff(Hlc(50)).await });
        let c2 = cutoff.clone();
        let mut h2 = tokio::spawn(async move { c2.set_cutoff(Hlc(100)).await });

        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        // Both must be blocked.
        assert!(!h1.is_finished());
        assert!(!h2.is_finished());

        drop(guard);
        tokio::time::timeout(Duration::from_secs(1), &mut h1)
            .await
            .expect("h1 timeout")
            .expect("h1 join");
        // After h1 completes, h2 must also complete (set_cutoff is serialised).
        tokio::time::timeout(Duration::from_secs(1), &mut h2)
            .await
            .expect("h2 timeout")
            .expect("h2 join");

        // Final cutoff is the max (100), regardless of serialisation order:
        // whichever of {50, 100} runs second, a no-regress set_cutoff never
        // lowers the cutoff below 100.
        assert_eq!(cutoff.current(), Some(Hlc(100)));
    }

    // ── post-set behaviour ──────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn check_after_set_uses_new_cutoff() {
        let cutoff = HlcGate::new();
        assert!(cutoff.set_cutoff(Hlc(200)).await, "advance from None");
        assert_eq!(cutoff.current(), Some(Hlc(200)));
        assert!(matches!(
            cutoff.check_hlc(Hlc(199)),
            Err(CutoffError::AtOrBeforeCutoff { .. })
        ));
        assert!(matches!(
            cutoff.check_hlc(Hlc(200)),
            Err(CutoffError::AtOrBeforeCutoff { .. })
        ));
        let _g = cutoff.check_hlc(Hlc(201)).expect("above-cutoff ok");
    }

    // ── Debug output ────────────────────────────────────────────────────

    #[test]
    fn debug_output_includes_cutoff() {
        let cutoff = HlcGate::new();
        let s = format!("{cutoff:?}");
        assert!(s.contains("HlcGate"), "debug: {s}");
        assert!(s.contains("None"), "debug shows no cutoff: {s}");
    }

    #[test]
    fn guard_debug_output_includes_cutoff() {
        let cutoff = HlcGate::new();
        let guard = cutoff.check_hlc(Hlc(42)).expect("guard");
        let s = format!("{guard:?}");
        assert!(s.contains("HlcGuard"), "debug: {s}");
    }

    // ── CutoffError display ─────────────────────────────────────────────

    #[test]
    fn cutoff_error_display_contains_context() {
        let err = CutoffError::AtOrBeforeCutoff {
            hlc: Hlc(5),
            cutoff: Hlc(10),
        };
        let msg = err.to_string();
        assert!(msg.contains("5"), "message should contain hlc: {msg}");
        assert!(msg.contains("10"), "message should contain cutoff: {msg}");
    }
}
