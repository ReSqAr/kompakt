//! A controllable wall-clock source: a constant offset (fast or slow) and a
//! freeze, adjustable while a node runs. Wire `wall_now_ms` into a clock's time
//! source to inject skew without touching the clock itself.

use std::sync::Arc;

use parking_lot::Mutex as SyncMutex;

struct SkewState {
    /// Wall-clock offset in milliseconds. Positive = clock runs fast.
    offset_ms: i64,
    /// When `Some(ms)`, the clock is frozen at this physical-ms value.
    frozen: Option<u64>,
}

/// Cloneable handle for manipulating a `SkewedClock` at runtime.
#[derive(Clone)]
pub struct SharedClockController {
    state: Arc<SyncMutex<SkewState>>,
}

impl SharedClockController {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(SyncMutex::new(SkewState {
                offset_ms: 0,
                frozen: None,
            })),
        }
    }

    /// Add a constant offset: positive = clock runs fast, negative = slow.
    pub fn set_offset_ms(&self, offset_ms: i64) {
        self.state.lock().offset_ms = offset_ms;
    }

    /// Freeze the clock at its current wall time.
    pub fn freeze(&self) {
        let now = wall_clock_ms_now();
        self.state.lock().frozen = Some(now);
    }

    /// Resume normal time progression.
    pub fn unfreeze(&self) {
        self.state.lock().frozen = None;
    }

    /// Current wall-clock reading from this controller, in milliseconds.
    #[must_use]
    pub fn wall_now_ms(&self) -> u64 {
        let state = self.state.lock();
        let base = state.frozen.unwrap_or_else(wall_clock_ms_now);
        if state.offset_ms >= 0 {
            base.saturating_add(state.offset_ms as u64)
        } else {
            base.saturating_sub((-state.offset_ms) as u64)
        }
    }
}

impl Default for SharedClockController {
    fn default() -> Self {
        Self::new()
    }
}

// ── Helper ─────────────────────────────────────────────────────────────

#[must_use]
fn wall_clock_ms_now() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
