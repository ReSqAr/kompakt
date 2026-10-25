//! The slots of an app's stores, changeable while they run.

/// How many requests a store takes at once, shared by every scheduler of a
/// process — runtime settings (`store.slots`, `store.reserve`): they change
/// while it runs.
///
/// Sized for a page behind Cloudflare: over HTTP/2 a browser asks for every
/// visible thumbnail at once (30–100 requests), not 6 at a time, and the
/// tunnel passes the burst on. Predictions leave `reserve` slots free.
#[derive(Debug)]
pub(crate) struct Tuning {
    slots: std::sync::atomic::AtomicUsize,
    reserve: std::sync::atomic::AtomicUsize,
}

impl Tuning {
    pub(crate) const SLOTS: usize = 64;

    pub(crate) fn new(slots: usize, reserve: usize) -> Self {
        Self {
            slots: slots.max(1).into(),
            reserve: reserve.into(),
        }
    }

    /// New settings: schedulers follow on their next fetch.
    pub(crate) fn set(&self, slots: usize, reserve: usize) {
        use std::sync::atomic::Ordering::SeqCst;
        self.slots.store(slots.max(1), SeqCst);
        self.reserve.store(reserve, SeqCst);
    }

    pub(super) fn slots(&self) -> usize {
        self.slots.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn reserve(&self) -> usize {
        self.reserve.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Default for Tuning {
    /// 64 slots, a quarter of them kept free of predictions.
    fn default() -> Self {
        Self::new(Self::SLOTS, Self::SLOTS / 4)
    }
}
