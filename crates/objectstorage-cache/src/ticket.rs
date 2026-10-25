//! A prediction's handle.
//!
//! Whoever predicted a file withdraws it when the prediction is replaced, the
//! cache promotes it when a request joins the download, and the scheduler
//! watches both while the fetch waits for a slot. Three parties, one small
//! shared value; nothing in it knows about files or stores.

/// Shared by who predicted a file (withdraws it when the prediction is
/// replaced), the cache (promotes it when a request joins the download) and
/// the scheduler (watches both while the fetch waits for a slot).
#[derive(Debug, Default)]
pub struct Ticket {
    withdrawn: std::sync::atomic::AtomicBool,
    promoted: std::sync::atomic::AtomicBool,
    pub(crate) wake: tokio::sync::Notify,
}

impl Ticket {
    /// Nobody expects it any more: drop it if it has not started.
    pub fn withdraw(&self) {
        self.withdrawn
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.wake.notify_one();
    }

    /// A request joined it: it is demand now.
    pub fn promote(&self) {
        self.promoted
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.wake.notify_one();
    }

    pub(crate) fn is_withdrawn(&self) -> bool {
        self.withdrawn.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn is_promoted(&self) -> bool {
        self.promoted.load(std::sync::atomic::Ordering::SeqCst)
    }
}
