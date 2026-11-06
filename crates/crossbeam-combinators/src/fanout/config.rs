//! Sizing and overflow policy of a fan-out.

use std::num::NonZeroUsize;

/// What the distribution thread does when a subscriber's queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Wait for the slowest subscriber. The data queue backpressures the
    /// producer while the element is delivered.
    Block,
    /// Drop a subscriber whose queue is full or whose consumer is gone.
    Shed,
}

/// The three queue bounds of a fan-out. The overflow policy is not here: it is
/// a semantic choice, not sizing, and every constructor takes it explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanoutConfig {
    /// Elements queued between the producer and the distribution thread.
    pub data_cap: NonZeroUsize,
    /// Registrations queued before `subscribe` reports `Full`.
    pub register_cap: NonZeroUsize,
    /// Per-subscriber queue depth: the lag a `Shed` subscriber may build up.
    pub sub_cap: NonZeroUsize,
}

impl FanoutConfig {
    const DEFAULT_CAP: NonZeroUsize = NonZeroUsize::new(1024).expect("nonzero");

    /// The default data and registration bounds with the given subscriber depth.
    #[must_use]
    pub const fn with_sub_cap(sub_cap: NonZeroUsize) -> Self {
        Self {
            data_cap: Self::DEFAULT_CAP,
            register_cap: Self::DEFAULT_CAP,
            sub_cap,
        }
    }
}

impl Default for FanoutConfig {
    fn default() -> Self {
        Self::with_sub_cap(Self::DEFAULT_CAP)
    }
}
