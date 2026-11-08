//! Pulling the replica logs into the local store: one long-lived subscription
//! per replica, supervised.
//!
//! [`manager`] owns the set — it follows the effective replica set (current ∪
//! next, so a joining node is subscribed before it is needed) and runs one task
//! per member. [`peer`] is that task's body: subscribe from the durable cursor,
//! feed a blocking integrate loop, and report back what the attempt actually
//! accomplished.
//!
//! Resuming is always safe and never exact. The cursor is durable but lags, so
//! a reconnect re-delivers a suffix (deduped by `data_id` downstream); a replica
//! that purged past the cursor delivers a forward jump instead, which is benign
//! — the skipped entries come back via snapshot load — and is only metered.
//!
//! A subscription is infinite by contract (CLAUDE.md §20), so *any* end is a
//! failure, including a declared close. The manager's backoff ladder is keyed on
//! durable progress rather than connection lifetime, because the failure that
//! matters here — a shed loop reconnecting every few milliseconds — looks
//! healthy by every duration-based measure.

mod manager;
pub mod metrics;
mod peer;

pub use manager::ReplicationManager;

error_union::error_union! {
    /// What this crate can fail with.
    #[derive(Debug, Clone, PartialEq)]
    pub Error {
        Lifecycle(zslg_lifecycle_types::LifecycleError),
        Config(zslg_lifecycle_types::ConfigError),
        Clock(zslg_time::ClockError),
        Log(zslg_log::LogError),
        Snapshot(zslg_snapshot::SnapshotError),
        Net(zslg_net::NetError),
        Consensus(zslg_consensus::ConsensusError),
    }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_quorum_read::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    into zslg_log::Error
    into zslg_raft::Error
    into zslg_quorum_read::Error
}
