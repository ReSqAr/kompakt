//! The learner's reader: serves stable reads from the stable log or the archive
//! and the unstable tail from the local store, spliced at the watermark, plus
//! the quorum-read freshness opt-in.
//!
//! `read` holds the splice and the gating (`GatedReader`); `quorum` holds the
//! freshness check and its debouncer.

pub mod metrics;
mod quorum;
mod read;

pub use quorum::{MultiReplicaQuorumCheck, QuorumReadDebouncer, QuorumReadDebouncerConfig};
pub use read::{GatedReader, ReaderImpl, can_serve_clients};

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
    bridge zslg_log::Error
    bridge zslg_consensus::Error
    bridge zslg_raft::Error
    bridge zslg_quorum_read::Error
    bridge zslg_error::Error
}
