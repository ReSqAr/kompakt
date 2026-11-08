//! The replica's own log in RocksDB: one flush loop turns queued writes into
//! batched, fsync-fenced appends, broadcasts them to subscribers, and holds each
//! write's admission until it is durable. Implements `ReplicaLog`.

use std::num::NonZeroUsize;
use std::time::Duration;

use zslg_consensus::NodeId;
use zslg_lifecycle_types::ConfigError;

mod log;
pub mod metrics;
#[cfg(test)]
mod testutil;

pub use log::PendingWrite;
pub use log::{REPLICA_LOG_CF, ReplicaLogStore, flush_loop};

/// Replica log persistence and purge configuration.
///
/// The store itself is a column family of the node's one
/// [`Storage`](rocksdb_kit::Storage), which [`spawn_replica`] is
/// handed; what is left here is the queue sizing and the purge policy.
#[derive(Debug, Clone)]
pub struct ReplicaLogConfig {
    pub purge_grace_period: Duration,
    pub fetch_sweep_interval: Duration,
    /// Live-tail channel depth per subscriber. Must be >= `max_flush_batch`
    /// (enforced in [`Self::validate`]): the flush loop `try_send`s a whole
    /// batch and evicts any subscriber whose channel is full, so a capacity
    /// below the batch cap sheds even subscribers that are keeping up. The
    /// evicted subscriber then reconnects and re-scans from its cursor, which
    /// burns the cores that caused the shed in the first place.
    ///
    /// Entries are `(LogIndex, Data)` where `Data.blob` is a `Bytes` handle,
    /// so this holds references, not payload copies — but the *slots* are
    /// allocated up front whether or not anything is queued, and an idle heap
    /// profile found the subscribe rings to be the single largest consumer in
    /// the process (36% of live heap across 9 rings per replica node). Default
    /// 1024 rather than 8192 because 1024-vs-8192 measured as noise on
    /// throughput (-0.1%, 2 of 4 reps) while the drop is worth ~6 MB/node.
    pub subscribe_channel_capacity: NonZeroUsize,
    /// Depth of the queue between `submit` (any RPC handler thread) and the
    /// single flush loop. Holds `Data` — payload — so it is a memory knob as
    /// well as a backpressure one: a full queue blocks the submitter, which is
    /// the intended behaviour, and the slots are allocated whether or not
    /// anything is queued.
    pub write_channel_capacity: NonZeroUsize,
    /// Depth of the per-connection outcome queue the flush loop `try_send`s
    /// into. Must stay at or above the in-flight window a caller drives
    /// (`quorum::coordinator::MAX_INFLIGHT`) or a healthy connection sheds:
    /// the flush loop counts a drop rather than blocking, so undersizing costs
    /// the writer an idempotent resend on reconnect. Tuning, not safety.
    pub write_out_capacity: NonZeroUsize,
    /// Max records drained into one append + fsync.
    pub max_flush_batch: NonZeroUsize,
    /// The owning node's durable identity — the one `replica_log_config()` was
    /// called with. Identity, not a tunable: filled by the producer.
    pub effective_node_id: NodeId,
}

impl ReplicaLogConfig {
    /// Default sizing/tunables. Test-only — production builds the config from
    /// the owning node config's flat fields. The store's location and
    /// compression are no longer here: it is a column family of the node's one
    /// DB, named in that node type's `COLUMN_FAMILIES`.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn for_test(effective_node_id: NodeId) -> Self {
        Self {
            purge_grace_period: Duration::from_secs(5 * 60),
            fetch_sweep_interval: Duration::from_secs(2),
            subscribe_channel_capacity: bytesize::nonzero(1024),
            write_channel_capacity: bytesize::nonzero(4096),
            write_out_capacity: bytesize::nonzero(2048),
            max_flush_batch: bytesize::nonzero(1024),
            effective_node_id,
        }
    }

    /// Worst-case bytes this node's replica-side pipeline queues can hold, at
    /// full depth and the [`QUEUE_REFERENCE_PAYLOAD_BYTES`] reference record.
    ///
    /// **This is deliberately not charged against `storage_memory`.** That
    /// budget bounds RocksDB and nothing else: the two are different sizing
    /// decisions with different failure modes — the engine stalls on its
    /// `WriteBufferManager`, the pipeline blocks or sheds at submit time — and
    /// folding them into one number makes both harder to set. The queues get
    /// their own reported number, beside it.
    ///
    /// Two payload-carrying queues are counted, and they are distinct
    /// instances rather than two views of one: `write_channel_capacity` holds
    /// submitted-but-not-yet-flushed writes, a subscriber's live-tail ring
    /// holds flushed-but-not-yet-delivered ones. The ring is counted **once**
    /// — the depth is per subscriber, and the subscriber count is dynamic, so
    /// a node serving N learners holds up to N times that term.
    /// `write_out_capacity` is not counted: it carries outcomes, not payloads.
    #[must_use]
    pub fn queue_capacity_bytes(&self) -> u64 {
        // saturating throughout: these are usize config knobs that an operator
        // sets, so a nonsense value must report a huge number rather than wrap
        // to a small one — the gauge exists to make oversizing visible.
        let slots = u64::try_from(self.write_channel_capacity.get())
            .unwrap_or(u64::MAX)
            .saturating_add(
                u64::try_from(self.subscribe_channel_capacity.get()).unwrap_or(u64::MAX),
            );
        slots.saturating_mul(QUEUE_REFERENCE_PAYLOAD_BYTES)
    }

    pub fn validate(&self) -> Result<(), crate::Error> {
        if self.subscribe_channel_capacity < self.max_flush_batch {
            return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                format!(
                    "replica_log_subscribe_channel_capacity ({}) must be >= \
                 replica_log_max_flush_batch ({}): one flush batch would shed \
                 every subscriber on the node",
                    self.subscribe_channel_capacity, self.max_flush_batch,
                ),
            )));
        }
        if self.fetch_sweep_interval.is_zero() {
            return Err(crate::Error::Config(ConfigError::InvalidConfiguration(
                "fetch_sweep_interval must be > 0".to_owned(),
            )));
        }
        Ok(())
    }
}

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
    bridge zslg_raft::Error
    bridge zslg_quorum_read::Error
    into zslg_error::Error
}

/// Reference record size used to turn a queue's *depth* into bytes.
///
/// The pipeline queues are sized in slots, never in bytes, so any byte figure
/// for them is depth times an assumed record size — there is no measurement to
/// read instead, because a slot's cost is whatever payload happens to sit in
/// it. 2 KiB is the midpoint of the harness workload (`examples/loadgen.rs`
/// defaults `--payload-min 64 --payload-max 4096`), which is what the
/// 2026-08-17 memory sweep ran, so the gauge is directly comparable with that
/// run's RSS. It is a planning figure, not a measurement, and the metric's help
/// string says so.
pub const QUEUE_REFERENCE_PAYLOAD_BYTES: u64 = 2 * 1024;

impl From<rocksdb_kit::StorageError> for Error {
    fn from(e: rocksdb_kit::StorageError) -> Self {
        match e {
            rocksdb_kit::StorageError::InvalidConfiguration(msg) => {
                Self::Config(ConfigError::InvalidConfiguration(msg))
            }
            rocksdb_kit::StorageError::Storage(msg) => Self::Log(zslg_log::LogError::Storage(msg)),
        }
    }
}
