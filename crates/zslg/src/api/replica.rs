//! What a replica offers the rest of the cluster: take writes, serve its log to
//! subscribers, report its state.
//!
//! Everything here is shaped by one fact — a writer holds *one* connection per
//! replica for its whole life and pushes every write down it. So the trait is
//! connection-oriented rather than call-oriented: `write` is opened once, and
//! the per-write cost is a channel pull rather than a TCP connect, a
//! `spawn_blocking` handoff and a fresh future.
//!
//! ```text
//!   write(input) ─> outcomes          unordered, correlated by data_id
//!        │
//!        ├─ Ok(Accepted|Rejected|Failed)   per-write; stream continues
//!        └─ Err                            terminal; stream ends, caller reopens
//! ```
//!
//! Out-of-order outcomes are inherent, not incidental: writes commit
//! concurrently and a replica answers each as it lands. `WriteOutcome` therefore
//! carries `data_id` in *every* variant — there is no positional correlation to
//! fall back on.
//!
//! Rejected and Failed are separate because they differ in what the writer
//! learns: a rejection (not-a-replica, HLC cutoff) is a decision made against a
//! known `RaftState` and updates the writer's generation tracking, while a
//! storage failure has no state to report (CLAUDE.md §9).

use crate::Error;
use crate::channel::Channels;
use crate::ids::{DataId, LogIndex};
use crate::meta::{Data, RaftState, ReplicaStatus};

/// A write with the wall-clock time it entered a queue, for *direct*
/// queue-residency measurement (the write→replica triangulation). Threaded
/// through the write path so a hop's residency is measured from a timestamp on
/// the queued object, never inferred from other metrics.
#[derive(Debug, Clone)]
pub(crate) struct StampedWrite {
    pub data: Data,
    pub enqueued: std::time::Instant,
}

impl StampedWrite {
    /// Stamp `data` as enqueued now. The entry point for a caller that is
    /// handing a write straight over rather than relaying one it received —
    /// the writer's own submissions, and every test double.
    #[must_use]
    pub(crate) fn now(data: Data) -> Self {
        Self {
            data,
            enqueued: std::time::Instant::now(),
        }
    }
}

/// Outcome of one write. Mirrors the wire `WriteStreamResponse` 1:1, so the
/// server maps between them without a decision of its own.
#[derive(Debug)]
pub(crate) enum WriteOutcome {
    /// Durably committed. Carries the post-commit `RaftState`.
    Accepted {
        data_id: DataId,
        raft_state: RaftState,
    },
    /// Rejected before append (not-a-replica, HLC cutoff). Carries the current
    /// `RaftState` for the writer's generation tracking.
    Rejected {
        data_id: DataId,
        error: Error,
        raft_state: RaftState,
    },
    /// Failed at storage (append/fsync) — no usable `RaftState`.
    Failed { data_id: DataId, error: Error },
}

impl WriteOutcome {
    /// The write this outcome answers.
    pub(crate) fn data_id(&self) -> DataId {
        match self {
            WriteOutcome::Accepted { data_id, .. }
            | WriteOutcome::Rejected { data_id, .. }
            | WriteOutcome::Failed { data_id, .. } => *data_id,
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait Replica: Send + Sync + 'static {
    /// Open a long-lived write connection: `input` yields writes, the returned
    /// channel their outcomes. Returns immediately; both halves are driven off
    /// the reactor, so the hot path never touches it.
    ///
    /// - Every accepted write is answered exactly once, and the output does not
    ///   disconnect while one it accepted is still in flight.
    /// - The outer `Err` is terminal (connection lost) and is the last item;
    ///   reopening is the caller's decision.
    /// - Both ends terminate by dropping — there is no cancel signal. Draining
    ///   `input` finishes the in-flight writes and then disconnects the output;
    ///   dropping the output fails the producer's sends and tears the
    ///   connection down.
    ///
    /// Items carry a [`StampedWrite`] enqueue timestamp so each hop's queue
    /// residency can be measured on the queued object itself — the peer
    /// transport reads it for write-stream input residency, the replica service
    /// for the server→raft dispatch wait. A caller with nothing to measure
    /// stamps at the point it hands the write over; the stamp costs an
    /// `Instant` and is never optional, because a second unstamped entry point
    /// is how the measurement silently stops covering a path.
    fn write(&self, input: Channels<StampedWrite>) -> Channels<Result<WriteOutcome, Error>>;

    /// Resumable subscription to the replica's append stream.
    fn subscribe_from(&self, from: Option<LogIndex>) -> Channels<Result<(LogIndex, Data), Error>>;

    /// Log frontier, Raft state, and the fully-drained effective HLC cutoff, in
    /// one call. All cheap in-memory reads, fetched together to keep the trait
    /// and the RPC surface narrow.
    async fn fetch_status(&self) -> Result<ReplicaStatus, Error>;

    /// Push-based status: the current [`ReplicaStatus`] first, then one item per
    /// `RaftState` change. Carries no log data, so a writer or learner can keep
    /// its `RaftTracker` fresh (CLAUDE.md §9) without polling and without
    /// subscribing to the log.
    async fn subscribe_status(
        &self,
    ) -> Result<futures::stream::BoxStream<'static, Result<ReplicaStatus, Error>>, Error>;
}
