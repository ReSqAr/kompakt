//! The crate-wide error type.
//!
//! Three derives carry requirements that are easy to break by accident:
//!
//! | `Clone` | one error fans out to several waiters (the quorum-read debouncer) |
//! | `Serialize` | an error crosses the RPC boundary and is rebuilt *exactly*, not flattened to a string |
//! | `PartialEq` | call sites match on variants, so payloads must stay comparable |
//!
//! Hence every variant carries enough context to diagnose without re-running
//! (CLAUDE.md §5), and module-local error enums convert in via `From` rather
//! than being re-described here.

use error_union::error_union;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zslg_lifecycle_types::{ConfigError, LifecycleError};

use crate::hlc::Hlc;
use crate::ids::NodeId;

/// A hybrid-logical-clock violation: a cutoff, drift or skew bound was crossed.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum ClockError {
    /// HLC below the current cutoff.
    #[error("hlc cutoff violation: {0}")]
    HlcCutoff(#[from] crate::hlc::CutoffError),

    /// Peer's HLC drifted beyond the configured skew.
    #[error("hlc drift too large: peer={peer:?} local={local:?}")]
    HlcDriftTooLarge { peer: Hlc, local: Hlc },

    /// Writer-replica wall-clock skew over max_clock_skew.
    #[error("clock skew too large: writer={writer:?} replica={replica:?}")]
    ClockSkewTooLarge { writer: Hlc, replica: Hlc },
}

/// The replicated and local logs and the storage under them: storage, codec, gaps, truncation, subscriptions.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum LogError {
    /// Storage I/O failure.
    #[error("storage error: {0}")]
    Storage(String),

    /// The leader's local stable log has not yet caught up with the committed
    /// manifest, so a snapshot's global index assignment cannot continue from
    /// it. Backpressure, not a fault: the leader re-ingests its own snapshots
    /// through the object store, so this clears on its own. Distinct from
    /// `Storage` precisely so it does not report as a storage failure
    /// (`SnapshotState::WaitingForStableLog`).
    #[error(
        "stable log behind manifest: end_index={end_index:?}, manifest_end_index={manifest_end_index:?}"
    )]
    StableLogBehindManifest {
        end_index: Option<crate::ids::LogIndex>,
        manifest_end_index: crate::ids::LogIndex,
    },

    /// A gap or ordering violation in the stable log's recoverable history:
    /// the never-GC'd snapshots that must tile an index window skipped an
    /// index, delivered one out of HLC order, or stopped short of the window;
    /// or a served stable read would emit a non-contiguous / HLC-regressing
    /// entry. The stable history is meant to be a gapless, HLC-monotonic,
    /// index-ordered stream fully tiled by object-store snapshots (design doc
    /// §794; CLAUDE.md §8) — a violation is a serious invariant breach,
    /// surfaced loudly instead of forwarding a gap to a reader. Distinct from a
    /// transient `Storage`/`ObjectStore` fault so callers and metrics can page
    /// on it specifically.
    #[error("stable log gap: {0}")]
    StableLogGap(String),

    /// postcard serialisation/deserialisation failure.
    #[error("codec error: {0}")]
    Codec(String),

    /// A finite stream ended without its producer declaring completion (see
    /// `channel::End::Aborted`): the producer panicked, or was discarded before
    /// it ran. Items may be missing, so a consumer that needs the whole stream
    /// must fail rather than treat it as a clean end.
    #[error("stream {what} ended without closing; it may be truncated")]
    StreamTruncated { what: String },

    /// A subscription stream ended. Subscriptions are infinite by contract — a
    /// replica's append stream never completes — so any end means the upstream
    /// went away: a dropped connection, or a peer that is shutting down or not
    /// routable. A *declared* close is no different: it only ever comes from a
    /// stub with nothing to serve. The subscriber backs off and resumes from its
    /// durable cursor.

    /// A subscription stream ended without naming its peer — produced inside
    /// `integrate_loop`, which serves any upstream and so cannot attribute it.
    /// The per-peer caller re-wraps it as [`Error::SubscriptionEnded`].
    #[error("subscription stream ended: {reason}")]
    SubscriptionStreamEnded { reason: String },

    /// A client write exceeded `WriterConfig::write_timeout` (default 10s),
    /// covering the admission-acquire + fan-out phases, so a leaked permit or a
    /// stalled self/loopback target surfaces as a bounded error instead of a
    /// silent hang.
    #[error("write timed out after {elapsed:?}")]
    WriteTimeout { elapsed: std::time::Duration },

    /// A quorum read could not confirm replica catch-up within the deadline.
    /// The system may be partitioned or an upstream may be unrecoverable;
    /// callers can retry or fall back to a weaker read level.
    #[error("quorum read timed out after {timeout:?}")]
    QuorumReadTimeout { timeout: std::time::Duration },
}

/// Snapshots in object storage: the backend and the compression.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum SnapshotError {
    /// Object-store I/O failure.
    #[error("object store error: {0}")]
    ObjectStore(String),

    /// zstd compression/decompression failure.
    #[error("compression error: {0}")]
    Compression(String),
}

/// The wire between nodes.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum NetError {
    /// RPC transport failure.
    #[error("rpc error: {0}")]
    Rpc(String),
}

/// Raft and the cluster around it: leadership, membership, quorum, replicated state.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum ConsensusError {
    /// RPC received by a node that does not match the caller's expected target.
    #[error(transparent)]
    UnexpectedNodeId(Box<UnexpectedNodeId>),

    /// No quorum could be reached against the current Raft generation.
    #[error("quorum unreachable")]
    QuorumUnreachable,

    #[error(transparent)]
    StaleStateWrite(Box<StaleStateWrite>),

    /// A fenced state write carried a manifest that failed the apply-fence shape
    /// check. Rejected as a *value* at the fence (never a fatal storage fault);
    /// the proposing leader maps that rejection to this error. `source` carries
    /// the structured reason so it reconstructs exactly across the RPC boundary.
    /// A fenced state write was structurally invalid — a malformed manifest, or
    /// a `manifest_watermark > hlc_cutoff` regression (a cutoff at or below
    /// committed snapshot coverage, which no future snapshot window could ever
    /// cover). Caught pre-flight at the propose site and rejected as a *value* at
    /// the apply fence, so it never rides the fatal apply path. `source` carries
    /// the structured reason so it reconstructs exactly across the RPC boundary.
    #[error(transparent)]
    InvalidRaftState(Box<InvalidRaftState>),

    /// Invariant violation: an applied state's generation would move backwards.
    /// Unreachable through the fenced Raft apply path; surfaced as an error
    /// (instead of a process-killing assert) so the host stays in control.
    #[error("raft state generation went backwards: current {current:?}, attempted {attempted:?}")]
    StateRegression {
        current: crate::ids::RaftGeneration,
        attempted: crate::ids::RaftGeneration,
    },

    /// Peer is not the current Raft leader.
    #[error("not leader (hint: {hint:?})")]
    NotLeader { hint: Option<NodeId> },

    /// RPC to a peer not in the routing table.
    #[error("unknown peer: {0}")]
    UnknownPeer(NodeId),

    /// openraft library failure.
    #[error("raft error: {0}")]
    Raft(String),

    /// Write rejected: this node is not in the current replica set.
    #[error("not a replica: {node_id:?}")]
    NotAReplica { node_id: NodeId },

    /// The leader refused to admit (or keep) this node because its `server_name`
    /// is not in the cluster's `config.peers`. The node has been evicted on
    /// purpose (scale-down) or is misconfigured; either way it must crash and let
    /// the orchestrator reap/restart it. No local data is deleted — that is a
    /// human decision. Crosses the wire as a structured `Error` variant (the
    /// whole error type is serializable), so the join client recognises it by
    /// variant rather than by string.
    #[error("raft membership denied: node {node_id:?} is not in config.peers")]
    MembershipDenied { node_id: NodeId },

    #[error(transparent)]
    SubscriptionEnded(Box<SubscriptionEnded>),
}

error_union! {
    /// Crate-wide error type: one variant per leaf error, named after its group.
    ///
    /// Every fallible public API returns `Result<T, Error>`. Clone is derived so
    /// the same error can fan out to multiple waiters (e.g. quorum-read debouncer).
    /// `Serialize`/`Deserialize` let an `Error` cross the RPC boundary and be
    /// reconstructed *exactly* on the caller side (no lossy string flattening).
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub Error {
        Lifecycle(LifecycleError),
        Config(ConfigError),
        Clock(ClockError),
        Log(LogError),
        Snapshot(SnapshotError),
        Net(NetError),
        Consensus(ConsensusError),
    }
}

// ── Boxed variant payloads ────────────────────────────────────────────────────
//
// `Error` rides in the item type of every streamed channel, and a `Channel`
// ring pre-allocates `capacity` slots — so the *error* arm sets the stride of
// queues that are empty in the common case. A measured 8192-deep subscribe ring
// paid ~143 B/slot for 88 B of useful payload, purely because one variant
// inlined an 80-byte `RaftStateError`.
//
// So the bar is `size_of::<Error>() <= size_of::<DataId>() + 8`, pinned by
// `error_is_within_the_size_bar`, and any variant whose payload would push
// past it boxes that payload here. The `+ 8` is the union's own tag: every
// leaf enum is a `DataId` wide (24 bytes of `String` plus its tag) and
// `Error` wraps one, so a `DataId`-sized bar is not reachable without
// boxing every `String` payload. Errors are rare by construction, so the extra
// allocation lands only on paths that were already exceptional — whereas the
// size is paid by every slot of every ring, occupied or not.
//
// Each struct carries the `Display` the variant used to carry, and the variant
// is `#[error(transparent)]`, so error text is unchanged.

/// Payload of [`Error::UnexpectedNodeId`].
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("unexpected node id: expected={expected:?} actual={actual:?}")]
pub struct UnexpectedNodeId {
    pub expected: NodeId,
    pub actual: NodeId,
}

/// Payload of [`Error::StaleStateWrite`].
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("stale state write ({context}): expected generation {expected:?}, current {current:?}")]
pub struct StaleStateWrite {
    pub context: String,
    pub expected: crate::ids::RaftGeneration,
    pub current: crate::ids::RaftGeneration,
}

/// Payload of [`Error::InvalidRaftState`].
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("invalid raft state rejected by fence ({context}): {source}")]
pub struct InvalidRaftState {
    pub context: String,
    #[source]
    pub source: crate::raft::RaftStateError,
}

/// Payload of [`Error::SubscriptionEnded`].
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("subscription to {peer_id} ended: {reason}")]
pub struct SubscriptionEnded {
    pub peer_id: NodeId,
    pub reason: String,
}

impl Error {
    /// Build [`Error::UnexpectedNodeId`].
    #[must_use]
    pub(crate) fn unexpected_node_id(expected: NodeId, actual: NodeId) -> Self {
        Self::Consensus(ConsensusError::UnexpectedNodeId(Box::new(
            UnexpectedNodeId { expected, actual },
        )))
    }

    /// Build [`Error::StaleStateWrite`].
    #[must_use]
    pub(crate) fn stale_state_write(
        context: impl Into<String>,
        expected: crate::ids::RaftGeneration,
        current: crate::ids::RaftGeneration,
    ) -> Self {
        Self::Consensus(ConsensusError::StaleStateWrite(Box::new(StaleStateWrite {
            context: context.into(),
            expected,
            current,
        })))
    }

    /// Build [`Error::InvalidRaftState`].
    #[must_use]
    pub(crate) fn invalid_raft_state(
        context: impl Into<String>,
        source: crate::raft::RaftStateError,
    ) -> Self {
        Self::Consensus(ConsensusError::InvalidRaftState(Box::new(
            InvalidRaftState {
                context: context.into(),
                source,
            },
        )))
    }

    /// Build [`Error::SubscriptionEnded`].
    #[must_use]
    pub(crate) fn subscription_ended(peer_id: NodeId, reason: impl Into<String>) -> Self {
        Self::Consensus(ConsensusError::SubscriptionEnded(Box::new(
            SubscriptionEnded {
                peer_id,
                reason: reason.into(),
            },
        )))
    }
}

impl Error {
    /// A stable, low-cardinality label naming the error variant — for metric
    /// labels (e.g. `loadgen_errors_total{kind=...}`) and structured logging.
    /// Lives on the error type so new variants are classified next to their
    /// definition. Keep these strings stable; they are a metric label dimension.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Error::Config(ConfigError::InvalidConfiguration(_)) => "invalid_configuration",
            Error::Config(ConfigError::Observability(_)) => "observability",
            Error::Clock(ClockError::HlcCutoff(_)) => "hlc_cutoff",
            Error::Clock(ClockError::HlcDriftTooLarge { .. }) => "hlc_drift_too_large",
            Error::Consensus(ConsensusError::UnexpectedNodeId { .. }) => "unexpected_node_id",
            Error::Clock(ClockError::ClockSkewTooLarge { .. }) => "clock_skew_too_large",
            Error::Consensus(ConsensusError::QuorumUnreachable) => "quorum_unreachable",
            Error::Consensus(ConsensusError::StaleStateWrite { .. }) => "stale_state_write",
            Error::Consensus(ConsensusError::InvalidRaftState { .. }) => "invalid_raft_state",
            Error::Log(LogError::QuorumReadTimeout { .. }) => "quorum_read_timeout",
            Error::Consensus(ConsensusError::StateRegression { .. }) => "state_regression",
            Error::Consensus(ConsensusError::NotLeader { .. }) => "not_leader",
            Error::Log(LogError::Storage(_)) => "storage",
            Error::Log(LogError::StableLogBehindManifest { .. }) => "stable_log_behind_manifest",
            Error::Log(LogError::StableLogGap(_)) => "stable_log_gap",
            Error::Snapshot(SnapshotError::ObjectStore(_)) => "object_store",
            Error::Net(NetError::Rpc(_)) => "rpc",
            Error::Consensus(ConsensusError::UnknownPeer(_)) => "unknown_peer",
            Error::Consensus(ConsensusError::SubscriptionEnded { .. }) => "subscription_ended",
            Error::Log(LogError::SubscriptionStreamEnded { .. }) => "subscription_stream_ended",
            Error::Consensus(ConsensusError::Raft(_)) => "raft",
            Error::Log(LogError::Codec(_)) => "codec",
            Error::Snapshot(SnapshotError::Compression(_)) => "compression",
            Error::Lifecycle(LifecycleError::Cancelled) => "cancelled",
            Error::Lifecycle(LifecycleError::StartupCancelled) => "startup_cancelled",
            Error::Lifecycle(LifecycleError::HardCrashDataLoss) => "hard_crash_data_loss",
            Error::Log(LogError::StreamTruncated { .. }) => "stream_truncated",
            Error::Log(LogError::WriteTimeout { .. }) => "write_timeout",
            Error::Consensus(ConsensusError::NotAReplica { .. }) => "not_a_replica",
            Error::Lifecycle(LifecycleError::NotYetImplemented(_)) => "not_yet_implemented",
            Error::Consensus(ConsensusError::MembershipDenied { .. }) => "membership_denied",
            Error::Lifecycle(LifecycleError::Shutdown) => "shutdown",
            Error::Lifecycle(LifecycleError::NotReady(_)) => "not_ready",
        }
    }

    /// The `zslg_snapshot_creation_total` failure bucket for this error. Lives on
    /// the error type so new variants get classified here, next to their
    /// definition, instead of in a caller-side catch-all.
    #[must_use]
    pub(crate) fn snapshot_state(&self) -> crate::metrics::SnapshotState {
        use crate::metrics::SnapshotState;
        match self {
            Error::Consensus(ConsensusError::StaleStateWrite { .. }) => {
                SnapshotState::FailureStaleGeneration
            }
            Error::Consensus(ConsensusError::QuorumUnreachable)
            | Error::Log(LogError::QuorumReadTimeout { .. }) => SnapshotState::FailureQuorumTimeout,
            Error::Snapshot(SnapshotError::ObjectStore(_)) => SnapshotState::FailureObjectStore,
            Error::Log(LogError::StableLogBehindManifest { .. }) => {
                SnapshotState::WaitingForStableLog
            }
            // HLC-cutoff rejection comes from a replica RPC error; the
            // leader sees it as a fan-out failure. Everything else counts
            // as a storage-side failure.
            _ => SnapshotState::FailureStorage,
        }
    }
}

impl From<rocksdb_kit::StorageError> for Error {
    fn from(e: rocksdb_kit::StorageError) -> Self {
        match e {
            rocksdb_kit::StorageError::InvalidConfiguration(msg) => {
                Error::Config(ConfigError::InvalidConfiguration(msg))
            }
            rocksdb_kit::StorageError::Storage(msg) => Error::Log(LogError::Storage(msg)),
        }
    }
}

impl From<shutdown::Stopped> for Error {
    fn from(_: shutdown::Stopped) -> Self {
        Error::Lifecycle(LifecycleError::Shutdown)
    }
}

impl From<framing::FrameError> for Error {
    fn from(e: framing::FrameError) -> Self {
        Error::Log(LogError::Codec(e.to_string()))
    }
}

impl From<bytesize::OutOfRange> for Error {
    fn from(e: bytesize::OutOfRange) -> Self {
        Error::Config(ConfigError::InvalidConfiguration(e.to_string()))
    }
}

pub(crate) trait ToRpc<T> {
    fn to_rpc(self, context: &str) -> Result<T, NetError>;
}

impl<T, E: std::fmt::Display> ToRpc<T> for Result<T, E> {
    fn to_rpc(self, context: &str) -> Result<T, NetError> {
        self.map_err(|e| NetError::Rpc(format!("{context}: {e}")))
    }
}

pub(crate) trait ToStorage<T> {
    fn to_storage(self, context: &str) -> Result<T, LogError>;
}

impl<T, E: std::fmt::Display> ToStorage<T> for Result<T, E> {
    fn to_storage(self, context: &str) -> Result<T, LogError> {
        self.map_err(|e| LogError::Storage(format!("{context}: {e}")))
    }
}

pub(crate) trait ToRaft<T> {
    fn to_raft(self, context: &str) -> Result<T, ConsensusError>;
}

impl<T, E: std::fmt::Display> ToRaft<T> for Result<T, E> {
    fn to_raft(self, context: &str) -> Result<T, ConsensusError> {
        self.map_err(|e| ConsensusError::Raft(format!("{context}: {e}")))
    }
}

pub(crate) trait ToCodec<T> {
    fn to_codec(self, context: &str) -> Result<T, LogError>;
}

impl<T, E: std::fmt::Display> ToCodec<T> for Result<T, E> {
    fn to_codec(self, context: &str) -> Result<T, LogError> {
        self.map_err(|e| LogError::Codec(format!("{context}: {e}")))
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
    use super::*;

    /// `Error` rides in every streamed channel item — a `Channel<Result<T, Error>>`
    /// ring pre-allocates `capacity` slots, so the *error* arm sets the slot
    /// stride for queues that are empty in the common case. A `DataId` (32 B) is
    /// the largest thing the payload arm genuinely needs; an error has no claim
    /// to be much bigger than that — the 8 bytes over are the union's tag over
    /// a leaf enum that is itself a `DataId` wide. Errors are rare, so any variant that would push
    /// past the bar boxes its payload — boxing costs an allocation on a path
    /// that is already exceptional.
    #[test]
    fn error_is_within_the_size_bar() {
        let error = size_of::<Error>();
        let bar = size_of::<crate::ids::DataId>() + 8;
        assert!(
            error <= bar,
            "size_of::<Error>() == {error}, over the {bar}-byte bar. \
             Box the payload of whichever variant grew instead of raising this.",
        );
    }

    #[test]
    fn error_is_send_sync_static() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<Error>();
    }

    #[test]
    fn display_messages_carry_context() {
        let node = NodeId::new_v4();
        let cases: Vec<(Error, &str)> = vec![
            (
                Error::Config(ConfigError::InvalidConfiguration("bad skew".into())),
                "bad skew",
            ),
            (
                Error::Consensus(ConsensusError::QuorumUnreachable),
                "quorum unreachable",
            ),
            (
                Error::Consensus(ConsensusError::NotLeader { hint: Some(node) }),
                "not leader",
            ),
            (Error::Log(LogError::Storage("disk".into())), "disk"),
            (
                Error::Log(LogError::StableLogGap("hole at 7".into())),
                "gap",
            ),
            (
                Error::Snapshot(SnapshotError::ObjectStore("s3".into())),
                "s3",
            ),
            (Error::Net(NetError::Rpc("timeout".into())), "timeout"),
            (
                Error::Consensus(ConsensusError::Raft("election".into())),
                "election",
            ),
            (Error::Log(LogError::Codec("postcard".into())), "postcard"),
            (
                Error::Snapshot(SnapshotError::Compression("zstd".into())),
                "zstd",
            ),
            (Error::Lifecycle(LifecycleError::Cancelled), "cancelled"),
            (
                Error::Lifecycle(LifecycleError::HardCrashDataLoss),
                "data loss",
            ),
            (
                Error::Consensus(ConsensusError::NotAReplica { node_id: node }),
                "not a replica",
            ),
            (
                Error::Lifecycle(LifecycleError::NotYetImplemented("M5b".to_owned())),
                "m5b",
            ),
            (
                Error::Consensus(ConsensusError::MembershipDenied { node_id: node }),
                "membership denied",
            ),
            (Error::Lifecycle(LifecycleError::Shutdown), "shutting down"),
            (
                Error::Lifecycle(LifecycleError::NotReady("not a voter".to_owned())),
                "not ready",
            ),
        ];
        for (err, needle) in cases {
            let msg = err.to_string();
            assert!(!msg.is_empty(), "empty display for {err:?}");
            assert!(
                msg.to_lowercase().contains(needle),
                "message {msg:?} should contain {needle:?}",
            );
        }
    }

    #[test]
    fn error_is_clone() {
        let e = Error::Log(LogError::Storage("x".into()));
        let c = e.clone();
        assert_eq!(e.to_string(), c.to_string());
    }

    // ── conversion traits ───────────────────────────────────────────────

    #[test]
    fn to_storage_wraps_with_context() {
        let r: Result<(), std::io::Error> = Err(std::io::Error::other("boom"));
        let mapped = r.to_storage("opening db");
        match mapped {
            Err(LogError::Storage(msg)) => {
                assert!(msg.contains("opening db"));
                assert!(msg.contains("boom"));
            }
            other => panic!("expected Storage error, got {other:?}"),
        }
    }

    #[test]
    fn to_rpc_wraps_with_context() {
        let r: Result<(), std::io::Error> = Err(std::io::Error::other("net"));
        assert!(
            matches!(r.to_rpc("send"), Err(NetError::Rpc(m)) if m.contains("send") && m.contains("net"))
        );
    }

    #[test]
    fn to_raft_wraps_with_context() {
        let r: Result<(), std::io::Error> = Err(std::io::Error::other("raft"));
        assert!(matches!(r.to_raft("init"), Err(ConsensusError::Raft(m)) if m.contains("init")));
    }

    #[test]
    fn conversion_traits_pass_through_ok() {
        let r: Result<u32, std::io::Error> = Ok(42);
        assert_eq!(r.to_storage("ctx").expect("ok"), 42);
    }

    /// A stable log trailing the manifest is backpressure, not failure: the
    /// leader simply has not re-ingested its own last snapshot yet. It must not
    /// land in a `failure_*` bucket, or `zslg_snapshot_creation_total{state=~"failure.*"}`
    /// alerts on normal operation.
    #[test]
    fn stable_log_behind_manifest_is_not_a_failure_state() {
        use crate::ids::LogIndex;
        use crate::metrics::SnapshotState;

        let e = Error::Log(LogError::StableLogBehindManifest {
            end_index: Some(LogIndex(41)),
            manifest_end_index: LogIndex(99),
        });
        assert_eq!(e.snapshot_state(), SnapshotState::WaitingForStableLog);
        assert_eq!(e.kind(), "stable_log_behind_manifest");
    }
}
