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

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::hlc::Hlc;
use crate::ids::NodeId;

/// Crate-wide error type.
///
/// Every fallible public API returns `Result<T, Error>`. Clone is derived so
/// the same error can fan out to multiple waiters (e.g. quorum-read debouncer).
/// `Serialize`/`Deserialize` let an `Error` cross the RPC boundary and be
/// reconstructed *exactly* on the caller side (no lossy string flattening) —
/// see `crate::network::messages::RpcFrame::Error`.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum Error {
    /// Configuration validation failed.
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Tracing or metrics initialization failed.
    #[error("observability init failed: {0}")]
    Observability(String),

    /// HLC below the current cutoff.
    #[error("hlc cutoff violation: {0}")]
    HlcCutoff(#[from] crate::hlc::CutoffError),

    /// Peer's HLC drifted beyond the configured skew.
    #[error("hlc drift too large: peer={peer:?} local={local:?}")]
    HlcDriftTooLarge { peer: Hlc, local: Hlc },

    /// RPC received by a node that does not match the caller's expected target.
    #[error(transparent)]
    UnexpectedNodeId(Box<UnexpectedNodeId>),

    /// Writer-replica wall-clock skew over max_clock_skew.
    #[error("clock skew too large: writer={writer:?} replica={replica:?}")]
    ClockSkewTooLarge { writer: Hlc, replica: Hlc },

    /// No quorum could be reached against the current Raft generation.
    #[error("quorum unreachable")]
    QuorumUnreachable,

    /// The node's shutdown token was cancelled while it was still catching up to
    /// a quorum, so it never became ready.
    ///
    /// Deliberately its own variant rather than a flavour of `Raft`: it is the
    /// *expected* outcome of an operator stopping a node that would never become
    /// ready (the freshness gate retries indefinitely by design), and a caller
    /// must be able to tell it apart from a genuine startup fault — one is a
    /// clean stop, the other is a reason to alert.
    #[error("node startup cancelled before the node became ready")]
    StartupCancelled,

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

    /// A quorum read could not confirm replica catch-up within the deadline.
    /// The system may be partitioned or an upstream may be unrecoverable;
    /// callers can retry or fall back to a weaker read level.
    #[error("quorum read timed out after {timeout:?}")]
    QuorumReadTimeout { timeout: std::time::Duration },

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

    /// Object-store I/O failure.
    #[error("object store error: {0}")]
    ObjectStore(String),

    /// RPC transport failure.
    #[error("rpc error: {0}")]
    Rpc(String),

    /// RPC to a peer not in the routing table.
    #[error("unknown peer: {0}")]
    UnknownPeer(NodeId),

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
    #[error(transparent)]
    SubscriptionEnded(Box<SubscriptionEnded>),

    /// openraft library failure.
    #[error("raft error: {0}")]
    Raft(String),

    /// postcard serialisation/deserialisation failure.
    #[error("codec error: {0}")]
    Codec(String),

    /// zstd compression/decompression failure.
    #[error("compression error: {0}")]
    Compression(String),

    /// Operation was cancelled.
    #[error("cancelled")]
    Cancelled,

    /// Local Raft state exists but cluster membership doesn't list this node.
    /// Must wipe state and restart — a fresh NodeId will be auto-generated.
    #[error("hard-crash data loss; wipe data directory and restart")]
    HardCrashDataLoss,

    /// A finite stream ended without its producer declaring completion (see
    /// `channel::End::Aborted`): the producer panicked, or was discarded before
    /// it ran. Items may be missing, so a consumer that needs the whole stream
    /// must fail rather than treat it as a clean end.
    #[error("stream {what} ended without closing; it may be truncated")]
    StreamTruncated { what: String },

    /// A client write exceeded `WriterConfig::write_timeout` (default 10s),
    /// covering the admission-acquire + fan-out phases, so a leaked permit or a
    /// stalled self/loopback target surfaces as a bounded error instead of a
    /// silent hang.
    #[error("write timed out after {elapsed:?}")]
    WriteTimeout { elapsed: std::time::Duration },

    /// Write rejected: this node is not in the current replica set.
    #[error("not a replica: {node_id:?}")]
    NotAReplica { node_id: NodeId },

    /// Write rejected by a remote replica. Bundles the *real* replica-side
    /// rejection error (`source`, e.g. `HlcCutoff` / `NotAReplica`) with the
    /// replica's current `RaftState`, so the quorum coordinator can track config
    /// changes on rejections while the underlying error is preserved exactly.
    #[error(transparent)]
    WriteRejected(Box<WriteRejected>),

    /// Feature not yet implemented.
    #[error("not yet implemented: {0}")]
    NotYetImplemented(String),

    /// The leader refused to admit (or keep) this node because its `server_name`
    /// is not in the cluster's `config.peers`. The node has been evicted on
    /// purpose (scale-down) or is misconfigured; either way it must crash and let
    /// the orchestrator reap/restart it. No local data is deleted — that is a
    /// human decision. Crosses the wire as a structured `Error` variant (the
    /// whole error type is serializable), so the join client recognises it by
    /// variant rather than by string.
    #[error("raft membership denied: node {node_id:?} is not in config.peers")]
    MembershipDenied { node_id: NodeId },

    #[error("node shutting down")]
    Shutdown,

    /// The node is running but cannot serve client reads or writes: it is not
    /// in the voter set, so its view of replicated state has stopped
    /// advancing. Serving from it would return a snapshot that silently ceases
    /// to move.
    ///
    /// Retryable. A node in this state stays up deliberately — being removed by
    /// the group is not grounds for the process to terminate itself.
    #[error("node is not ready to serve clients: {0}")]
    NotReady(String),
}

// ── Boxed variant payloads ────────────────────────────────────────────────────
//
// `Error` rides in the item type of every streamed channel, and a `Channels`
// ring pre-allocates `capacity` slots — so the *error* arm sets the stride of
// queues that are empty in the common case. A measured 8192-deep subscribe ring
// paid ~143 B/slot for 88 B of useful payload, purely because one variant
// inlined an 80-byte `RaftStateError`.
//
// So the bar is `size_of::<Error>() <= size_of::<DataId>()`, pinned by
// `error_is_no_larger_than_a_data_id`, and any variant whose payload would push
// past it boxes that payload here. Errors are rare by construction, so the extra
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

/// Payload of [`Error::WriteRejected`].
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[error("write rejected by {node_id:?}: {source}")]
pub struct WriteRejected {
    pub node_id: NodeId,
    #[source]
    pub source: Error,
    pub raft_state: Arc<crate::meta::RaftState>,
}

impl Error {
    /// Build [`Error::UnexpectedNodeId`].
    #[must_use]
    pub(crate) fn unexpected_node_id(expected: NodeId, actual: NodeId) -> Self {
        Self::UnexpectedNodeId(Box::new(UnexpectedNodeId { expected, actual }))
    }

    /// Build [`Error::StaleStateWrite`].
    #[must_use]
    pub(crate) fn stale_state_write(
        context: impl Into<String>,
        expected: crate::ids::RaftGeneration,
        current: crate::ids::RaftGeneration,
    ) -> Self {
        Self::StaleStateWrite(Box::new(StaleStateWrite {
            context: context.into(),
            expected,
            current,
        }))
    }

    /// Build [`Error::InvalidRaftState`].
    #[must_use]
    pub(crate) fn invalid_raft_state(
        context: impl Into<String>,
        source: crate::raft::RaftStateError,
    ) -> Self {
        Self::InvalidRaftState(Box::new(InvalidRaftState {
            context: context.into(),
            source,
        }))
    }

    /// Build [`Error::SubscriptionEnded`].
    #[must_use]
    pub(crate) fn subscription_ended(peer_id: NodeId, reason: impl Into<String>) -> Self {
        Self::SubscriptionEnded(Box::new(SubscriptionEnded {
            peer_id,
            reason: reason.into(),
        }))
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
            Error::InvalidConfiguration(_) => "invalid_configuration",
            Error::Observability(_) => "observability",
            Error::HlcCutoff(_) => "hlc_cutoff",
            Error::HlcDriftTooLarge { .. } => "hlc_drift_too_large",
            Error::UnexpectedNodeId { .. } => "unexpected_node_id",
            Error::ClockSkewTooLarge { .. } => "clock_skew_too_large",
            Error::QuorumUnreachable => "quorum_unreachable",
            Error::StaleStateWrite { .. } => "stale_state_write",
            Error::InvalidRaftState { .. } => "invalid_raft_state",
            Error::QuorumReadTimeout { .. } => "quorum_read_timeout",
            Error::StateRegression { .. } => "state_regression",
            Error::NotLeader { .. } => "not_leader",
            Error::Storage(_) => "storage",
            Error::StableLogBehindManifest { .. } => "stable_log_behind_manifest",
            Error::StableLogGap(_) => "stable_log_gap",
            Error::ObjectStore(_) => "object_store",
            Error::Rpc(_) => "rpc",
            Error::UnknownPeer(_) => "unknown_peer",
            Error::SubscriptionEnded { .. } => "subscription_ended",
            Error::SubscriptionStreamEnded { .. } => "subscription_stream_ended",
            Error::Raft(_) => "raft",
            Error::Codec(_) => "codec",
            Error::Compression(_) => "compression",
            Error::Cancelled => "cancelled",
            Error::StartupCancelled => "startup_cancelled",
            Error::HardCrashDataLoss => "hard_crash_data_loss",
            Error::StreamTruncated { .. } => "stream_truncated",
            Error::WriteTimeout { .. } => "write_timeout",
            Error::NotAReplica { .. } => "not_a_replica",
            // The wire-bundled rejection: classify by the *underlying* error so
            // a write rejected for an HLC cutoff is counted as `hlc_cutoff`,
            // not lumped under a generic `write_rejected`.
            Error::WriteRejected(w) => w.source.kind(),
            Error::NotYetImplemented(_) => "not_yet_implemented",
            Error::MembershipDenied { .. } => "membership_denied",
            Error::Shutdown => "shutdown",
            Error::NotReady(_) => "not_ready",
        }
    }

    /// The `zslg_snapshot_creation_total` failure bucket for this error. Lives on
    /// the error type so new variants get classified here, next to their
    /// definition, instead of in a caller-side catch-all.
    #[must_use]
    pub(crate) fn snapshot_state(&self) -> crate::metrics::SnapshotState {
        use crate::metrics::SnapshotState;
        match self {
            Error::StaleStateWrite { .. } => SnapshotState::FailureStaleGeneration,
            Error::QuorumUnreachable | Error::QuorumReadTimeout { .. } => {
                SnapshotState::FailureQuorumTimeout
            }
            Error::ObjectStore(_) => SnapshotState::FailureObjectStore,
            Error::StableLogBehindManifest { .. } => SnapshotState::WaitingForStableLog,
            // HLC-cutoff rejection comes from a replica RPC error; the
            // leader sees it as a fan-out failure. Everything else counts
            // as a storage-side failure.
            _ => SnapshotState::FailureStorage,
        }
    }
}

pub(crate) trait ToRpc<T> {
    fn to_rpc(self, context: &str) -> Result<T, Error>;
}

impl<T, E: std::fmt::Display> ToRpc<T> for Result<T, E> {
    fn to_rpc(self, context: &str) -> Result<T, Error> {
        self.map_err(|e| Error::Rpc(format!("{context}: {e}")))
    }
}

pub(crate) trait ToStorage<T> {
    fn to_storage(self, context: &str) -> Result<T, Error>;
}

impl<T, E: std::fmt::Display> ToStorage<T> for Result<T, E> {
    fn to_storage(self, context: &str) -> Result<T, Error> {
        self.map_err(|e| Error::Storage(format!("{context}: {e}")))
    }
}

pub(crate) trait ToRaft<T> {
    fn to_raft(self, context: &str) -> Result<T, Error>;
}

impl<T, E: std::fmt::Display> ToRaft<T> for Result<T, E> {
    fn to_raft(self, context: &str) -> Result<T, Error> {
        self.map_err(|e| Error::Raft(format!("{context}: {e}")))
    }
}

pub(crate) trait ToCodec<T> {
    fn to_codec(self, context: &str) -> Result<T, Error>;
}

impl<T, E: std::fmt::Display> ToCodec<T> for Result<T, E> {
    fn to_codec(self, context: &str) -> Result<T, Error> {
        self.map_err(|e| Error::Codec(format!("{context}: {e}")))
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

    /// `Error` rides in every streamed channel item — a `Channels<Result<T, Error>>`
    /// ring pre-allocates `capacity` slots, so the *error* arm sets the slot
    /// stride for queues that are empty in the common case. A `DataId` (32 B) is
    /// the largest thing the payload arm genuinely needs; an error has no claim
    /// to be bigger than that. Errors are rare, so any variant that would push
    /// past the bar boxes its payload — boxing costs an allocation on a path
    /// that is already exceptional.
    #[test]
    fn error_is_no_larger_than_a_data_id() {
        let error = size_of::<Error>();
        let data_id = size_of::<crate::ids::DataId>();
        assert!(
            error <= data_id,
            "size_of::<Error>() == {error}, over the {data_id}-byte bar. \
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
            (Error::InvalidConfiguration("bad skew".into()), "bad skew"),
            (Error::QuorumUnreachable, "quorum unreachable"),
            (Error::NotLeader { hint: Some(node) }, "not leader"),
            (Error::Storage("disk".into()), "disk"),
            (Error::StableLogGap("hole at 7".into()), "gap"),
            (Error::ObjectStore("s3".into()), "s3"),
            (Error::Rpc("timeout".into()), "timeout"),
            (Error::Raft("election".into()), "election"),
            (Error::Codec("postcard".into()), "postcard"),
            (Error::Compression("zstd".into()), "zstd"),
            (Error::Cancelled, "cancelled"),
            (Error::HardCrashDataLoss, "data loss"),
            (Error::NotAReplica { node_id: node }, "not a replica"),
            (Error::NotYetImplemented("M5b".to_owned()), "m5b"),
            (
                Error::MembershipDenied { node_id: node },
                "membership denied",
            ),
            (Error::Shutdown, "shutting down"),
            (Error::NotReady("not a voter".to_owned()), "not ready"),
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
        let e = Error::Storage("x".into());
        let c = e.clone();
        assert_eq!(e.to_string(), c.to_string());
    }

    // ── conversion traits ───────────────────────────────────────────────

    #[test]
    fn to_storage_wraps_with_context() {
        let r: Result<(), std::io::Error> = Err(std::io::Error::other("boom"));
        let mapped = r.to_storage("opening db");
        match mapped {
            Err(Error::Storage(msg)) => {
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
            matches!(r.to_rpc("send"), Err(Error::Rpc(m)) if m.contains("send") && m.contains("net"))
        );
    }

    #[test]
    fn to_raft_wraps_with_context() {
        let r: Result<(), std::io::Error> = Err(std::io::Error::other("raft"));
        assert!(matches!(r.to_raft("init"), Err(Error::Raft(m)) if m.contains("init")));
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

        let e = Error::StableLogBehindManifest {
            end_index: Some(LogIndex(41)),
            manifest_end_index: LogIndex(99),
        };
        assert_eq!(e.snapshot_state(), SnapshotState::WaitingForStableLog);
        assert_eq!(e.kind(), "stable_log_behind_manifest");
    }
}
