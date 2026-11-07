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
use zslg_consensus_types::{ConsensusError, InvalidRaftState, StaleStateWrite, SubscriptionEnded};
use zslg_lifecycle_types::{ConfigError, LifecycleError};
use zslg_log_types::LogError;
use zslg_net_types::NetError;
use zslg_snapshot_types::SnapshotError;
use zslg_time_types::ClockError;

use zslg_node_types::NodeId;

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
    from zslg_time::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_log::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_snapshot::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_consensus::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net_wire::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_quorum_read::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_object_store_config::Error { Snapshot }
    from zslg_node_id::Error { Config, Log }
    from zslg_raft_tracker::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_raft_storage::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net_client::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
    from zslg_net_server::Error { Lifecycle, Config, Clock, Log, Snapshot, Net, Consensus }
}

/// Bring a value that carries a port crate's error back to zslg's own `Error`:
/// `Result<T, E>` and the (ordered) channels of results. The boundary verb for
/// code that calls a port; the other direction is `map_err(Into::into)` /
/// `err_into()`. Goes away with the unions' narrowing.
pub(crate) trait Home {
    type Out;
    fn home(self) -> Self::Out;
}

impl<T, E> Home for Result<T, E>
where
    Error: From<E>,
{
    type Out = Result<T, Error>;
    fn home(self) -> Self::Out {
        self.map_err(Into::into)
    }
}

impl<A, E, K> Home for crossbeam_combinators::Channel<Result<A, E>, K>
where
    A: Send + 'static,
    Error: From<E>,
    E: Send + 'static,
    K: crossbeam_combinators::Kind,
{
    type Out = crossbeam_combinators::Channel<Result<A, Error>, K>;
    fn home(self) -> Self::Out {
        self.err_into()
    }
}

impl<A, E, P, K> Home for crossbeam_combinators::OrderedChannel<Result<A, E>, P, K>
where
    A: Send + 'static,
    Error: From<E>,
    E: Send + 'static,
    P: crossbeam_combinators::OrderPolicy,
    P::Key: crossbeam_combinators::Key<Result<A, Error>>,
    K: crossbeam_combinators::Kind,
{
    type Out = crossbeam_combinators::OrderedChannel<Result<A, Error>, P, K>;
    fn home(self) -> Self::Out {
        self.err_into()
    }
}

macro_rules! port_error_bridge {
    ($($port:path),+ $(,)?) => {$(
        impl From<Error> for $port {
            fn from(e: Error) -> Self {
                match e {
                    Error::Lifecycle(x) => Self::Lifecycle(x),
                    Error::Config(x) => Self::Config(x),
                    Error::Clock(x) => Self::Clock(x),
                    Error::Log(x) => Self::Log(x),
                    Error::Snapshot(x) => Self::Snapshot(x),
                    Error::Net(x) => Self::Net(x),
                    Error::Consensus(x) => Self::Consensus(x),
                }
            }
        }
    )+};
}

port_error_bridge!(
    zslg_time::Error,
    zslg_log::Error,
    zslg_snapshot::Error,
    zslg_consensus::Error,
    zslg_raft::Error,
    zslg_net::Error,
    zslg_net_wire::Error,
    zslg_quorum_read::Error
);

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

impl Error {
    /// Build [`Error::StaleStateWrite`].
    #[must_use]
    pub(crate) fn stale_state_write(
        context: impl Into<String>,
        expected: zslg_consensus_types::RaftGeneration,
        current: zslg_consensus_types::RaftGeneration,
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
        source: zslg_consensus_types::RaftStateError,
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
    use zslg_consensus::ToRaft as _;
    use zslg_log::ToStorage as _;
    use zslg_net_types::ToRpc as _;

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
        let bar = size_of::<zslg_log_types::DataId>() + 8;
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
        use crate::metrics::SnapshotState;
        use zslg_log_types::LogIndex;

        let e = Error::Log(LogError::StableLogBehindManifest {
            end_index: Some(LogIndex(41)),
            manifest_end_index: LogIndex(99),
        });
        assert_eq!(e.snapshot_state(), SnapshotState::WaitingForStableLog);
        assert_eq!(e.kind(), "stable_log_behind_manifest");
    }
}
