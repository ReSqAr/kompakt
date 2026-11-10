//! How a node's lifecycle and configuration can fail: asked to stop, stopped by itself, not ready yet, or started with a configuration it cannot run with.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// What a node's lifecycle can end in: asked to stop, stopped by itself, or not ready yet.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum LifecycleError {
    /// Operation was cancelled.
    #[error("cancelled")]
    Cancelled,

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

    /// Local Raft state exists but cluster membership doesn't list this node.
    /// Must wipe state and restart — a fresh NodeId will be auto-generated.
    #[error("hard-crash data loss; wipe data directory and restart")]
    HardCrashDataLoss,

    /// Feature not yet implemented.
    #[error("not yet implemented: {0}")]
    NotYetImplemented(String),
}

impl LifecycleError {
    /// A stable, low-cardinality label naming the variant — for metric labels
    /// and structured logging. Keep these strings stable; they are a metric
    /// label dimension.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::StartupCancelled => "startup_cancelled",
            Self::HardCrashDataLoss => "hard_crash_data_loss",
            Self::NotYetImplemented(_) => "not_yet_implemented",
            Self::Shutdown => "shutdown",
            Self::NotReady(_) => "not_ready",
        }
    }
}

/// A configuration the node cannot run with.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum ConfigError {
    /// Configuration validation failed.
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Tracing or metrics initialization failed.
    #[error("observability init failed: {0}")]
    Observability(String),
}

impl ConfigError {
    /// A stable, low-cardinality label naming the variant — for metric labels
    /// and structured logging. Keep these strings stable; they are a metric
    /// label dimension.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::InvalidConfiguration(_) => "invalid_configuration",
            Self::Observability(_) => "observability",
        }
    }
}
