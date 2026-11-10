//! The startup decision: the durable facts it reads, and the outcome they pick.

use super::{JOIN_INITIAL_BACKOFF, JOIN_MAX_BACKOFF};
use crate::Error;
use std::sync::Arc;
use tracing::warn;
use zslg_snapshot::SnapshotStore;

/// Read the object-store bootstrap fence, retrying forever on a transient
/// object-store error: during a full cold start the object store may itself
/// still be coming up, and a single failed read must not abort the node. A
/// persistent misconfiguration loops loudly rather than crash-looping —
/// consistent with unbounded startup (header).
pub(crate) async fn resolve_bootstrap_fence(
    snapshot_store: &Arc<dyn SnapshotStore>,
) -> Result<bool, Error> {
    let mut backoff = JOIN_INITIAL_BACKOFF;
    loop {
        match snapshot_store.has_bootstrap_fence().await {
            Ok(present) => return Ok(present),
            Err(e) => {
                warn!(
                    error = %e,
                    ?backoff,
                    "raft startup: bootstrap-fence check failed (object store not ready?); retrying"
                );
                let jittered = backoff_kit::jitter(backoff);
                tokio::time::sleep(jittered).await;
                backoff = backoff.saturating_mul(2).min(JOIN_MAX_BACKOFF);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BootstrapInputs {
    pub has_local_state: bool,
    pub fence_present: bool,
    pub is_bootstrap_node: bool,
    pub in_membership: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BootstrapDecision {
    Rejoin,
    JoinExisting,
    Bootstrap,
    HardCrashRestart,
}

#[must_use]
pub(crate) fn bootstrap_decision(
    BootstrapInputs {
        has_local_state,
        fence_present,
        is_bootstrap_node,
        in_membership,
    }: BootstrapInputs,
) -> BootstrapDecision {
    if has_local_state {
        if in_membership {
            BootstrapDecision::Rejoin
        } else {
            BootstrapDecision::HardCrashRestart
        }
    } else if fence_present {
        BootstrapDecision::JoinExisting
    } else if is_bootstrap_node {
        BootstrapDecision::Bootstrap
    } else {
        BootstrapDecision::JoinExisting
    }
}
