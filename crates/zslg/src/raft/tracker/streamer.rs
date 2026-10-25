//! Peer-watcher-driven streaming raft-state tracker for non-Raft nodes.
//!
//! Both the [`WriterNode`](crate::WriterNode) and the
//! [`LearnerNode`](crate::LearnerNode) learn the cluster's `RaftState` and
//! `NodeId → addr` mapping this way: each peer holds an open `subscribe_status`
//! stream that pushes the current `ReplicaStatus` baseline and then a fresh
//! status on every `RaftState` change. The tracker integrates each one, so it
//! stays continuously fresh — without polling, and without draining the
//! replica's data log (only low-volume status updates flow). The supervisor
//! follows a [`PeersWatcher`] reconcile pattern: one independent worker per peer
//! address, each with its own failure backoff, so a slow or dead replica never
//! stalls the others.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::StreamExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::api::{RaftStateTracker, Router};
use crate::error::Error;
use crate::ids::PeerAddr;
use crate::raft::{RaftTracker, RaftTrackerConfig};
use crate::shutdown::Shutdown;

/// Create a fresh [`RaftTracker`], register the stream supervisor with
/// `shutdown`, and block until a quorum of write targets have been discovered
/// (or `startup_timeout` elapses). Once this returns the tracker is ready for
/// use by `spawn_learner`.
pub(crate) async fn spawn_raft_tracker_streamer(
    router: Arc<dyn Router>,
    config: RaftTrackerConfig,
    shutdown: &mut Shutdown,
) -> Result<Arc<RaftTracker>, Error> {
    let raft_tracker = Arc::new(RaftTracker::new());
    let cancel = shutdown.token();
    shutdown.spawn(
        "raft:tracker_streamer",
        run_supervisor(router, raft_tracker.clone(), config.clone(), cancel),
    );

    raft_tracker.wait_for_ready(config.startup_timeout).await?;

    Ok(raft_tracker)
}

async fn run_supervisor(
    router: Arc<dyn Router>,
    raft_tracker: Arc<RaftTracker>,
    config: RaftTrackerConfig,
    cancel: CancellationToken,
) -> Result<(), Error> {
    tracing::info!("raft-tracker streamer: starting");
    let mut rx = config.peers.subscribe();
    let mut workers: HashMap<PeerAddr, CancellationToken> = HashMap::new();
    let mut join_set: JoinSet<()> = JoinSet::new();

    // Seed from the initial peer list.
    let initial = rx.borrow_and_update().clone();
    reconcile(
        &initial,
        &mut workers,
        &mut join_set,
        &router,
        &raft_tracker,
        &config,
        &cancel,
    );

    loop {
        // Reap workers that exited so the set stays bounded.
        while join_set.try_join_next().is_some() {}

        tokio::select! {
            () = cancel.cancelled() => break,
            changed = rx.changed() => {
                if changed.is_err() {
                    // PeersWatcher dropped — idle until global shutdown fires.
                    cancel.cancelled().await;
                    break;
                }
                let current = rx.borrow_and_update().clone();
                reconcile(
                    &current,
                    &mut workers,
                    &mut join_set,
                    &router,
                    &raft_tracker,
                    &config,
                    &cancel,
                );
            }
        }
    }

    // Child tokens cascade from `cancel`, so the workers are already winding
    // down; drain them to completion.
    join_set.shutdown().await;
    tracing::info!("raft-tracker streamer: shutdown");
    Ok(())
}

/// Spawn a worker for every newly-present address and cancel the worker for
/// every address that has dropped out of `current`.
#[allow(
    clippy::too_many_arguments,
    reason = "reconcile threads through all worker dependencies; kept private"
)]
fn reconcile(
    current: &[PeerAddr],
    workers: &mut HashMap<PeerAddr, CancellationToken>,
    join_set: &mut JoinSet<()>,
    router: &Arc<dyn Router>,
    raft_tracker: &Arc<RaftTracker>,
    config: &RaftTrackerConfig,
    parent_cancel: &CancellationToken,
) {
    let desired: HashSet<&PeerAddr> = current.iter().collect();
    workers.retain(|addr, token| {
        let keep = desired.contains(addr);
        if !keep {
            debug!(%addr, "raft-tracker streamer: peer removed, stopping worker");
            token.cancel();
        }
        keep
    });
    for addr in current {
        if !workers.contains_key(addr) {
            let child = parent_cancel.child_token();
            workers.insert(addr.clone(), child.clone());
            debug!(%addr, "raft-tracker streamer: peer added, starting worker");
            join_set.spawn(run_stream_worker(
                addr.clone(),
                router.clone(),
                raft_tracker.clone(),
                config.clone(),
                child,
            ));
        }
    }
}

/// Hold one peer's `subscribe_status` stream open forever: each item updates the
/// tracker (register `NodeId → addr`, integrate `RaftState`, note discovered).
/// The first item is the baseline; subsequent items arrive on every `RaftState`
/// change, so the tracker stays continuously fresh. On stream error/end, back
/// off exponentially before reconnecting; the backoff is only grown for a
/// subscription that never went live, so a previously-healthy peer reconnects
/// promptly. Owns its own backoff so a slow peer never stalls the others.
async fn run_stream_worker(
    addr: PeerAddr,
    router: Arc<dyn Router>,
    raft_tracker: Arc<RaftTracker>,
    config: RaftTrackerConfig,
    cancel: CancellationToken,
) {
    let mut backoff = config.min_backoff;
    loop {
        if cancel.is_cancelled() {
            return;
        }

        let client = router.remote_direct(addr.clone());
        match client.subscribe_status().await {
            Ok(mut stream) => {
                let mut established = false;
                loop {
                    tokio::select! {
                        () = cancel.cancelled() => return,
                        item = stream.next() => match item {
                            Some(Ok(status)) => {
                                router.register_peer(status.node_id, addr.clone());
                                raft_tracker.integrate(&status.raft_state);
                                raft_tracker.note_discovered(status.node_id);
                                // Reset backoff only once the subscription has
                                // delivered — a peer that accepts the stream but
                                // never sends a baseline can't defeat the backoff.
                                if !established {
                                    established = true;
                                    backoff = config.min_backoff;
                                }
                            }
                            Some(Err(e)) => {
                                debug!(%addr, error = %e,
                                    "raft-tracker streamer: status stream error, reconnecting");
                                break;
                            }
                            None => {
                                debug!(%addr,
                                    "raft-tracker streamer: status stream ended, reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                debug!(%addr, ?backoff, error = %e,
                    "raft-tracker streamer: subscribe_status failed");
            }
        }

        // Back off before retrying.
        let delay = backoff;
        backoff = backoff.saturating_mul(2).min(config.max_backoff);
        let jittered = crate::jitter(delay);
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(jittered) => {}
        }
    }
}
