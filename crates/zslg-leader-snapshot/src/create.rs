//! Building one snapshot object from an open window.
//!
//! ```text
//!   await_quorum_ready(cutoff)   replicas have drained past it
//!   scan (watermark, cutoff]     DataId order == HLC order, so no sort buffer
//!   write object                 streamed, off-reactor encode
//!   CommitSnapshot intent        manifest entry + new watermark
//! ```
//!
//! `max_snapshot_entries` caps a build, and the cap is a **floor**:
//! `take_at_least` runs to the end of the HLC group it lands in, because the
//! next chunk resumes at an exclusive bound and would otherwise skip the rest of
//! that HLC. The watermark is chosen from what was written:
//!
//! ```text
//!   written < cap   the scan ran out first         watermark = cutoff
//!   written ≥ cap   maybe stopped by the cap        watermark = last written HLC
//! ```
//!
//! The second rule is conservative: a window that ends exactly at the cap costs
//! one more iteration, which finds `(hlc, cutoff]` empty and commits `cutoff`.
//!
//! Ordering is crash-safety-critical: the cutoff appears in both the opening
//! proposal and the committing one, so a crash between them is safe.

use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};
use zslg_consensus::ConsensusError;
use zslg_consensus::RaftStateView;
use zslg_log::LogError;
use zslg_net::Router;
use zslg_snapshot::SnapshotStore;

use crate::Error;
use crate::metrics::{self, SnapshotState, SnapshotStep};
use crossbeam_combinators::{Channel, Finite};
use zslg_log::LogIndex;
use zslg_log::SnapshotEntry;
use zslg_log::ToStorage;
use zslg_log::{LocalStoreView, StableLogSnapshot};
use zslg_quorum_read::ReplicaPoller;
use zslg_quorum_read::{FanOutResponse, fan_out_quorum};
use zslg_snapshot::CompressionLevel;
use zslg_snapshot::SnapshotId;
use zslg_snapshot::SnapshotMetadata;
use zslg_time::Hlc;

pub struct SnapshotEntryInputs<'a> {
    pub local_store: &'a dyn LocalStoreView,
    pub stable_log: &'a dyn StableLogSnapshot,
    pub snapshot_store: &'a dyn SnapshotStore,
    pub raft_state: &'a dyn RaftStateView,
    pub compression: CompressionLevel,
    /// Soft cap on entries in this chunk — see [`crate::SnapshotJobConfig::max_snapshot_entries`].
    pub max_snapshot_entries: std::num::NonZeroUsize,
}

/// A described change, not a new whole state: the entry plus the manifest it
/// was computed against. The integrator re-checks that premise, so a build that
/// raced is rejected rather than rebased onto indices it no longer continues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotResult {
    Committed {
        /// Absent when the window held no entries: the watermark advances and
        /// no object is written.
        object: Option<WrittenObject>,
        entries: usize,
        new_watermark: Hlc,
        /// Premise: the committed watermark this window started above.
        prev_watermark: Option<Hlc>,
        /// Premise: the manifest end index this build's global indices continue
        /// from.
        prev_end_index: Option<LogIndex>,
    },
    Skipped,
}

/// What a build actually wrote. The three facts arrive together or not at all,
/// so an entry without its object id is unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenObject {
    pub snapshot_id: SnapshotId,
    pub entry: SnapshotMetadata,
    pub end_index: LogIndex,
}

impl SnapshotResult {
    /// The intent that publishes this build, if there is anything to publish.
    #[must_use]
    pub fn commit_intent(&self) -> Option<zslg_consensus::RaftIntent> {
        match self {
            Self::Skipped => None,
            Self::Committed {
                object,
                new_watermark,
                prev_watermark,
                prev_end_index,
                ..
            } => Some(zslg_consensus::RaftIntent::CommitSnapshot {
                entry: object.as_ref().map(|o| o.entry.clone()),
                new_watermark: *new_watermark,
                prev_watermark: *prev_watermark,
                prev_end_index: *prev_end_index,
            }),
        }
    }
}

pub async fn create_snapshot_entry(
    inputs: &SnapshotEntryInputs<'_>,
    new_cutoff: Hlc,
) -> Result<SnapshotResult, Error> {
    let prev_state = inputs.raft_state.snapshot();
    let lower_bound = prev_state.manifest_state.manifest_watermark;

    if lower_bound.is_some_and(|mw| new_cutoff <= mw) {
        return Ok(SnapshotResult::Skipped);
    }

    let last_manifest_end = prev_state
        .manifest_state
        .manifest
        .last()
        .map(|m| m.end_index);
    let stable_end_index = inputs.stable_log.end_index();

    let next_index_start = match stable_end_index {
        None => 0u64,
        Some(f) => f
            .raw()
            .checked_add(1)
            .ok_or_else(|| Error::Log(LogError::Storage("global index overflow".into())))?,
    };

    let snapshot_id = SnapshotId::open(LogIndex(next_index_start));
    let cap = inputs.max_snapshot_entries;

    // Channel built here so `local_store` need not be 'static.
    let window = inputs
        .local_store
        .stream_window(lower_bound, new_cutoff)
        .into_channel()
        .take_at_least::<Hlc>(cap);

    type PeekOk = Option<(Channel<Result<SnapshotEntry, Error>, Finite>, Hlc)>;

    // The peek: it runs before the upload starts, so an empty window creates no
    // object, and it is where a lagging stable log is caught.
    let peek_result: PeekOk = tokio::task::spawn_blocking(move || -> Result<PeekOk, Error> {
        let mut window = window;
        let first = match window.recv() {
            Ok(Ok(d)) => d,
            Ok(Err(e)) => return Err(e.into()),
            Err(crossbeam_combinators::End::Complete) => return Ok(None),
            Err(crossbeam_combinators::End::Aborted { reason }) => {
                return Err(Error::Log(LogError::StreamTruncated {
                    what: reason.label().to_owned(),
                }));
            }
        };

        if last_manifest_end.is_some_and(|last| stable_end_index.is_none_or(|f| f < last)) {
            return Err(Error::Log(LogError::StableLogBehindManifest {
                end_index: stable_end_index,
                manifest_end_index: last_manifest_end.unwrap_or(LogIndex(0)),
            }));
        }

        let start_hlc = first.data_id.hlc();
        let mut next_index = next_index_start;
        let entries = Channel::once(Ok(first)).chain(window).map(move |r| {
            r.map(|data| {
                let global_index = LogIndex(next_index);
                next_index = next_index.saturating_add(1);
                SnapshotEntry { global_index, data }
            })
        });
        Ok(Some((entries.err_into(), start_hlc)))
    })
    .await
    .to_storage("snapshot stream scan panicked")??;
    let Some((entries, start_hlc)) = peek_result else {
        return Ok(SnapshotResult::Committed {
            object: None,
            entries: 0,
            new_watermark: new_cutoff,
            prev_watermark: lower_bound,
            prev_end_index: last_manifest_end,
        });
    };

    let written = inputs
        .snapshot_store
        .write_snapshot(
            snapshot_id,
            entries.err_into(),
            inputs.compression,
            zslg_snapshot::ObjectStoreReason::SnapshotCreate,
        )
        .await?;
    let entry_count = written.count;
    let Some(last) = written.last else {
        return Err(Error::Log(LogError::Storage(
            "snapshot write reported no last entry after a non-empty peek".into(),
        )));
    };
    let end_hlc = last.hlc;
    let new_watermark = if entry_count < cap.get() {
        new_cutoff
    } else {
        // A capped chunk covers only `(lower_bound, last]`: the watermark names
        // what was written, or the uncovered tail would count as snapshotted.
        end_hlc
    };

    let end_index = last.index;
    let start_index = LogIndex(next_index_start);

    Ok(SnapshotResult::Committed {
        object: Some(WrittenObject {
            snapshot_id,
            end_index,
            entry: SnapshotMetadata {
                start_hlc,
                end_hlc,
                start_index,
                end_index,
                snapshot_id,
            },
        }),
        entries: entry_count,
        new_watermark,
        prev_watermark: lower_bound,
        prev_end_index: last_manifest_end,
    })
}

/// Everything [`build_snapshot`] needs. No replicator and no clock: it proposes
/// nothing and works from a cutoff it is handed.
pub struct SnapshotInputs<'a> {
    pub local_store: Arc<dyn LocalStoreView>,
    pub stable_log: &'a dyn StableLogSnapshot,
    pub snapshot_store: &'a dyn SnapshotStore,
    pub raft_state: &'a dyn RaftStateView,
    pub router: Arc<dyn Router>,
    /// For the re-check after the quorum wait — see [`build_snapshot`].
    pub leadership: Arc<dyn zslg_consensus::RaftLeadership>,
    pub self_node_id: zslg_consensus::NodeId,
    pub quorum_poll_interval: Duration,
    pub quorum_wait_timeout: Duration,
    pub compression: CompressionLevel,
    /// Soft cap on entries in one chunk — see [`crate::SnapshotJobConfig::max_snapshot_entries`].
    pub max_snapshot_entries: std::num::NonZeroUsize,
}

/// Build the snapshot for an already-committed cutoff, proposing nothing.
///
/// Keeping the proposals out (step 1 in `jobs::cutoff`, step 5 in the
/// integrator) is what lets the long waits here run without blocking other
/// leader work — including the membership drain that depends on it (§18).
pub async fn build_snapshot(
    inputs: &SnapshotInputs<'_>,
    cutoff: Hlc,
) -> Result<SnapshotResult, Error> {
    let t_start = tokio::time::Instant::now();

    if inputs
        .raft_state
        .snapshot()
        .manifest_state
        .manifest_watermark
        .is_some_and(|wm| cutoff <= wm)
    {
        metrics::snapshot_creation(SnapshotState::NoopEmptyWindow);
        return Ok(SnapshotResult::Skipped);
    }

    await_quorum_ready(inputs, cutoff).await?;
    let t_quorum = t_start.elapsed();

    // Not a safety check — the integrator and apply fence would refuse the
    // commit anyway. This just avoids an upload nobody will reference.
    if !inputs.leadership.is_leader(inputs.self_node_id) {
        debug!(
            ?cutoff,
            "demoted during the quorum wait; abandoning the build"
        );
        return Ok(SnapshotResult::Skipped);
    }

    let entry_inputs = SnapshotEntryInputs {
        local_store: inputs.local_store.as_ref(),
        stable_log: inputs.stable_log,
        snapshot_store: inputs.snapshot_store,
        raft_state: inputs.raft_state,
        compression: inputs.compression,
        max_snapshot_entries: inputs.max_snapshot_entries,
    };
    let result = create_snapshot_entry(&entry_inputs, cutoff).await?;
    let t_total = t_start.elapsed();
    let t_entry_step = t_total.saturating_sub(t_quorum);

    tracing::debug!(
        quorum_ms = t_quorum.as_millis(),
        entry_ms = t_entry_step.as_millis(),
        total_ms = t_total.as_millis(),
        "snapshot build timing",
    );

    metrics::snapshot_creation_step(SnapshotStep::AwaitQuorum, t_quorum);
    metrics::snapshot_creation_step(SnapshotStep::WriteObject, t_entry_step);
    metrics::snapshot_creation_step(SnapshotStep::Total, t_total);

    match &result {
        SnapshotResult::Committed { entries, .. } => {
            metrics::snapshot_creation(SnapshotState::Success);
            metrics::snapshot_entries(*entries);
        }
        SnapshotResult::Skipped => {
            metrics::snapshot_creation(SnapshotState::NoopEmptyWindow);
        }
    }

    Ok(result)
}

/// Poll one replica until it reports having drained `cutoff`.
///
/// Unbounded on purpose — `await_quorum_ready` owns the deadline, and a quorum
/// only needs *some* replicas to answer, so a peer that never does must not cap
/// how long the others get.
async fn await_cutoff_drained(
    transport: &Arc<dyn zslg_net::ReplicaClient>,
    peer: zslg_consensus::NodeId,
    cutoff: Hlc,
    poll: Duration,
) {
    let mut delay = poll;
    loop {
        match transport.fetch_status().await.map(|s| s.effective_cutoff) {
            Ok(Some(c)) if c >= cutoff => return,
            Ok(_) => debug!(?peer, ?cutoff, "cutoff not yet drained"),
            Err(e) => debug!(?peer, error = %e, "fetch_status probe failed"),
        }
        tokio::time::sleep(delay).await;
        delay = delay.saturating_mul(2).min(poll.saturating_mul(16));
    }
}

async fn await_quorum_ready(inputs: &SnapshotInputs<'_>, new_cutoff: Hlc) -> Result<(), Error> {
    let current_state = (*inputs.raft_state.snapshot()).clone();
    let router = inputs.router.clone();
    let local_store = inputs.local_store.clone();
    let cutoff = new_cutoff;
    let poll = inputs.quorum_poll_interval;

    let quorum_result = tokio::time::timeout(
        inputs.quorum_wait_timeout,
        fan_out_quorum(current_state, move |peer| {
            let transport = router.remote(peer);
            let local_store = local_store.clone();
            async move {
                await_cutoff_drained(&transport, peer, cutoff, poll).await;
                match ReplicaPoller::check(transport, local_store, peer).await {
                    Ok(Some(state)) => Ok(FanOutResponse::Ready(state)),
                    Ok(None) => Ok(FanOutResponse::Failed),
                    Err(e) => Err(e.into()),
                }
            }
        }),
    )
    .await;

    match quorum_result {
        Ok(Ok((_state, _successful))) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_elapsed) => {
            warn!(
                timeout = ?inputs.quorum_wait_timeout,
                "replica quorum not reached within timeout",
            );
            Err(Error::Consensus(ConsensusError::QuorumUnreachable))
        }
    }
}
