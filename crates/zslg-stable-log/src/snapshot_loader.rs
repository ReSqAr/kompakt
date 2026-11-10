//! Loads object-store snapshots (referenced by the raft manifest) into the
//! learner's `stable_log`. Consumed by `spawn_learner` — a learner concern, not
//! a leader one (snapshot *creation* lives in `crate::leader::snapshot`).
//!
//! Snapshot *selection* lives entirely behind [`StableArchiveReader`]: this
//! loader only names an index window (`from` the current stable end, unbounded
//! above) and drains the one resulting stream into the stable log. It keeps the
//! manifest solely for its own bookkeeping — the completeness check and the
//! watermark advance — never to pick snapshots.

use std::sync::Arc;
use zslg_consensus::RaftStateView;
use zslg_log::LogError;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::Error;
use zslg_consensus::RaftManifestState;
use zslg_log::LogIndex;
use zslg_log::{ArchiveBounds, ArchiveUpperBound, StableArchiveReader, StableLogSnapshot};

pub async fn integrate_manifest(
    archive: &dyn StableArchiveReader,
    stable_log: &dyn StableLogSnapshot,
    manifest_state: &RaftManifestState,
) -> Result<(), Error> {
    // Stream from just past the current stable end — or the earliest snapshot
    // when the log is empty. Snapshots below the lower bound are never opened.
    // `append` is the single gap fence: it runs the encode/commit/fsync loop on
    // its own blocking thread (CLAUDE.md §8), commits contiguous batches, and
    // refuses an input that jumps past `end + 1`, so a missing or partial
    // snapshot cannot land as a durable gap (docs/STABLE_LOG_GAP_FIX.md).
    // Debug, not info: this runs on every raft-state change, which on an idle
    // cluster is every cutoff tick.
    debug!(
        manifest_entries = manifest_state.manifest.len(),
        "integrating stable-log manifest from object store",
    );
    let rx = archive.stream(ArchiveBounds {
        after: stable_log.end_index(),
        until: ArchiveUpperBound::Unbounded,
    });
    stable_log.append(rx.into_channel()).await?;

    // Completeness (belt to `append`'s gap fence): `append` returning Ok proves
    // the stream ended, not that it covered the manifest. A snapshot that
    // short-loaded its tail (e.g. a stream ending early without an error item)
    // leaves the stable end below the manifest's covered end — abort so the
    // watermark advance below never claims coverage past a never-loaded tail.
    // The load stays resumable: committed batches persist and the next attempt
    // re-streams from the new end.
    let manifest_end = manifest_state.manifest.iter().map(|m| m.end_index).max();
    if let Some(target) = manifest_end {
        let loaded = stable_log.end_index();
        if loaded.is_none_or(|end| end < target) {
            return Err(Error::Log(LogError::Storage(format!(
                "stable-log manifest integration incomplete: loaded end_index {:?} < manifest \
                 end_index {} — a snapshot stream ended early or a snapshot is missing; \
                 aborting manifest integration (resumable on the next attempt)",
                loaded.map(LogIndex::raw),
                target.raw(),
            ))));
        }
    }

    if let Some(manifest_wm) = manifest_state.manifest_watermark
        && stable_log.end_hlc().is_none_or(|l| manifest_wm > l)
    {
        // After integrating a manifest, `end_hlc` always becomes its
        // watermark — even a watermark-only advance with no snapshot files. The
        // loaded index is just `end_index`, already advanced by the appends
        // above. This keeps the load path mechanical (no empty-manifest special
        // case) so downstream waiters can simply track `end_hlc`.
        stable_log.record_end_hlc(manifest_wm).await?;
    }

    Ok(())
}

pub struct SnapshotLoader {
    archive: Arc<dyn StableArchiveReader>,
    stable_log: Arc<dyn StableLogSnapshot>,
    raft_state: Arc<dyn RaftStateView>,
    cancel: CancellationToken,
}

impl SnapshotLoader {
    #[must_use]
    pub fn new(
        archive: Arc<dyn StableArchiveReader>,
        stable_log: Arc<dyn StableLogSnapshot>,
        raft_state: Arc<dyn RaftStateView>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            archive,
            stable_log,
            raft_state,
            cancel,
        }
    }

    /// Load the current manifest once, awaited at startup *before* the live
    /// subscription starts (`spawn_learner`). This establishes `end_hlc` so the
    /// local-store integrate gate skips the whole already-stable prefix instead
    /// of writing then purging it. Best-effort: `maybe_integrate` logs-and-
    /// continues on error, so a transient object-store failure just means the
    /// gate skips less and [`run`](Self::run) retries on the next manifest change.
    pub async fn load_initial(&self) {
        let initial = self.raft_state.snapshot();
        info!(
            manifest_entries = initial.manifest_state.manifest.len(),
            "snapshot loader: initial load before subscription",
        );
        self.maybe_integrate(&initial.manifest_state).await;
    }

    pub async fn run(self) -> Result<(), Error> {
        let mut rx = self.raft_state.subscribe();

        // Re-integrate the current state after subscribing to close the gap
        // between `load_initial` (at startup) and this subscription — idempotent
        // (already-loaded snapshots are skipped, `end_hlc` is monotonic).
        let initial = self.raft_state.snapshot();
        info!(
            manifest_entries = initial.manifest_state.manifest.len(),
            "snapshot loader: starting with initial manifest",
        );
        self.maybe_integrate(&initial.manifest_state).await;

        loop {
            let state = tokio::select! {
                () = self.cancel.cancelled() => {
                    info!("snapshot loader: shutdown");
                    return Ok(());
                }
                changed = rx.changed() => match changed {
                    Ok(()) => rx.borrow_and_update().clone(),
                    Err(_) => {
                        info!("snapshot loader: raft state channel closed");
                        return Ok(());
                    }
                },
            };
            self.maybe_integrate(&state.manifest_state).await;
        }
    }

    async fn maybe_integrate(&self, manifest_state: &RaftManifestState) {
        if let Err(e) = integrate_manifest(
            self.archive.as_ref(),
            self.stable_log.as_ref(),
            manifest_state,
        )
        .await
        {
            warn!(error = %e, "snapshot load failed");
        }
    }
}
