//! The blocking batch appender that loads snapshot entries.

use std::sync::Arc;
use zslg_log::LogError;

use parking_lot::Mutex as SyncMutex;
use rocksdb_kit::Batch;

use crate::engine::settled_below;
use crate::key::encode_key;
use crate::{Error, STABLE_LOG_CF, metrics};
use crossbeam_combinators::{Fanout, LevelSetter};
use rocksdb_kit::Storage;
use zslg_log::LogIndex;
use zslg_log::ToCodec;
use zslg_log::metrics::LogType;
use zslg_log::{Data, SnapshotEntry, StableLogEvent};
use zslg_time::Hlc;

/// Entries per `WriteBatch` + fsync while loading a snapshot, and the depth of the
/// stream→channel buffer that feeds the blocking append loop. Kept well below
/// `StableLogConfig::appended_broadcast_capacity` so a single batch's `Appended`
/// broadcast doesn't
/// immediately swamp a subscriber. This is a storage/broadcast batching constant,
/// NOT runtime pacing: `append` drains, encodes, commits and fsyncs entirely on a
/// blocking thread (`append_blocking`, CLAUDE.md §8) and the heavy zstd/postcard
/// decode runs off the reactor upstream in `integrate_manifest`
/// (docs/PERFORMANCE.md entry 23), so nothing here touches the runtime worker and
/// no inter-chunk sleep is needed.
pub(crate) const STABLE_LOAD_BATCH: usize = 256;

/// Drains snapshot entries from a channel on one blocking thread, batching
/// into [`STABLE_LOAD_BATCH`]-sized writes with a gapless ingest fence and
/// HLC-monotonicity assertion. Owns its handles — the caller clones Arcs from
/// `StableLog` and passes them in.
///
/// Mirrors `replica/log.rs::flush_loop_blocking`. A partial load is safe and
/// resumable: the frontier/watermark reflect only committed entries and the next
/// `integrate_manifest` re-reads the snapshot, skipping what is already loaded.
/// Shutdown needs no cancel token: the snapshot store cancels its decode worker
/// on shutdown, which drops the sender, so `blocking_recv` returns `None` and
/// the loop exits within ~one in-flight batch.
#[allow(
    clippy::expect_used,
    reason = "cumulative append count bounded by log size, far below usize::MAX"
)]
pub(crate) struct BatchAppender {
    pub(crate) rx: crossbeam_combinators::OrderedChannel<
        Result<SnapshotEntry, Error>,
        crossbeam_combinators::Contiguous<LogIndex>,
        crossbeam_combinators::Finite,
    >,
    pub(crate) storage: Arc<Storage>,
    pub(crate) end_index: Arc<SyncMutex<Option<LogIndex>>>,
    pub(crate) end_hlc: Arc<SyncMutex<Option<Hlc>>>,
    pub(crate) frontier_hlc: Arc<SyncMutex<Option<Hlc>>>,
    pub(crate) events: Arc<Fanout<StableLogEvent>>,
    pub(crate) watermark: Arc<LevelSetter<Option<Hlc>>>,
}

impl BatchAppender {
    #[allow(
        clippy::expect_used,
        reason = "cumulative append count bounded by log size, far below usize::MAX"
    )]
    pub(crate) fn run(mut self) -> Result<usize, Error> {
        let mut total = 0usize;
        let mut frontier = *self.end_index.lock();
        let mut frontier_entry_hlc = *self.frontier_hlc.lock();
        let mut watermark = *self.end_hlc.lock();

        loop {
            // An aborted input is a partial load: refuse it rather than report
            // a total that looks complete. `integrate_manifest` re-checks the
            // loaded end against the manifest — belt and suspenders, per
            // CLAUDE.md §20.
            let batch = match self.rx.ready_batch(STABLE_LOAD_BATCH) {
                Ok(batch) => batch,
                Err(crossbeam_combinators::End::Complete) => return Ok(total),
                Err(crossbeam_combinators::End::Aborted { reason }) => {
                    return Err(Error::Log(LogError::StreamTruncated {
                        what: reason.label().to_owned(),
                    }));
                }
            };
            let _probe = metrics::stable_log_commit_probe();
            let prev_watermark = watermark;
            let mut committed: Vec<(LogIndex, Data)> = Vec::new();

            // A terminal error is held until the contiguous prefix already in
            // this batch has been committed. `ready_batch` boundaries are
            // arbitrary — whether an error lands beside the entries before it or
            // in the next batch is a producer/consumer race — so returning early
            // would make *how much progress the load makes* depend on that race.
            // Committing the prefix first is what makes the documented
            // "resumable: committed batches persist and the next attempt
            // re-streams from the new end" actually hold.
            let mut pending_err: Option<Error> = None;

            for res in batch {
                let entry = match res {
                    Ok(entry) => entry,
                    Err(e) => {
                        pending_err = Some(e);
                        break;
                    }
                };
                let idx = entry.global_index;
                debug_assert_eq!(
                    idx,
                    LogIndex::next_after(frontier),
                    "contiguous loader yielded a non-successor"
                );
                let hlc = entry.data.data_id.hlc();
                if let Some(prev) = watermark.max(frontier_entry_hlc)
                    && hlc < prev
                {
                    pending_err = Some(Error::Log(LogError::StableLogGap(format!(
                        "snapshot stream must be HLC-sorted: got {hlc:?} after {prev:?}"
                    ))));
                    break;
                }
                // Settled only strictly below this entry: several entries can
                // share an HLC and a batch boundary may fall inside the group,
                // so `hlc` itself waits for `record_end_hlc` (see `StableLog`).
                watermark = watermark.max(settled_below(hlc));
                frontier = Some(idx);
                frontier_entry_hlc = Some(hlc);
                committed.push((entry.global_index, entry.data));
            }

            let n = committed.len();
            if n == 0 {
                if let Some(e) = pending_err {
                    return Err(e);
                }
                continue;
            }

            // Open the batch here, stage the whole drained window into it, then
            // commit and fence — all on this one blocking thread, with nothing
            // between the fence and the post-commit work below (CLAUDE.md §8).
            // No global write lock: batches commit atomically and are ordered at
            // the WAL, so opening one never blocks on another writer.
            let cf = self.storage.cf(STABLE_LOG_CF)?;
            let mut batch = Batch::new();
            for (idx, data) in &committed {
                let bytes = postcard::to_allocvec(&data).to_codec("encode stable entry")?;
                batch.put(&cf, encode_key(*idx, data.data_id.hlc()), &bytes);
            }
            drop(cf);
            self.storage.write(batch)?; // atomic across the batch
            self.storage.fence()?; // durable — the §8 commit fence

            // Post-fsync: mirror frontier/watermark into shared mutexes and
            // broadcast — entries first, then any `Watermark` that now covers them.
            let new_watermark = watermark.filter(|&wm| prev_watermark.is_none_or(|prev| wm > prev));
            if let Some(f) = frontier {
                *self.end_index.lock() = Some(f);
                zslg_log::metrics::log_index_frontier(LogType::Stable, f.raw());
            }
            if frontier_entry_hlc.is_some() {
                *self.frontier_hlc.lock() = frontier_entry_hlc;
            }
            zslg_log::metrics::log_writes(LogType::Stable, n);
            // `Stopped` only while the log is being dropped: no subscriber is
            // left to miss these.
            for (index, data) in committed {
                let _ = self
                    .events
                    .publish(StableLogEvent::Appended { index, data });
            }
            if let Some(wm) = new_watermark {
                *self.end_hlc.lock() = Some(wm);
                let _ = self.events.publish(StableLogEvent::Watermark(wm));
                self.watermark.set(Some(wm));
                metrics::stable_log_end_hlc_physical_ms(wm);
                metrics::stable_log_end_advanced();
            }

            total = total.checked_add(n).expect("append total fits usize");

            // Prefix is durable; now surface the error that ended the batch.
            if let Some(e) = pending_err {
                return Err(e);
            }
        }
    }
}
