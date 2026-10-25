//! The learner's stable prefix: the [`StableLog`] storage plus the
//! [`snapshot_loader`] that populates it from object-store snapshots.
//!
//! Storage is a RocksDB column family: primary = the gapless global
//! `LogIndex`, secondary = the entry's creation HLC. With TTL purge disabled
//! (the default) no retire threshold is ever armed, so the log never retires (it
//! is the CQRS replay source); with purge on, an HLC-keyed threshold retires an
//! old index prefix, served just-in-time from object store on read
//! ([`crate::learner::stable_archive`]). The watermark (`end_hlc`) is purely
//! in-memory: recovered from the log's own tail at open (just below the
//! frontier entry's HLC) and re-raised by the first manifest event after
//! startup, so it can never claim more coverage than the entries this node
//! actually holds.

mod purge;
mod snapshot_loader;

/// Public configuration of the stable log: where it lives, how its runs are
/// stored, and how deep the append broadcast is. Arrivals are index-ordered
/// (time-like), so the log grows without retirement by default — it is the CQRS
/// replay source.
#[derive(Debug, Clone)]
pub(crate) struct StableLogConfig {
    /// Depth of each stable-log subscriber's own event queue — how far behind
    /// a consumer may fall before the fan-out hangs up on it and it has to
    /// replay from its cursor (`learner::fanout`).
    ///
    /// Per subscriber, not shared: an `Appended` carries the entry `Data`, so
    /// this bounds ~capacity x payload x live subscribers. A latency/memory
    /// trade, never a correctness one — a hung-up consumer re-reads what it
    /// missed from the durable log, so a shallow queue costs re-scans
    /// (`zslg_stable_read_resyncs_total`), not data.
    pub(crate) subscriber_queue_capacity: std::num::NonZeroUsize,
}

impl StableLogConfig {
    /// Default depth of each subscriber's queue — see the field.
    pub(crate) const DEFAULT_SUBSCRIBER_QUEUE_CAPACITY: std::num::NonZeroUsize =
        crate::unit::nonzero(256);

    /// Default sizing. Test-only — production builds the config from the
    /// owning node config's flat fields. The store's location and compression
    /// live in the node type's `COLUMN_FAMILIES`: it is a column family of the
    /// node's one DB.
    #[cfg(test)]
    #[must_use]
    pub(in crate::learner) fn for_test() -> Self {
        Self {
            subscriber_queue_capacity: Self::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        // A single load batch fans out one `Appended` per entry. A queue
        // shallower than the batch is overrun by one batch on its own, so every
        // subscriber resyncs for reasons unrelated to how fast it drains.
        // Stated in prose next to STABLE_LOAD_BATCH since it was introduced;
        // executable here.
        if self.subscriber_queue_capacity.get() < STABLE_LOAD_BATCH {
            return Err(Error::InvalidConfiguration(format!(
                "stable_log_subscriber_queue_capacity ({}) must be >= the load \
                 batch ({STABLE_LOAD_BATCH}); a shallower queue is overrun by one batch",
                self.subscriber_queue_capacity,
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod config_tests {
    use super::{STABLE_LOAD_BATCH, StableLogConfig};

    #[test]
    fn subscriber_queue_shallower_than_a_load_batch_is_rejected() {
        let mut cfg = StableLogConfig::for_test();
        cfg.subscriber_queue_capacity = crate::unit::nonzero(STABLE_LOAD_BATCH - 1);
        let err = cfg
            .validate()
            .expect_err("one batch would overrun the queue");
        assert!(
            format!("{err}").contains("subscriber_queue_capacity"),
            "the error must name the knob to change: {err}",
        );
    }

    #[test]
    fn the_default_broadcast_depth_clears_the_rule() {
        StableLogConfig::for_test()
            .validate()
            .expect("defaults validate");
    }
}

// Consumed only by `spawn_learner`.
pub(in crate::learner) use purge::run_stable_purge_worker;
pub(in crate::learner) use snapshot_loader::SnapshotLoader;
// Re-exported for leader-side snapshot round-trip tests; production callers live
// inside `snapshot_loader` and call `integrate_manifest` directly through the
// shared `StableArchiveReader`.
#[cfg(test)]
pub(crate) use snapshot_loader::integrate_manifest_for_test;

use std::sync::Arc;

use parking_lot::Mutex as SyncMutex;
use rust_rocksdb::WriteBatch;
use tokio::sync::{Mutex as AsyncMutex, watch};

use crate::Error;
use crate::channel::Channels;
use crate::database::Subsystem;
use crate::database::storage::Storage;
use crate::error::{ToCodec, ToStorage};
use crate::hlc::Hlc;
use crate::ids::LogIndex;
use crate::learner::fanout::{Fanout, Wakes};
use crate::meta::{Data, SnapshotEntry, StableLogEvent};
use crate::metrics::{self, LogType, OpState};

// ── key codec ───────────────────────────────────────────────────────────────
//
// `key = index ‖ hlc`: the primary is the gapless global [`LogIndex`], the
// secondary the entry's creation HLC (`data_id.hlc()`), which drives the
// optional TTL retirement (`purge.rs`). Because arrivals are index-ordered and
// index order == HLC order here (snapshot windows are keyed `(hlc, data_id)`
// and disjoint above the prior watermark), retiring by HLC secondary retires
// exactly an index prefix — see [`StableLog::set_purge_threshold`]. When purge
// is disabled (the default) no threshold is ever armed, so the log never
// retires.
//
// Both halves are big-endian, so the byte ordering IS the tuple ordering —
// load-bearing, because retirement is a `DeleteRange` over encoded-byte
// bounds (`stable_key_order_matches_tuple_order`): if the byte order were not
// the tuple order, `set_purge_threshold` would drop the wrong entries.
// Fixed width, so [`decode_key`] splits at a constant.
//
// Folding the secondary into the key is lossless because no store here ever
// holds two live versions of one primary with differing secondaries — see
// docs/ROCKSDB_MIGRATION.md §1.2.

const PRIMARY_WIDTH: usize = 8;
const SECONDARY_WIDTH: usize = 8;
const KEY_WIDTH: usize = PRIMARY_WIDTH + SECONDARY_WIDTH;

#[must_use]
fn encode_key(index: LogIndex, hlc: Hlc) -> [u8; KEY_WIDTH] {
    let mut k = [0u8; KEY_WIDTH];
    k[..PRIMARY_WIDTH].copy_from_slice(&index.raw().to_be_bytes());
    k[PRIMARY_WIDTH..].copy_from_slice(&hlc.raw().to_be_bytes());
    k
}

#[must_use]
fn encode_primary(index: LogIndex) -> [u8; PRIMARY_WIDTH] {
    index.raw().to_be_bytes()
}

/// The key's secondary half, encoded alone. No production caller needs this
/// any more — purge now walks and range-deletes rather than comparing an
/// armed threshold against this encoding — but the key-order tests still
/// build one to check ordering byte-for-byte.
#[cfg(test)]
#[must_use]
fn encode_secondary(hlc: Hlc) -> [u8; SECONDARY_WIDTH] {
    hlc.raw().to_be_bytes()
}

#[must_use]
fn decode_key(bytes: &[u8]) -> Option<(LogIndex, Hlc)> {
    let index = u64::from_be_bytes(bytes.get(..PRIMARY_WIDTH)?.try_into().ok()?);
    let hlc = u64::from_be_bytes(bytes.get(PRIMARY_WIDTH..KEY_WIDTH)?.try_into().ok()?);
    Some((LogIndex(index), Hlc(hlc)))
}

fn undecodable(key: &[u8]) -> Error {
    Error::Storage(format!(
        "undecodable stable log key ({} bytes): {key:02x?}",
        key.len()
    ))
}

/// A live key whose value is missing. Unreachable through RocksDB's iterator
/// contract (`value()` is `None` only when `key()` is); erroring rather than
/// substituting an empty payload keeps the durability layer from serving
/// garbage silently (§20).
fn missing_value(key: &[u8]) -> Error {
    Error::Storage(format!(
        "stable log key with no value ({} bytes): {key:02x?}",
        key.len()
    ))
}

// ── storage access ──────────────────────────────────────────────────────────
//
// The facade's `max_key` / `scan_range` / `get` / `maintain`, as local
// functions over this store's [`Storage`]. Purge is a durable `DeleteRange`
// (see [`StableLog::set_purge_threshold`]), so a purged prefix is genuinely
// absent — no read-side clamp to consult here any more.

/// The newest live entry — the facade's `max_key`, plus the secondary the same
/// key already carries.
fn newest_live_entry(storage: &Storage) -> Result<Option<(LogIndex, Hlc)>, Error> {
    let cf = storage.cf(Subsystem::StableLog)?;
    let mut it = storage.db().raw_iterator_cf(&cf);
    it.seek_to_last();
    if let Some(key) = it.key() {
        // PINNING RULE: `key` borrows the iterator's current block, which stays
        // pinned in the block cache for as long as the borrow lives. It must not
        // cross an `.await` or a batch commit — decode it here and let the borrow
        // end before the iterator moves.
        return decode_key(key).map(Some).ok_or_else(|| undecodable(key));
    }
    it.status()
        .map_err(|e| Error::Storage(format!("rocksdb reverse scan (stable log): {e}")))?;
    Ok(None)
}

/// The watermark an entry at `hlc` settles: everything strictly below it.
///
/// The log is gapless and HLC-ordered by index, so once an entry at `hlc` is
/// present every smaller HLC's entries are too — but not necessarily every
/// entry *at* `hlc`, which may straddle a load batch or an interrupted load.
/// `None` only below the first representable HLC.
fn settled_below(hlc: Hlc) -> Option<Hlc> {
    hlc.raw().checked_sub(1).map(Hlc)
}

/// The exclusive index bound of `hlc <= threshold`.
///
/// Index order == HLC order in the stable log (see the module header), so
/// those entries are a key-order prefix — but the key is indexed by `index`,
/// so the boundary cannot be computed from `threshold` alone. Range
/// tombstones are honoured by iterators, so `seek_to_first` lands on the
/// first *live* key and this walk costs O(newly covered), not
/// O(everything ever purged).
fn purge_boundary(storage: &Storage, threshold: Hlc) -> Result<Option<LogIndex>, Error> {
    let cf = storage.cf(Subsystem::StableLog)?;
    let mut it = storage.db().raw_iterator_cf(&cf);
    it.seek_to_first();
    let mut boundary = None;
    while let Some(key) = it.key() {
        // PINNING RULE: `key` borrows the iterator's current block and pins
        // it. Decode here; let the borrow end before the iterator moves.
        let (index, hlc) = decode_key(key).ok_or_else(|| undecodable(key))?;
        if hlc > threshold {
            break;
        }
        boundary = Some(LogIndex::next_after(Some(index)));
        it.next();
    }
    it.status()
        .map_err(|e| Error::Storage(format!("rocksdb purge-boundary scan (stable log): {e}")))?;
    Ok(boundary)
}

/// What a range scan feeds. The payload is borrowed from the iterator's block
/// for the duration of the call only — a sink that keeps it must copy.
type EntrySink<'a> = dyn FnMut(LogIndex, &[u8]) -> bool + 'a;

/// Index-ordered scan of `[from, until)`. `sink` returning `false` stops it.
fn for_each_entry(
    storage: &Storage,
    from: LogIndex,
    until: LogIndex,
    sink: &mut EntrySink<'_>,
) -> Result<(), Error> {
    let cf = storage.cf(Subsystem::StableLog)?;
    let mut it = storage.db().raw_iterator_cf(&cf);
    it.seek(encode_primary(from));
    while let Some(key) = it.key() {
        // PINNING RULE: `key` and `value` below both borrow the iterator's
        // current block and pin it. Neither may cross an `.await` or a batch
        // commit; the sink copies what it wants to keep.
        let (index, _hlc) = decode_key(key).ok_or_else(|| undecodable(key))?;
        if index >= until {
            break;
        }
        let Some(value) = it.value() else {
            return Err(missing_value(key));
        };
        if !sink(index, value) {
            return Ok(());
        }
        it.next();
    }
    it.status()
        .map_err(|e| Error::Storage(format!("rocksdb scan (stable log): {e}")))
}

/// Is `index` live — present and not covered by the armed retire threshold?
///
/// A point `get` is impossible: the key carries the secondary, which the caller
/// does not know, so this seeks the primary prefix instead.
#[cfg(test)]
fn index_is_live(storage: &Storage, index: LogIndex) -> Result<bool, Error> {
    let mut found = false;
    // saturating: `index + 1` is the exclusive end of a one-index window. At
    // `u64::MAX` the window collapses to empty, which is the right answer for a
    // log that cannot hold an index above it anyway.
    for_each_entry(
        storage,
        index,
        LogIndex(index.raw().saturating_add(1)),
        &mut |_idx, _payload| {
            found = true;
            false
        },
    )?;
    Ok(found)
}

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
const STABLE_LOAD_BATCH: usize = 256;

/// `end_hlc` is the single stable watermark: the HLC up to which this node's
/// stable prefix is settled/loaded — every entry at or below it is present.
/// It is advanced (monotonically, under `write_lock`) by `append` (to just
/// below the last appended HLC: several entries may share an HLC, and a batch
/// boundary may fall inside the group) and by `record_end_hlc` (to
/// the manifest watermark, which settles the last group and can sit above the
/// last entry's HLC via empty windows). So mid-load it may sit *below* the
/// tail entry's HLC — an entry then shows in both halves of a blended read,
/// which dedup by `data_id` absorbs; the reverse, claiming an HLC whose group
/// is still partly unloaded, would hide the rest from both halves. It is
/// IN-MEMORY ONLY: at open it is derived from the recovered log tail the same
/// way, so after a restart it never claims more coverage than the entries
/// actually present — the first manifest event re-raises it to the current
/// watermark. Readers blend `stable_tail` with `unstable_tail` at this
/// boundary, so data below it disappears from the unstable view by the next
/// snapshot — that is what makes nodes converge.
pub(crate) struct StableLog {
    storage: Arc<Storage>,
    write_lock: AsyncMutex<()>,
    end_index: Arc<SyncMutex<Option<LogIndex>>>,
    end_hlc: Arc<SyncMutex<Option<Hlc>>>,
    /// The frontier *entry's* creation HLC — distinct from `end_hlc`, which can
    /// float above it via empty-window `record_end_hlc`, and sits below it
    /// while the frontier's HLC group may be incomplete. This is the purge
    /// clamp's cap (`set_purge_threshold` keeps the threshold strictly below it,
    /// so the frontier entry — the recovery anchor for `max_key`/`end_index` —
    /// is never retired).
    frontier_hlc: Arc<SyncMutex<Option<Hlc>>>,
    /// Data plane: entries and watermarks, per-subscriber, on crossbeam.
    events: Arc<Fanout<StableLogEvent>>,
    /// Control plane: the current watermark, level-triggered. Consumers that
    /// only want to know "has the watermark reached X" (`learner::quorum`) or
    /// "did it move" (`local_store::purge`) read this instead of the event
    /// fan-out — no payload crosses it, and a watch cannot lag.
    watermark: watch::Sender<Option<Hlc>>,
    /// The same level for blocking consumers: a crossbeam wake-up on each
    /// move, merged into a data pipeline (`learner::read`'s unstable tail).
    watermark_wakes: Arc<Wakes>,
}

impl StableLog {
    /// Open the stable log on the node's [`Storage`], with full crash
    /// recovery. `end_index`/`end_hlc` come from the recovered tail — the max
    /// stored index (exact; this log never deletes) and just below its entry's
    /// HLC — local reality, nothing more. Production callers go through
    /// [`spawn_stable_store`].
    pub(in crate::learner) async fn open(
        config: &StableLogConfig,
        storage: Arc<Storage>,
    ) -> Result<Self, Error> {
        Self::open_sized(storage, config.subscriber_queue_capacity).await
    }

    /// Open on an existing [`Storage`] at the default broadcast depth, for the
    /// tests that share one DB between this log and the local store.
    #[cfg(test)]
    pub(crate) async fn open_on(storage: Arc<Storage>) -> Result<Self, Error> {
        Self::open_sized(storage, StableLogConfig::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY).await
    }

    /// Open a store of this log's own at `dir` — the directory-shaped entry
    /// point the test constructors use. Production goes through
    /// [`open`](Self::open) with the node's one [`Storage`].
    #[cfg(test)]
    pub(crate) async fn open_engine(stable_dir: std::path::PathBuf) -> Result<Self, Error> {
        Self::open_on(Storage::open_for_test(&stable_dir)?).await
    }

    /// The storage boundary: an injected [`Storage`] plus the broadcast depth.
    async fn open_sized(
        storage: Arc<Storage>,
        subscriber_queue_capacity: std::num::NonZeroUsize,
    ) -> Result<Self, Error> {
        // Genuine corruption is fatal here too: although every byte is
        // re-derivable from object-store snapshots, a corrupt stable log is
        // a bug, and silently discarding would hide it (CLAUDE.md §8).
        //
        // Tail recovery needs no value read: the newest stored key is the
        // frontier entry (this log never deletes) and the key itself carries
        // both halves — the index and the creation HLC the purge clamp needs.
        // No retire threshold is armed yet at open, so the read cannot be
        // clamped away.
        let storage_cl = Arc::clone(&storage);
        let recovered = tokio::task::spawn_blocking(move || newest_live_entry(&storage_cl))
            .await
            .to_storage("stable log frontier recovery panicked")??;

        let end_index = recovered.map(|(idx, _)| idx);
        let frontier_hlc = recovered.map(|(_, hlc)| hlc);
        // Not the tail's own HLC: a load interrupted inside the last HLC group
        // left only part of it, and nothing on disk says which. Everything
        // strictly below it is present, so that is what is settled; the
        // manifest re-delivered after a restart re-raises it through
        // `record_end_hlc`.
        let end_hlc = frontier_hlc.and_then(settled_below);
        // Republish the frontier gauge from the recovered value (it is
        // otherwise only set on writes — see the replica log's open for the
        // restart-shows-zero artifact this avoids).
        if let Some(idx) = end_index {
            metrics::log_index_frontier(LogType::Stable, idx.raw());
        }

        let events = Arc::new(Fanout::new(
            subscriber_queue_capacity,
            "stable log subscription",
        ));
        let (watermark, _) = watch::channel(end_hlc);

        Ok(Self {
            storage,
            write_lock: AsyncMutex::new(()),
            end_index: Arc::new(SyncMutex::new(end_index)),
            end_hlc: Arc::new(SyncMutex::new(end_hlc)),
            frontier_hlc: Arc::new(SyncMutex::new(frontier_hlc)),
            events,
            watermark,
            watermark_wakes: Arc::new(Wakes::new("stable watermark moves")),
        })
    }
}

/// One-stop node-startup constructor: open the stable log, recovery included.
/// There is no background loop to cancel — RocksDB owns its own compaction
/// threads and stops them when the DB drops.
pub(crate) async fn spawn_stable_store(
    stable_log_cfg: &StableLogConfig,
    storage: Arc<Storage>,
) -> Result<Arc<StableLog>, Error> {
    let stable_log = Arc::new(StableLog::open(stable_log_cfg, storage).await?);
    Ok(stable_log)
}

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
struct BatchAppender {
    rx: Channels<Result<SnapshotEntry, Error>>,
    storage: Arc<Storage>,
    end_index: Arc<SyncMutex<Option<LogIndex>>>,
    end_hlc: Arc<SyncMutex<Option<Hlc>>>,
    frontier_hlc: Arc<SyncMutex<Option<Hlc>>>,
    events: Arc<Fanout<StableLogEvent>>,
    watermark: watch::Sender<Option<Hlc>>,
    watermark_wakes: Arc<Wakes>,
}

impl BatchAppender {
    #[allow(
        clippy::expect_used,
        reason = "cumulative append count bounded by log size, far below usize::MAX"
    )]
    fn run(mut self) -> Result<usize, Error> {
        let mut total = 0usize;
        let mut frontier = *self.end_index.lock();
        let mut frontier_entry_hlc = *self.frontier_hlc.lock();
        let mut watermark = *self.end_hlc.lock();

        loop {
            let Some(batch) = self.rx.ready_batch(STABLE_LOAD_BATCH) else {
                break;
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
                let expected = LogIndex::next_after(frontier);
                if idx > expected {
                    pending_err = Some(Error::StableLogGap(format!(
                        "input stream jumped to index {} but the next appendable index is {} — \
                         refusing to commit a gap",
                        idx.raw(),
                        expected.raw(),
                    )));
                    break;
                }
                if idx == expected {
                    let hlc = entry.data.data_id.hlc();
                    if let Some(prev) = watermark.max(frontier_entry_hlc)
                        && hlc < prev
                    {
                        pending_err = Some(Error::StableLogGap(format!(
                            "snapshot stream must be HLC-sorted: got {hlc:?} after {prev:?}"
                        )));
                        break;
                    }
                    // Settled only strictly below this entry: several entries
                    // can share an HLC and a batch boundary may fall inside the
                    // group, so `hlc` itself waits for `record_end_hlc` (see
                    // `StableLog`).
                    watermark = watermark.max(settled_below(hlc));
                    frontier = Some(idx);
                    frontier_entry_hlc = Some(hlc);
                    committed.push((entry.global_index, entry.data));
                }
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
            let cf = self.storage.cf(Subsystem::StableLog)?;
            let mut batch = WriteBatch::default();
            for (idx, data) in &committed {
                let bytes = postcard::to_allocvec(&data).to_codec("encode stable entry")?;
                batch.put_cf(&cf, encode_key(*idx, data.data_id.hlc()), &bytes);
            }
            drop(cf);
            self.storage.write(batch)?; // atomic across the batch
            self.storage.fence()?; // durable — the §8 commit fence

            // Post-fsync: mirror frontier/watermark into shared mutexes and
            // broadcast — entries first, then any `Watermark` that now covers them.
            let new_watermark = watermark.filter(|&wm| prev_watermark.is_none_or(|prev| wm > prev));
            if let Some(f) = frontier {
                *self.end_index.lock() = Some(f);
                metrics::log_index_frontier(LogType::Stable, f.raw());
            }
            if frontier_entry_hlc.is_some() {
                *self.frontier_hlc.lock() = frontier_entry_hlc;
            }
            metrics::log_writes(LogType::Stable, n);
            for (index, data) in committed {
                self.events.send(&StableLogEvent::Appended { index, data });
            }
            if let Some(wm) = new_watermark {
                *self.end_hlc.lock() = Some(wm);
                self.events.send(&StableLogEvent::Watermark(wm));
                self.watermark.send_replace(Some(wm));
                self.watermark_wakes.wake();
                metrics::stable_log_end_hlc_physical_ms(wm);
                metrics::stable_log_end_advanced();
            }

            total = total.checked_add(n).expect("append total fits usize");

            // Prefix is durable; now surface the error that ended the batch.
            if let Some(e) = pending_err {
                return Err(e);
            }
        }

        // The drain loop ends on disconnect, which by itself says nothing about
        // whether the producer finished. An aborted input is a partial load:
        // refuse it rather than report a total that looks complete.
        // `integrate_manifest` re-checks the loaded end against the manifest —
        // belt and suspenders, per CLAUDE.md §20.
        // Exhaustive on purpose: a new non-`Closed` end must not pass as a
        // complete load by default.
        match self.rx.end_reason() {
            crate::channel::End::Closed => Ok(total),
            crate::channel::End::Aborted { what } => Err(Error::StreamTruncated {
                what: what.to_owned(),
            }),
            crate::channel::End::Cancelled => Err(Error::StreamTruncated {
                what: "cancelled".to_owned(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl crate::api::StableLogSnapshot for StableLog {
    fn end_index(&self) -> Option<LogIndex> {
        *self.end_index.lock()
    }

    fn end_hlc(&self) -> Option<Hlc> {
        *self.end_hlc.lock()
    }

    async fn append(
        &self,
        entries: Channels<Result<SnapshotEntry, Error>>,
    ) -> Result<usize, Error> {
        // The entire drain → encode → commit → fsync → frontier-advance →
        // broadcast loop runs on ONE blocking thread (CLAUDE.md §8; mirrors
        // `replica/log.rs::flush_loop_blocking`). The caller (`integrate_manifest`)
        // decodes the snapshot on its own `spawn_blocking` worker and feeds the
        // decoded entries straight into `entries` — one bounded channel directly
        // between the two blocking threads, no reactor hop on the data path.
        let probe = metrics::stable_log_append_probe();

        // Serialize the frontier/watermark read-modify-write against
        // `record_end_hlc`; held across the blocking task.
        let _g = self.write_lock.lock().await;
        let appender = BatchAppender {
            rx: entries,
            storage: Arc::clone(&self.storage),
            end_index: Arc::clone(&self.end_index),
            end_hlc: Arc::clone(&self.end_hlc),
            frontier_hlc: Arc::clone(&self.frontier_hlc),
            events: Arc::clone(&self.events),
            watermark: self.watermark.clone(),
            watermark_wakes: Arc::clone(&self.watermark_wakes),
        };

        let total = tokio::task::spawn_blocking(move || appender.run())
            .await
            .to_storage("append spawn_blocking panicked")??;

        probe.finish(OpState::Success);
        Ok(total)
    }

    async fn record_end_hlc(&self, end_hlc: Hlc) -> Result<(), Error> {
        // Purely in-memory: the watermark is derived from the log tail at
        // recovery, so an empty-window advance needs no durability — the
        // manifest that carries it is re-delivered after a restart. Serialized
        // against `append` (which holds `write_lock` across its whole blocking
        // body), so the read-check-store below cannot interleave with the
        // append path's watermark store.
        let _g = self.write_lock.lock().await;
        let advanced = {
            let mut guard = self.end_hlc.lock();
            let prev = *guard;
            if prev.is_some_and(|p| end_hlc < p) {
                return Err(Error::Storage(format!(
                    "end_hlc would regress from {prev:?} to {end_hlc:?}"
                )));
            }
            // `advanced` distinguishes a genuine forward move (stamp the age
            // gauge) from a redundant re-record of the same watermark.
            let advanced = prev.is_none_or(|p| end_hlc > p);
            *guard = Some(end_hlc);
            advanced
        };

        // Only a genuine advance is broadcast: a redundant re-record (same value)
        // must not emit a `Watermark` event — the event would misstate the
        // current end_hlc and the gauge would re-stamp without progress.
        if advanced {
            metrics::stable_log_end_hlc_physical_ms(end_hlc);
            metrics::stable_log_end_advanced();
            self.events.send(&StableLogEvent::Watermark(end_hlc));
            self.watermark.send_replace(Some(end_hlc));
            self.watermark_wakes.wake();
        }
        Ok(())
    }

    fn subscribe_events(&self) -> Channels<StableLogEvent> {
        self.events.subscribe()
    }

    fn watch_watermark(&self) -> watch::Receiver<Option<Hlc>> {
        self.watermark.subscribe()
    }

    fn watermark_moves(&self) -> Channels<()> {
        self.watermark_wakes.subscribe()
    }

    fn range(&self, from: LogIndex, until: LogIndex) -> Channels<Result<(LogIndex, Data), Error>> {
        // `[from, until)` — the half-open index window, streamed off a blocking
        // scan through a bounded channel.
        let storage = Arc::clone(&self.storage);
        let (tx, rx) = crossbeam_channel::bounded::<Result<(LogIndex, Data), Error>>(32);
        let (ch, closer) = Channels::from(rx, "StableLogSnapshot::range");
        tokio::task::spawn_blocking(move || {
            let mut decode_err: Option<Error> = None;
            let scan = for_each_entry(
                &storage,
                from,
                until,
                // PINNING RULE: `payload` borrows the iterator's block and pins
                // it in the block cache. It is decoded into an owned `Data`
                // here — the copy the sink contract requires — and the borrow
                // never crosses an `.await` or a batch commit.
                &mut |idx, payload| match postcard::from_bytes::<Data>(payload)
                    .to_codec("decode stable entry")
                {
                    Ok(d) => tx.send(Ok((idx, d))).is_ok(),
                    Err(e) => {
                        decode_err = Some(e);
                        false
                    }
                },
            );
            if let Err(e) = scan {
                let _ = tx.send(Err(e));
            } else if let Some(e) = decode_err {
                let _ = tx.send(Err(e));
            }
            // Normal path only: a panic drops `closer` and the consumer sees
            // `End::Aborted` instead of a clean end.
            closer.close();
        });
        ch
    }

    fn set_purge_threshold(&self, hlc_threshold: Hlc) -> Result<(), Error> {
        // Mirror `ReplicaLogStore::set_purge_threshold`: cap the threshold at
        // `frontier_hlc − 1`, the largest value that leaves the frontier entry
        // (secondary == frontier_hlc) uncovered. An empty log or a frontier at
        // HLC 0 has no such threshold — nothing to purge.
        let Some(frontier_hlc) = *self.frontier_hlc.lock() else {
            return Ok(());
        };
        let Some(cap) = frontier_hlc.raw().checked_sub(1).map(Hlc) else {
            return Ok(());
        };
        let effective = hlc_threshold.min(cap);
        // Index order == HLC order here, so `hlc <= effective` is a key-order
        // prefix and a durable range delete says exactly that. `effective` is
        // capped at `frontier_hlc - 1` above, so the frontier entry has
        // `hlc > effective` and stops the walk: the recovery anchor is never
        // covered (CLAUDE.md §8).
        let Some(boundary) = purge_boundary(&self.storage, effective)? else {
            return Ok(());
        };
        let cf = self.storage.cf(Subsystem::StableLog)?;
        let mut batch = WriteBatch::default();
        batch.delete_range_cf(
            &cf,
            &encode_primary(LogIndex::ZERO)[..],
            &encode_primary(boundary)[..],
        );
        self.storage.write(batch)?;
        self.storage.fence()
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
    use crate::api::StableLogSnapshot;
    use crate::tests::make_data;

    /// Build a closed `Channels` pre-loaded with `items` — test convenience
    /// for the channel-based [`StableLogSnapshot::append`] signature (mirrors how
    /// `integrate_manifest`'s decode worker feeds the channel, then drops its
    /// sender at end-of-stream).
    fn rx_of(
        items: impl IntoIterator<Item = Result<(LogIndex, Data), Error>>,
    ) -> Channels<Result<SnapshotEntry, Error>> {
        Channels::from_vec(
            items
                .into_iter()
                .map(|item| item.map(|(global_index, data)| SnapshotEntry { global_index, data }))
                .collect(),
        )
    }

    /// Open a [`StableLog`] rooted at `dir` with the default sizing.
    async fn open_at(dir: std::path::PathBuf) -> StableLog {
        StableLog::open_engine(dir).await.expect("open stable log")
    }

    /// A terminal error arriving in the **same** `ready_batch` as good entries
    /// must not discard them.
    ///
    /// `ready_batch` boundaries are arbitrary: whether the error lands beside the
    /// entries before it or in the next batch is a producer/consumer race, so
    /// returning early made how much progress a load makes depend on that race —
    /// which is what made `snapshot_loader::integrate_rejects_gap_between_snapshots`
    /// flaky under load. `from_vec` puts everything in one batch, so this
    /// reproduces the losing side deterministically.
    #[tokio::test]
    async fn error_in_the_same_batch_still_commits_the_prefix() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = open_at(dir.path().join("stable")).await;

        let err = log
            .append(rx_of([
                Ok((LogIndex(0), make_data(1, 100, b"a"))),
                Ok((LogIndex(1), make_data(2, 200, b"b"))),
                Err(Error::StableLogGap(
                    "archive stream: expected index 5".into(),
                )),
            ]))
            .await
            .expect_err("the terminal error must still surface");
        assert!(
            matches!(err, Error::StableLogGap(_)),
            "expected the original error, got: {err:?}"
        );
        assert_eq!(
            log.end_index(),
            Some(LogIndex(1)),
            "the contiguous prefix before the error must be durable"
        );
    }

    #[tokio::test]
    async fn append_advances_frontier_and_emits() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = open_at(dir.path().join("stable")).await;
        assert!(log.end_index().is_none());

        let mut rx = log.subscribe_events();
        let d = make_data(1, 100, b"a");
        log.append(rx_of([Ok((LogIndex(0), d.clone()))]))
            .await
            .expect("ok");
        assert_eq!(log.end_index(), Some(LogIndex(0)));

        let event = rx
            .recv()
            .expect("the awaited append above queued this event");
        match event {
            StableLogEvent::Appended { index, data } => {
                assert_eq!(index, LogIndex(0));
                assert_eq!(data, d);
            }
            other => panic!("expected Appended event, got {other:?}"),
        }
    }

    /// Index order == HLC order in the stable log: entries are index-contiguous
    /// and HLC-ascending in lockstep, so an HLC-keyed retire threshold drops
    /// exactly an index prefix. This is the invariant the purge policy leans on.
    #[tokio::test]
    async fn invariant_stable_log_index_order_is_hlc_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = open_at(dir.path().join("stable")).await;

        for (idx, hlc) in [(0u64, 100u64), (1, 150), (2, 200), (3, 275), (4, 300)] {
            log.append(rx_of([Ok((
                LogIndex(idx),
                make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
            ))]))
            .await
            .expect("append");
        }

        let all: Vec<(u64, u64)> = log
            .range(LogIndex(0), LogIndex(5))
            .into_iter()
            .filter_map(|r| r.ok().map(|(i, d)| (i.raw(), d.data_id.hlc().raw())))
            .collect();
        // Strictly ascending in both index and HLC, in lockstep.
        assert_eq!(all, vec![(0, 100), (1, 150), (2, 200), (3, 275), (4, 300)]);

        // An HLC threshold drops exactly the index prefix at or below it.
        log.set_purge_threshold(Hlc(200)).expect("purge");
        let kept: Vec<u64> = log
            .range(LogIndex(0), LogIndex(5))
            .into_iter()
            .filter_map(|r| r.ok().map(|(i, _)| i.raw()))
            .collect();
        assert_eq!(kept, vec![3, 4], "hlc<=200 → index prefix 0,1,2 dropped");
    }

    /// The purge threshold is always clamped strictly below the frontier entry's
    /// HLC, so even a threshold of `Hlc::MAX` keeps the frontier — the recovery
    /// anchor `max_key`/`end_index` reads from — and it survives a reopen.
    #[tokio::test]
    async fn stable_purge_keeps_frontier_even_under_full_coverage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("stable");
        {
            let log = open_at(path.clone()).await;
            for (idx, hlc) in [(0u64, 100u64), (1, 200), (2, 300)] {
                log.append(rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .await
                .expect("append");
            }

            // A threshold that covers every entry must still spare the frontier.
            log.set_purge_threshold(Hlc(u64::MAX)).expect("purge");
            assert_eq!(
                log.end_index(),
                Some(LogIndex(2)),
                "frontier index is unaffected by purge",
            );
            assert!(
                index_is_live(&log.storage, LogIndex(2)).expect("probe"),
                "frontier entry is not clamped away",
            );
        }

        // Reopen: the frontier must still recover — the purge already
        // durably deleted the covered prefix, and the frontier run was never
        // covered in the first place.
        let reopened = open_at(path).await;
        assert_eq!(reopened.end_index(), Some(LogIndex(2)));
        // Just below the frontier entry: its HLC group may be partial.
        assert_eq!(reopened.end_hlc(), Some(Hlc(299)));
    }

    /// After physical retirement (set a max threshold, seal + maintain to
    /// sweep), the frontier entry — the recovery anchor `max_key`/`end_index`
    /// reads from — is still present: the threshold is clamped below it.
    #[tokio::test]
    async fn frontier_survives_after_retired_runs_are_swept() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("stable");
        {
            let log = open_at(path.clone()).await;
            for (idx, hlc) in [(0u64, 100u64), (1, 200), (2, 300)] {
                log.append(rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .await
                .expect("append");
            }
            // The range delete is durable and fenced synchronously — no
            // separate compaction step needed to physically retire the runs.
            log.set_purge_threshold(Hlc(u64::MAX)).expect("purge");
        }

        // Reopen: the frontier recovers even though all non-frontier
        // entries were physically swept.
        let reopened = open_at(path).await;
        assert_eq!(reopened.end_index(), Some(LogIndex(2)));
        assert_eq!(reopened.end_hlc(), Some(Hlc(299)));
        assert!(
            index_is_live(&reopened.storage, LogIndex(2)).expect("probe"),
            "frontier entry survives physical retirement",
        );
    }

    /// Purge is durable, so the prefix stays gone across a reopen without
    /// anything re-establishing a clamp first. This replaces
    /// `arm_from_watermark_reclamps_prefix_after_open`, which pinned the
    /// opposite requirement: that an in-memory clamp be re-armed
    /// synchronously, before the reader existed, or `learner::read` would
    /// serve a purged prefix as live.
    #[tokio::test]
    async fn purged_prefix_stays_gone_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("stable");
        {
            let log = open_at(path.clone()).await;
            for (idx, hlc) in [(0u64, 100u64), (1, 101), (2, 102)] {
                log.append(rx_of([Ok((
                    LogIndex(idx),
                    make_data(u8::try_from(idx + 1).expect("fits"), hlc, b""),
                ))]))
                .await
                .expect("append");
            }
            log.set_purge_threshold(Hlc(101)).expect("purge");
        }

        let reopened = open_at(path).await;
        let live: Vec<u64> = reopened
            .range(LogIndex(0), LogIndex(3))
            .into_iter()
            .filter_map(|r| r.ok().map(|(i, _)| i.raw()))
            .collect();
        assert_eq!(
            live,
            vec![2],
            "indices 0..=1 were purged and must not come back after a reopen"
        );
        assert_eq!(
            reopened.end_index(),
            Some(LogIndex(2)),
            "the frontier entry is never covered by a purge"
        );
    }

    #[tokio::test]
    async fn read_snapshot_then_append_matches() {
        use crate::api::SnapshotStore as _;
        use crate::ids::SnapshotId;
        use crate::meta::SnapshotEntry;
        use crate::object_store::CompressionLevel;

        // End-to-end of the production load path: write a snapshot through the
        // public SnapshotStore API, then `read_snapshot` (off-reactor decode →
        // bounded channel) straight into `append` — no byte/framing detail here.
        let store = crate::object_store::RemoteSnapshotStore::new(
            crate::object_store::SnapshotStoreConfig {
                object_store: Arc::new(object_store::memory::InMemory::new()),
            },
            tokio_util::sync::CancellationToken::new(),
        );
        let id = SnapshotId::open(LogIndex(0));
        let entries: Vec<SnapshotEntry> = (0..5u64)
            .map(|i| SnapshotEntry {
                global_index: LogIndex(i),
                data: make_data(1, 100 + i, b"x"),
            })
            .collect();
        store
            .write_snapshot(
                id,
                Channels::from_vec(entries.into_iter().map(Ok).collect()),
                CompressionLevel::standard(),
                crate::metrics::ObjectStoreReason::SnapshotCreate,
            )
            .await
            .expect("write_snapshot");

        let dir = tempfile::tempdir().expect("tempdir");
        let log = open_at(dir.path().join("s.stable")).await;
        let mut rx = log.subscribe_events();

        let entries_rx = store
            .read_snapshot(id, crate::metrics::ObjectStoreReason::ArchiveRead)
            .await
            .expect("read_snapshot");
        let n = log.append(entries_rx).await.expect("append");
        assert_eq!(n, 5);
        assert_eq!(log.end_index(), Some(LogIndex(4)));
        assert_eq!(
            log.end_hlc(),
            Some(Hlc(103)),
            "end_hlc settles just below the max appended HLC, whose group may be partial"
        );

        // Causal order: the first event is the first Appended entry.
        let ev = rx
            .recv()
            .expect("the awaited append above queued this event");
        assert!(
            matches!(ev, StableLogEvent::Appended { index, .. } if index == LogIndex(0)),
            "first event must be Appended(0), got {ev:?}"
        );
    }

    #[tokio::test]
    async fn loaded_hlc_advances_with_append_and_persists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable = dir.path().join("stable");
        let log = open_at(stable.clone()).await;
        assert!(log.end_hlc().is_none());

        let mut rx = log.subscribe_events();
        log.append(rx_of([Ok((LogIndex(0), make_data(0, 123, b"a")))]))
            .await
            .expect("ok");
        assert_eq!(log.end_hlc(), Some(Hlc(122)));
        // The broadcast reflects the watermark after the entries.
        // Skip the Appended event first, then read the Watermark.
        loop {
            let ev = rx
                .recv()
                .expect("the awaited append above queued this event");
            match ev {
                StableLogEvent::Watermark(hlc) => {
                    assert_eq!(hlc, Hlc(122));
                    break;
                }
                _ => continue,
            }
        }

        drop(log);
        let log = open_at(stable).await;
        assert_eq!(
            log.end_hlc(),
            Some(Hlc(122)),
            "watermark re-derived from the recovered log tail"
        );
    }

    /// The append stream is HLC-sorted by construction; feeding an unsorted
    /// stream is refused with `Error::StableLogGap` at the point of use
    /// instead of silently committing a watermark that recovery would derive
    /// differently.
    #[tokio::test]
    async fn append_rejects_unsorted_hlc_stream() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = open_at(dir.path().join("stable")).await;

        log.append(rx_of([Ok((LogIndex(0), make_data(0, 200, b"a")))]))
            .await
            .expect("ok");
        assert_eq!(log.end_hlc(), Some(Hlc(199)));

        // HLC 100 after 200 violates the sorted-stream contract.
        let err = log
            .append(rx_of([Ok((LogIndex(1), make_data(1, 100, b"b")))]))
            .await
            .expect_err("unsorted stream must be refused");
        assert!(
            matches!(err, Error::StableLogGap(ref m) if m.contains("HLC-sorted")),
            "expected a StableLogGap naming the HLC ordering violation, got: {err:?}"
        );
        assert_eq!(log.end_hlc(), Some(Hlc(199)), "watermark unchanged");
    }

    /// `record_end_hlc` advances `end_hlc` above the last appended HLC
    /// (watermark-only advance) — in memory only: a reopen re-derives the
    /// watermark from the log tail, and the manifest event re-raises it.
    #[tokio::test]
    async fn loaded_hlc_advances_past_appended_via_record_loaded_up_to() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable = dir.path().join("stable");
        let log = open_at(stable.clone()).await;

        log.append(rx_of([Ok((LogIndex(0), make_data(0, 200, b"a")))]))
            .await
            .expect("ok");
        assert_eq!(log.end_hlc(), Some(Hlc(199)));

        // Watermark-only advance past the appended data.
        log.record_end_hlc(Hlc(5000)).await.expect("record");
        assert_eq!(log.end_hlc(), Some(Hlc(5000)));

        // The empty-window advance is deliberately NOT durable: after a
        // restart the watermark reflects only local entries — it must never
        // claim coverage the log cannot back (the manifest event re-raises
        // it once integration confirms the coverage).
        drop(log);
        let log = open_at(stable).await;
        assert_eq!(log.end_hlc(), Some(Hlc(199)));
    }

    /// Gapless ingest fence (docs/STABLE_LOG_GAP_FIX.md): an input stream that
    /// JUMPS past `frontier + 1` must be rejected before anything commits — a
    /// committed gap would be hidden forever by the manifest-integration skip.
    #[tokio::test]
    async fn invariant_append_rejects_gap_in_global_indices() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = open_at(dir.path().join("stable")).await;
        log.append(rx_of([
            Ok((LogIndex(0), make_data(1, 100, b"a"))),
            Ok((LogIndex(1), make_data(2, 200, b"b"))),
        ]))
        .await
        .expect("contiguous prefix ok");

        // Index 3 with no index 2 — must be refused, nothing committed.
        let err = log
            .append(rx_of([Ok((LogIndex(3), make_data(4, 400, b"d")))]))
            .await
            .expect_err("gap must be rejected");
        assert!(
            err.to_string().contains("gap"),
            "typed gap error expected, got: {err}"
        );

        assert_eq!(log.end_index(), Some(LogIndex(1)), "frontier unchanged");
        let entries: Vec<(LogIndex, Data)> = log
            .range(LogIndex(0), LogIndex(10))
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(entries.len(), 2, "the gapped entry must not be committed");
    }

    /// Re-delivering entries at or below the frontier is legitimate (a re-read
    /// of a partially-loaded snapshot overlaps what is already present): the
    /// overlap is skipped idempotently and the stream extends the log from
    /// `frontier + 1` onwards.
    #[tokio::test]
    async fn append_tolerates_overlapping_reappend() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = open_at(dir.path().join("stable")).await;
        log.append(rx_of([
            Ok((LogIndex(0), make_data(1, 100, b"a"))),
            Ok((LogIndex(1), make_data(2, 200, b"b"))),
            Ok((LogIndex(2), make_data(3, 300, b"c"))),
        ]))
        .await
        .expect("prefix");

        // Overlapping resume: 1..=2 already present (skipped), 3..=4 appended.
        let n = log
            .append(rx_of([
                Ok((LogIndex(1), make_data(2, 200, b"b"))),
                Ok((LogIndex(2), make_data(3, 300, b"c"))),
                Ok((LogIndex(3), make_data(4, 400, b"d"))),
                Ok((LogIndex(4), make_data(5, 500, b"e"))),
            ]))
            .await
            .expect("overlapping re-append is legitimate");
        assert_eq!(n, 2, "only the genuinely-new entries count as appended");
        assert_eq!(log.end_index(), Some(LogIndex(4)));

        let entries: Vec<(LogIndex, Data)> = log
            .range(LogIndex(0), LogIndex(10))
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let indices: Vec<u64> = entries.iter().map(|(i, _)| i.raw()).collect();
        assert_eq!(indices, vec![0, 1, 2, 3, 4]);
    }

    /// `record_end_hlc` must broadcast a `Watermark` event only on a genuine
    /// advance — a redundant re-record or a rejected regression emits nothing.
    #[tokio::test]
    async fn record_end_hlc_emits_watermark_only_on_advance() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = open_at(dir.path().join("stable")).await;
        let mut rx = log.subscribe_events();

        log.record_end_hlc(Hlc(100)).await.expect("first advance");
        let ev = rx
            .recv()
            .expect("the awaited append above queued this event");
        assert!(matches!(ev, StableLogEvent::Watermark(Hlc(100))));

        // Redundant re-record: Ok, but no event.
        log.record_end_hlc(Hlc(100)).await.expect("redundant ok");
        // Regression: rejected, no event.
        log.record_end_hlc(Hlc(50))
            .await
            .expect_err("regression rejected");

        // The very next event must be the next genuine advance — nothing was
        // queued for the redundant or regressed records.
        log.record_end_hlc(Hlc(200)).await.expect("second advance");
        let ev = rx
            .recv()
            .expect("the awaited append above queued this event");
        assert!(matches!(ev, StableLogEvent::Watermark(Hlc(200))));
    }

    /// Startup integrity: a foreign/manufactured WAL file in the log dir must
    /// NOT be silently accepted. The stable log is re-derivable from
    /// object-store snapshots, but it is still opened as a durability source
    /// of truth — so instead of self-healing by discarding (which would hide
    /// the bug that produced the file), recovery REFUSES to open, and the
    /// corrupt file is preserved for forensics.
    #[tokio::test]
    async fn startup_refuses_open_on_corrupt_committed_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stable_dir = dir.path().join("stable");
        {
            let log = open_at(stable_dir.clone()).await;
            log.append(rx_of([
                Ok((LogIndex(0), make_data(1, 100, b"a"))),
                Ok((LogIndex(1), make_data(2, 200, b"b"))),
            ]))
            .await
            .expect("prefix");
        }

        // Manufacture the corruption: the DB's MANIFEST, the committed-state
        // record RocksDB validates at open. The claim is CLAUDE.md §8 —
        // genuine corruption of committed state refuses to open rather than
        // self-healing by discard. The DB root is the log's own directory.
        let db_root = stable_dir.as_path();
        let manifest = std::fs::read_dir(db_root)
            .expect("read db root")
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("MANIFEST-"))
            })
            .expect("a MANIFEST must exist once the db has been opened");
        std::fs::write(&manifest, b"not-a-manifest").expect("corrupt the manifest");

        let err = match StableLog::open_engine(stable_dir.clone()).await {
            Ok(_) => panic!("corrupt committed state must refuse to open, not be absorbed"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("open rocksdb"),
            "the error must name the failing open: {err}"
        );
        assert!(
            manifest.exists(),
            "the corrupt file is preserved for forensics"
        );
    }

    #[tokio::test]
    async fn range_returns_in_index_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = open_at(dir.path().join("stable")).await;
        for i in 0..5u8 {
            log.append(rx_of([Ok((
                LogIndex(u64::from(i)),
                make_data(i, 100 + u64::from(i), &[i]),
            ))]))
            .await
            .expect("ok");
        }
        let entries: Vec<(crate::ids::LogIndex, crate::meta::Data)> = log
            .range(LogIndex(0), LogIndex(5))
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(entries.len(), 5);
        for (n, (idx, _)) in entries.iter().enumerate() {
            assert_eq!(idx.raw(), u64::try_from(n).expect("fits"));
        }
    }

    // ── key codec ───────────────────────────────────────────────────────

    use proptest::prelude::{any, prop_assert_eq, proptest};

    #[test]
    fn stable_key_roundtrips_and_primary_dominates() {
        let idx = LogIndex(0xDEAD_BEEF);
        let hlc = Hlc(0xCAFE_BABE);
        let k = encode_key(idx, hlc);
        assert_eq!(k.len(), KEY_WIDTH);
        assert_eq!(decode_key(&k), Some((idx, hlc)));
        // The primary half is the seek prefix: a key's first PRIMARY_WIDTH
        // bytes are exactly `encode_primary`, which is what makes an index
        // seek land on the right entry.
        assert_eq!(&k[..PRIMARY_WIDTH], &encode_primary(idx)[..]);
        assert_eq!(&k[PRIMARY_WIDTH..], &encode_secondary(hlc)[..]);
    }

    /// Retirement is a `DeleteRange` over **encoded bytes**, so a byte compare
    /// has to be the `Ord` compare or `set_purge_threshold` drops the wrong
    /// records — the same property `learner::read`'s object-store fallback
    /// relies on to know which reads fall below the purged prefix
    /// (CLAUDE.md §8).
    #[test]
    fn stable_key_order_matches_tuple_order() {
        proptest!(|(
            (i1, h1) in (any::<u64>(), any::<u64>()),
            (i2, h2) in (any::<u64>(), any::<u64>()),
        )| {
            let k1 = encode_key(LogIndex(i1), Hlc(h1));
            let k2 = encode_key(LogIndex(i2), Hlc(h2));
            prop_assert_eq!(k1.cmp(&k2), (i1, h1).cmp(&(i2, h2)));
        });
    }

    /// The secondary encoding is what the retire threshold is expressed in, so
    /// its byte order must be the `Hlc` order on its own too.
    #[test]
    fn stable_secondary_order_matches_hlc_order() {
        proptest!(|(a in any::<u64>(), b in any::<u64>())| {
            prop_assert_eq!(
                encode_secondary(Hlc(a)).cmp(&encode_secondary(Hlc(b))),
                a.cmp(&b),
            );
        });
    }

    #[test]
    fn stable_key_decode_rejects_wrong_length() {
        assert_eq!(decode_key(&[0u8; 4]), None);
        assert_eq!(decode_key(&[]), None);
    }

    /// Entries sharing an HLC can straddle a load batch: `ready_batch` cuts
    /// wherever it cuts. A `Watermark` claims everything at or below it is
    /// loaded, so none may cover the group's HLC until the whole group is in —
    /// otherwise the unloaded rest is in neither half of a blended read (the
    /// unstable tail starts strictly above the watermark).
    #[tokio::test]
    async fn a_watermark_never_covers_an_hlc_group_split_across_batches() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = Storage::open_for_test(&dir.path().join("stable")).expect("storage");
        // Deep enough to hold every event of the load without a hang-up.
        let log = StableLog::open_sized(storage, crate::unit::nonzero(1024))
            .await
            .expect("open");
        let mut events = log.subscribe_events();

        // One more entry than a batch holds: the first batch ends on the
        // group's first entry, the second batch carries the rest of it.
        let batch = u64::try_from(STABLE_LOAD_BATCH).expect("fits");
        let group_hlc = 5_000u64;
        let hlc_of = |i: u64| {
            if i.saturating_add(1) >= batch {
                group_hlc
            } else {
                1_000u64.saturating_add(i)
            }
        };
        let entries: Vec<_> = (0..=batch)
            .map(|i| {
                let seed = u8::try_from(i.checked_rem(200).expect("nonzero")).expect("fits");
                Ok((LogIndex(i), make_data(seed, hlc_of(i), b"")))
            })
            .collect();
        log.append(rx_of(entries)).await.expect("append");

        let mut emitted: Vec<Hlc> = Vec::new();
        let mut watermarks = 0usize;
        while let Some(event) = events.try_recv() {
            match event {
                StableLogEvent::Appended { data, .. } => emitted.push(data.data_id.hlc()),
                StableLogEvent::Watermark(wm) => {
                    watermarks = watermarks.saturating_add(1);
                    let owed = (0..=batch).filter(|&i| hlc_of(i) <= wm.raw()).count();
                    assert!(
                        emitted.len() >= owed,
                        "Watermark({wm:?}) went out after only {} of the {owed} entries at or \
                         below it",
                        emitted.len(),
                    );
                }
            }
        }
        assert!(watermarks > 0, "the load must have advanced the watermark");
        assert_eq!(
            emitted.len(),
            usize::try_from(batch).expect("fits").saturating_add(1)
        );
        assert!(
            log.end_hlc() < Some(Hlc(group_hlc)),
            "the group is settled by the manifest's `record_end_hlc`, not by append"
        );

        log.record_end_hlc(Hlc(group_hlc)).await.expect("record");
        assert_eq!(log.end_hlc(), Some(Hlc(group_hlc)));
    }
}
