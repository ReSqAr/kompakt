//! [`StableLog`]: open, recover, and the `StableLogSnapshot` port.

use std::sync::Arc;
use zslg_log::LogError;

use parking_lot::Mutex as SyncMutex;
use rocksdb_kit::Batch;
use tokio::sync::Mutex as AsyncMutex;

use crate::appender::BatchAppender;
use crate::engine::{for_each_entry, newest_live_entry, purge_boundary, settled_below};
use crate::key::encode_primary;
use crate::{Error, STABLE_LOG_CF, StableLogConfig, metrics};
use crossbeam_combinators::{
    Channel, Fanout, FanoutConfig, Finite, Infinite, Level, LevelSetter, OverflowPolicy, Registry,
    broadcast,
};
use metrics_kit::OpState;
use rocksdb_kit::Storage;
use zslg_log::LogIndex;
use zslg_log::ToCodec;
use zslg_log::ToStorage;
use zslg_log::metrics::LogType;
use zslg_log::{Data, SnapshotEntry, StableLogEvent};
use zslg_time::Hlc;

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
pub struct StableLog {
    pub(crate) storage: Arc<Storage>,
    pub(crate) write_lock: AsyncMutex<()>,
    pub(crate) end_index: Arc<SyncMutex<Option<LogIndex>>>,
    pub(crate) end_hlc: Arc<SyncMutex<Option<Hlc>>>,
    /// The frontier *entry's* creation HLC — distinct from `end_hlc`, which can
    /// float above it via empty-window `record_end_hlc`, and sits below it
    /// while the frontier's HLC group may be incomplete. This is the purge
    /// clamp's cap (`set_purge_threshold` keeps the threshold strictly below it,
    /// so the frontier entry — the recovery anchor for `max_key`/`end_index` —
    /// is never retired).
    pub(crate) frontier_hlc: Arc<SyncMutex<Option<Hlc>>>,
    /// Data plane: entries and watermarks, per-subscriber, on crossbeam.
    /// Dropping the log stops the fan-out's thread.
    pub(crate) events: Arc<Fanout<StableLogEvent>>,
    pub(crate) events_registry: Registry<StableLogEvent>,
    /// Control plane: the current watermark, level-triggered, for blocking
    /// and async readers alike. Consumers that only want "has it reached X"
    /// (`learner::quorum`) or "did it move" (`local_store::purge`,
    /// `learner::read`'s unstable tail) read this instead of the event fan-out
    /// — no payload crosses it, and a level cannot lag.
    pub(crate) watermark: Arc<LevelSetter<Option<Hlc>>>,
    pub(crate) watermark_level: Level<Option<Hlc>>,
}

impl StableLog {
    /// Open the stable log on the node's [`Storage`], with full crash
    /// recovery. `end_index`/`end_hlc` come from the recovered tail — the max
    /// stored index (exact; this log never deletes) and just below its entry's
    /// HLC — local reality, nothing more. Production callers go through
    /// [`spawn_stable_store`].
    pub async fn open(config: &StableLogConfig, storage: Arc<Storage>) -> Result<Self, Error> {
        Self::open_sized(storage, config.subscriber_queue_capacity).await
    }

    /// Open on an existing [`Storage`] at the default broadcast depth, for the
    /// harnesses that share one DB between this log and other stores.
    pub async fn open_on(storage: Arc<Storage>) -> Result<Self, Error> {
        Self::open_sized(storage, StableLogConfig::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY).await
    }

    /// The storage boundary: an injected [`Storage`] plus the broadcast depth.
    pub(crate) async fn open_sized(
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
            zslg_log::metrics::log_index_frontier(LogType::Stable, idx.raw());
        }

        let (events, events_registry) = broadcast(
            "stable log subscription",
            FanoutConfig::with_sub_cap(subscriber_queue_capacity),
            OverflowPolicy::Shed,
        );
        let (watermark, watermark_level) = Level::new("stable watermark moves", end_hlc);

        Ok(Self {
            storage,
            write_lock: AsyncMutex::new(()),
            end_index: Arc::new(SyncMutex::new(end_index)),
            end_hlc: Arc::new(SyncMutex::new(end_hlc)),
            frontier_hlc: Arc::new(SyncMutex::new(frontier_hlc)),
            events: Arc::new(events),
            events_registry,
            watermark: Arc::new(watermark),
            watermark_level,
        })
    }
}

/// One-stop node-startup constructor: open the stable log, recovery included.
/// There is no background loop to cancel — RocksDB owns its own compaction
/// threads and stops them when the DB drops.
pub async fn spawn_stable_store(
    stable_log_cfg: &StableLogConfig,
    storage: Arc<Storage>,
) -> Result<Arc<StableLog>, Error> {
    let stable_log = Arc::new(StableLog::open(stable_log_cfg, storage).await?);
    Ok(stable_log)
}

#[async_trait::async_trait]
impl zslg_log::StableLogSnapshot for StableLog {
    fn end_index(&self) -> Option<LogIndex> {
        *self.end_index.lock()
    }

    fn end_hlc(&self) -> Option<Hlc> {
        *self.end_hlc.lock()
    }

    async fn append(
        &self,
        entries: Channel<Result<SnapshotEntry, zslg_log::Error>, Finite>,
    ) -> Result<usize, zslg_log::Error> {
        let entries: Channel<Result<SnapshotEntry, Error>, Finite> = entries.err_into();
        async {
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
            // The loader (§9.7): an overlapping re-append is deduplicated, the
            // stored prefix skipped, and the rest must continue the log exactly.
            let end = *self.end_index.lock();
            let entries = entries
                .dedup_by_key::<LogIndex>()
                .filter_above::<LogIndex>(end)
                .ordered::<crossbeam_combinators::Contiguous<LogIndex>>(end, "stable log loader");
            let appender = BatchAppender {
                rx: entries,
                storage: Arc::clone(&self.storage),
                end_index: Arc::clone(&self.end_index),
                end_hlc: Arc::clone(&self.end_hlc),
                frontier_hlc: Arc::clone(&self.frontier_hlc),
                events: Arc::clone(&self.events),
                watermark: Arc::clone(&self.watermark),
            };

            let total = tokio::task::spawn_blocking(move || appender.run())
                .await
                .to_storage("append spawn_blocking panicked")??;

            probe.finish(OpState::Success);
            Ok::<_, Error>(total)
        }
        .await
        .map_err(Into::into)
    }

    async fn record_end_hlc(&self, end_hlc: Hlc) -> Result<(), zslg_log::Error> {
        async {
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
                    return Err(Error::Log(LogError::Storage(format!(
                        "end_hlc would regress from {prev:?} to {end_hlc:?}"
                    ))));
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
                let _ = self.events.publish(StableLogEvent::Watermark(end_hlc));
                self.watermark.set(Some(end_hlc));
            }
            Ok(())
        }
        .await
        .map_err(Into::into)
    }

    fn subscribe_events(&self) -> Channel<StableLogEvent, Infinite> {
        self.events_registry.subscribe_infinite()
    }

    fn watermark(&self) -> Level<Option<Hlc>> {
        self.watermark_level.clone()
    }

    fn range(
        &self,
        after: Option<LogIndex>,
        until: LogIndex,
    ) -> Channel<Result<(LogIndex, Data), zslg_log::Error>, Finite> {
        let run = || -> Channel<Result<(LogIndex, Data), Error>, Finite> {
            // `(after, until)`, scanned as the inclusive `[from, until)` storage
            // takes, off a blocking scan through a bounded channel.
            let from = LogIndex::next_after(after);
            let storage = Arc::clone(&self.storage);
            Channel::produce_blocking(32, "StableLogSnapshot::range", move |feed| {
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
                        Ok(d) => feed.send(Ok((idx, d))).is_ok(),
                        Err(e) => {
                            decode_err = Some(e.into());
                            false
                        }
                    },
                );
                if let Err(e) = scan {
                    let _ = feed.send(Err(e));
                } else if let Some(e) = decode_err {
                    let _ = feed.send(Err(e));
                }
                feed.close()
            })
        };
        run().err_into()
    }

    fn set_purge_threshold(&self, hlc_threshold: Hlc) -> Result<(), zslg_log::Error> {
        let run = || -> Result<(), Error> {
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
            let cf = self.storage.cf(STABLE_LOG_CF)?;
            let mut batch = Batch::new();
            batch.delete_range(
                &cf,
                &encode_primary(LogIndex::ZERO)[..],
                &encode_primary(boundary)[..],
            );
            self.storage.write(batch)?;
            self.storage.fence().map_err(Into::into)
        };
        run().map_err(Into::into)
    }
}
