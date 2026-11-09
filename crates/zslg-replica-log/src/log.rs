//! The replica's gapless append-only log — one column family of a
//! [`Storage`].
//!
//! ```text
//!   submit ──> [write channel] ──> flush loop ──> write ──> fence (fsync)
//!                                  one thread                     │
//!                                  sole appender                  v
//!                                          broadcast + completion ACK
//! ```
//!
//! **One flush loop is the only appender.** Indices are handed out sequentially
//! by that single thread, so the log is monotonic and contiguous by construction
//! rather than by check. A `WriteBatch` is atomic, so a crash leaves a clean
//! prefix and the newest stored key + 1 is always the next index — there is no
//! separate durable cursor to disagree with the data.
//!
//! **Nothing is ACKed before the fsync.** The broadcast and the completion both
//! fire after [`Storage::fence`] returns, inside the same blocking body, which is
//! also what holds each `HlcGuard` until its write is durable (CLAUDE.md §8).
//!
//! **The batching write queue is load-bearing twice over and must not be
//! restructured.** docs/ROCKSDB_MIGRATION.md §1.2 measured per-writer writes at
//! roughly half this shape's throughput, and §6.7 measured the same queue as the
//! system's fsync coalescer: batch size rises 30 → 47 → 65 as fsync gets slower,
//! collapsing a 3.2x/10.2x penalty at `inflight=1` to 1.16x/2.3x at
//! `inflight=128`.
//!
//! **Purge retires by HLC, durably, and never reaches the frontier.** The
//! key's secondary is a running maximum HLC, so the covered set is a key-order
//! prefix and a `DeleteRange` expresses it exactly — there is no compaction
//! filter and no read-side clamp. The threshold is clamped strictly below the
//! newest entry, which keeps the durable record of `next_index` recoverable.
//!
//! This log is a durability source of truth, so genuine corruption refuses to
//! open rather than repairing itself by discarding; wipe and rejoin under a new
//! `NodeId` is the operator procedure.

use rocksdb_kit::ColumnFamily;
use std::sync::Arc;
use zslg_log::LogError;

use rocksdb_kit::Batch as RocksBatch;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::Error;
use crossbeam_combinators::{
    Channel, Fanout, FanoutConfig, OverflowPolicy, Registry, Segment, broadcast,
};
use rocksdb_kit::Storage;
use zslg_log::Data;
use zslg_log::LogIndex;
use zslg_log::ToStorage;
use zslg_log::metrics::LogType;
use zslg_log::{Admission, Completion, ReplicaLog};
use zslg_time::Hlc;

/// This replica's dense local log.
pub const REPLICA_LOG_CF: ColumnFamily = ColumnFamily::new("replica_log");

/// Headroom for the non-payload part of a record (the 16-byte key, the value's
/// 32-byte `data_id` prefix, and RocksDB's per-record framing — well under
/// 1 KiB). A blob above the payload cap minus this is rejected per-write here
/// rather than at commit, where it would cost the whole batch.
const FRAME_OVERHEAD_SLACK: u64 = 1024;

/// Per-record payload cap. RocksDB has no frame cap of its own, so the limit is
/// stated here.
const MAX_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;

mod batch;
mod codec;
mod flush;
mod frontier;
mod scan;

pub(crate) use self::batch::{BatchBuilder, Committer};
pub(crate) use self::codec::{
    decode_key, decode_value, encode_key, encode_primary, missing_value, truncated_value,
    undecodable,
};
pub(crate) use self::flush::ReplicaSeam;
pub use self::flush::flush_loop;
pub(crate) use self::frontier::Frontier;
pub use self::frontier::PendingWrite;
pub(crate) use self::scan::{for_each_entry, newest_live_entry, purge_boundary};

/// Test-only seam that widens the historic→live handover window.
///
/// The ordering inside [`ReplicaLogStore::subscribe_from`] is a correctness
/// invariant whose violation window is a couple of instructions wide — a purely
/// concurrent test passes just as happily with the ordering reversed, so it
/// proves nothing. This lets a test park inside the window and commit an entry
/// there deliberately, which is the only way to assert the invariant rather than
/// hope to trip over it.
#[cfg(test)]
static SUBSCRIBE_HANDOFF_PAUSE_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
fn subscribe_handoff_pause() {
    let ms = SUBSCRIBE_HANDOFF_PAUSE_MS.load(std::sync::atomic::Ordering::Relaxed);
    if ms > 0 {
        std::thread::sleep(std::time::Duration::from_millis(ms));
    }
}

/// The replica's append-only log. The single writer task (`flush_loop`) holds
/// a `Weak<ReplicaLogStore>` and upgrades only while committing a batch, so
/// dropping the last strong handle frees the store promptly.
pub struct ReplicaLogStore {
    storage: Arc<Storage>,
    frontier: Frontier,
    batch_builder: BatchBuilder,
    /// The live tail's fan-out; dropping the store stops its thread.
    live: Fanout<(LogIndex, Data)>,
    live_registry: Registry<(LogIndex, Data)>,
    /// Sync MPSC into the flush loop. crossbeam (not tokio): `submit` is called
    /// from blocking threads and never `.await`s the enqueue.
    write_tx: crossbeam_channel::Sender<PendingWrite>,
    /// Live-tail depth handed to each subscriber. See
    /// [`ReplicaLogConfig::subscribe_channel_capacity`](crate::ReplicaLogConfig::subscribe_channel_capacity)
    /// — it must stay >= `max_flush_batch` or one batch sheds every
    /// subscriber, which `ReplicaLogConfig::validate` enforces.
    subscribe_channel_capacity: usize,
    /// Max records drained into one append + fsync.
    max_flush_batch: usize,
}

impl ReplicaLogStore {
    pub async fn open(
        storage: Arc<Storage>,
        write_tx: crossbeam_channel::Sender<PendingWrite>,
        config: &crate::ReplicaLogConfig,
    ) -> Result<Self, Error> {
        Self::open_sized(
            storage,
            write_tx,
            config.subscribe_channel_capacity.get(),
            config.max_flush_batch.get(),
        )
        .await
    }

    /// As [`Self::open`], with the two queue sizes supplied rather than taken
    /// from a config — the entry point the test constructors use. Production
    /// reaches it through [`Self::open`], carrying the operator's config.
    async fn open_sized(
        storage: Arc<Storage>,
        write_tx: crossbeam_channel::Sender<PendingWrite>,
        subscribe_channel_capacity: usize,
        max_flush_batch: usize,
    ) -> Result<Self, Error> {
        // The replica log is a durability source of truth — genuine corruption
        // refuses to open (CLAUDE.md §8: wipe + new NodeId).
        let max_payload_bytes = MAX_PAYLOAD_BYTES;

        // `max_hlc` is the bound the leader samples for the joint-drain exit
        // (CLAUDE.md §8) and must be EXACT. The key's secondary is a running
        // max, so the last key carries it directly — and purge cannot lower it,
        // because the threshold is clamped strictly below the frontier entry.
        // No sweep, no stored value prefix: one `seek_to_last`.
        let storage_cl = Arc::clone(&storage);
        let frontier = tokio::task::spawn_blocking(move || newest_live_entry(&storage_cl))
            .await
            .to_storage("frontier recovery panicked")??;
        info!(frontier = ?frontier, "replica log initialized from disk");
        if let Some((idx, _)) = frontier {
            zslg_log::metrics::log_index_frontier(LogType::Replica, idx.raw());
        }

        let (recovered_index, recovered_max_hlc) = match frontier {
            Some((idx, max_hlc)) => (Some(idx), Some(max_hlc)),
            None => (None, None),
        };
        let frontier = Frontier::new(recovered_index, recovered_max_hlc);
        let batch_builder = BatchBuilder::new(max_flush_batch, max_payload_bytes);

        let (live, live_registry) = broadcast(
            "replica live tail",
            FanoutConfig::with_sub_cap(bytesize::nonzero(subscribe_channel_capacity)),
            OverflowPolicy::Shed,
        );
        Ok(Self {
            storage,
            frontier,
            batch_builder,
            live,
            live_registry,
            write_tx,
            subscribe_channel_capacity,
            max_flush_batch,
        })
    }

    /// Open the store (as an `Arc`, so the flush loop can hold a `Weak`) on
    /// `storage` and spawn the flush-loop background task. For harnesses that
    /// run a store without the node's `Shutdown`.
    #[cfg(any(test, feature = "testing"))]
    pub async fn open_with_flush(
        storage: Arc<Storage>,
        cancel: CancellationToken,
    ) -> Result<(Arc<Self>, tokio::task::JoinHandle<Result<(), Error>>), Error> {
        let sizing = crate::ReplicaLogConfig::for_test(zslg_consensus::NodeId::new_v4());
        let (write_tx, write_rx) = crossbeam_channel::bounded(sizing.write_channel_capacity.get());
        let store = Arc::new(
            Self::open_sized(
                storage,
                write_tx,
                sizing.subscribe_channel_capacity.get(),
                sizing.max_flush_batch.get(),
            )
            .await?,
        );
        let weak = Arc::downgrade(&store);
        let handle = tokio::spawn(flush_loop(weak, write_rx, cancel));
        Ok((store, handle))
    }

    /// An empty historic phase: complete by construction — the window *was*
    /// fully served, it just held nothing.
    fn empty_historic() -> Channel<Result<(LogIndex, Data), Error>, crossbeam_combinators::Finite> {
        Channel::empty()
    }

    /// The historical catch-up phase of a subscription: a **finite** scan of
    /// `[from, to]`. Fire-and-forget `spawn_blocking`, so the `Feed` is what
    /// distinguishes "the window was fully served" from "the scan died" — a
    /// panic here would otherwise truncate a subscriber's catch-up and still look
    /// like a clean handoff to the live tail.
    fn pump_range(
        &self,
        from: LogIndex,
        to: LogIndex,
    ) -> Channel<Result<(LogIndex, Data), Error>, crossbeam_combinators::Finite> {
        debug_assert!(from <= to);
        let storage = Arc::clone(&self.storage);
        Channel::produce_blocking(
            self.subscribe_channel_capacity,
            "replica historic tail",
            move |feed| {
                // PINNING RULE: `payload` is borrowed from the iterator's pinned
                // block for the duration of the call, and this `Data` is sent
                // onward, so it takes an owned copy — the copy happens only
                // here, where the value outlives the iterator.
                let scan = for_each_entry(&storage, from, to, &mut |idx, data_id, payload| {
                    let blob = bytes::Bytes::copy_from_slice(payload);
                    feed.send(Ok((idx, Data { data_id, blob }))).is_ok()
                });
                if let Err(e) = scan {
                    let _ = feed.send(Err(e));
                }
                feed.close()
            },
        )
    }

    /// Test-only async wrapper around [`submit`](Self::submit) that awaits the
    /// assigned `LogIndex` — keeps the existing `log.rs` unit tests unchanged.
    #[cfg(test)]
    pub async fn append(&self, record: Data, guard: Admission) -> Result<LogIndex, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // Bridge: use a crossbeam pair to get the result from the flush-loop thread
        // into the test. A plain crossbeam `recv()` would block the test's tokio
        // thread and deadlock the flush loop.
        let (cb_tx, cb_rx) = crossbeam_channel::bounded(1);
        self.submit(record, guard, Arc::new(cb_tx));
        tokio::task::spawn_blocking(move || {
            let res = cb_rx
                .recv()
                .map_err(|_| {
                    Error::Log(LogError::Storage(
                        "flush task exited before committing write".into(),
                    ))
                })
                .and_then(|(_, inner)| inner.map_err(Error::from));
            let _ = tx.send(res);
        });
        rx.await.map_err(|_| {
            Error::Log(LogError::Storage(
                "flush task exited before committing write".into(),
            ))
        })?
    }
}

impl ReplicaLog for ReplicaLogStore {
    fn frontier(&self) -> Option<LogIndex> {
        self.frontier.index()
    }

    fn max_hlc(&self) -> Option<Hlc> {
        self.frontier.max_hlc()
    }

    /// Submit a write into the flush loop **synchronously** (no `.await`): enqueue
    /// the record + its `HlcGuard` + the sender to route the result to, and return.
    /// The flush loop sends the assigned `LogIndex` (or a storage error) to
    /// `completion` after the batch is durable + broadcast. Blocks only on channel
    /// backpressure (bounded, cap `ReplicaLogConfig::write_channel_capacity`);
    /// safe to call from
    /// any thread (a sync channel, not tokio). If the store has shut down, the
    /// error is sent inline to `completion`.
    fn submit(&self, record: Data, admission: Admission, completion: Completion) {
        let data_id = record.data_id;
        let pending = PendingWrite {
            data: record,
            completion,
            guard: Some(admission),
            enqueued_at: std::time::Instant::now(),
        };
        if let Err(crossbeam_channel::SendError(pending)) = self.write_tx.send(pending) {
            // Shutdown: the flush loop is gone. Resolve inline so the caller still
            // hears back (the HlcGuard drops with the PendingWrite here).
            let _ = pending.completion.send((
                data_id,
                Err(zslg_log::Error::Log(LogError::Storage(
                    "replica log store shut down".into(),
                ))),
            ));
        }
    }

    fn subscribe_from(self: Arc<Self>, from: Option<LogIndex>) -> zslg_log::ReplicaStream {
        let seam = ReplicaSeam { log: self, from };
        (Segment::open(&seam)
            .chain()
            .ordered(from, "replica subscription"))
        .err_into()
    }

    /// Retire everything whose running-max secondary is at or below
    /// `threshold`, durably.
    ///
    /// The secondary is monotone, so that set is a key-order prefix and a
    /// `DeleteRange` says exactly it. The threshold is clamped strictly below
    /// the frontier entry's secondary, so the newest entry always survives and
    /// the durable record of `next_index` stays recoverable (CLAUDE.md §8).
    /// Without the clamp, an idle log whose watermark passed every entry's HLC
    /// would purge the frontier and restart at index 0.
    fn set_purge_threshold(&self, threshold: Hlc) -> Result<(), zslg_log::Error> {
        let purge = || -> Result<(), Error> {
            let Some(clamped) = self.frontier.clamp_threshold(threshold) else {
                return Ok(());
            };
            let Some(boundary) = purge_boundary(&self.storage, clamped)? else {
                return Ok(());
            };
            let cf = self.storage.cf(REPLICA_LOG_CF)?;
            let mut batch = RocksBatch::new();
            batch.delete_range(
                &cf,
                &encode_primary(LogIndex::ZERO)[..],
                &encode_primary(boundary)[..],
            );
            self.storage.write(batch)?;
            self.storage.fence().map_err(Into::into)
        };
        purge().map_err(Into::into)
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
mod tests;
