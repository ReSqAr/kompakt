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
use std::sync::{Arc, Weak};
use zslg_log::LogError;

use rocksdb_kit::Batch as RocksBatch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::Error;
use crate::metrics::{self, FlushTrigger};
use crossbeam_combinators::{
    Channel, Fanout, FanoutConfig, OverflowPolicy, Registry, Seam, Segment, broadcast,
};
use metrics_kit::OpState;
use rocksdb_kit::Storage;
use zslg_log::Data;
use zslg_log::ToStorage;
use zslg_log::metrics::LogType;
use zslg_log::{Admission, Completion, ReplicaLog};
use zslg_log::{DataId, LogIndex};
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

// ── key codec ───────────────────────────────────────────────────────────────
//
// `key = index ‖ running_max_hlc`, `value = data_id ‖ blob`. Both key halves
// are big-endian, so the byte ordering IS the tuple ordering.
//
// **The secondary is not a property of its own entry.** It is the largest HLC
// over the whole log prefix ENDING AT this entry — a running maximum, not this
// entry's `data_id.hlc()`. Reading it as "this entry's HLC" is wrong, and looks
// right on a quiet log where the two coincide.
//
// Why: retirement's threshold is an HLC, but writers append out of HLC order,
// so `index` order is assignment order and the set of entries with
// `own_hlc <= T` is scattered through the keyspace. A running max is monotone
// by construction, so `runmax <= T` is a contiguous prefix — which is exactly
// what `DeleteRange` can express, and it is why this log needs no compaction
// filter and no read-side clamp. `runmax >= own_hlc`, so the rule is
// conservative in the safe direction: it never retires an entry whose own HLC
// is above the threshold, only sometimes retains one whose HLC is below it.
//
// Consequences worth knowing:
//   * entry N's key cannot be built from entry N alone — it needs the log's
//     history. Nothing tries: appends take it from the live frontier, and every
//     read seeks by the 8-byte `encode_primary(index)` prefix.
//   * two entries may share a secondary. Uniqueness is `index`, as it always
//     was — the old `data_id` secondary never contributed to it either.
//   * the last key's secondary IS the log's `max_hlc`, so recovering that bound
//     at open is a `seek_to_last`, not a sweep.
//
// `data_id` moved into the value because it is no longer part of the key's
// identity or its ordering. `WriteBatch::put_cf_vectored` writes the two halves
// without concatenating them, so the blob is still copied exactly once.

const PRIMARY_WIDTH: usize = 8;
const SECONDARY_WIDTH: usize = 8;
const KEY_WIDTH: usize = PRIMARY_WIDTH + SECONDARY_WIDTH;
/// The `data_id` prefix every value carries ahead of the caller's blob.
const VALUE_PREFIX_WIDTH: usize = 32;

#[must_use]
fn encode_key(index: LogIndex, running_max: Hlc) -> [u8; KEY_WIDTH] {
    let mut k = [0u8; KEY_WIDTH];
    k[..PRIMARY_WIDTH].copy_from_slice(&index.raw().to_be_bytes());
    k[PRIMARY_WIDTH..].copy_from_slice(&running_max.raw().to_be_bytes());
    k
}

#[must_use]
fn encode_primary(index: LogIndex) -> [u8; PRIMARY_WIDTH] {
    index.raw().to_be_bytes()
}

#[must_use]
fn decode_key(bytes: &[u8]) -> Option<(LogIndex, Hlc)> {
    let index = u64::from_be_bytes(bytes.get(..PRIMARY_WIDTH)?.try_into().ok()?);
    let running_max = u64::from_be_bytes(bytes.get(PRIMARY_WIDTH..KEY_WIDTH)?.try_into().ok()?);
    Some((LogIndex(index), Hlc(running_max)))
}

/// Split a stored value into its `data_id` prefix and the caller's blob. The
/// blob borrows the input, so an iterator's pinned block must outlive the call.
#[must_use]
fn decode_value(bytes: &[u8]) -> Option<(DataId, &[u8])> {
    let (id, value) = bytes.split_first_chunk::<VALUE_PREFIX_WIDTH>()?;
    Some((DataId(*id), value))
}

fn undecodable(key: &[u8]) -> Error {
    Error::Log(LogError::Storage(format!(
        "undecodable replica log key ({} bytes): {key:02x?}",
        key.len()
    )))
}

/// A live key whose value is missing. Unreachable through RocksDB's iterator
/// contract (`value()` is `None` only when `key()` is), but the alternative —
/// substituting an empty payload — would broadcast an empty `Data` to
/// subscribers rather than fail. §20: never serve garbage silently.
fn missing_value(key: &[u8]) -> Error {
    Error::Log(LogError::Storage(format!(
        "replica log key with no value ({} bytes): {key:02x?}",
        key.len()
    )))
}

/// A live key whose value is present but shorter than `VALUE_PREFIX_WIDTH` —
/// too short to even hold a `data_id`. A distinct fault from
/// [`missing_value`]: an absent value can only mean RocksDB's iterator
/// contract was violated, while a truncated one points at corruption or a
/// writer bug in the value encoding, which is worth telling apart when
/// diagnosing without re-running (CLAUDE.md §5).
fn truncated_value(key: &[u8], len: usize) -> Error {
    Error::Log(LogError::Storage(format!(
        "replica log value too short to hold a data_id ({len} bytes) for key ({} bytes): {key:02x?}",
        key.len()
    )))
}

// ── storage access ──────────────────────────────────────────────────────────
//
// The facade's `max_key` / `scan_range` / `get`, as local functions over the
// node's [`Storage`]. Purge is a durable `DeleteRange`
// (see [`ReplicaLogStore::set_purge_threshold`]), so a retired prefix is
// genuinely absent — no read-side clamp to consult here any more.

/// The newest entry — the facade's `max_key`, whose secondary IS the log's
/// `max_hlc` because the secondary is a running maximum. One `seek_to_last`;
/// the sweep this replaced (`max_live_secondary`) was O(live entries).
fn newest_live_entry(storage: &Storage) -> Result<Option<(LogIndex, Hlc)>, Error> {
    let cf = storage.cf(REPLICA_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek_to_last();
    if let Some(key) = it.key() {
        // PINNING RULE: `key` borrows the iterator's current block and pins it.
        // Decode here; the borrow must not cross an `.await` or a batch commit.
        return decode_key(key).map(Some).ok_or_else(|| undecodable(key));
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb reverse scan (replica log): {e}"
        )))
    })?;
    Ok(None)
}

/// The exclusive index bound of `running_max <= threshold`.
///
/// The secondary is monotone, so those entries are a prefix — but the key is
/// indexed by `index`, so the boundary is not computable from `threshold`
/// alone. Range tombstones are honoured by iterators, so `seek_to_first` lands
/// on the first *live* key and this walk costs O(newly covered), not
/// O(everything ever purged).
fn purge_boundary(storage: &Storage, threshold: Hlc) -> Result<Option<LogIndex>, Error> {
    let cf = storage.cf(REPLICA_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek_to_first();
    let mut boundary = None;
    while let Some(key) = it.key() {
        // PINNING RULE: `key` borrows the iterator's current block and pins it.
        // Decode here; let the borrow end before the iterator moves.
        let (index, running_max) = decode_key(key).ok_or_else(|| undecodable(key))?;
        if running_max > threshold {
            break;
        }
        boundary = Some(LogIndex::next_after(Some(index)));
        it.next();
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb purge-boundary scan (replica log): {e}"
        )))
    })?;
    Ok(boundary)
}

/// What a range scan feeds. The payload is borrowed from the iterator's block
/// for the duration of the call only — a sink that keeps it must copy.
type EntrySink<'a> = dyn FnMut(LogIndex, DataId, &[u8]) -> bool + 'a;

/// Index-ordered scan of `[from, to]`. `sink` returning `false` stops it.
fn for_each_entry(
    storage: &Storage,
    from: LogIndex,
    to: LogIndex,
    sink: &mut EntrySink<'_>,
) -> Result<(), Error> {
    let cf = storage.cf(REPLICA_LOG_CF)?;
    let mut it = storage.scan(&cf);
    it.seek(encode_primary(from));
    while let Some(key) = it.key() {
        // PINNING RULE: `key` and `value` below both borrow the iterator's
        // current block and pin it. Neither may cross an `.await` or a batch
        // commit; the sink copies what it wants to keep.
        let (index, _running_max) = decode_key(key).ok_or_else(|| undecodable(key))?;
        if index > to {
            break;
        }
        let Some(value) = it.value() else {
            return Err(missing_value(key));
        };
        let (data_id, payload) =
            decode_value(value).ok_or_else(|| truncated_value(key, value.len()))?;
        if !sink(index, data_id, payload) {
            return Ok(());
        }
        it.next();
    }
    it.status().map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "rocksdb scan (replica log): {e}"
        )))
    })
}

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

pub struct PendingWrite {
    data: Data,
    /// Where to send the assigned `LogIndex` (or storage error) after the batch is
    /// durable. An `Arc<Sender>` instead of a per-write `Box<dyn FnOnce>` — the
    /// caller attaches the same sender to every write in its stream with an
    /// `Arc::clone` (a refcount bump), and the flush loop sends the raw result.
    /// Mapping to `WriteOutcome` happens upstream, at the caller's consumer.
    /// The `data_id` the caller needs to build a `WriteOutcome` downstream.
    completion: Completion,
    guard: Option<Admission>,
    /// When `submit()` enqueued this write — used to measure the channel batching
    /// wait (enqueue → flush-loop dequeue).
    enqueued_at: std::time::Instant,
}

#[derive(Debug, Clone, Copy, Default)]
struct FrontierState {
    index: Option<LogIndex>,
    /// Largest HLC over every entry ever appended — and, because the key's
    /// secondary is a running max, also the frontier entry's own secondary.
    /// The two used to be separate fields (`hlc` was the frontier entry's own
    /// HLC, needed by the purge clamp); under the running-max encoding they are
    /// the same number, and the clamp now speaks retirement's own coordinate.
    max_hlc: Option<Hlc>,
}

struct Frontier {
    inner: parking_lot::Mutex<FrontierState>,
}

impl Frontier {
    fn new(index: Option<LogIndex>, max_hlc: Option<Hlc>) -> Self {
        Self {
            inner: parking_lot::Mutex::new(FrontierState { index, max_hlc }),
        }
    }

    fn next_index(&self) -> LogIndex {
        LogIndex::next_after(self.inner.lock().index)
    }

    /// The frontier's index, if the log is non-empty.
    fn index(&self) -> Option<LogIndex> {
        self.inner.lock().index
    }

    fn max_hlc(&self) -> Option<Hlc> {
        self.inner.lock().max_hlc
    }

    /// Record a committed batch. `running_max` is the secondary written into
    /// the batch's last key, so it is already the max over the whole log — the
    /// fold happens in [`Committer::append_and_flush`], where the key needs
    /// it. Folding again here with `.max()` (rather than assigning) is
    /// belt-and-braces on top of that, not a replacement: today
    /// `Committer::append_and_flush` is the sole caller, so it is a no-op, but
    /// `max_hlc` is CLAUDE.md §8's *exact* field — the leader samples it
    /// across a quorum to build the joint-membership drain bound — so this
    /// keeps under-reporting it structurally impossible rather than merely an
    /// obligation on every future caller.
    fn advance(&self, index: LogIndex, running_max: Hlc) {
        let mut f = self.inner.lock();
        f.index = Some(index);
        f.max_hlc = Some(
            f.max_hlc
                .map_or(running_max, |current| current.max(running_max)),
        );
    }

    /// Clamp a purge threshold strictly below the frontier entry's secondary,
    /// so retirement can never cover the newest entry — its index is the
    /// durable record `newest_live_entry` recovers `next_index` from at open.
    fn clamp_threshold(&self, threshold: Hlc) -> Option<Hlc> {
        let frontier = self.inner.lock().max_hlc?;
        // checked_sub: a frontier at HLC 0 leaves nothing purgeable.
        let cap = Hlc(frontier.raw().checked_sub(1)?);
        Some(threshold.min(cap))
    }
}

struct BatchBuilder {
    max_batch: usize,
    max_payload: u64,
}

struct Batch {
    /// The accepted writes, in assigned-index order. This **is** the batch: the
    /// `WriteBatch` is built straight off it at commit time, so there is no
    /// second copy of the records and no second ordering to keep in step.
    staged: Vec<(LogIndex, Data, Admission)>,
    completions: Vec<Completion>,
}

impl BatchBuilder {
    fn new(max_batch: usize, max_payload: u64) -> Self {
        Self {
            max_batch,
            max_payload,
        }
    }

    /// Drain `first` plus up to `max_batch - 1` queued writes into one batch.
    ///
    /// Every record's value **is** the caller's blob — `Data.blob` is already
    /// a `Bytes`, so staging it costs a refcount bump and nothing is copied.
    ///
    /// This used to be `[log_max_hlc: u64 big-endian][blob]`, assembled in a
    /// scratch `Vec` purely so a restart could recover `FrontierState::max_hlc`
    /// (the frontier entry's own HLC will not do — writers append out of HLC
    /// order). That prefix was the only reason the scratch existed, and it cost
    /// a memcpy of every payload on the hottest path in the node.
    /// The bound is recovered from the last key's running-max secondary instead
    /// ([`newest_live_entry`]), so both are gone.
    ///
    /// A write that cannot be encoded (over-cap payload) is **rejected on its
    /// own** and simply left out: its completion is answered and it consumes no
    /// index. A `WriteBatch` is all-or-nothing and a storage error fail-stops
    /// the node, so letting one bad record abort the batch would strand every
    /// good write beside it and take the node down with it.
    ///
    /// `batch_count` — and therefore the assigned `LogIndex` — advances only for
    /// *accepted* writes, so a rejection never consumes an index and the log
    /// stays gapless.
    fn build(
        &self,
        first: PendingWrite,
        rx: &crossbeam_channel::Receiver<PendingWrite>,
        start_index: LogIndex,
    ) -> Result<Batch, Error> {
        let mut staged = Vec::with_capacity(self.max_batch);
        let mut completions = Vec::with_capacity(self.max_batch);
        let cap: u64 = u64::try_from(self.max_batch)
            .map_err(|_| Error::Log(LogError::Storage("max_batch too large".into())))?;
        let mut batch_count: u64 = 0;
        let mut pending = Some(first);
        while batch_count < cap {
            let mut pw = match pending.take() {
                Some(p) => p,
                None => {
                    let Ok(p) = rx.try_recv() else { break };
                    metrics::replica_append_queue_wait(p.enqueued_at.elapsed());
                    p
                }
            };
            let blob_len = u64::try_from(pw.data.blob.len()).unwrap_or(u64::MAX);
            if blob_len > self.max_payload.saturating_sub(FRAME_OVERHEAD_SLACK) {
                // Reject this write alone (as today): answer its completion,
                // consume no index, take the next.
                if pw
                    .completion
                    .try_send((
                        pw.data.data_id,
                        Err(zslg_log::Error::Log(LogError::Codec(format!(
                            "payload of {} bytes exceeds the replica log frame cap",
                            pw.data.blob.len(),
                        )))),
                    ))
                    .is_err()
                {
                    metrics::replica_outcome_dropped();
                }
                continue;
            }
            let index = LogIndex(
                start_index
                    .raw()
                    .checked_add(batch_count)
                    .ok_or_else(|| Error::Log(LogError::Storage("batch index overflow".into())))?,
            );
            let guard = pw
                .guard
                .take()
                .ok_or_else(|| Error::Log(LogError::Storage("guard already extracted".into())))?;
            staged.push((index, pw.data, guard));
            completions.push(pw.completion);
            batch_count = batch_count.saturating_add(1);
        }
        Ok(Batch {
            staged,
            completions,
        })
    }
}

struct Committer;

impl Committer {
    /// Steps 2 and 4 of CLAUDE.md §8's write-batch pattern, in one blocking
    /// body: open the batch, stage every drained record into it, commit it
    /// atomically, then fence it durable. Nothing is ACKed until this returns.
    ///
    /// `start_max` is the frontier's current `max_hlc`; the returned value is
    /// the running max after this batch, which is what the caller folds back
    /// into the frontier. It is threaded rather than recomputed because it is
    /// written INTO each key — see the key-codec comment.
    fn append_and_flush(
        storage: &Storage,
        staged: &[(LogIndex, Data, Admission)],
        start_max: Option<Hlc>,
    ) -> Result<Option<Hlc>, Error> {
        let cf = storage.cf(REPLICA_LOG_CF)?;
        // No global write lock: batches commit atomically and are ordered at
        // the WAL, so opening one never blocks on another writer.
        let mut batch = RocksBatch::new();
        let mut running = start_max;
        for (index, data, _) in staged {
            let hlc = data.data_id.hlc();
            let rm = running.map_or(hlc, |cur| cur.max(hlc));
            running = Some(rm);
            let key = encode_key(*index, rm);
            // Vectored so `data.blob` is copied into the batch buffer once,
            // exactly as a single put_cf would — no concatenation buffer.
            batch
                .put_vectored(
                    &cf,
                    &[std::io::IoSlice::new(&key)],
                    &[
                        std::io::IoSlice::new(&data.data_id.0),
                        std::io::IoSlice::new(&data.blob),
                    ],
                )
                .map_err(|e| {
                    Error::Log(LogError::Storage(format!(
                        "rocksdb vectored put (replica log): {e}"
                    )))
                })?;
        }
        storage.write(batch)?; // atomic across the batch
        storage.fence()?; // durable — the §8 commit fence
        Ok(running)
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

/// The live subscription and the frontier it is anchored to.
///
/// [`Seam::subscribe`] registers the fan-out before [`Seam::cut`] reads the
/// frontier. The flush loop advances the frontier before it broadcasts, so
/// an entry broadcast before registration is at or below the cut and the
/// historic scan serves it; one broadcast after registration is on the live
/// tail. The overlap is filtered by [`Seam::above`]; [`Segment::open`] fixes the order. Reversing the order
/// would lose entries committed between the snapshot and the registration.
struct ReplicaSeam {
    log: Arc<ReplicaLogStore>,
    from: Option<LogIndex>,
}

impl Seam for ReplicaSeam {
    type Item = Result<(LogIndex, Data), Error>;
    type Cut = Option<LogIndex>;

    fn subscribe(&self) -> Channel<Self::Item, crossbeam_combinators::Infinite> {
        let live = self.log.live_registry.subscribe_infinite();
        #[cfg(test)]
        subscribe_handoff_pause();
        live.map(Ok)
    }

    fn cut(&self) -> Self::Cut {
        self.log.frontier.index()
    }

    fn above(
        &self,
        live: Channel<Self::Item, crossbeam_combinators::Infinite>,
        cut: Self::Cut,
    ) -> Channel<Self::Item, crossbeam_combinators::Infinite> {
        live.filter_above::<LogIndex>(cut)
    }

    fn range(&self, cut: Option<LogIndex>) -> Channel<Self::Item, crossbeam_combinators::Finite> {
        let start = LogIndex::next_after(self.from);
        // How far behind this subscriber is resuming. Rising across
        // successive connects for the same peer = the deficit is diverging,
        // i.e. shedding has stopped being protection and started preventing
        // catch-up.
        metrics::subscription_catchup_backlog(match cut {
            Some(end) => end.raw().saturating_sub(start.raw()),
            None => 0,
        });
        match cut {
            // A subscriber already at the frontier gives `start == end + 1`:
            // an empty window, not an error — the routine state of a peer
            // that reconnects with nothing to catch up on.
            Some(end) if start <= end => self.log.pump_range(start, end),
            // Nothing to replay (no committed data, or already caught up):
            // live-only tail.
            _ => ReplicaLogStore::empty_historic(),
        }
    }
}

/// Flush loop — the single appender of the dense log.
///
/// Thin async wrapper: runs `flush_loop_blocking` on the blocking pool for its
/// lifetime and **joins it** on either exit — a detached body would outlive the
/// node's teardown, still holding the DB. The blocking body uses a plain
/// (timer-free) crossbeam `recv` for the idle wait — no per-batch async↔blocking
/// hop. The signature is kept (`async` + `CancellationToken` + `Result`) so all
/// spawn sites and tests are unchanged. A plain crossbeam `Receiver` (not
/// `crossbeam_combinators::Channel`): the loop only recv/try_recv-batches — no transforms
/// — so the `Channel` combinators would add nothing.
pub async fn flush_loop(
    weak: Weak<ReplicaLogStore>,
    rx: crossbeam_channel::Receiver<PendingWrite>,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let cancel_cl = cancel.clone();
    // The wake channel exists so cancellation can *interrupt* the idle `recv`,
    // not merely be noticed between batches: the cancel arm below joins the
    // thread, and a thread parked in the timerless `recv` would never reach the
    // join — `rx` closes only when the store drops its `Sender`, which happens
    // after shutdown has drained its workers. Dropping `wake_tx` disconnects
    // `wake_rx`, which is the signal; no value is ever sent.
    let (wake_tx, wake_rx) = crossbeam_channel::bounded::<()>(1);
    let mut handle =
        tokio::task::spawn_blocking(move || flush_loop_blocking(weak, &rx, &wake_rx, &cancel_cl));
    let res = tokio::select! {
        // Cancellation is only checked *between* batches, so at this point the
        // blocking body may still be inside `flush_one_batch_blocking` holding an
        // `Arc<ReplicaLogStore>` → `Arc<Storage>`. Returning here would let the
        // node finish teardown and drop the DB while that thread still uses
        // it — the reopen-LOCK race this migration already fixed for the retire
        // compactions and the local-store integrator. Join, don't detach.
        () = cancel.cancelled() => {
            drop(wake_tx);
            (&mut handle).await
        }
        r = &mut handle => r,
    };
    // A fail-stop error that lands after cancellation is still a real storage
    // failure; the shutdown machinery logs a worker error at `debug` during a
    // graceful drain, so log it here at `error` before returning it.
    let out = res.unwrap_or(Ok(()));
    if let Err(e) = &out {
        error!(error = %e, "replica flush loop failed");
    }
    out
}

/// The sole appender, on its own thread for the store's lifetime.
///
/// Idles in `recv` with no timer, so an empty log costs nothing. Three exits,
/// and between them nothing can wedge shutdown: the channel closing (the store's
/// only `Sender` lives inside it, so this doubles as "the store is gone"), the
/// `wake` channel disconnecting (cancellation, which must interrupt the idle
/// wait because the wrapper joins this thread), and the cancellation check
/// between batches for the loaded case.
///
/// Holds a `Weak` and upgrades only while a batch is in flight — parked in
/// `recv` it pins nothing, so the last `Arc<ReplicaLogStore>` really does free
/// the store.
fn flush_loop_blocking(
    weak: Weak<ReplicaLogStore>,
    rx: &crossbeam_channel::Receiver<PendingWrite>,
    wake: &crossbeam_channel::Receiver<()>,
    cancel: &CancellationToken,
) -> Result<(), Error> {
    info!("replica flush loop: starting");
    loop {
        let first = crossbeam_channel::select! {
            recv(rx) -> msg => match msg {
                Ok(w) => w,
                Err(_) => {
                    info!("replica flush loop: write channel closed");
                    return Ok(());
                }
            },
            // `wake` never carries a value: its disconnect is the cancellation
            // signal, sent by the async wrapper so it can then join this thread.
            recv(wake) -> _ => {
                info!("replica flush loop: cancelled while idle");
                return Ok(());
            }
        };
        let Some(store) = weak.upgrade() else {
            return Ok(());
        };

        // Keep draining inline while items are queued, rather than bouncing
        // back through `recv` between consecutive batches under load.
        let mut item = Some(first);
        while let Some(w) = item.take() {
            flush_one_batch_blocking(&store, w, rx)?;
            item = rx.try_recv().ok();
        }

        if cancel.is_cancelled() {
            info!("replica flush loop: cancelled");
            return Ok(());
        }
        // `store` drops here, so the idle wait above holds only the `Weak`.
    }
}

/// Drains `first` plus up to `max_flush_batch` more into one append + fsync, then
/// broadcasts and answers. All in one blocking body, which is what keeps the
/// header's fence intact.
///
/// **A commit error fails the whole loop, and with it the node.** Nothing was
/// ACKed, so per-batch recovery looks tempting — but a failed fsync may be
/// reported only once (fsyncgate), and continuing would let a later append
/// paper over a durable gap. Crashing loudly and recovering from known-good
/// state is the only option that cannot silently lose an entry.
fn flush_one_batch_blocking(
    store: &ReplicaLogStore,
    first: PendingWrite,
    rx: &crossbeam_channel::Receiver<PendingWrite>,
) -> Result<(), Error> {
    let flush_cycle_probe = metrics::replica_log_flush_cycle();
    metrics::replica_append_queue_wait(first.enqueued_at.elapsed());
    let next = store.frontier.next_index();
    let batch_build_probe = metrics::replica_log_batch_build();
    let batch = store.batch_builder.build(first, rx, next)?;
    batch_build_probe.finish(OpState::Success);

    let flush_entries = batch.staged.len();
    if flush_entries == 0 {
        metrics::replica_log_queue_depth(rx.len());
        return Ok(());
    }

    let mut flush_bytes: usize = 0;
    for (_, data, _) in &batch.staged {
        flush_bytes = flush_bytes.saturating_add(data.blob.len());
    }

    let drained = flush_entries;
    metrics::replica_log_flush_trigger(if drained >= store.max_flush_batch {
        FlushTrigger::BatchFull
    } else {
        FlushTrigger::Idle
    });

    let commit_t0 = std::time::Instant::now();
    match Committer::append_and_flush(&store.storage, &batch.staged, store.frontier.max_hlc()) {
        Ok(committed_max) => {
            metrics::replica_log_commit_elapsed(commit_t0.elapsed());
            let postcommit_probe = metrics::replica_append_postcommit_probe();

            let Some((last_index, _, _)) = batch.staged.last() else {
                return Err(Error::Log(LogError::Storage(
                    "internal: empty staged batch".into(),
                )));
            };
            // Every staged entry is folded in `append_and_flush` (it is what the
            // key carries), so a non-empty batch always yields a running max.
            let Some(running_max) = committed_max else {
                return Err(Error::Log(LogError::Storage(
                    "internal: non-empty batch produced no running max".into(),
                )));
            };
            store.frontier.advance(*last_index, running_max);

            zslg_log::metrics::log_writes(LogType::Replica, flush_entries);
            zslg_log::metrics::log_index_frontier(LogType::Replica, last_index.raw());
            metrics::replica_log_flush_bytes(flush_bytes);
            metrics::replica_log_flush_entries(flush_entries);
            metrics::replica_log_flushed();
            for (i, (index, data, guard)) in batch.staged.into_iter().enumerate() {
                let data_id = data.data_id;
                // `Stopped` only while the store is being dropped: the live
                // tail has no reader left to miss it.
                let _ = store.live.publish((index, data));
                metrics::replica_log_append_probe().finish(OpState::Success);
                // `try_send`, never `send`: this runs on the replica's *single
                // shared* flush thread, so blocking here for one slow connection
                // would stop appends for every connection on the node. Mirrors
                // the fan-out `try_send` above.
                //
                // Dropping an outcome is safe by construction — the write IS
                // durable. The writer's reconnect path resends every un-acked
                // write and the whole downstream dedups by `data_id` (CLAUDE.md
                // §8), so a drop costs a resend, never correctness.
                if batch.completions[i].try_send((data_id, Ok(index))).is_err() {
                    metrics::replica_outcome_dropped();
                }
                drop(guard);
            }
            drop(postcommit_probe);
            metrics::replica_log_queue_depth(rx.len());
            flush_cycle_probe.finish(OpState::Success);
            Ok(())
        }
        Err(e) => {
            metrics::replica_log_commit_elapsed(commit_t0.elapsed());
            let msg = format!("replica log append failed: {e}");
            for (i, (_, data, _)) in batch.staged.iter().enumerate() {
                if batch.completions[i]
                    .try_send((
                        data.data_id,
                        Err(zslg_log::Error::Log(LogError::Storage(msg.clone()))),
                    ))
                    .is_err()
                {
                    metrics::replica_outcome_dropped();
                }
            }
            Err(e)
        }
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
