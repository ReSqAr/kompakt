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

use std::sync::{Arc, Weak};

use rust_rocksdb::WriteBatch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::database::Subsystem;
use crate::database::storage::Storage;
use crate::error::{Error, ToStorage};
use crate::hlc::{Hlc, HlcGuard};
use crate::ids::{DataId, LogIndex};
use crate::meta::Data;
use crate::metrics::{self, FlushTrigger, LogType, OpState, ShedSite};
use crossbeam_combinators::{Channels, Ranged, Tail, blend_chain};

/// Fallback sizing for [`ReplicaLogStore::open_with_flush`], the
/// directory-shaped entry point used by the test constructors. Production goes
/// through [`ReplicaLogStore::open`], which carries the operator-configured
/// values from `ReplicaLogConfig`. Kept in step with `ReplicaLogConfig::for_test`
/// by `open_engine_defaults_match_the_config_defaults`.
#[cfg(test)]
const DEFAULT_SUBSCRIBE_CHANNEL_CAPACITY: usize = 1024;
#[cfg(test)]
const DEFAULT_MAX_FLUSH_BATCH: usize = 1024;

/// Headroom for the non-payload part of a record (the 16-byte key, the value's
/// 32-byte `data_id` prefix, and RocksDB's per-record framing — well under
/// 1 KiB). A blob above the payload cap minus this is rejected per-write here
/// rather than at commit, where it would cost the whole batch.
const FRAME_OVERHEAD_SLACK: u64 = 1024;

/// Per-record payload cap. RocksDB has no frame cap of its own, so the limit is
/// stated here.
const MAX_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;

type Completion = Arc<crossbeam_channel::Sender<(DataId, Result<LogIndex, Error>)>>;

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
    Error::Storage(format!(
        "undecodable replica log key ({} bytes): {key:02x?}",
        key.len()
    ))
}

/// A live key whose value is missing. Unreachable through RocksDB's iterator
/// contract (`value()` is `None` only when `key()` is), but the alternative —
/// substituting an empty payload — would broadcast an empty `Data` to
/// subscribers rather than fail. §20: never serve garbage silently.
fn missing_value(key: &[u8]) -> Error {
    Error::Storage(format!(
        "replica log key with no value ({} bytes): {key:02x?}",
        key.len()
    ))
}

/// A live key whose value is present but shorter than `VALUE_PREFIX_WIDTH` —
/// too short to even hold a `data_id`. A distinct fault from
/// [`missing_value`]: an absent value can only mean RocksDB's iterator
/// contract was violated, while a truncated one points at corruption or a
/// writer bug in the value encoding, which is worth telling apart when
/// diagnosing without re-running (CLAUDE.md §5).
fn truncated_value(key: &[u8], len: usize) -> Error {
    Error::Storage(format!(
        "replica log value too short to hold a data_id ({len} bytes) for key ({} bytes): {key:02x?}",
        key.len()
    ))
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
    let cf = storage.cf(Subsystem::ReplicaLog)?;
    let mut it = storage.db().raw_iterator_cf(&cf);
    it.seek_to_last();
    if let Some(key) = it.key() {
        // PINNING RULE: `key` borrows the iterator's current block and pins it.
        // Decode here; the borrow must not cross an `.await` or a batch commit.
        return decode_key(key).map(Some).ok_or_else(|| undecodable(key));
    }
    it.status()
        .map_err(|e| Error::Storage(format!("rocksdb reverse scan (replica log): {e}")))?;
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
    let cf = storage.cf(Subsystem::ReplicaLog)?;
    let mut it = storage.db().raw_iterator_cf(&cf);
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
    it.status()
        .map_err(|e| Error::Storage(format!("rocksdb purge-boundary scan (replica log): {e}")))?;
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
    let cf = storage.cf(Subsystem::ReplicaLog)?;
    let mut it = storage.db().raw_iterator_cf(&cf);
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
    it.status()
        .map_err(|e| Error::Storage(format!("rocksdb scan (replica log): {e}")))
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

pub(in crate::replica) struct PendingWrite {
    data: Data,
    /// Where to send the assigned `LogIndex` (or storage error) after the batch is
    /// durable. An `Arc<Sender>` instead of a per-write `Box<dyn FnOnce>` — the
    /// caller attaches the same sender to every write in its stream with an
    /// `Arc::clone` (a refcount bump), and the flush loop sends the raw result.
    /// Mapping to `WriteOutcome` happens upstream, at the caller's consumer.
    /// The `data_id` the caller needs to build a `WriteOutcome` downstream.
    completion: Completion,
    guard: Option<HlcGuard>,
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
    staged: Vec<(LogIndex, Data, HlcGuard)>,
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
            .map_err(|_| Error::Storage("max_batch too large".into()))?;
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
                        Err(Error::Codec(format!(
                            "payload of {} bytes exceeds the replica log frame cap",
                            pw.data.blob.len(),
                        ))),
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
                    .ok_or_else(|| Error::Storage("batch index overflow".into()))?,
            );
            let guard = pw
                .guard
                .take()
                .ok_or_else(|| Error::Storage("guard already extracted".into()))?;
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
        staged: &[(LogIndex, Data, HlcGuard)],
        start_max: Option<Hlc>,
    ) -> Result<Option<Hlc>, Error> {
        let cf = storage.cf(Subsystem::ReplicaLog)?;
        // No global write lock: batches commit atomically and are ordered at
        // the WAL, so opening one never blocks on another writer.
        let mut batch = WriteBatch::default();
        let mut running = start_max;
        for (index, data, _) in staged {
            let hlc = data.data_id.hlc();
            let rm = running.map_or(hlc, |cur| cur.max(hlc));
            running = Some(rm);
            let key = encode_key(*index, rm);
            // Vectored so `data.blob` is copied into the batch buffer once,
            // exactly as a single put_cf would — no concatenation buffer.
            batch
                .put_cf_vectored(
                    &cf,
                    &[std::io::IoSlice::new(&key)],
                    &[
                        std::io::IoSlice::new(&data.data_id.0),
                        std::io::IoSlice::new(&data.blob),
                    ],
                )
                .map_err(|e| Error::Storage(format!("rocksdb vectored put (replica log): {e}")))?;
        }
        storage.write(batch)?; // atomic across the batch
        storage.fence()?; // durable — the §8 commit fence
        Ok(running)
    }
}

/// The replica's append-only log. The single writer task (`flush_loop`) holds
/// a `Weak<ReplicaLogStore>` and upgrades only while committing a batch, so
/// dropping the last strong handle frees the store promptly.
pub(crate) struct ReplicaLogStore {
    storage: Arc<Storage>,
    frontier: Frontier,
    batch_builder: BatchBuilder,
    fan_out: parking_lot::Mutex<Vec<crossbeam_channel::Sender<(LogIndex, Data)>>>,
    /// Sync MPSC into the flush loop. crossbeam (not tokio): `submit` is called
    /// from blocking threads and never `.await`s the enqueue.
    write_tx: crossbeam_channel::Sender<PendingWrite>,
    /// Live-tail depth handed to each subscriber. See
    /// [`ReplicaLogConfig::subscribe_channel_capacity`](crate::replica::ReplicaLogConfig::subscribe_channel_capacity)
    /// — it must stay >= `max_flush_batch` or one batch sheds every
    /// subscriber, which `ReplicaLogConfig::validate` enforces.
    subscribe_channel_capacity: usize,
    /// Max records drained into one append + fsync.
    max_flush_batch: usize,
}

impl ReplicaLogStore {
    pub(in crate::replica) async fn open(
        storage: Arc<Storage>,
        write_tx: crossbeam_channel::Sender<PendingWrite>,
        config: &crate::replica::ReplicaLogConfig,
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
            metrics::log_index_frontier(LogType::Replica, idx.raw());
        }

        let (recovered_index, recovered_max_hlc) = match frontier {
            Some((idx, max_hlc)) => (Some(idx), Some(max_hlc)),
            None => (None, None),
        };
        let frontier = Frontier::new(recovered_index, recovered_max_hlc);
        let batch_builder = BatchBuilder::new(max_flush_batch, max_payload_bytes);

        let fan_out = parking_lot::Mutex::new(Vec::new());
        Ok(Self {
            storage,
            frontier,
            batch_builder,
            fan_out,
            write_tx,
            subscribe_channel_capacity,
            max_flush_batch,
        })
    }

    /// Open the store (as an `Arc`, so the flush loop can hold a `Weak`) and
    /// spawn the flush-loop background task. A [`Storage`] of this store's own
    /// is opened at `dir` rather than injected, so every test constructor keeps
    /// the directory-shaped signature it had.
    #[cfg(test)]
    pub(crate) async fn open_with_flush(
        dir: std::path::PathBuf,
        cancel: CancellationToken,
    ) -> Result<(Arc<Self>, tokio::task::JoinHandle<Result<(), Error>>), Error> {
        let (write_tx, write_rx) = crossbeam_channel::bounded(
            crate::replica::ReplicaLogConfig::for_test(crate::ids::NodeId::new_v4())
                .write_channel_capacity
                .get(),
        );
        let storage = Storage::open_for_test(&dir)?;
        let store = Arc::new(
            Self::open_sized(
                storage,
                write_tx,
                DEFAULT_SUBSCRIBE_CHANNEL_CAPACITY,
                DEFAULT_MAX_FLUSH_BATCH,
            )
            .await?,
        );
        let weak = Arc::downgrade(&store);
        let handle = tokio::spawn(flush_loop(weak, write_rx, cancel));
        Ok((store, handle))
    }

    #[must_use]
    pub(in crate::replica) fn frontier(&self) -> Option<LogIndex> {
        self.frontier.index()
    }

    /// Upper bound on the HLC of every entry in this log — the membership drain
    /// bound (see `ReplicaStatus::max_hlc`). Updated before a write is
    /// acknowledged, so any HLC a writer has seen acked is included.
    #[must_use]
    pub(in crate::replica) fn max_hlc(&self) -> Option<Hlc> {
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
    pub(in crate::replica) fn submit(&self, record: Data, guard: HlcGuard, completion: Completion) {
        let data_id = record.data_id;
        let pending = PendingWrite {
            data: record,
            completion,
            guard: Some(guard),
            enqueued_at: std::time::Instant::now(),
        };
        if let Err(crossbeam_channel::SendError(pending)) = self.write_tx.send(pending) {
            // Shutdown: the flush loop is gone. Resolve inline so the caller still
            // hears back (the HlcGuard drops with the PendingWrite here).
            let _ = pending.completion.send((
                data_id,
                Err(Error::Storage("replica log store shut down".into())),
            ));
        }
    }

    pub(in crate::replica) fn subscribe_from(
        self: &Arc<Self>,
        from: Option<LogIndex>,
    ) -> Channels<Result<(LogIndex, Data), Error>> {
        let seam = ReplicaSeam {
            log: Arc::clone(self),
            from,
        };
        blend_chain(&seam, &seam)
    }

    /// An empty historic phase: complete by construction, so it is closed
    /// rather than aborted — the window *was* fully served, it just held
    /// nothing.
    fn empty_historic() -> Channels<Result<(LogIndex, Data), Error>> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        drop(tx);
        let (ch, closer) = Channels::from(rx, "replica historic tail");
        closer.close();
        ch
    }

    /// The historical catch-up phase of a subscription: a **finite** scan of
    /// `[from, to]`. Fire-and-forget `spawn_blocking`, so the `Closer` is what
    /// distinguishes "the window was fully served" from "the scan died" — a
    /// panic here would otherwise truncate a subscriber's catch-up and still look
    /// like a clean handoff to the live tail.
    fn pump_range(
        &self,
        from: LogIndex,
        to: LogIndex,
    ) -> Channels<Result<(LogIndex, Data), Error>> {
        debug_assert!(from <= to);
        let (tx, rx) = crossbeam_channel::bounded(self.subscribe_channel_capacity);
        let (ch, closer) = Channels::from(rx, "replica historic tail");
        let storage = Arc::clone(&self.storage);
        tokio::task::spawn_blocking(move || {
            // PINNING RULE: `payload` is borrowed from the iterator's pinned
            // block for the duration of the call, and this `Data` is sent
            // onward, so it takes an owned copy — the copy happens only here,
            // where the value outlives the iterator.
            let scan = for_each_entry(&storage, from, to, &mut |idx, data_id, payload| {
                let blob = bytes::Bytes::copy_from_slice(payload);
                tx.send(Ok((idx, Data { data_id, blob }))).is_ok()
            });
            if let Err(e) = scan {
                let _ = tx.send(Err(e));
            }
            // Normal path only: a panic in the scan drops `closer`, so the
            // chained subscription reports `End::Aborted` instead of a clean end.
            closer.close();
        });
        ch
    }

    /// Test-only async wrapper around [`submit`](Self::submit) that awaits the
    /// assigned `LogIndex` — keeps the existing `log.rs` unit tests unchanged.
    #[cfg(test)]
    pub(in crate::replica) async fn append(
        &self,
        record: Data,
        guard: HlcGuard,
    ) -> Result<LogIndex, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // Bridge: use a crossbeam pair to get the result from the flush-loop thread
        // into the test. A plain crossbeam `recv()` would block the test's tokio
        // thread and deadlock the flush loop.
        let (cb_tx, cb_rx) = crossbeam_channel::bounded(1);
        self.submit(record, guard, Arc::new(cb_tx));
        tokio::task::spawn_blocking(move || {
            let res = cb_rx
                .recv()
                .map_err(|_| Error::Storage("flush task exited before committing write".into()))
                .and_then(|(_, inner)| inner);
            let _ = tx.send(res);
        });
        rx.await
            .map_err(|_| Error::Storage("flush task exited before committing write".into()))?
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
    pub(in crate::replica) fn set_purge_threshold(&self, threshold: Hlc) -> Result<(), Error> {
        let Some(clamped) = self.frontier.clamp_threshold(threshold) else {
            return Ok(());
        };
        let Some(boundary) = purge_boundary(&self.storage, clamped)? else {
            return Ok(());
        };
        let cf = self.storage.cf(Subsystem::ReplicaLog)?;
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

/// The live subscription and the frontier it is anchored to.
///
/// [`Tail::subscribe`] registers the fan-out before [`Tail::cut`] reads the
/// frontier. The flush loop advances the frontier before it broadcasts, so
/// an entry broadcast before registration is at or below the cut and the
/// historic scan serves it; one broadcast after registration is on the live
/// tail. The overlap is filtered by [`Tail::above`]. Reversing the order
/// would lose entries committed between the snapshot and the registration.
struct ReplicaSeam {
    log: Arc<ReplicaLogStore>,
    from: Option<LogIndex>,
}

impl Tail for ReplicaSeam {
    type Item = Result<(LogIndex, Data), Error>;
    type Cut = Option<LogIndex>;

    fn subscribe(&self) -> Channels<Self::Item> {
        let (live_tx, live_rx) = crossbeam_channel::bounded(self.log.subscribe_channel_capacity);
        self.log.fan_out.lock().push(live_tx);
        #[cfg(test)]
        subscribe_handoff_pause();
        Channels::from_infinite(live_rx, "replica live tail").map(Ok)
    }

    fn cut(&self) -> Self::Cut {
        self.log.frontier.index()
    }

    fn above(&self, live: Channels<Self::Item>, cut: Self::Cut) -> Channels<Self::Item> {
        live.filter_map(move |item| match item {
            Ok((idx, _)) if cut.is_some_and(|c| idx <= c) => None,
            other => Some(other),
        })
    }
}

impl Ranged<Option<LogIndex>> for ReplicaSeam {
    type Item = Result<(LogIndex, Data), Error>;

    fn range(&self, cut: Option<LogIndex>) -> Channels<Self::Item> {
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
/// Thin async wrapper: runs [`flush_loop_blocking`] on the blocking pool for its
/// lifetime and **joins it** on either exit — a detached body would outlive the
/// node's teardown, still holding the DB. The blocking body uses a plain
/// (timer-free) crossbeam `recv` for the idle wait — no per-batch async↔blocking
/// hop. The signature is kept (`async` + `CancellationToken` + `Result`) so all
/// spawn sites and tests are unchanged. A plain crossbeam `Receiver` (not
/// `crossbeam_combinators::Channels`): the loop only recv/try_recv-batches — no transforms
/// — so the `Channels` combinators would add nothing.
pub(in crate::replica) async fn flush_loop(
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
                return Err(Error::Storage("internal: empty staged batch".into()));
            };
            // Every staged entry is folded in `append_and_flush` (it is what the
            // key carries), so a non-empty batch always yields a running max.
            let Some(running_max) = committed_max else {
                return Err(Error::Storage(
                    "internal: non-empty batch produced no running max".into(),
                ));
            };
            store.frontier.advance(*last_index, running_max);

            metrics::log_writes(LogType::Replica, flush_entries);
            metrics::log_index_frontier(LogType::Replica, last_index.raw());
            metrics::log_subscriptions_count(store.fan_out.lock().len());
            metrics::replica_log_flush_bytes(flush_bytes);
            metrics::replica_log_flush_entries(flush_entries);
            metrics::replica_log_flushed();
            let mut shed_this_batch: u64 = 0;
            for (i, (index, data, guard)) in batch.staged.into_iter().enumerate() {
                let data_id = data.data_id;
                store
                    .fan_out
                    .lock()
                    .retain(|tx| match tx.try_send((index, data.clone())) {
                        Ok(()) => true,
                        // Deliberate load shedding, unchanged: a subscriber that
                        // cannot keep up is evicted rather than allowed to block
                        // this single shared flush thread. Counted so the choice
                        // is visible — until this metric existed, the eviction
                        // was silent here and surfaced only on the *learner* as
                        // "stream ended", which names the peer, not the cause.
                        Err(crossbeam_channel::TrySendError::Full(_)) => {
                            metrics::subscription_shed(ShedSite::ReplicaFanout);
                            shed_this_batch = shed_this_batch.saturating_add(1);
                            false
                        }
                        Err(crossbeam_channel::TrySendError::Disconnected(_)) => false,
                    });
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
            if shed_this_batch > 0 {
                // One line per flush batch, not per eviction: a batch is at most
                // `max_flush_batch` records, so this is naturally rate-limited by the
                // flush cadence. `warn` because it is degraded-but-functional —
                // the subscriber must reconnect and re-scan from its cursor.
                tracing::warn!(
                    shed = shed_this_batch,
                    remaining_subscribers = store.fan_out.lock().len(),
                    "replica log: evicted subscriber(s) with a full live channel \
                     (load shedding; they reconnect and resume from their cursor)",
                );
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
                    .try_send((data.data_id, Err(Error::Storage(msg.clone()))))
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
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::TempDir;

    use super::*;
    use crate::api::{LocalStoreMut, LocalStoreView};

    use crate::hlc::Hlc;
    use crate::hlc::HlcGate;
    use crate::ids::{DataId, LogIndex, NodeId};
    use crate::learner::LocalStore;

    use tokio_util::sync::CancellationToken;

    async fn integrate_batch(
        store: &LocalStore,
        source: NodeId,
        records: Vec<(LogIndex, Data)>,
    ) -> Result<(), crate::Error> {
        if records.is_empty() {
            return Ok(());
        }
        let (tx, rx) = crossbeam_channel::bounded(records.len().max(1));
        for (idx, rec) in records {
            let _ = tx.send(Ok((source, idx, rec)));
        }
        let (ch, closer) = crossbeam_combinators::Channels::from(rx, "test integrate");
        // Close before the last sender drops, or the consumer forms its
        // verdict as "disconnected, not yet closed" — an abort.
        closer.close();
        drop(tx);
        store.integrate_loop(ch).await
    }

    /// Create a guard for a test HLC. A fresh `HlcGate` has no cutoff, so
    /// `check_hlc` always succeeds.
    fn test_guard(hlc: Hlc) -> HlcGuard {
        HlcGate::new()
            .check_hlc(hlc)
            .expect("fresh cutoff admits any HLC")
    }

    /// Open a store at `dir` with its flush loop, at the default sizing —
    /// [`ReplicaLogStore::open_with_flush`] under a name the tests already use.
    /// The `Storage` (and so the transitional root, budget and CF list) comes
    /// from the production helper, so tests cannot drift from it.
    async fn open_for_test(
        dir: PathBuf,
    ) -> Result<
        (
            Arc<ReplicaLogStore>,
            tokio::task::JoinHandle<Result<(), Error>>,
        ),
        Error,
    > {
        ReplicaLogStore::open_with_flush(dir, CancellationToken::new()).await
    }

    /// A point read, for the tests that assert the raw stored shape. Production
    /// has no point-read path: the log is scanned, never probed.
    fn get_entry(storage: &Storage, index: LogIndex) -> Result<Option<(DataId, Bytes)>, Error> {
        let mut found = None;
        for_each_entry(storage, index, index, &mut |_, data_id, payload| {
            // PINNING RULE: `payload` borrows the iterator's pinned block, so it
            // is copied here rather than escaping the callback.
            found = Some((data_id, Bytes::copy_from_slice(payload)));
            false
        })?;
        Ok(found)
    }

    /// The oldest surviving index — the assertion a durable purge needs, since
    /// there is no production accessor for it (the log is scanned from a
    /// cursor, never probed for its floor).
    fn first_live_index(storage: &Storage) -> Result<Option<LogIndex>, Error> {
        let mut first = None;
        for_each_entry(
            storage,
            LogIndex::ZERO,
            LogIndex(u64::MAX),
            &mut |idx, _, _| {
                first = Some(idx);
                false
            },
        )?;
        Ok(first)
    }

    /// Every stored key, decoded. Key-only: no value copies, and the secondary
    /// is what the monotonicity assertions are about.
    fn scan_keys(storage: &Storage) -> Result<Vec<(LogIndex, Hlc)>, Error> {
        let cf = storage.cf(Subsystem::ReplicaLog)?;
        let mut it = storage.db().raw_iterator_cf(&cf);
        it.seek_to_first();
        let mut out = Vec::new();
        while let Some(key) = it.key() {
            out.push(decode_key(key).ok_or_else(|| undecodable(key))?);
            it.next();
        }
        it.status()
            .map_err(|e| Error::Storage(format!("rocksdb key scan (replica log): {e}")))?;
        Ok(out)
    }

    use crate::tests::{make_data, upstream};

    async fn fresh_store() -> (TempDir, Arc<ReplicaLogStore>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");
        let (store, _flush) = open_for_test(path).await.expect("open_for_test");
        (dir, store)
    }

    async fn store_at(path: &Path) -> Arc<ReplicaLogStore> {
        let (store, _flush) = open_for_test(path.to_path_buf())
            .await
            .expect("open_for_test");
        store
    }

    /// CLAUDE.md §20: a subscriber stream that goes backwards is the
    /// log-corruption signal, and `subscribe_writer_loop` closes the connection
    /// on it. So the broadcast order has to be the index order, however many
    /// writers are pushing.
    ///
    /// The single appender gives this by construction — it assigns indexes and
    /// broadcasts on one thread, so there is no window. The test exists because
    /// the alternative shape does not: when writers committed and published
    /// their own batches, two of them could become contiguous in order and
    /// still reach the fan-out out of order, and this fired ~100-130 times per
    /// 6 s loadgen rep at `--inflight 128`. Keep this pinned against any future
    /// attempt to move publishing back onto the writers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_broadcast_in_index_order() {
        const WRITERS: u8 = 24;

        let (_dir, store) = fresh_store().await;
        let mut ch = store.subscribe_from(None);

        // Keep the completion receivers alive so an ACK is not counted dropped.
        let mut keep = Vec::new();
        let mut writers = Vec::new();
        for i in 1..=WRITERS {
            let (tx, rx) = crossbeam_channel::bounded(1);
            keep.push(rx);
            let store = Arc::clone(&store);
            writers.push(tokio::task::spawn_blocking(move || {
                store.submit(
                    make_data(i, u64::from(i) * 100, &[i]),
                    test_guard(Hlc::ZERO),
                    Arc::new(tx),
                );
            }));
        }
        for w in writers {
            w.await.expect("writer");
        }

        let mut seen: Vec<u64> = Vec::new();
        for _ in 0..WRITERS {
            let (idx, _) = ch.recv().expect("record").expect("ok");
            seen.push(idx.raw());
        }
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        assert_eq!(
            seen, sorted,
            "broadcast went backwards — a subscriber closes the stream on this \
             (CLAUDE.md §20)",
        );
        assert_eq!(
            seen,
            (0..u64::from(WRITERS)).collect::<Vec<_>>(),
            "the log must be gapless and start at 0",
        );
    }

    #[tokio::test]
    async fn append_assigns_zero_and_advances_frontier() {
        let (_dir, store) = fresh_store().await;
        assert!(store.frontier().is_none());

        let idx0 = store
            .append(
                make_data(1, u64::from(1u8) * 100, b"hello"),
                test_guard(Hlc::ZERO),
            )
            .await
            .expect("append");
        assert_eq!(idx0, LogIndex::ZERO);
        assert_eq!(store.frontier(), Some(LogIndex::ZERO));

        let idx1 = store
            .append(
                make_data(2, u64::from(2u8) * 100, b"world"),
                test_guard(Hlc::ZERO),
            )
            .await
            .expect("append");
        assert_eq!(idx1, LogIndex(1));
        assert_eq!(store.frontier(), Some(LogIndex(1)));
    }

    /// A connection whose outcome channel is full must not stall the flush loop:
    /// it is shared by every connection on the node, so blocking there would stop
    /// appends cluster-wide for one slow consumer. The completion is dropped
    /// (counted as `zslg_replica_outcome_dropped_total`) and the writes still commit —
    /// the writer resends un-acked writes on reconnect.
    #[tokio::test]
    async fn outcome_channel_full_does_not_block_the_flush_loop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, flush_handle) = open_for_test(dir.path().join("replica-log"))
            .await
            .expect("open_for_test");

        // A completion channel with no room at all: every send would block.
        let (full_tx, full_rx) = crossbeam_channel::bounded(1);
        full_tx
            .send((DataId([0u8; 32]), Ok(LogIndex(0))))
            .expect("prefill");
        let stuck: Completion = Arc::new(full_tx);

        for i in 0..8u8 {
            store.submit(
                make_data(i, 100u64.saturating_add(u64::from(i)), b"x"),
                test_guard(Hlc::ZERO),
                stuck.clone(),
            );
        }

        // The flush loop must keep making progress: a later write on a healthy
        // channel still completes, and the frontier advanced past all 8.
        let idx = store
            .append(make_data(200, 5_000, b"healthy"), test_guard(Hlc::ZERO))
            .await
            .expect("flush loop must still serve other connections");
        assert_eq!(idx, LogIndex(8), "all 8 stuck writes committed before it");
        assert!(!flush_handle.is_finished(), "flush loop must stay alive");
        drop(full_rx);
    }

    /// An over-cap payload is rejected **on its own**: the writes queued beside
    /// it in the same batch still commit, the flush loop stays alive, and the
    /// rejected write consumes no `LogIndex` (the log stays gapless). Letting one
    /// bad record abort the batch would strand every good write and fail-stop the
    /// node — a `WriteBatch` is all-or-nothing.
    #[tokio::test]
    async fn oversized_write_is_rejected_without_failing_the_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, flush_handle) = open_for_test(dir.path().join("replica-log"))
            .await
            .expect("open_for_test");

        // Cap is `max_payload_bytes - FRAME_OVERHEAD_SLACK`; overshoot it.
        let too_big = vec![0u8; 64 * 1024 * 1024];
        let (ok_a, bad, ok_b) = tokio::join!(
            store.append(make_data(1, 100, b"a"), test_guard(Hlc::ZERO)),
            store.append(make_data(2, 200, &too_big), test_guard(Hlc::ZERO)),
            store.append(make_data(3, 300, b"b"), test_guard(Hlc::ZERO)),
        );

        let err = bad.expect_err("over-cap write must be rejected");
        assert!(
            matches!(err, Error::Codec(ref m) if m.contains("exceeds the replica log frame cap")),
            "got: {err:?}"
        );
        let a = ok_a.expect("neighbouring write must still commit");
        let b = ok_b.expect("neighbouring write must still commit");

        // The rejection consumed no index: the two survivors are consecutive.
        let (lo, hi) = if a.raw() < b.raw() { (a, b) } else { (b, a) };
        assert_eq!(
            hi.raw(),
            lo.raw().saturating_add(1),
            "a rejected write must not consume a LogIndex ({lo:?} → {hi:?})"
        );

        // The flush loop is still running (fail-stop would have ended it).
        assert!(!flush_handle.is_finished(), "flush loop must stay alive");
    }

    /// A storage write failure is a FATAL storage fault: the flush loop must
    /// fail-stop (return `Err`, which the `Shutdown` supervisor turns into a
    /// node shutdown) rather than swallow the error and keep accepting writes.
    /// Carrying on is unsafe — on Linux a failed fsync can be reported only once
    /// (fsyncgate), so a later "successful" flush could leave a silent durable
    /// gap. This pins the fail-stop so it can't silently regress.
    #[tokio::test]
    async fn append_failure_fails_the_flush_loop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");
        let (store, flush_handle) = open_for_test(path.clone()).await.expect("open_for_test");

        store
            .append(make_data(1, 100, b"a"), test_guard(Hlc::ZERO))
            .await
            .expect("first append succeeds");

        // The failure is injected rather than provoked through the filesystem:
        // RocksDB writes into an already-open WAL and buffers in the memtable,
        // so no permission trick (the read-only directory this test used to
        // rely on) fails a write deterministically. The claim is unchanged.
        store.storage.fail_writes_from_now_on();
        let err = store
            .append(make_data(2, 200, b"a"), test_guard(Hlc::ZERO))
            .await
            .expect_err("a storage write failure must surface to the caller");
        assert!(err.to_string().contains("append failed"), "got: {err}");

        // ...and the tracked flush loop terminates with `Err` (fail-stop)
        // rather than hanging or continuing to run.
        let joined = tokio::time::timeout(std::time::Duration::from_secs(5), flush_handle)
            .await
            .expect("flush loop must terminate (fail-stop), not hang");
        assert!(
            matches!(joined, Ok(Err(_))),
            "a storage write failure must fail the flush loop, got {joined:?}"
        );
    }

    /// Cross-batch DataId dedup is best-effort (CLAUDE.md §8):
    /// DataId has 64 random bits, making collision effectively impossible in
    /// practice. The hot path only checks intra-batch duplicates via `seen:
    /// HashSet`; there is no durable reverse index anymore at all.
    #[tokio::test]
    async fn cross_batch_duplicate_is_not_rejected() {
        let (_dir, store) = fresh_store().await;
        let d = make_data(1, u64::from(1u8) * 100, b"hello");
        let hlc = d.data_id.hlc();
        store
            .append(d.clone(), test_guard(hlc))
            .await
            .expect("first ok");
        // Cross-batch duplicate — not rejected (best-effort dedup).
        // In production this path is unreachable: DataId carries 64 random bits.
        store
            .append(d, test_guard(hlc))
            .await
            .expect("cross-batch dup accepted (best-effort dedup)");
        assert_eq!(store.frontier(), Some(LogIndex(1)));
    }

    #[tokio::test]
    async fn frontier_persists_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");
        {
            let (store, flush) = open_for_test(path.clone()).await.expect("open_for_test");
            for i in 1..=3u8 {
                store
                    .append(
                        make_data(i, u64::from(i) * 100, &[i]),
                        test_guard(Hlc::ZERO),
                    )
                    .await
                    .expect("ok");
            }
            drop(store);
            let _ = flush.await;
        }
        let store = store_at(&path).await;
        assert_eq!(store.frontier(), Some(LogIndex(2)));

        let idx = store
            .append(
                make_data(99, u64::from(99u8) * 100, b"after-reopen"),
                test_guard(Hlc::ZERO),
            )
            .await
            .expect("ok");
        assert_eq!(idx, LogIndex(3));
    }

    // ── max_hlc: the membership drain bound ──────────────────────────────
    //
    // `max_hlc` must be an upper bound on the HLC of *every* entry in the log,
    // which is what makes it safe to sample as a joint-config exit bound
    // (`leader::membership_change::arm_exit_condition`). The frontier entry's
    // own HLC is not such a bound, because concurrent writers append out of
    // HLC order.

    #[tokio::test]
    async fn max_hlc_exact_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");
        {
            let (store, flush) = open_for_test(path.clone()).await.expect("open_for_test");
            // Largest HLC in the middle, so recovery cannot get the right answer
            // by reading the frontier entry's own HLC.
            for (seed, hlc) in [(1u8, 100u64), (2, 900), (3, 300)] {
                store
                    .append(make_data(seed, hlc, &[seed]), test_guard(Hlc::ZERO))
                    .await
                    .expect("ok");
            }
            assert_eq!(store.max_hlc(), Some(Hlc(900)));
            drop(store);
            let _ = flush.await;
        }
        let store = store_at(&path).await;
        assert_eq!(
            store.max_hlc(),
            Some(Hlc(900)),
            "the bound is durable, recovered from the frontier record"
        );
        // And it keeps accumulating after the reopen.
        store
            .append(make_data(4, 950, b"d"), test_guard(Hlc::ZERO))
            .await
            .expect("ok");
        assert_eq!(store.max_hlc(), Some(Hlc(950)));
        store
            .append(make_data(5, 400, b"e"), test_guard(Hlc::ZERO))
            .await
            .expect("ok");
        assert_eq!(store.max_hlc(), Some(Hlc(950)));
    }

    /// The case that would break the bound if recovery read anything but the
    /// running max: the largest HLC sits on the *oldest* entry, and purge then
    /// tries to sweep most of the log before a restart.
    ///
    /// It survives because the secondary is a running maximum — every entry
    /// after the 9000 carries 9000 too — so a threshold below it covers
    /// nothing, and the bound is still the last key's secondary after a reopen.
    /// An under-report here would let a joint config collapse before the
    /// outgoing set had drained (CLAUDE.md §8), which is why this is asserted
    /// rather than reasoned about.
    #[tokio::test]
    async fn max_hlc_survives_purge_of_the_entry_that_carries_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");
        {
            let (store, flush) = open_for_test(path.clone()).await.expect("open_for_test");
            // Entry 0 carries the max HLC; everything after it is lower, so a
            // purge that swept by *index* or by arrival order would drop it.
            for (seed, hlc) in [(0u8, 9000u64), (1, 100), (2, 200), (3, 300), (4, 400)] {
                store
                    .append(
                        make_data(seed, hlc, &vec![seed; 1024]),
                        test_guard(Hlc::ZERO),
                    )
                    .await
                    .expect("append");
            }
            assert_eq!(store.max_hlc(), Some(Hlc(9000)));
            store
                .storage
                .compact(Subsystem::ReplicaLog)
                .expect("compact");

            // A threshold above every *low* entry. It covers nothing: entry 0's
            // running max is 9000, and so is every later entry's.
            store.set_purge_threshold(Hlc(400)).expect("purge");
            store
                .storage
                .compact(Subsystem::ReplicaLog)
                .expect("compact");
            drop(store);
            let _ = flush.await;
        }
        let reopened = store_at(&path).await;
        assert_eq!(
            reopened.max_hlc(),
            Some(Hlc(9000)),
            "the running max floors every later secondary, so nothing is covered",
        );
    }

    /// The raw on-disk value layout round-trips and recovery reads the prefix.
    #[tokio::test]
    async fn raw_value_round_trips_and_recovery_recovers_the_bound() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");
        let (store, flush) = {
            let (store, flush) = open_for_test(path.clone()).await.expect("open");
            // Out-of-order HLCs so max_hlc != frontier hlc.
            for (seed, hlc) in [(1u8, 100u64), (2, 900), (3, 300)] {
                store
                    .append(make_data(seed, hlc, &[seed]), test_guard(Hlc::ZERO))
                    .await
                    .expect("append");
            }
            assert_eq!(store.max_hlc(), Some(Hlc(900)));
            (store, flush)
        };
        // The store holds RocksDB's LOCK on the root until the last `Arc`
        // drops, so it must be released before reopening.
        drop(store);
        let _ = flush.await;

        let (reopened, flush2) = open_for_test(path).await.expect("reopen");
        // The stored value is `data_id ‖ blob`, and `decode_value` splits it
        // back apart. Each append above awaits its completion before the next
        // submits, so the records land at consecutive indices starting at 0
        // (seed 1 here).
        let (secondary, value) = get_entry(&reopened.storage, LogIndex(0))
            .expect("ok")
            .expect("present");
        assert_eq!(secondary, make_data(1, 100, &[1]).data_id);
        assert_eq!(&value[..], &[1u8]);
        assert_eq!(
            reopened.max_hlc(),
            Some(Hlc(900)),
            "bound recovered from the last key's running-max secondary",
        );
        flush2.abort();
    }

    /// The ordering the drain bound depends on: `max_hlc` covers an HLC *before*
    /// the writer is told the write succeeded. Otherwise step 2b could sample a
    /// bound that misses a write the writer already counted as acknowledged.
    #[tokio::test]
    async fn max_hlc_advances_before_the_write_ack() {
        let (_dir, store) = fresh_store().await;
        // `append` resolves off the completion channel, which the flush loop
        // sends only after `frontier.advance` (see `flush_one_batch_blocking`).
        store
            .append(make_data(7, 777, b"g"), test_guard(Hlc::ZERO))
            .await
            .expect("ok");
        assert_eq!(
            store.max_hlc(),
            Some(Hlc(777)),
            "the bound must already cover an acked write"
        );
    }

    #[tokio::test]
    async fn broadcast_fires_after_commit() {
        let (_dir, store) = fresh_store().await;
        let mut ch = store.subscribe_from(None);

        let record = make_data(42, u64::from(42u8) * 100, b"broadcast");
        let expected = record.clone();
        let hlc = record.data_id.hlc();
        store.append(record, test_guard(hlc)).await.expect("ok");

        let (idx, data) = ch.recv().unwrap().unwrap();
        assert_eq!(idx, LogIndex::ZERO);
        assert_eq!(data.data_id, expected.data_id);
    }

    #[tokio::test]
    async fn broadcast_skipped_when_no_subscribers() {
        let (_dir, store) = fresh_store().await;
        store
            .append(
                make_data(1, u64::from(1u8) * 100, b"silent"),
                test_guard(Hlc::ZERO),
            )
            .await
            .expect("ok despite no subscribers");
        assert_eq!(store.frontier(), Some(LogIndex::ZERO));
        // No subscribers — fan_out is empty, append succeeds silently
    }

    /// A subscriber already at the frontier asks for `frontier + 1`: an empty
    /// historic window, which is the routine state of a peer that reconnects
    /// with nothing to catch up on. It must hand straight over to the live
    /// tail, not panic on the `from <= to` precondition of `pump_range`.
    #[tokio::test]
    async fn subscribe_from_frontier_serves_live_tail() {
        let (_dir, store) = fresh_store().await;
        for i in 1..=3u8 {
            store
                .append(
                    make_data(i, u64::from(i) * 100, &[i]),
                    test_guard(Hlc::ZERO),
                )
                .await
                .expect("ok");
        }
        let frontier = store.frontier().expect("frontier");

        let mut ch = store.subscribe_from(Some(frontier));

        // Nothing historic to replay; the next append arrives on the live tail.
        let record = make_data(9, 900, b"after");
        let expected = record.data_id;
        let hlc = record.data_id.hlc();
        store.append(record, test_guard(hlc)).await.expect("ok");

        let (idx, rec) = ch.recv().unwrap().unwrap();
        assert_eq!(idx, LogIndex(frontier.0 + 1));
        assert_eq!(rec.data_id, expected);
    }

    /// The handover between the historic scan and the live tail must not drop a
    /// record — the delicate part of `subscribe_from`, and the reason the live
    /// tail is opened before anything else.
    ///
    /// The window is a couple of instructions wide, so a merely-concurrent test
    /// passes with the ordering reversed and proves nothing (verified: it did).
    /// `SUBSCRIBE_HANDOFF_PAUSE_MS` parks execution *inside* the window so an
    /// entry can be committed there on purpose.
    ///
    /// Why the current order is correct: the flush loop advances the frontier
    /// **before** it broadcasts, so an entry broadcast too early to reach a
    /// subscriber that has just registered is necessarily at or below the
    /// frontier this call then snapshots — i.e. inside the scan's range. Register
    /// first, snapshot second, and every entry is served by exactly one of the
    /// two phases (overlap is filtered; §8 makes duplicates benign anyway).
    ///
    /// Reversed, the entry committed in the window is above `cutover` (outside
    /// the scan) and was broadcast before this subscriber existed (never on the
    /// live tail). It is lost, and downstream it looks like a benign purge gap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_record_is_lost_in_the_historic_to_live_handover() {
        const SEEDED: u8 = 3; // indices 0..=2
        const PAUSE_MS: u64 = 400;

        let (_dir, store) = fresh_store().await;
        for i in 1..=SEEDED {
            store
                .append(
                    make_data(i, u64::from(i) * 100, &[i]),
                    test_guard(Hlc::ZERO),
                )
                .await
                .expect("seed append");
        }

        SUBSCRIBE_HANDOFF_PAUSE_MS.store(PAUSE_MS, std::sync::atomic::Ordering::Relaxed);
        let subscribing = {
            let store = Arc::clone(&store);
            tokio::task::spawn_blocking(move || store.subscribe_from(None))
        };

        // Land squarely inside the paused window, then commit index 3 there.
        tokio::time::sleep(std::time::Duration::from_millis(PAUSE_MS / 4)).await;
        let raced = make_data(9, 900, b"committed inside the handover window");
        let raced_id = raced.data_id;
        let raced_index = store
            .append(raced, test_guard(Hlc::ZERO))
            .await
            .expect("append inside the window");

        let mut ch = subscribing.await.expect("subscribe_from");
        SUBSCRIBE_HANDOFF_PAUSE_MS.store(0, std::sync::atomic::Ordering::Relaxed);

        let want = usize::from(SEEDED).saturating_add(1);
        let collected = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking(move || {
                let mut got = Vec::new();
                while got.len() < want {
                    match ch.recv() {
                        Ok(Ok((idx, data))) => got.push((idx, data.data_id)),
                        Ok(Err(e)) => panic!("stream error: {e}"),
                        Err(_end) => break, // stream ended early — the record is gone
                    }
                }
                got
            }),
        )
        .await
        .expect("the racing record was never delivered — it fell into the handover gap")
        .expect("collector");

        let indices: Vec<u64> = collected.iter().map(|(i, _)| i.raw()).collect();
        assert_eq!(
            indices,
            vec![0, 1, 2, 3],
            "every index must be served exactly once across the handover",
        );
        assert_eq!(
            collected[3].1, raced_id,
            "the entry committed inside the handover window must be delivered",
        );
        assert_eq!(raced_index, LogIndex(3));
    }

    /// The `open_with_flush` fallbacks are what the test constructors run at, so
    /// they must not drift from the values operators actually get.
    #[test]
    fn open_engine_defaults_match_the_config_defaults() {
        let cfg = crate::replica::ReplicaLogConfig::for_test(crate::ids::NodeId::new_v4());
        assert_eq!(
            cfg.subscribe_channel_capacity.get(),
            DEFAULT_SUBSCRIBE_CHANNEL_CAPACITY
        );
        assert_eq!(cfg.max_flush_batch.get(), DEFAULT_MAX_FLUSH_BATCH);
    }

    /// A subscriber that drains promptly must survive a full-size flush batch.
    /// The arithmetic invariant lives in `ReplicaLogConfig::validate`; this is
    /// the behaviour it exists to protect, and it is what regressed when the
    /// capacity (256) sat below the batch cap (1024).
    #[tokio::test]
    async fn full_flush_batch_does_not_shed_a_draining_subscriber() {
        let (_dir, store) = fresh_store().await;
        let mut ch = store.subscribe_from(None);

        // One batch's worth of records, appended before anything is drained.
        let batch = DEFAULT_MAX_FLUSH_BATCH;
        for i in 0..batch {
            let seed = u8::try_from(i % 251).expect("mod 251 fits u8");
            store
                .append(
                    make_data(seed, u64::try_from(i).expect("small") + 1, &[seed]),
                    test_guard(Hlc::ZERO),
                )
                .await
                .expect("append");
        }

        let mut seen = 0usize;
        while seen < batch {
            let Some(items) = ch.ready_batch(batch) else {
                panic!("subscriber was shed after {seen}/{batch} records");
            };
            seen = seen.saturating_add(items.count());
        }
        assert_eq!(seen, batch, "every record reached the subscriber");
    }

    #[tokio::test]
    async fn subscribe_from_returns_in_index_order() {
        let (_dir, store) = fresh_store().await;
        for i in 1..=5u8 {
            store
                .append(
                    make_data(i, u64::from(i) * 100, &[i]),
                    test_guard(Hlc::ZERO),
                )
                .await
                .expect("ok");
        }
        let ch = store.subscribe_from(None);
        let entries: Vec<_> = ch
            .into_iter()
            .take(5)
            .filter_map(|r| match r {
                Ok((idx, data)) => Some((idx, data)),
                _ => None,
            })
            .collect();
        assert_eq!(entries.len(), 5);
        for (n, (idx, _)) in entries.iter().enumerate() {
            let expected = u64::try_from(n).expect("fits");
            assert_eq!(idx.raw(), expected, "out-of-order at slot {n}");
        }
    }

    /// Purge retires by HLC, clamped strictly below the frontier entry: covered
    /// entries vanish from every read and the newest entry always survives, so
    /// `next_index` recovers across a reopen. Pins the hard case — an idle
    /// cluster whose threshold has passed *every* entry.
    #[tokio::test]
    async fn purge_keeps_frontier_even_under_full_coverage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");

        let (store, flush) = open_for_test(path.clone()).await.expect("open_for_test");
        for i in 0..9u64 {
            let d = make_data(
                u8::try_from(i).expect("fits"),
                i * 100,
                &vec![u8::try_from(i).expect("fits"); 1024],
            );
            store
                .append(d, test_guard(Hlc::ZERO))
                .await
                .expect("append");
        }
        assert_eq!(store.frontier(), Some(LogIndex(8)));
        // Rewrite the SSTs so the purge below is exercised against files: a
        // DeleteRange tombstone written over keys already resident in an SST
        // is a different shape than one that only ever touches the memtable.
        store
            .storage
            .compact(Subsystem::ReplicaLog)
            .expect("compact");

        // Threshold covers entries 0..=3 (running max <= 300); the frontier's
        // max_hlc is 800, so the clamp leaves the threshold untouched. Purge
        // is a durable `DeleteRange`, fenced synchronously by this call — the
        // compact() above is only to exercise the SST-resident path.
        store.set_purge_threshold(Hlc(300)).expect("purge");
        store
            .storage
            .compact(Subsystem::ReplicaLog)
            .expect("compact");
        {
            // `subscribe_from(Some(LogIndex(3)))` → start = 4: entries 4..=8
            // survive (5). The live tail is infinite, so bound the drain.
            let ch = store.subscribe_from(Some(LogIndex(3)));
            let entries: Vec<_> = ch.into_iter().filter_map(|r| r.ok()).take(5).collect();
            assert_eq!(entries.len(), 5, "indices 4..=8 should survive");
            for (n, (idx, _)) in entries.iter().enumerate() {
                let expected = LogIndex(u64::try_from(n).expect("fits").saturating_add(4));
                assert_eq!(*idx, expected, "expected {expected:?} at slot {n}");
            }
        }
        // Before reopening the root: join the flush loop, which exits once the
        // dropped store closes its write channel. The node's shutdown path
        // drains `replica:log_flush`.
        drop(store);
        let _ = flush.await;

        let store = store_at(&path).await;
        assert_eq!(store.frontier(), Some(LogIndex(8)));
        {
            let ch = store.subscribe_from(Some(LogIndex(3)));
            let entries: Vec<_> = ch.into_iter().filter_map(|r| r.ok()).take(5).collect();
            assert_eq!(entries.len(), 5, "indices 4..=8 should survive reopen");
        }

        // A threshold past EVERY entry clamps to `max_hlc - 1`: the frontier
        // entry itself survives — including across a reopen.
        store.set_purge_threshold(Hlc(u64::MAX)).expect("purge");
        store
            .storage
            .compact(Subsystem::ReplicaLog)
            .expect("compact");
        assert_eq!(store.frontier(), Some(LogIndex(8)));
        {
            // start = 8 (next_after 7): only the frontier entry survives.
            let ch = store.subscribe_from(Some(LogIndex(7)));
            let entries: Vec<_> = ch.into_iter().filter_map(|r| r.ok()).take(1).collect();
            assert_eq!(entries.len(), 1, "only the frontier entry survives");
            assert_eq!(entries[0].0, LogIndex(8));
        }
        drop(store);
        let store = store_at(&path).await;
        assert_eq!(
            store.frontier(),
            Some(LogIndex(8)),
            "frontier survives reopen after full-coverage purge"
        );
        let next = store
            .append(
                make_data(99, u64::from(99u8) * 100, b"post-purge"),
                test_guard(Hlc::ZERO),
            )
            .await
            .expect("ok");
        assert_eq!(next, LogIndex(9), "append continues from frontier + 1");
    }

    /// Two entries in ONE flush batch, staged high-HLC-first.
    ///
    /// The fold that `flush_one_batch_blocking` used to do took only the
    /// batch's *last* entry, so this reported 200 while the log held 900 — an
    /// under-report of the bound the leader samples for the joint-drain exit
    /// (CLAUDE.md §8). Co-batching cannot be forced through `append`, which
    /// awaits each completion, so this drives one batch by hand: the store is
    /// opened without a flush loop and `flush_one_batch_blocking` is called
    /// directly, with the second write already queued behind the first.
    #[tokio::test(flavor = "multi_thread")]
    async fn max_hlc_folds_every_entry_of_one_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = Storage::open_for_test(&dir.path().join("replica-log")).expect("storage");
        let (tx, rx) = crossbeam_channel::bounded(4);
        let store = Arc::new(
            ReplicaLogStore::open_sized(storage, tx.clone(), 16, 16)
                .await
                .expect("open"),
        );
        let (completion_tx, _completion_rx) = crossbeam_channel::bounded(4);
        let completion = Arc::new(completion_tx);
        let pending = |seed: u8, hlc: u64| PendingWrite {
            data: make_data(seed, hlc, &[seed]),
            completion: Arc::clone(&completion),
            guard: Some(test_guard(Hlc::ZERO)),
            enqueued_at: std::time::Instant::now(),
        };
        // Queued first, so `BatchBuilder::build` drains it into the same batch
        // *behind* the high-HLC entry passed as `first`.
        tx.send(pending(2, 200)).expect("queue");
        let first = pending(1, 900);
        let store_cl = Arc::clone(&store);
        // The commit fence coalesces through a runtime task, so the blocking
        // body must not sit on a worker thread that task needs.
        tokio::task::spawn_blocking(move || flush_one_batch_blocking(&store_cl, first, &rx))
            .await
            .expect("join")
            .expect("flush");

        assert_eq!(store.frontier(), Some(LogIndex(1)), "both entries landed");
        assert_eq!(
            store.max_hlc(),
            Some(Hlc(900)),
            "the fold must cover every entry of the batch, not just its last"
        );
        // And the key ordering the durable purge depends on survives it: the
        // low-HLC entry inherits the running max rather than lowering it.
        assert_eq!(
            scan_keys(&store.storage).expect("scan"),
            vec![(LogIndex(0), Hlc(900)), (LogIndex(1), Hlc(900))],
        );
    }

    /// Purge is durable. The in-memory clamp it replaced started empty at every
    /// open, so an un-compacted purged prefix became readable again — this is
    /// the assertion that fails under that design and passes under a
    /// `DeleteRange`.
    #[tokio::test]
    async fn purged_prefix_stays_gone_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");
        {
            let (store, flush) = open_for_test(path.clone()).await.expect("open");
            for (seed, hlc) in [(1u8, 100u64), (2, 200), (3, 300)] {
                store
                    .append(make_data(seed, hlc, &[seed]), test_guard(Hlc::ZERO))
                    .await
                    .expect("append");
            }
            store.set_purge_threshold(Hlc(150)).expect("purge");
            // Rewrite the purged prefix's tombstone into an SST before the
            // reopen below, so "stays gone" is checked against the
            // SST-resident shape, not only the WAL/memtable one.
            store
                .storage
                .compact(Subsystem::ReplicaLog)
                .expect("compact");
            assert_eq!(
                first_live_index(&store.storage).expect("scan"),
                Some(LogIndex(1)),
                "index 0 is covered by the threshold"
            );
            drop(store);
            let _ = flush.await;
        }
        let store = store_at(&path).await;
        assert_eq!(
            first_live_index(&store.storage).expect("scan"),
            Some(LogIndex(1)),
            "index 0 was purged and must not come back after a reopen"
        );
    }

    #[test]
    fn proptest_gapless_across_reopen() {
        use proptest::prelude::*;

        // Arbitrary HLCs, deliberately NOT index-ordered: writers append out of
        // HLC order, which is the whole reason the secondary is a running max
        // rather than the entry's own HLC. A monotone generator would make the
        // monotonicity assertion below vacuous.
        proptest!(ProptestConfig::with_cases(16), |(records in proptest::collection::vec((any::<Vec<u8>>(), any::<u64>()), 1..16))| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rt");

            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("replica-log");
            let count = records.len();

            rt.block_on(async {
                let (store, flush) = open_for_test(path.clone())
                    .await
                    .expect("open_for_test");
                for (i, (blob, raw_hlc)) in records.iter().enumerate() {
                    let hlc = Hlc(*raw_hlc);
                    let record = Data {
                        // `i` as the tiebreaker keeps every `data_id` distinct
                        // even when two entries draw the same HLC.
                        data_id: DataId::test_id(hlc, u64::try_from(i).expect("fits")),
                        blob: Bytes::copy_from_slice(blob),
                    };
                    store.append(record, test_guard(hlc)).await.expect("append ok");
                }
                // Interleave a compaction pass so reopen recovers from
                // rewritten SSTs, not only the WAL/memtable.
                store.storage.compact(Subsystem::ReplicaLog).expect("compact");
                drop(store);
                let _ = flush.await;
            });

            let (frontier_val, entries, recovered_keys) = rt.block_on(async {
                let store = store_at(&path).await;
                let frontier = store.frontier();
                let keys = scan_keys(&store.storage).expect("key scan");
                let mut entries: Vec<(LogIndex, Data)> = Vec::new();
                let mut ch = store.subscribe_from(None);
                for _ in 0..count {
                    let (idx, data) = ch.recv().unwrap().unwrap();
                    entries.push((idx, data));
                }
                (frontier, entries, keys)
            });

            let last_expected = u64::try_from(count)
                .expect("fits")
                .checked_sub(1)
                .expect("count >= 1");
            prop_assert_eq!(frontier_val, Some(LogIndex(last_expected)));
            prop_assert_eq!(entries.len(), count);
            for (n, (idx, _)) in entries.iter().enumerate() {
                let expected = u64::try_from(n).expect("fits");
                prop_assert_eq!(idx.raw(), expected);
            }

            // The secondary is a RUNNING MAX, so it never decreases across the log.
            // This is what makes the retired set a key-order prefix, and it is the
            // assertable form of "max_hlc folds every entry": a batch that staged a
            // high HLC before a low one would write a decreasing secondary here.
            prop_assert_eq!(recovered_keys.len(), count);
            let mut prev: Option<Hlc> = None;
            for (_, secondary) in &recovered_keys {
                if let Some(p) = prev {
                    prop_assert!(
                        *secondary >= p,
                        "replica-log secondary must be non-decreasing: {p:?} then {secondary:?}"
                    );
                }
                prev = Some(*secondary);
            }
        });
    }

    #[tokio::test]
    async fn replica_log_frontier_resumes_at_next_free_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica-log");

        {
            let (store, flush) =
                ReplicaLogStore::open_with_flush(path.clone(), CancellationToken::new())
                    .await
                    .expect("open_for_test");
            for i in 1..=5u8 {
                store
                    .append(
                        make_data(i, u64::from(i), &[i]),
                        test_guard(Hlc(u64::from(i))),
                    )
                    .await
                    .expect("append");
            }
            drop(store);
            let _ = flush.await;
        }

        let (store, _flush) = ReplicaLogStore::open_with_flush(path, CancellationToken::new())
            .await
            .expect("open_for_test");
        assert_eq!(store.frontier(), Some(LogIndex(4)));

        let appended = store
            .append(make_data(99, 100, b"after-recovery"), test_guard(Hlc(100)))
            .await
            .expect("append");
        assert_eq!(appended, LogIndex(5));
        assert_eq!(store.frontier(), Some(LogIndex(5)));
    }

    #[tokio::test]
    async fn local_store_subscription_cursor_persists_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("local.db");

        {
            let (store, worker) = crate::learner::open_test_local_store(&path)
                .await
                .expect("open");
            for i in 1..=5u8 {
                integrate_batch(
                    &store,
                    upstream(1),
                    vec![(LogIndex(u64::from(i)), make_data(i, u64::from(i), &[i]))],
                )
                .await
                .expect("integrate");
            }
            // Drop the store and DRAIN its worker before reopening the root:
            // a worker mid-batch holds the DB's LOCK (`open_test_local_store`).
            drop(store);
            worker.stop().await;
        }

        let (store, worker) = crate::learner::open_test_local_store(&path)
            .await
            .expect("reopen");
        assert_eq!(
            store.subscription_cursor(upstream(1)).await.expect("ok"),
            Some(LogIndex(5))
        );
        drop(store);
        worker.stop().await;
    }

    // ── key codec ───────────────────────────────────────────────────────

    #[test]
    fn replica_key_roundtrips_and_primary_dominates() {
        let a = encode_key(LogIndex(1), Hlc(u64::MAX));
        let b = encode_key(LogIndex(2), Hlc(1));
        assert!(a < b, "primary must dominate the byte ordering");
        assert_eq!(decode_key(&a), Some((LogIndex(1), Hlc(u64::MAX))));
        assert_eq!(
            encode_primary(LogIndex(0xDEAD_BEEF)),
            0xDEAD_BEEFu64.to_be_bytes()
        );
    }

    /// The value carries the `data_id` the key no longer does.
    #[test]
    fn replica_value_roundtrips() {
        let id = DataId::from_parts(Hlc(42), NodeId::new_v4(), 7);
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&id.0);
        encoded.extend_from_slice(b"payload");
        assert_eq!(decode_value(&encoded), Some((id, &b"payload"[..])));
        // An empty blob is a legal record; a short value is not a value at all.
        assert_eq!(decode_value(&id.0), Some((id, &[][..])));
        assert_eq!(decode_value(&[0u8; VALUE_PREFIX_WIDTH - 1]), None);
    }

    #[test]
    fn replica_key_decode_rejects_wrong_length() {
        assert_eq!(decode_key(&[]), None);
        assert_eq!(decode_key(&[0u8; 4]), None);
        // Exactly `encode_primary`'s width (`PRIMARY_WIDTH`) — the shape a
        // seek bound actually has, so it's the more interesting rejection
        // case than an arbitrary short length.
        assert_eq!(decode_key(&[0u8; 8]), None);
        assert_eq!(decode_key(&[0u8; KEY_WIDTH - 1]), None);
    }

    /// Purge is a `DeleteRange` over encoded bounds, so a byte compare has to
    /// be the `Ord` compare or retirement covers the wrong records.
    #[test]
    fn replica_key_order_matches_tuple_order() {
        use proptest::prelude::*;

        proptest!(|(
            (i1, h1) in (any::<u64>(), any::<u64>()),
            (i2, h2) in (any::<u64>(), any::<u64>()),
        )| {
            let k1 = encode_key(LogIndex(i1), Hlc(h1));
            let k2 = encode_key(LogIndex(i2), Hlc(h2));
            prop_assert_eq!(k1.cmp(&k2), (i1, h1).cmp(&(i2, h2)));
        });
    }
}
