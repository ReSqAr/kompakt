//! Building and committing one flush batch.

use super::{FRAME_OVERHEAD_SLACK, PendingWrite, REPLICA_LOG_CF, encode_key};
use crate::Error;
use crate::metrics::{self};
use rocksdb_kit::Batch as RocksBatch;
use rocksdb_kit::Storage;
use zslg_log::Data;
use zslg_log::LogError;
use zslg_log::LogIndex;
use zslg_log::{Admission, Completion};
use zslg_time::Hlc;

pub(crate) struct BatchBuilder {
    pub(crate) max_batch: usize,
    pub(crate) max_payload: u64,
}

pub(crate) struct Batch {
    /// The accepted writes, in assigned-index order. This **is** the batch: the
    /// `WriteBatch` is built straight off it at commit time, so there is no
    /// second copy of the records and no second ordering to keep in step.
    pub(crate) staged: Vec<(LogIndex, Data, Admission)>,
    pub(crate) completions: Vec<Completion>,
}

impl BatchBuilder {
    pub(crate) fn new(max_batch: usize, max_payload: u64) -> Self {
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
    pub(crate) fn build(
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

pub(crate) struct Committer;

impl Committer {
    /// Steps 2 and 4 of CLAUDE.md §8's write-batch pattern, in one blocking
    /// body: open the batch, stage every drained record into it, commit it
    /// atomically, then fence it durable. Nothing is ACKed until this returns.
    ///
    /// `start_max` is the frontier's current `max_hlc`; the returned value is
    /// the running max after this batch, which is what the caller folds back
    /// into the frontier. It is threaded rather than recomputed because it is
    /// written INTO each key — see the key-codec comment.
    pub(crate) fn append_and_flush(
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
