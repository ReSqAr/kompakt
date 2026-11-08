//! The write path: `LocalStoreMut`.

use zslg_log::LogError;

use rocksdb_kit::Batch;

use crate::key::DATA_PRIMARY_WIDTH;
use crossbeam_combinators::{Channel, Finite};

use super::{IntegrateItem, LOCAL_STORE_DATA_CF, LocalStore, MAX_BATCH};
use crate::Error;
use zslg_consensus::NodeId;
use zslg_log::Data;
use zslg_log::LogIndex;
use zslg_log::ToStorage;
use zslg_time::Hlc;

#[async_trait::async_trait]
impl zslg_log::LocalStoreMut for LocalStore {
    async fn integrate_loop(
        &self,
        records: Channel<Result<(NodeId, LogIndex, Data), zslg_log::Error>, Finite>,
    ) -> Result<(), zslg_log::Error> {
        let mut records: Channel<Result<(NodeId, LogIndex, Data), Error>, Finite> =
            records.err_into();
        let result: Result<(), Error> = async {
            let tx = self.integrate_tx.clone();
            // Forwarding blocks on both halves (Channel::recv and a bounded
            // send), so it belongs on the blocking pool. It does no IO — the
            // single worker owns every commit.
            let forwarded = tokio::task::spawn_blocking(move || -> (Result<(), Error>, bool) {
                let mut sent_any = false;
                loop {
                    // A producer that *declared* completion finished its work, so
                    // `Complete` is a clean end. Whether a completed stream is
                    // acceptable is the caller's call — a subscription cannot
                    // complete, and `subscriber/peer.rs` treats even a clean end
                    // as a fault for that reason. An abort is a fault here, though:
                    // items may be missing (CLAUDE.md §20).
                    let batch = match records.ready_batch(MAX_BATCH) {
                        Ok(batch) => batch,
                        Err(crossbeam_combinators::End::Complete) => return (Ok(()), sent_any),
                        Err(crossbeam_combinators::End::Aborted { reason }) => {
                            return (
                                Err(Error::Log(LogError::SubscriptionStreamEnded {
                                    reason: reason.to_string(),
                                })),
                                sent_any,
                            );
                        }
                    };
                    for item in batch {
                        let (node, idx, data) = match item {
                            Ok(rec) => rec,
                            // In-band: the reason reaches the caller directly.
                            Err(e) => return (Err(e), sent_any),
                        };
                        if tx.send(IntegrateItem::Record(node, idx, data)).is_err() {
                            return (
                                Err(Error::Log(LogError::Storage(
                                    "local store integrator gone".into(),
                                ))),
                                sent_any,
                            );
                        }
                        sent_any = true;
                    }
                }
            })
            .await
            .to_storage("integrate forward task panicked")?;

            let (outcome, sent_any) = forwarded;
            // Wait for what we forwarded to commit before returning: the caller
            // measures progress by re-reading the durable cursor, so returning
            // early would under-report it.
            if sent_any {
                let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                if self
                    .integrate_tx
                    .send(IntegrateItem::Barrier(done_tx))
                    .is_ok()
                {
                    // A dropped sender resolves this too: the worker is gone, so
                    // waiting longer cannot make the records appear.
                    let _ = done_rx.await;
                }
            }
            outcome
        }
        .await;
        result.map_err(Into::into)
    }

    async fn purge_below_hlc(&self, threshold: Hlc) -> Result<(), zslg_log::Error> {
        let result: Result<(), Error> = async {
            // The data key is the `DataId` itself, which is HLC-major (ids.rs:170,
            // bytes 0..8 are the HLC big-endian), so key order IS HLC order and
            // `hlc <= threshold` is a key-order prefix. A durable range delete says
            // exactly that — no compaction filter, no in-memory clamp, and nothing
            // to re-arm after a reopen.
            let cf = self.storage.cf(LOCAL_STORE_DATA_CF)?;
            // An 8-byte key sorts before every 32-byte key sharing its prefix, so
            // `threshold + 1` big-endian is the exclusive upper bound. checked_add:
            // a threshold of u64::MAX means "purge everything", and an all-0xFF key
            // longer than any real one is above the whole family.
            let end: Vec<u8> = match threshold.raw().checked_add(1) {
                Some(next) => next.to_be_bytes().to_vec(),
                None => vec![0xFFu8; DATA_PRIMARY_WIDTH.saturating_add(1)],
            };
            let mut batch = Batch::new();
            // The start bound is an 8-byte HLC-width prefix of zeros, matching the
            // 8-byte end bound above — neither is a full (32-byte) key.
            batch.delete_range(&cf, &[0u8; 8][..], &end[..]);
            self.storage.write(batch)?;
            // `fence_async`, not `fence`: this is an async fn called directly from
            // the local-store purge worker's task, with no blocking body of its
            // own to sit in (CLAUDE.md §19) — `fence` is documented "call it from
            // spawn_blocking", which this isn't.
            self.storage.fence_async().await?;
            Ok(())
        }
        .await;
        result.map_err(Into::into)
    }
}
