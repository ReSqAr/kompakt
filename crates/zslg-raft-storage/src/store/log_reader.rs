//! `RaftLogReader`: reading entries and log state back out of the raft log.

use crate::Error;
use crate::metrics::{self};
use raft_kit::Entry;
use raft_kit::OptionalSend;
use raft_kit::RaftLogReader;
use raft_kit::StorageError;
use raft_kit::StorageIOError;
use std::fmt::Debug;
use std::ops::RangeBounds;
use zslg_consensus::NodeId;
use zslg_log::LogError;
use zslg_log::ToCodec;
use zslg_raft::TypeConfig;

use super::{RocksRaftStorage, bounds_to_pair, for_each_log_entry, storage_io_read};
// SUPER

#[allow(
    clippy::result_large_err,
    reason = "raft_kit::StorageError is inherently large (240+ bytes); we cannot change it"
)]
impl RaftLogReader<TypeConfig> for RocksRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.clone();
        let (lo, hi) = bounds_to_pair(&range);
        tokio::task::spawn_blocking(move || {
            let inner = inner.lock();
            let mut out = Vec::new();
            for_each_log_entry(&inner.storage, lo, Some(hi), &mut |_index, payload| {
                out.push(
                    postcard::from_bytes::<Entry<TypeConfig>>(payload)
                        .to_codec("decode raft entry")?,
                );
                Ok(true)
            })
            .map_err(storage_io_read)?;
            // A non-empty request that yielded nothing means the whole range
            // sat below the purge floor (retired records are dropped on
            // read) — legal here, so counted, not failed; it is an error in
            // `limited_get_log_entries` (header).
            if out.is_empty() && lo < hi {
                metrics::raft_log_read_empty_range();
            }
            Ok(out)
        })
        .await
        .map_err(|e| {
            StorageIOError::read(&Error::Log(LogError::Storage(format!(
                "spawn_blocking panicked: {e}"
            ))))
        })?
    }

    /// Upholds the contract `try_get_log_entries` cannot: a non-empty range
    /// must never answer empty. The header explains why the default's empty
    /// answer is a whole-node crash under `panic = "abort"`, and why refusing
    /// (rather than a retention constant) is the fix. A partially-purged range
    /// stays a short read — openraft tolerates `logs.len() <= end - start`.
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let entries = self.try_get_log_entries(start..end).await?;
        if entries.is_empty() && start < end {
            // The counter was already incremented by `try_get_log_entries`.
            tracing::warn!(
                log_index = start,
                end,
                "raft log: replication asked for [{start}, {end}), which is \
                 entirely below the purge floor — the logs were already purged \
                 and snapshotted. Returning a storage error so openraft \
                 re-syncs this follower from a snapshot; returning the empty \
                 result openraft's default would produce makes it unwrap None \
                 and abort the process. If this is frequent, the follower is \
                 falling behind faster than max_in_snapshot_log_to_keep covers.",
            );
            return Err(storage_io_read(Error::Log(LogError::Storage(format!(
                "raft log range [{start}, {end}) is entirely purged; \
                 replication must fall back to a snapshot"
            )))));
        }
        Ok(entries)
    }
}
