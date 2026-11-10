//! The replica's own log, as the replica service sees it: accept a write, say
//! how far the log extends, and stream it.

use std::sync::Arc;

use zslg_log_types::{Data, DataId, LogIndex};
use zslg_time_types::{Admission, Hlc};

use crate::{Error, ReplicaStream};

/// Where the outcome of a submitted write is delivered: the assigned index once
/// the batch holding it is durable and broadcast, or the storage error.
pub type Completion = Arc<crossbeam_channel::Sender<(DataId, Result<LogIndex, Error>)>>;

pub trait ReplicaLog: Send + Sync + 'static {
    /// The highest index in the log.
    #[must_use]
    fn frontier(&self) -> Option<LogIndex>;

    /// Upper bound on the HLC of every entry in this log — the membership drain
    /// bound. Updated before a write is acknowledged, so any HLC a writer has
    /// seen acked is included.
    #[must_use]
    fn max_hlc(&self) -> Option<Hlc>;

    /// Hand a write to the log **synchronously** (no `.await`): the log holds
    /// `admission` until the write is durable and reports to `completion`.
    /// Blocks only on bounded backpressure; safe from any thread.
    fn submit(&self, record: Data, admission: Admission, completion: Completion);

    /// The log from `from` onward, then live.
    #[must_use]
    fn subscribe_from(self: Arc<Self>, from: Option<LogIndex>) -> ReplicaStream;

    /// Entries at or below `threshold` may be purged.
    ///
    /// # Errors
    /// If the log cannot record the threshold.
    fn set_purge_threshold(&self, threshold: Hlc) -> Result<(), Error>;
}
