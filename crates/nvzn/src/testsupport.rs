//! nvzn's own fixtures: one event set with a version-upgrade chain, the two
//! projections that consume it, and the store wrappers a test reaches for when
//! it needs to observe or break one.
//!
//! The rig itself — [`TestLog`] and the store constructors — lives in
//! [`crate::testing`], which ships to consumers. What is here is test data, and
//! stays behind `cfg(test)`.
#![allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
#![allow(
    unreachable_pub,
    reason = "crate-internal fixtures; `pub` items are consumed within the crate"
)]

use crate::Store;
use crossbeam_combinators::Channel;
use std::sync::{Arc, Mutex};
use zslg::{Data, Hlc, LogIndex, StableLogEvent};

pub use crate::testing::{FLUSH_INTERVAL, TestLog, data_id, inmemory, rocksdb, rocksdb_store};
pub use nvzn_fixtures::{
    NewTodoV1, NewUserV1, NewUserV2, NewUserV3, PingedV1, Todo, TodoDeletedV1, TodoId, User,
    UserDeletedV1, UserId, commit_row, event, payload, tenant, tenant_id,
};
pub type Calls = Arc<Mutex<Vec<&'static str>>>;

#[must_use]
pub fn calls() -> Calls {
    Arc::new(Mutex::new(Vec::new()))
}

#[must_use]
pub fn recorded(calls: &Calls) -> Vec<&'static str> {
    calls.lock().expect("test mutex").clone()
}

fn record(calls: &Calls, what: &'static str) {
    calls.lock().expect("test mutex").push(what);
}

pub struct ObservingStore {
    inner: Arc<dyn Store>,
    calls: Calls,
}

impl ObservingStore {
    #[must_use]
    pub fn new(inner: Arc<dyn Store>, calls: Calls) -> Self {
        Self { inner, calls }
    }
}

impl Store for ObservingStore {
    fn config_hash(&self) -> Result<Option<crate::ConfigHash>, crate::Error> {
        self.inner.config_hash()
    }
    fn set_config_hash(&self, hash: &crate::ConfigHash) -> Result<(), crate::Error> {
        self.inner.set_config_hash(hash)
    }
    fn read(&self) -> Result<crate::ArcReadTransaction<'_>, crate::Error> {
        record(&self.calls, "read");
        self.inner.read()
    }
    fn write(&self) -> Result<crate::WriteTransactionImpl<'_>, crate::Error> {
        record(&self.calls, "write");
        Ok(crate::WriteTransactionImpl::new(self.inner.read()?, self))
    }
    fn commit_batch(
        &self,
        batch: crate::CommitBatch,
        watermark: crate::Watermark,
    ) -> Result<(), crate::Error> {
        record(&self.calls, "commit");
        self.inner.commit_batch(batch, watermark)
    }
    fn flush(&self) -> Result<(), crate::Error> {
        self.inner.flush()
    }
    fn reset(&self) -> Result<(), crate::Error> {
        self.inner.reset()
    }
}

/// A zslg reader whose first `failures` syncs fail (a quorum read timing
/// out at startup); everything else is `inner`'s.
pub struct FailingSyncReader {
    inner: Arc<dyn zslg::Reader>,
    failures: Mutex<usize>,
}

impl FailingSyncReader {
    #[must_use]
    pub fn new(inner: Arc<dyn zslg::Reader>, failures: usize) -> Self {
        Self {
            inner,
            failures: Mutex::new(failures),
        }
    }
}

#[async_trait::async_trait]
impl zslg::Reader for FailingSyncReader {
    async fn sync(&self) -> Result<(), zslg::LogApiError> {
        let failing = {
            let mut left = self.failures.lock().expect("test mutex");
            let failing = *left > 0;
            *left = left.saturating_sub(1);
            failing
        };
        if failing {
            return Err(zslg::LogApiError::Log(zslg::LogError::QuorumReadTimeout {
                timeout: std::time::Duration::from_millis(1),
            }));
        }
        self.inner.sync().await
    }

    fn stable_watermark(&self) -> Option<Hlc> {
        self.inner.stable_watermark()
    }

    fn hlc_now(&self) -> Hlc {
        self.inner.hlc_now()
    }

    fn subscribe_stable_log(&self, after: Option<LogIndex>) -> zslg::StableStream {
        self.inner.subscribe_stable_log(after)
    }

    fn unstable_tail(
        &self,
    ) -> Channel<Result<Data, zslg::LogApiError>, crossbeam_combinators::Finite> {
        self.inner.unstable_tail()
    }

    fn subscribe_unstable_tail(
        &self,
    ) -> Channel<Result<zslg::UnstableEvent, zslg::LogApiError>, crossbeam_combinators::Infinite>
    {
        self.inner.subscribe_unstable_tail()
    }
}

pub struct UnfenceableStore {
    inner: Arc<dyn Store>,
}

impl UnfenceableStore {
    #[must_use]
    pub fn new(inner: Arc<dyn Store>) -> Self {
        Self { inner }
    }
}

impl Store for UnfenceableStore {
    fn config_hash(&self) -> Result<Option<crate::ConfigHash>, crate::Error> {
        self.inner.config_hash()
    }
    fn set_config_hash(&self, hash: &crate::ConfigHash) -> Result<(), crate::Error> {
        self.inner.set_config_hash(hash)
    }
    fn read(&self) -> Result<crate::ArcReadTransaction<'_>, crate::Error> {
        self.inner.read()
    }
    fn write(&self) -> Result<crate::WriteTransactionImpl<'_>, crate::Error> {
        Ok(crate::WriteTransactionImpl::new(self.inner.read()?, self))
    }
    fn commit_batch(
        &self,
        batch: crate::CommitBatch,
        watermark: crate::Watermark,
    ) -> Result<(), crate::Error> {
        self.inner.commit_batch(batch, watermark)
    }
    fn flush(&self) -> Result<(), crate::Error> {
        Err(crate::Error::Io("fsync refused".to_string()))
    }
    fn reset(&self) -> Result<(), crate::Error> {
        self.inner.reset()
    }
}

pub type StableFeed = crossbeam_channel::Sender<Result<StableLogEvent, zslg::LogApiError>>;

#[must_use]
pub fn hlc(ms: u64) -> Hlc {
    Hlc::from_bytes(ms.to_be_bytes())
}

/// A [`zslg::Reader`] whose halves the test drives separately, so the stable
/// watermark can run ahead of what the fold has been fed — the window between
/// zslg loading a snapshot and the fold catching up, which [`TestLog`] never
/// opens because it moves both together.
pub struct ScriptedReader {
    watermark: Mutex<Option<Hlc>>,
    unstable: Mutex<Vec<Data>>,
    stable: Mutex<Option<crossbeam_channel::Receiver<Result<StableLogEvent, zslg::LogApiError>>>>,
}

impl ScriptedReader {
    /// The reader, and the sender that feeds its one stable-log subscription.
    /// Dropping the sender ends the subscription as `Aborted`.
    #[must_use]
    pub fn new() -> (Arc<Self>, StableFeed) {
        let (tx, rx) = crossbeam_channel::unbounded();
        let reader = Self {
            watermark: Mutex::new(None),
            unstable: Mutex::new(Vec::new()),
            stable: Mutex::new(Some(rx)),
        };
        (Arc::new(reader), tx)
    }

    pub fn set_stable_watermark(&self, hlc_ms: u64) {
        *self.watermark.lock().expect("test mutex") = Some(hlc(hlc_ms));
    }
}

#[async_trait::async_trait]
impl zslg::Reader for ScriptedReader {
    async fn sync(&self) -> Result<(), zslg::LogApiError> {
        Ok(())
    }

    fn stable_watermark(&self) -> Option<Hlc> {
        *self.watermark.lock().expect("test mutex")
    }

    /// No writes go through a scripted reader: its "now" is its watermark.
    fn hlc_now(&self) -> Hlc {
        self.stable_watermark().unwrap_or_else(|| hlc(0))
    }

    fn subscribe_stable_log(&self, after: Option<LogIndex>) -> zslg::StableStream {
        let rx = self
            .stable
            .lock()
            .expect("test mutex")
            .take()
            .expect("a scripted reader serves one subscription");
        zslg::StableStream::new(Channel::from_infinite(rx, "scripted"), after)
    }

    fn unstable_tail(
        &self,
    ) -> Channel<Result<Data, zslg::LogApiError>, crossbeam_combinators::Finite> {
        let items = self
            .unstable
            .lock()
            .expect("test mutex")
            .iter()
            .cloned()
            .map(Ok)
            .collect();
        Channel::from_vec(items)
    }

    /// The staged unstable entries above the watermark, once: a scripted
    /// reader has no live leg, so the stream then ends `Aborted`.
    fn subscribe_unstable_tail(
        &self,
    ) -> Channel<Result<zslg::UnstableEvent, zslg::LogApiError>, crossbeam_combinators::Infinite>
    {
        let watermark = self.stable_watermark();
        let entries = self.unstable.lock().expect("test mutex").clone();
        let (tx, rx) = crossbeam_channel::unbounded();
        if let Some(w) = watermark {
            let _ = tx.send(Ok(zslg::UnstableEvent::Watermark(w)));
        }
        for data in entries {
            if watermark.is_none_or(|w| data.data_id.hlc() > w) {
                let _ = tx.send(Ok(zslg::UnstableEvent::Appended(data)));
            }
        }
        Channel::from_infinite(rx, "scripted unstable")
    }
}
