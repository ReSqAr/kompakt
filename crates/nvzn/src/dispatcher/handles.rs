//! The handles a tenant writes and waits through, beside its
//! [`Reader`](crate::Reader).
//!
//! ```text
//!   zslg Data blob = tenant_id(16) ‖ Ev::encode(events)
//! ```
//!
//! [`Encoder`] writes that prefix and the fold's `apply` splits on it, which
//! is how one shared stable log carries every tenant. [`Syncer`] waits on the
//! fold's own progress, so it fails rather than blocks once the fold has
//! stopped.

use super::FoldState;
use crate::metrics;
use crate::{Error, Tenant, WithWireFormat};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::watch;

const RESERVED_CAPACITY: usize = 128;

pub struct Encoder<Ev> {
    tenant: Tenant,
    phantom: PhantomData<Ev>,
}

impl<Ev> Encoder<Ev>
where
    Ev: WithWireFormat,
{
    pub fn encode(&self, events: &[Ev]) -> Result<bytes::Bytes, postcard::Error>
    where
        Ev: WithWireFormat,
    {
        let mut buf = bytes::BytesMut::with_capacity(
            self.tenant
                .id()
                .as_slice()
                .len()
                .saturating_add(RESERVED_CAPACITY),
        );
        buf.extend_from_slice(&self.tenant.id().as_slice());
        let buf = Ev::encode(events, buf)?;
        Ok(buf.freeze())
    }
}

impl<Ev> Encoder<Ev> {
    pub(crate) fn new(tenant: Tenant) -> Self {
        Self {
            tenant,
            phantom: PhantomData,
        }
    }

    pub(crate) fn tenant(&self) -> Tenant {
        self.tenant
    }
}

#[derive(Clone)]
pub struct Syncer {
    reader: Arc<dyn zslg::Reader>,
    watermark: watch::Receiver<Option<zslg::Hlc>>,
    fold_state: FoldState,
}

impl Syncer {
    pub(super) fn new(
        reader: Arc<dyn zslg::Reader>,
        watermark: watch::Receiver<Option<zslg::Hlc>>,
        fold_state: FoldState,
    ) -> Self {
        Self {
            reader,
            watermark,
            fold_state,
        }
    }

    /// The fold's watermark: the HLC committed through, as it advances.
    pub(crate) fn watermark(&self) -> watch::Receiver<Option<zslg::Hlc>> {
        self.watermark.clone()
    }

    /// Wait until the fold has reached the watermark the cluster reports.
    ///
    /// Fails with [`Error::FoldStopped`] if the fold is no longer running —
    /// this wait is on the fold's own progress, so without it there is nothing
    /// to wait for and a caller would otherwise block on a value that can
    /// never arrive.
    pub async fn sync(&self) -> Result<(), Error> {
        self.fold_state.check()?;
        self.reader.sync().await?;
        let Some(watermark) = self.reader.stable_watermark() else {
            tracing::debug!("sync: no stable watermark yet; nothing to fold up to");
            return Ok(());
        };

        // Only a wait that actually waits is worth a line — and a `sync` parked
        // here while the target sits still is a fold that is no longer running.
        let folded = *self.watermark.borrow();
        let waiting = folded < Some(watermark);
        if waiting {
            tracing::debug!(
                target = ?watermark,
                ?folded,
                "sync: waiting for the fold to reach the stable watermark"
            );
        }
        let started = std::time::Instant::now();
        // A closed watch means the fold dropped its sender: report why it
        // stopped rather than the channel mechanics.
        if let Err(closed) = self
            .watermark
            .clone()
            .wait_for(|w| w >= &Some(watermark))
            .await
        {
            self.fold_state.check()?;
            return Err(closed.into());
        }
        if waiting {
            tracing::debug!(
                target = ?watermark,
                waited_ms = started.elapsed().as_millis(),
                "sync: fold caught up"
            );
        }

        Ok(())
    }
}

pub struct EventWriter<Ev> {
    encoder: Encoder<Ev>,
    writer: Arc<dyn zslg::Writer>,
}

// By hand: a derive would require `Ev: Clone`, and the writer holds no `Ev`.
impl<Ev> Clone for EventWriter<Ev> {
    fn clone(&self) -> Self {
        Self {
            encoder: Encoder {
                tenant: self.encoder.tenant,
                phantom: PhantomData,
            },
            writer: Arc::clone(&self.writer),
        }
    }
}

impl<Ev> EventWriter<Ev> {
    pub(super) fn new(encoder: Encoder<Ev>, writer: Arc<dyn zslg::Writer>) -> Self {
        Self { encoder, writer }
    }
}

impl<Ev> EventWriter<Ev>
where
    Ev: WithWireFormat,
{
    pub async fn write(&self, events: &[Ev]) -> Result<zslg::DataId, Error> {
        let b = self.encoder.encode(events)?;
        let data_id = self.writer.write(b).await?;
        metrics::write(self.encoder.tenant(), events.len());
        Ok(data_id)
    }
}
