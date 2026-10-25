//! L3: one object store, cached and scheduled.
//!
//! ```text
//!   open(kind, key, version)
//!     id = (app, kind, hash(app, namespace, key, version))
//!     core.open(id, fetch)                       hit: the file, pinned
//!       fetch = scheduler.fetch(urgency, kind, || origin.get(key))
//!                                              miss: slot, deadlines, retry, hedge
//! ```
//!
//! The kind is a parameter of every call, not of the store: one store holds
//! several kinds (a derived store has thumbnails and previews). This is the
//! one place where the core and the scheduler meet.

use crate::core::{CachedFile, Core, Streamed};
use crate::disk::{Group, Id};
use crate::handles::{KindHandle, Namespace, Shared};
use crate::scheduler::{Kind, Scheduler, Urgency};
use crate::{Download, Error, Fetch, Ticket, Version};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

/// One listing entry. `key` is the path relative to the store's root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub last_modified_seconds: i64,
}

/// Cheap to clone: one pointer. (Callers hold stores in deep async frames,
/// and a store carries a scheduler's worth of state.)
#[derive(Clone)]
pub struct CachedStore(Arc<StoreInner>);

#[derive(Clone)]
struct StoreInner {
    app: &'static str,
    origin: Arc<dyn ObjectStore>,
    namespace: Namespace,
    shared: Arc<Shared>,
    scheduler: Scheduler,
    /// Who waits for this store's fetches: a browser (`Demand`) or a
    /// pipeline (`Background`).
    urgency: Urgency,
}

impl CachedStore {
    pub(crate) fn new(
        app: &'static str,
        origin: Arc<dyn ObjectStore>,
        namespace: Namespace,
        shared: Arc<Shared>,
        scheduler: Scheduler,
        urgency: Urgency,
    ) -> Self {
        Self(Arc::new(StoreInner {
            app,
            origin,
            namespace,
            shared,
            scheduler,
            urgency,
        }))
    }

    /// The same store, fetched for `urgency` (a web server: `Demand`).
    #[must_use]
    pub fn with_urgency(self, urgency: Urgency) -> Self {
        let mut inner = (*self.0).clone();
        inner.urgency = urgency;
        Self(Arc::new(inner))
    }

    /// The file as a local path. A miss downloads it once, however many
    /// callers ask at the same time; the returned file is pinned until
    /// dropped.
    pub async fn open(
        &self,
        kind: &KindHandle,
        key: &str,
        version: Version,
    ) -> Result<CachedFile, Error> {
        let (core, id, fetch) = self.prepare(kind, key, version, self.0.urgency)?;
        // Boxed: the core's future is large, and callers sit in deep frames.
        core.open(id, key, fetch).boxed().await
    }

    /// The file as a stream that starts before its download ends.
    pub async fn stream(
        &self,
        kind: &KindHandle,
        key: &str,
        version: Version,
    ) -> Result<Streamed, Error> {
        let (core, id, fetch) = self.prepare(kind, key, version, self.0.urgency)?;
        core.stream(id, key, fetch).boxed().await
    }

    /// Upload `bytes` under `key`. Returns once the store has them: a
    /// checkpoint written after this may refer to the key. Not cached: the
    /// next `open` downloads it. (A copy cached from before stays valid —
    /// `(key, version)` names one content, so it holds the same bytes.)
    pub async fn put(
        &self,
        kind: &KindHandle,
        key: &str,
        version: Version,
        bytes: Bytes,
    ) -> Result<(), Error> {
        self.0.shared.core()?;
        let path = parse_key(key)?;
        self.id(kind, key, version)?;
        let origin = self.0.origin.clone();
        self.0
            .scheduler
            .run(async move { Ok(origin.put(&path, bytes.into()).await?) })
            .await?;
        Ok(())
    }

    /// Expect the file until `until`: fetched in the background through the
    /// prefetch lane, into spare room only. A request for it joins the
    /// download and promotes `ticket`.
    pub fn predict(
        &self,
        kind: &KindHandle,
        key: &str,
        version: Version,
        ticket: Arc<Ticket>,
        until: SystemTime,
    ) {
        let Ok(core) = self.0.shared.core() else {
            return;
        };
        let (Ok(id), Ok(path)) = (self.id(kind, key, version), parse_key(key)) else {
            return;
        };
        let (origin, scheduler, labels) = (
            self.0.origin.clone(),
            self.0.scheduler.clone(),
            self.labels(kind),
        );
        let t = ticket.clone();
        let fetch = move || -> BoxFuture<'static, Result<Download, Error>> {
            async move {
                let started = Instant::now();
                let result = scheduler
                    .fetch_predicted(t, labels.kind, || get(origin.clone(), path.clone()))
                    .await;
                labels.observe(result, started)
            }
            .boxed()
        };
        core.predict(id, key, until, ticket, fetch);
    }

    /// The caller is done with the file: it goes first when room is needed.
    pub fn done_with(&self, kind: &KindHandle, key: &str, version: Version) {
        if let (Ok(core), Ok(id)) = (self.0.shared.core(), self.id(kind, key, version)) {
            core.done_with(id);
        }
    }

    /// The objects under `prefix`, straight from the store (not cached).
    pub fn list(
        &self,
        prefix: Option<&str>,
    ) -> impl Stream<Item = Result<ObjectMeta, Error>> + Send + 'static {
        let prefix = prefix.map(Path::from);
        self.0.origin.list(prefix.as_ref()).map(|m| {
            let m = m?;
            Ok(ObjectMeta {
                key: m.location.to_string(),
                size: m.size,
                last_modified_seconds: m.last_modified.timestamp(),
            })
        })
    }

    /// The first `len` bytes of an object, straight from the store (for a
    /// header; not cached).
    pub async fn head_bytes(&self, key: &str, len: u64) -> Result<Bytes, Error> {
        Ok(self.0.origin.get_range(&parse_key(key)?, 0..len).await?)
    }

    /// The object's size if the store holds `key` (a `HEAD`; the cache is
    /// not consulted, so a file evicted locally still counts).
    pub async fn size(&self, key: &str) -> Result<Option<u64>, Error> {
        match self.0.origin.head(&parse_key(key)?).await {
            Ok(meta) => Ok(Some(meta.size)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn id(&self, kind: &KindHandle, key: &str, version: Version) -> Result<Id, Error> {
        if kind.app != self.0.app {
            return Err(Error::Config(format!(
                "kind {} of {} used on a store of {}",
                kind.name, kind.app, self.0.app
            )));
        }
        let mut h = Sha256::new();
        h.update(self.0.app.as_bytes());
        h.update([0u8]);
        h.update(self.0.namespace.id.as_bytes());
        h.update([0u8]);
        h.update(key.as_bytes());
        h.update([0u8]);
        version.feed(&mut h);
        Ok(Id {
            group: Group {
                app: self.0.app,
                kind: kind.name,
            },
            hash: h.finalize().into(),
        })
    }

    fn labels(&self, kind: &KindHandle) -> Labels {
        Labels {
            app: self.0.app,
            store: self.0.namespace.label,
            kind: Kind {
                name: kind.name,
                hedge: kind.hedge,
            },
        }
    }

    fn prepare(
        &self,
        kind: &KindHandle,
        key: &str,
        version: Version,
        urgency: Urgency,
    ) -> Result<(Core, Id, impl Fetch), Error> {
        let core = self.0.shared.core()?;
        let path = parse_key(key)?;
        let id = self.id(kind, key, version)?;
        let (origin, scheduler, labels) = (
            self.0.origin.clone(),
            self.0.scheduler.clone(),
            self.labels(kind),
        );
        let fetch = move || -> BoxFuture<'static, Result<Download, Error>> {
            async move {
                let started = Instant::now();
                let result = scheduler
                    .fetch(urgency, labels.kind, || get(origin.clone(), path.clone()))
                    .await;
                labels.observe(result, started)
            }
            .boxed()
        };
        Ok((core, id, fetch))
    }
}

/// One read of `path`, as a body.
async fn get(origin: Arc<dyn ObjectStore>, path: Path) -> Result<Download, Error> {
    let got = origin.get(&path).await.map_err(|e| match e {
        object_store::Error::NotFound { .. } => Error::NotFound(path.to_string()),
        e => e.into(),
    })?;
    let size = got.meta.size;
    let stream = got.into_stream().map_err(Error::from);
    Ok((size, stream.boxed()))
}

/// A key as the store sees it. `Path::parse` rejects `.`/`..` and empty
/// segments, so a key taken from a URL cannot climb out of the store's root.
fn parse_key(key: &str) -> Result<Path, Error> {
    Path::parse(key).map_err(|_| Error::InvalidKey(key.to_owned()))
}

/// What a download is, for its metrics: which app, store and kind of file —
/// a 30 MB video and a 16 KB thumbnail must not share a latency histogram.
#[derive(Clone, Copy)]
struct Labels {
    app: &'static str,
    store: &'static str,
    kind: Kind,
}

impl Labels {
    /// Time the fetch to its first byte and to its end, whichever way it
    /// ends; the body is passed through unchanged.
    fn observe(self, result: Result<Download, Error>, started: Instant) -> Result<Download, Error> {
        let (size, body) = match result {
            Ok(got) => got,
            Err(e) => {
                // A dropped prediction never reached the store: no failure.
                if !matches!(e, Error::Dropped(_) | Error::NoRoom) {
                    self.failed(started);
                }
                return Err(e);
            }
        };
        let state = (body, 0u64, true, false);
        let observed =
            futures::stream::unfold(state, move |(mut body, bytes, first, ended)| async move {
                if ended {
                    return None;
                }
                match body.next().await {
                    Some(Ok(chunk)) => {
                        if first {
                            self.first_byte(started);
                        }
                        let bytes =
                            bytes.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
                        Some((Ok(chunk), (body, bytes, false, false)))
                    }
                    Some(Err(e)) => {
                        self.failed(started);
                        Some((Err(e), (body, bytes, first, true)))
                    }
                    None => {
                        self.done(started, bytes);
                        None
                    }
                }
            });
        Ok((size, observed.boxed()))
    }

    fn first_byte(self, started: Instant) {
        metrics::histogram!("objcache_store_first_byte_seconds", "app" => self.app, "store" => self.store, "kind" => self.kind.name)
            .record(started.elapsed());
    }

    fn done(self, started: Instant, bytes: u64) {
        let took = started.elapsed();
        metrics::histogram!("objcache_store_fetch_seconds", "app" => self.app, "store" => self.store, "kind" => self.kind.name, "outcome" => "ok")
            .record(took);
        metrics::counter!("objcache_store_fetched_bytes_total", "app" => self.app, "store" => self.store, "kind" => self.kind.name)
            .increment(bytes);
        // Whole bytes per second (capped at 4 GB/s: far above any store).
        let micros = u64::try_from(took.as_micros()).unwrap_or(u64::MAX).max(1);
        let rate = bytes
            .saturating_mul(1_000_000)
            .checked_div(micros)
            .unwrap_or(0);
        metrics::histogram!("objcache_store_bytes_per_second", "app" => self.app, "store" => self.store, "kind" => self.kind.name)
            .record(f64::from(u32::try_from(rate).unwrap_or(u32::MAX)));
    }

    fn failed(self, started: Instant) {
        metrics::histogram!("objcache_store_fetch_seconds", "app" => self.app, "store" => self.store, "kind" => self.kind.name, "outcome" => "error")
            .record(started.elapsed());
    }
}
