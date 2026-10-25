//! L1: the byte budget. Entries, LRU within a weighted mix, pins,
//! singleflight, stream-while-fill, predictions. Knows no object store: what
//! produces bytes on a miss is a closure ([`Fetch`]).
//!
//! ```text
//!             used (complete files) + reserved (downloads in flight)
//!   0 ─────────────── soft ─────────────────── hard
//!   │  retained files │  the evictor drains    │  a download asks the evictor
//!   │  (after use)    │  back to soft, in the  │  for room, or waits — the
//!   │                 │  background            │  in-use budget's backpressure
//! ```
//!
//! **Two limits, two roles.** `hard` bounds everything on disk: pinned
//! files, downloads in flight, retained files. `soft` is the retention
//! budget: what stays after its last use (0 on a worker that keeps nothing).
//!
//! **Who goes** (`state.rs`, over the order index in `order.rs`): files the
//! caller is done with (hinted) first, then the least recently used file of
//! the (app, kind) group furthest above its weighted target — so the cache
//! drifts toward the configured composition (`Shape`) without ever sitting
//! empty, and an app is never crowded out below its target while another is
//! above its own. A live prediction is held until its deadline: only room
//! under hard takes one, last. Choosing a victim is O(log n).
//!
//! **The evictor** (`evictor.rs`) alone evicts and deletes: callers update
//! the bookkeeping under the lock and wake it (over soft) or ask it for room
//! (at hard).
//!
//! **Pinned** = some [`CachedFile`] for it is alive; pinned files are never
//! evicted. Pins are bounded by stage concurrency, so waiting at `hard`
//! cannot deadlock. **Singleflight**: concurrent opens of one missing file
//! share one download. Files are written as `.part` and renamed when
//! complete (`Disk`), so a crash leaves only `.part` files, which the
//! start-up scan removes. The bookkeeping lives in memory and is rebuilt from
//! the scan (mtime = recency); the evictor writes recency back to the files
//! once a minute (`flush_recency`).

mod evictor;
mod file;
mod inner;
mod order;
mod policy;
mod state;

pub use file::{CachedFile, Streamed};

use crate::disk::{Disk, Group, Id, Scan};
use crate::shape::Shape;
use crate::{Error, Fetch, Ticket};
use futures::StreamExt;
use parking_lot::Mutex;
use state::{Entry, Mode, State};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// The byte budget: retain up to `soft` after use; a download waits above
/// `hard`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Limits {
    pub soft: u64,
    pub hard: u64,
}

#[derive(Clone)]
pub(crate) struct Core {
    inner: Arc<Inner>,
}

struct Inner {
    disk: Disk,
    shape: Arc<Shape>,
    state: Mutex<State>,
    /// Notified by every eviction, release and finished download: a
    /// download waiting at hard looks again.
    room: Notify,
    /// The evictor's: woken when a change leaves the cache over soft
    /// (coalesced), asked for room under hard, told to stop.
    wake: Arc<Notify>,
    asks: mpsc::UnboundedSender<evictor::Ask>,
    stop: watch::Sender<bool>,
    evictor: Mutex<Option<JoinHandle<()>>>,
}

/// One group's holdings, for reports and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Held {
    pub(crate) group: Group,
    pub(crate) bytes: u64,
    pub(crate) files: u64,
    pub(crate) target: u64,
}

impl Core {
    /// The bookkeeping, rebuilt from what the scan found, and the evictor
    /// started; its first pass evicts down to `soft` (the limits may have
    /// shrunk since the last run). Needs a tokio runtime.
    pub(crate) fn new(disk: Disk, scan: Scan, limits: Limits, shape: Arc<Shape>) -> Self {
        let mut found = scan.files;
        found.sort_by_key(|(id, _, modified)| (*modified, *id));
        let mut st = State {
            limits,
            ..State::default()
        };
        for (id, size, modified) in found {
            let last_used = st.tick();
            st.insert(
                id,
                Entry {
                    size,
                    last_used,
                    used_at: modified.unwrap_or_else(SystemTime::now),
                    dirty: false,
                    pins: 0,
                    hinted: false,
                    predicted: None,
                },
            );
        }
        let (asks, asked) = mpsc::unbounded_channel();
        let (stop, stopped) = watch::channel(false);
        let wake = Arc::new(Notify::new());
        let inner = Arc::new(Inner {
            disk,
            shape,
            state: Mutex::new(st),
            room: Notify::new(),
            wake: wake.clone(),
            asks,
            stop,
            evictor: Mutex::new(None),
        });
        inner.publish();
        let channels = evictor::Channels {
            wake,
            asks: asked,
            stop: stopped,
        };
        let task = tokio::spawn(evictor::run(Arc::downgrade(&inner), channels));
        *inner.evictor.lock() = Some(task);
        Self { inner }
    }

    /// Stop the evictor and wait for it. Room requests sent later get no
    /// answer: their callers wait on `room` as if everything were pinned.
    pub(crate) async fn shutdown(&self) {
        self.inner.stop.send_replace(true);
        let task = self.inner.evictor.lock().take();
        if let Some(task) = task {
            task.await.ok();
        }
    }

    /// Wait until the evictor is idle: every request served, the drain done.
    #[cfg(test)]
    pub(crate) async fn settled(&self) {
        let (reply, done) = oneshot::channel();
        if self.inner.asks.send(evictor::Ask::Settle(reply)).is_ok() {
            done.await.ok();
        }
    }

    /// Ask the evictor for room for `bytes`, as a download would.
    #[cfg(test)]
    pub(crate) async fn ask_room(&self, bytes: u64) {
        let (reply, done) = oneshot::channel();
        if self
            .inner
            .asks
            .send(evictor::Ask::Room { bytes, reply })
            .is_ok()
        {
            done.await.ok();
        }
    }

    /// Run the evictor's tick now (expiry, idle sweep); how many idle files
    /// went.
    #[cfg(test)]
    pub(crate) async fn tick_now(&self) -> usize {
        let (reply, done) = oneshot::channel();
        if self.inner.asks.send(evictor::Ask::Tick(reply)).is_err() {
            return 0;
        }
        done.await.unwrap_or(0)
    }

    /// The file, downloaded once however many callers ask at the same time;
    /// pinned until the returned value is dropped. `what` names it in errors.
    pub(crate) async fn open(
        &self,
        id: Id,
        what: &str,
        fetch: impl Fetch,
    ) -> Result<CachedFile, Error> {
        let mut fetch = Some(fetch);
        loop {
            let waiting = {
                let mut st = self.inner.state.lock();
                if let Some(file) = self.inner.pin(&mut st, id) {
                    metrics::counter!("objcache_hits_total", "app" => id.group.app, "kind" => id.group.kind).increment(1);
                    return Ok(file);
                }
                if let Some(ticket) = st.flights.promote.get(&id) {
                    ticket.promote();
                }
                st.flights.inflight.get(&id).cloned()
            };

            if let Some(mut rx) = waiting {
                let outcome = loop {
                    if let Some(outcome) = rx.borrow_and_update().clone() {
                        break outcome;
                    }
                    if rx.changed().await.is_err() {
                        break Err(Arc::from("the downloading task went away"));
                    }
                };
                outcome.map_err(|reason| Error::Download {
                    key: what.to_owned(),
                    reason,
                })?;
                continue;
            }

            // Ours to download. A second miss after our own download can only
            // mean it was evicted before we pinned it, which `finish` prevents.
            let Some(fetch) = fetch.take() else {
                return Err(Error::Download {
                    key: what.to_owned(),
                    reason: Arc::from("evicted before use"),
                });
            };
            let (tx, rx) = watch::channel(None);
            {
                let mut st = self.inner.state.lock();
                st.flights.inflight.insert(id, rx);
                st.flights.started = st.flights.started.saturating_add(1);
            }
            metrics::counter!("objcache_misses_total", "app" => id.group.app, "kind" => id.group.kind).increment(1);

            // The download is the cache's, not this caller's: it runs as its
            // own task, so a caller that goes away (a browser cancelling an
            // image) cannot cut it off halfway — leaving its in-flight entry
            // with no sender (every later request for the key failed, found
            // by the e2e run), its reservation held and its `.part` behind.
            let (core, what_owned) = (self.clone(), what.to_owned());
            let task = tokio::spawn(async move {
                let result = core
                    .download(id, &what_owned, Mode::Demand, fetch)
                    .await
                    .and_then(|f| {
                        f.ok_or_else(|| Error::Download {
                            key: what_owned.clone(),
                            reason: Arc::from("the finished file was not registered"),
                        })
                    });
                let outcome = result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|e| Arc::from(e.to_string()));
                core.inner.state.lock().flights.inflight.remove(&id);
                tx.send_replace(Some(outcome));
                result
            });
            return task.await.map_err(|e| Error::Download {
                key: what.to_owned(),
                reason: Arc::from(format!("the download task failed: {e}")),
            })?;
        }
    }

    /// Expect the file until `until`: mark it if cached, mark it on arrival
    /// if downloading, otherwise download it in the background through
    /// `fetch` (the prefetch lane), admitted only into spare room. Never
    /// waits. A request that joins the download promotes `ticket`; if the
    /// prediction is dropped, the request fetches the file itself (it never
    /// inherits a prediction's failure).
    pub(crate) fn predict(
        &self,
        id: Id,
        what: &str,
        until: SystemTime,
        ticket: Arc<Ticket>,
        fetch: impl Fetch,
    ) {
        let (tx, rx) = watch::channel(None);
        {
            let mut st = self.inner.state.lock();
            let known = st.update(&id, |e| {
                if e.pins == 0 {
                    if e.predicted.is_none() {
                        metrics::counter!("objcache_predicted_total", "app" => id.group.app, "kind" => id.group.kind).increment(1);
                    }
                    e.predicted = e.predicted.max(Some(until));
                }
            });
            if known.is_some() {
                return;
            }
            if st.flights.inflight.contains_key(&id) {
                let p = st.flights.predictions.entry(id).or_insert(until);
                *p = (*p).max(until);
                return;
            }
            st.flights.inflight.insert(id, rx);
            st.flights.promote.insert(id, ticket);
            st.flights.started = st.flights.started.saturating_add(1);
        }
        let (core, what_owned) = (self.clone(), what.to_owned());
        tokio::spawn(async move {
            let result = core
                .download(id, &what_owned, Mode::Predict(until), fetch)
                .await;
            if let Err(e) = &result
                && !matches!(e, Error::NoRoom | Error::Dropped(_))
            {
                tracing::debug!(%e, key = what_owned, "a prediction's fetch failed");
            }
            {
                let mut st = core.inner.state.lock();
                st.flights.inflight.remove(&id);
                st.flights.promote.remove(&id);
            }
            // Always "done": a joined request then finds the file, or fetches
            // it itself — never fails because a guess was dropped.
            tx.send_replace(Some(Ok(())));
        });
    }

    /// The file as a byte stream that starts **before** the download ends.
    ///
    /// A complete file streams from the cache. Otherwise an [`Core::open`]
    /// runs as its own task — joining a download already in flight, or
    /// starting one — so it finishes and fills the cache even if this reader
    /// goes away, and the returned stream follows the file as it arrives. A
    /// slow reader never slows the download. The task's result is this
    /// stream's: a failed download fails it, however fast it failed.
    pub(crate) async fn stream(
        &self,
        id: Id,
        what: &str,
        fetch: impl Fetch,
    ) -> Result<Streamed, Error> {
        if let Some(file) = self.inner.pin(&mut self.inner.state.lock(), id) {
            return Ok(Streamed::Complete(file));
        }
        let (core, what_c) = (self.clone(), what.to_owned());
        let mut opened = tokio::spawn(async move { core.open(id, &what_c, fetch).await });
        // Until the download is writing (follow it) or the open has ended
        // (its file, or its error).
        loop {
            let progress = self.inner.state.lock().flights.progress.get(&id).cloned();
            if let Some(progress) = progress {
                let part = self.inner.disk.part_path(&id);
                // Renamed between the lock and the open: it is complete, and
                // the open's result says so.
                if let Ok(file) = tokio::fs::File::open(&part).await {
                    return Ok(Streamed::Following { file, progress });
                }
            }
            tokio::select! {
                result = &mut opened => {
                    return match result {
                        Ok(file) => file.map(Streamed::Complete),
                        Err(e) => Err(Error::Download {
                            key: what.to_owned(),
                            reason: Arc::from(format!("the download task failed: {e}")),
                        }),
                    };
                }
                // Not writing yet (reserving room, or the store not answered):
                // look again shortly.
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    }

    async fn download(
        &self,
        id: Id,
        what: &str,
        mode: Mode,
        fetch: impl Fetch,
    ) -> Result<Option<CachedFile>, Error> {
        let (announced, mut stream) = fetch().await?;
        match mode {
            Mode::Demand => self.reserve(announced).await,
            Mode::Predict(_) => {
                let admitted = self.inner.state.lock().reserve_prediction(announced);
                if !admitted {
                    metrics::counter!("objcache_predictions_total", "app" => id.group.app, "kind" => id.group.kind, "outcome" => "no_room").increment(1);
                    return Err(Error::NoRoom);
                }
            }
        }

        let part = self.inner.disk.part_path(&id);
        let (progress, progress_rx) = watch::channel((announced, 0u64));
        let written = async {
            self.inner.disk.prepare(&id).await?;
            let mut file = tokio::fs::File::create(&part).await?;
            self.inner
                .state
                .lock()
                .flights
                .progress
                .insert(id, progress_rx);
            let mut written = 0u64;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk).await?;
                // tokio's `write_all` returns before the bytes reach the file:
                // announce only what a reader can read (found by the e2e run:
                // originals streamed from a cold cache ended early).
                file.flush().await?;
                written = written.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
                progress.send_replace((announced, written));
            }
            file.flush().await?;
            tokio::fs::rename(&part, self.inner.disk.path(&id)).await?;
            Ok::<u64, Error>(written)
        }
        .await;
        self.inner.state.lock().flights.progress.remove(&id);

        match written {
            Ok(size) => {
                metrics::counter!("objcache_downloaded_bytes_total", "app" => id.group.app, "kind" => id.group.kind).increment(size);
                let predicted = match mode {
                    Mode::Demand => None,
                    Mode::Predict(until) => Some(until),
                };
                let file = self.inner.finish(id, announced, Some(size), predicted);
                match (mode, file) {
                    (Mode::Predict(_), _) => Ok(None),
                    (Mode::Demand, Some(file)) => Ok(Some(file)),
                    (Mode::Demand, None) => Err(Error::Download {
                        key: what.to_owned(),
                        reason: Arc::from("the finished file was not registered"),
                    }),
                }
            }
            Err(e) => {
                tokio::fs::remove_file(&part).await.ok();
                self.inner.finish(id, announced, None, None);
                Err(e)
            }
        }
    }

    /// Wait until `size` more bytes fit under `hard`. When they do not, the
    /// evictor is asked for room (it alone evicts); when even that is not
    /// enough, wait for a pin to drop or a download to finish, and ask again.
    async fn reserve(&self, size: u64) {
        let mut asked = false;
        loop {
            let notified = self.inner.room.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.state.lock().try_reserve(size, asked) {
                return;
            }
            if asked {
                asked = false;
                notified.await;
                continue;
            }
            let (reply, done) = oneshot::channel();
            if self
                .inner
                .asks
                .send(evictor::Ask::Room { bytes: size, reply })
                .is_ok()
            {
                done.await.ok();
            }
            asked = true;
        }
    }
}
