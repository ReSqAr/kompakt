//! The fetch scheduler: one per store — how many requests at once, how
//! patient, what first (blob-access design §5).
//!
//! ```text
//!   fetch(urgency) ─▶ wait for a slot (FIFO) ─▶ attempt, under urgency's deadline ─▶ body
//!                         │                                                         │
//!                         └──── the slot travels with the body: held until it ends ─┘
//! ```
//!
//! **Urgency is who waits.** `Demand` — a browser: at most 5 s until the first
//! byte, queueing included, then at most 5 s without progress (a video may
//! stream for minutes; a hang may not); a retryable failure is tried once
//! more, quickly, if the deadline allows (failures that escaped object_store's
//! own retries, or a local disk's — it has none). `Background` — the worker:
//! object_store's patience, none of ours.
//!
//! A slot is a request to the store in flight, so it is held until the body
//! has been read: N slots bound the store's load, whatever the callers.
//!
//! **Circuit breaker** (design §5.5): after 5 failed fetches in a row the
//! store is *open* for 5 s — a browser's fetch fails at once (no queue behind
//! a dead store), the worker's waits — then *half-open*: the next fetch is
//! the probe, its success closes the breaker, its failure opens it again.
//! A failure is the store's: an error (never a missing object), or a
//! timeout after at least a second with the store. Time spent waiting for one
//! of our own slots is not the store failing — a burst of a hundred
//! thumbnails queues behind 64 slots — and neither is a prediction giving up:
//! counting those once opened the breaker on a healthy store, and every
//! thumbnail failed for 5 s. Opening logs the failures that did it.
//!
//! **Hedging** (design §5.3): a browser's fetch of a thumbnail or preview
//! that has no first byte after the store's recent p95 (50–500 ms) gets a
//! second, identical attempt — only with a slot free at once, the breaker
//! closed, and within a budget of ~10 % of demand fetches. The first success
//! wins (and keeps its slot for the body); the other is dropped.

mod body;
mod breaker;
mod hedge;
mod tuning;
mod urgency;

use crate::{Download, Error, Ticket};
use body::{first_chunk, guarded, retryable, within};
use breaker::{Breaker, Health, Pass, ProbeGuard};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

pub(crate) use tuning::Tuning;
pub use urgency::Urgency;
pub(crate) use urgency::{Kind, Limits};

/// One store's fetches. Cheap to clone; clones share the slots and breaker.
#[derive(Clone)]
pub(crate) struct Scheduler {
    slots: Arc<Semaphore>,
    app: &'static str,
    store: &'static str,
    limits: Limits,
    health: Arc<Health>,
    tuning: Arc<Tuning>,
    /// The slots the semaphore holds now (it follows `tuning.slots`).
    granted: Arc<parking_lot::Mutex<usize>>,
}

impl Scheduler {
    /// `app` and `store` label the metrics (`store`: low cardinality, such as
    /// `originals`, never an id).
    pub(crate) fn new(app: &'static str, store: &'static str, tuning: Arc<Tuning>) -> Self {
        Self::with(app, store, tuning, Limits::default())
    }

    fn with(app: &'static str, store: &'static str, tuning: Arc<Tuning>, limits: Limits) -> Self {
        let slots = tuning.slots();
        // Exported from the start: a breaker that never tripped reads 0 on a
        // chart, not "no data".
        metrics::gauge!("objcache_store_breaker_state", "app" => app, "store" => store).set(0.0);
        Self {
            slots: Arc::new(Semaphore::new(slots)),
            app,
            store,
            limits,
            health: Arc::default(),
            tuning,
            granted: Arc::new(parking_lot::Mutex::new(slots)),
        }
    }

    #[cfg(test)]
    fn for_test(slots: usize, reserve: usize, limits: Limits) -> Self {
        Self::with("app", "test", Arc::new(Tuning::new(slots, reserve)), limits)
    }

    /// Follow `store.slots`: more slots at once; fewer as fetches finish (a
    /// task takes the surplus out of circulation when they come free).
    fn resize(&self) {
        let want = self.tuning.slots();
        let mut granted = self.granted.lock();
        if want > *granted {
            self.slots.add_permits(want.saturating_sub(*granted));
        } else if want < *granted {
            let surplus = u32::try_from(granted.saturating_sub(want)).unwrap_or(u32::MAX);
            let slots = self.slots.clone();
            tokio::spawn(async move {
                if let Ok(permits) = slots.acquire_many_owned(surplus).await {
                    permits.forget();
                }
            });
        }
        *granted = want;
    }

    /// Fetch through this store's slots, under `urgency`'s deadlines.
    /// `attempt` may be called twice (the demand retry).
    pub(crate) async fn fetch<F, Fut>(
        &self,
        urgency: Urgency,
        kind: Kind,
        attempt: F,
    ) -> Result<Download, Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Download, Error>>,
    {
        self.resize();
        let started = Instant::now();
        let left =
            |deadline: Option<Duration>| deadline.map(|d| d.saturating_sub(started.elapsed()));
        let deadline = urgency.first_byte(&self.limits);

        // A probe that ends without reporting (it timed out waiting for a
        // slot, or was cancelled) must give the probe back — otherwise the
        // breaker stays "pappg" and refuses everything for good (found by
        // the e2e outage run: 334 thumbnails refused after recovery).
        let mut _probe = None;
        loop {
            match self.pass(urgency) {
                Pass::Go => break,
                Pass::Probe => {
                    _probe = Some(ProbeGuard(self.health.clone()));
                    break;
                }
                Pass::Refused => {
                    metrics::counter!("objcache_store_refused_total", "app" => self.app, "store" => self.store, "kind" => kind.name)
                        .increment(1);
                    return Err(Error::Unavailable);
                }
                Pass::Wait(d) => tokio::time::sleep(d.max(Duration::from_millis(50))).await,
            }
        }

        let mut permit = within(
            left(deadline),
            self.slots.clone().acquire_owned(),
            "a free slot",
        )
        .await?
        .map_err(|_| Error::Deadline("the store's slots were closed"))?;
        metrics::histogram!("objcache_store_queue_seconds", "app" => self.app, "store" => self.store, "urgency" => urgency.label(), "kind" => kind.name)
            .record(started.elapsed());

        let queued = started.elapsed();
        let mut retried = false;
        loop {
            let since = self.epoch();
            let asked = Instant::now();
            let raced = within(
                left(deadline),
                self.attempt_once(urgency, kind, &attempt),
                "the first byte",
            )
            .await;
            let tried = match raced {
                Ok((r, hedge_slot)) => {
                    // The hedge won: its slot carries the body; ours is released.
                    let permit = hedge_slot.unwrap_or(permit);
                    (Ok(r), permit)
                }
                Err(e) => (Err(e), permit),
            };
            let (tried, permit_now) = tried;
            permit = permit_now;
            match &tried {
                Ok(Ok(_)) => self.succeeded(since),
                Ok(Err(e)) if retryable(e) => {
                    self.failed(since, "error", format!("{} {kind}: {e}", urgency.label()));
                }
                Err(Error::Deadline(_)) if asked.elapsed() >= self.limits.store_time_counts => {
                    self.failed(
                        since,
                        "timeout",
                        format!(
                            "{} {kind}: no first byte after {} ms with the store ({} ms queued)",
                            urgency.label(),
                            asked.elapsed().as_millis(),
                            queued.as_millis()
                        ),
                    );
                }
                // The deadline ran out in our own queue: the store was hardly
                // asked, its health is unknown.
                Err(Error::Deadline(_)) => {}
                // A missing or refused object: the store answered.
                Ok(Err(_)) | Err(_) => self.succeeded(since),
            }
            match tried {
                Ok(Ok((size, body))) => {
                    return Ok((size, guarded(body, permit, urgency.stall(&self.limits))));
                }
                Ok(Err(e))
                    if urgency == Urgency::Demand
                        && !retried
                        && retryable(&e)
                        && left(deadline).is_some_and(|l| l > self.limits.retry_needs) =>
                {
                    retried = true;
                    metrics::counter!("objcache_store_retries_total", "app" => self.app, "store" => self.store, "kind" => kind.name)
                        .increment(1);
                    tokio::time::sleep(self.limits.retry_after).await;
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(e),
            }
        }
    }

    /// A prediction's fetch: the prefetch lane. It takes a slot only while
    /// more than `reserve` are free, and is dropped when withdrawn, after
    /// waiting `prefetch_first_byte`, or while the breaker is not closed. A
    /// promoted one continues as a demand fetch (a request joined it).
    pub(crate) async fn fetch_predicted<F, Fut>(
        &self,
        ticket: Arc<Ticket>,
        kind: Kind,
        attempt: F,
    ) -> Result<Download, Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Download, Error>>,
    {
        self.resize();
        let (queued, urgency) = (Instant::now(), Urgency::Prefetch);
        let limit = urgency
            .first_byte(&self.limits)
            .unwrap_or(self.limits.prefetch_first_byte);
        let dropped = |reason: &'static str| {
            metrics::counter!("objcache_store_dropped_total", "app" => self.app, "store" => self.store, "reason" => reason)
                .increment(1);
            Err(Error::Dropped(reason))
        };
        loop {
            if ticket.is_promoted() {
                return self.fetch(Urgency::Demand, kind, attempt).await;
            }
            if ticket.is_withdrawn() {
                return dropped("withdrawn");
            }
            if queued.elapsed() >= limit {
                return dropped("stale");
            }
            if !matches!(*self.health.breaker.lock(), Breaker::Closed(_)) {
                return dropped("breaker");
            }
            if self.slots.available_permits() > self.tuning.reserve()
                && let Ok(permit) = self.slots.clone().try_acquire_owned()
            {
                metrics::histogram!("objcache_store_queue_seconds", "app" => self.app, "store" => self.store, "urgency" => urgency.label(), "kind" => kind.name)
                    .record(queued.elapsed());
                let since = self.epoch();
                return match within(Some(limit), first_chunk(attempt()), "the first byte").await {
                    Ok(Ok((size, body))) => {
                        self.succeeded(since);
                        Ok((size, guarded(body, permit, urgency.stall(&self.limits))))
                    }
                    Ok(Err(e)) => {
                        // A missing object is an answer; a failure counts.
                        if retryable(&e) {
                            self.failed(since, "error", format!("prefetch {kind}: {e}"));
                        } else {
                            self.succeeded(since);
                        }
                        Err(e)
                    }
                    // A prediction out of patience: its own deadline, not the
                    // store failing.
                    Err(e) => Err(e),
                };
            }
            // Wait for a withdrawal, a promotion, or a slot to come free.
            tokio::select! {
                () = ticket.wake.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    }

    /// Any other request (an upload) through this store's slots: the slot is
    /// held for the call.
    pub(crate) async fn run<T, Fut>(&self, f: Fut) -> Result<T, Error>
    where
        Fut: Future<Output = Result<T, Error>>,
    {
        self.resize();
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| Error::Deadline("the store's slots were closed"))?;
        f.await
    }
}

#[cfg(test)]
mod tests;
