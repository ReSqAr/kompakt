//! One attempt to the first byte, duplicated when slow (hedging).

use super::body::first_chunk;
use super::breaker::Breaker;
use super::{Kind, Scheduler, Urgency};
use crate::{Download, Error};
use std::future::Future;
use std::time::Instant;
use tokio::sync::OwnedSemaphorePermit;

impl Scheduler {
    /// One attempt to the first byte — hedged for a demand fetch of a small
    /// derived file. Returns the winner, and the hedge's slot if the hedge won.
    pub(super) async fn attempt_once<F, Fut>(
        &self,
        urgency: Urgency,
        kind: Kind,
        attempt: &F,
    ) -> (Result<Download, Error>, Option<OwnedSemaphorePermit>)
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Download, Error>>,
    {
        let started = Instant::now();
        let hedgeable = urgency == Urgency::Demand && kind.hedge;
        if hedgeable {
            let mut tail = self.health.tail.lock();
            tail.tokens = tail.tokens.saturating_add(1).min(100);
        }
        let primary = first_chunk(attempt());
        tokio::pin!(primary);
        if !hedgeable {
            return (primary.await, None);
        }
        let delay = self.hedge_delay();
        let early = tokio::select! {
            r = &mut primary => Some(r),
            () = tokio::time::sleep(delay) => None,
        };
        if let Some(r) = early {
            if r.is_ok() {
                self.sample(started.elapsed());
            }
            return (r, None);
        }
        // Slow: hedge, if a slot is free now, the breaker closed, the budget allows.
        let closed = matches!(*self.health.breaker.lock(), Breaker::Closed(_));
        let second = if closed {
            self.slots.clone().try_acquire_owned().ok()
        } else {
            None
        };
        let Some(second) = second.filter(|_| self.hedge_allowed()) else {
            let r = primary.await;
            if r.is_ok() {
                self.sample(started.elapsed());
            }
            return (r, None);
        };
        metrics::counter!("objcache_store_hedges_total", "app" => self.app, "store" => self.store, "kind" => kind.name)
            .increment(1);
        let hedge = first_chunk(attempt());
        tokio::pin!(hedge);
        // The first success wins; a failure waits for the other.
        let (r, hedged) = tokio::select! {
            r = &mut primary => match r {
                Ok(d) => (Ok(d), false),
                Err(_) => (hedge.await, true),
            },
            r = &mut hedge => match r {
                Ok(d) => (Ok(d), true),
                Err(_) => (primary.await, false),
            },
        };
        if r.is_ok() {
            self.sample(started.elapsed());
            if hedged {
                metrics::counter!("objcache_store_hedge_wins_total", "app" => self.app, "store" => self.store, "kind" => kind.name)
                    .increment(1);
            }
        }
        (r, hedged.then_some(second))
    }
}
