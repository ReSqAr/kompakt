//! Scheduler tests.

use super::*;

const MS: Duration = Duration::from_millis(1);

fn store_error() -> Error {
    Error::Store(object_store::Error::Generic {
        store: "test",
        source: "down".into(),
    })
}

fn scheduler() -> Scheduler {
    Scheduler::for_test(
        4,
        1,
        Limits {
            breaker_failures: 3,
            store_time_counts: MS * 50,
            breaker_open: MS * 100,
            demand_first_byte: MS * 200,
            demand_stall: MS * 200,
            // No quick retry here: each failure counts once.
            retry_after: MS,
            retry_needs: Duration::from_secs(60),
            hedge_min: MS * 20,
            hedge_max: MS * 20,
            prefetch_first_byte: MS * 200,
        },
    )
}

async fn ok() -> Result<Download, Error> {
    Ok((
        1,
        futures::stream::iter([Ok(bytes::Bytes::from_static(b"x"))]).boxed(),
    ))
}

async fn fail() -> Result<Download, Error> {
    Err(store_error())
}

#[tokio::test]
async fn failures_open_the_breaker_then_a_probe_closes_it() {
    let s = scheduler();
    for _ in 0..3 {
        assert!(s.fetch(Urgency::Demand, T, fail).await.is_err());
    }
    // Open: a browser's fetch fails at once, without trying.
    let started = Instant::now();
    assert!(matches!(
        s.fetch(Urgency::Demand, T, ok).await,
        Err(Error::Unavailable)
    ));
    assert!(started.elapsed() < MS * 20);
    // Half-open after 100 ms: the probe succeeds and closes it.
    tokio::time::sleep(MS * 120).await;
    assert!(s.fetch(Urgency::Demand, T, ok).await.is_ok());
    assert_eq!(*s.health.breaker.lock(), Breaker::Closed(0));
}

#[tokio::test]
async fn a_failed_probe_opens_it_again() {
    let s = scheduler();
    for _ in 0..3 {
        let _ = s.fetch(Urgency::Demand, T, fail).await;
    }
    tokio::time::sleep(MS * 120).await;
    assert!(
        s.fetch(Urgency::Demand, T, fail).await.is_err(),
        "the probe fails"
    );
    assert!(matches!(
        s.fetch(Urgency::Demand, T, ok).await,
        Err(Error::Unavailable)
    ));
}

#[tokio::test]
async fn a_probe_that_never_reports_gives_the_probe_back() {
    let s = scheduler();
    for _ in 0..3 {
        let _ = s.fetch(Urgency::Demand, T, fail).await;
    }
    tokio::time::sleep(MS * 120).await;
    // The probe waits for a slot that never frees, and gives up.
    let held: Vec<_> = (0..4)
        .map(|_| s.slots.clone().try_acquire_owned().unwrap())
        .collect();
    let r = s.fetch(Urgency::Demand, T, ok).await;
    assert!(matches!(r, Err(Error::Deadline("a free slot"))));
    drop(held);
    // Not wedged: the next fetch probes, succeeds, closes the breaker.
    assert!(s.fetch(Urgency::Demand, T, ok).await.is_ok());
    assert_eq!(*s.health.breaker.lock(), Breaker::Closed(0));
}

#[tokio::test]
async fn a_failure_from_before_the_breaker_closed_does_not_reopen_it() {
    let s = scheduler();
    // A request hangs in a dying store …
    let straggler = {
        let s = s.clone();
        tokio::spawn(async move {
            s.fetch(Urgency::Demand, T, || async {
                tokio::time::sleep(MS * 400).await;
                ok().await
            })
            .await
            .map(|_| ())
        })
    };
    for _ in 0..3 {
        let _ = s.fetch(Urgency::Demand, T, fail).await;
    }
    // … the store recovers, a probe closes the breaker …
    tokio::time::sleep(MS * 120).await;
    assert!(s.fetch(Urgency::Demand, T, ok).await.is_ok());
    // … and the straggler's deadline (200 ms) passes after that: old
    // news. Two more stale failures would have reopened it at 3.
    assert!(straggler.await.unwrap().is_err());
    assert_eq!(*s.health.breaker.lock(), Breaker::Closed(0));
}

#[tokio::test]
async fn the_worker_waits_for_an_open_breaker_instead_of_failing() {
    let s = scheduler();
    for _ in 0..3 {
        let _ = s.fetch(Urgency::Demand, T, fail).await;
    }
    let started = Instant::now();
    assert!(s.fetch(Urgency::Background, T, ok).await.is_ok());
    assert!(
        started.elapsed() >= MS * 90,
        "it waited for the breaker to half-open"
    );
}

#[tokio::test]
async fn a_missing_object_never_opens_the_breaker() {
    let s = scheduler();
    for _ in 0..10 {
        let r = s
            .fetch(Urgency::Demand, T, || async {
                Err::<Download, _>(Error::NotFound("x".into()))
            })
            .await;
        assert!(matches!(r, Err(Error::NotFound(_))));
    }
    assert_eq!(*s.health.breaker.lock(), Breaker::Closed(0));
}

async fn slow_ok() -> Result<Download, Error> {
    tokio::time::sleep(MS * 100).await;
    ok().await
}

/// A burst that waits out its deadline in our own queue is not the store
/// failing: the fetches time out with the store barely asked (30 ms of
/// 200), and the breaker stays closed.
#[tokio::test]
async fn timeouts_spent_queued_do_not_open_the_breaker() {
    let s = scheduler();
    for _ in 0..5 {
        let held: Vec<_> = (0..4)
            .map(|_| s.slots.clone().try_acquire_owned().unwrap())
            .collect();
        tokio::spawn(async move {
            tokio::time::sleep(MS * 170).await;
            drop(held);
        });
        let r = s.fetch(Urgency::Demand, T, slow_ok).await;
        assert!(matches!(r, Err(Error::Deadline(_))), "{:?}", r.err());
    }
    assert_eq!(*s.health.breaker.lock(), Breaker::Closed(0));
}

/// A store that holds a free slot past the deadline is failing.
#[tokio::test]
async fn timeouts_with_the_store_open_the_breaker() {
    let s = scheduler();
    for _ in 0..3 {
        let r = s
            .fetch(Urgency::Demand, T, || async {
                tokio::time::sleep(MS * 300).await;
                ok().await
            })
            .await;
        assert!(matches!(r, Err(Error::Deadline(_))));
    }
    assert!(matches!(*s.health.breaker.lock(), Breaker::Open { .. }));
}

/// A prediction that runs out of patience is not the store failing.
#[tokio::test]
async fn predictions_out_of_patience_do_not_open_the_breaker() {
    let s = scheduler();
    for _ in 0..5 {
        let r = s
            .fetch_predicted(Arc::new(Ticket::default()), T, || async {
                tokio::time::sleep(MS * 300).await;
                ok().await
            })
            .await;
        assert!(r.is_err());
    }
    assert_eq!(*s.health.breaker.lock(), Breaker::Closed(0));
}
