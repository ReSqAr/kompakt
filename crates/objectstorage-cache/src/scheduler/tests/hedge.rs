//! Scheduler tests.

use super::*;

use futures::FutureExt;
use std::sync::atomic::{AtomicUsize, Ordering};

const MS: Duration = Duration::from_millis(1);

fn scheduler() -> Scheduler {
    let s = Scheduler::for_test(
        4,
        1,
        Limits {
            breaker_failures: 1000,
            store_time_counts: MS * 50,
            breaker_open: MS * 100,
            demand_first_byte: MS * 2000,
            demand_stall: MS * 2000,
            retry_after: MS,
            retry_needs: MS * 50,
            hedge_min: MS * 30,
            hedge_max: MS * 30,
            prefetch_first_byte: MS * 200,
        },
    );
    s.health.tail.lock().tokens = 100; // a full hedge budget
    s
}

/// The first call is slow (`slow`), later ones answer at once.
fn first_slow(
    calls: Arc<AtomicUsize>,
    slow: Duration,
) -> impl Fn() -> futures::future::BoxFuture<'static, Result<Download, Error>> {
    move || {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        async move {
            if n == 0 {
                tokio::time::sleep(slow).await;
            }
            Ok((
                1u64,
                futures::stream::iter([Ok(bytes::Bytes::from_static(b"x"))]).boxed(),
            ))
        }
        .boxed()
    }
}

#[tokio::test]
async fn a_slow_thumbnail_is_hedged_and_the_hedge_wins() {
    let s = scheduler();
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let d = s
        .fetch(Urgency::Demand, THUMB, first_slow(calls.clone(), MS * 1000))
        .await;
    assert!(d.is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 2, "a second attempt was made");
    assert!(
        started.elapsed() < MS * 500,
        "the hedge answered, not the slow one"
    );
}

#[tokio::test]
async fn videos_and_the_worker_are_never_hedged() {
    let s = scheduler();
    for (urgency, kind) in [(Urgency::Demand, VIDEO), (Urgency::Background, THUMB)] {
        let calls = Arc::new(AtomicUsize::new(0));
        assert!(
            s.fetch(urgency, kind, first_slow(calls.clone(), MS * 100))
                .await
                .is_ok()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{kind}");
    }
}

#[tokio::test]
async fn no_budget_no_hedge() {
    let s = scheduler();
    s.health.tail.lock().tokens = 0;
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        s.fetch(Urgency::Demand, THUMB, first_slow(calls.clone(), MS * 100))
            .await
            .is_ok()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
