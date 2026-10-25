//! Scheduler tests.

use super::*;

const MS: Duration = Duration::from_millis(1);

fn scheduler(slots: usize, reserve: usize) -> Scheduler {
    Scheduler::for_test(
        slots,
        reserve,
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
            prefetch_first_byte: MS * 150,
        },
    )
}

async fn ok() -> Result<Download, Error> {
    Ok((
        1,
        futures::stream::iter([Ok(bytes::Bytes::from_static(b"x"))]).boxed(),
    ))
}

#[tokio::test]
async fn a_prediction_leaves_the_reserve_to_demand_and_goes_stale() {
    // 2 slots, 1 reserved: one held by demand leaves only the reserve.
    let s = scheduler(2, 1);
    let held = s.fetch(Urgency::Demand, T, ok).await.unwrap();
    let p = s.fetch_predicted(Arc::default(), T, ok).await;
    assert!(matches!(p, Err(Error::Dropped("stale"))));
    drop(held);
    assert!(s.fetch_predicted(Arc::default(), T, ok).await.is_ok());
}

#[tokio::test]
async fn a_withdrawn_prediction_is_dropped_and_a_promoted_one_is_served() {
    let s = scheduler(2, 1);
    let held = s.fetch(Urgency::Demand, T, ok).await.unwrap();
    let ticket = Arc::new(Ticket::default());
    let waiting = {
        let (s, t) = (s.clone(), ticket.clone());
        tokio::spawn(async move { s.fetch_predicted(t, T, ok).await.map(|_| ()) })
    };
    tokio::time::sleep(MS * 20).await;
    ticket.withdraw();
    assert!(matches!(
        waiting.await.unwrap(),
        Err(Error::Dropped("withdrawn"))
    ));

    let ticket = Arc::new(Ticket::default());
    let waiting = {
        let (s, t) = (s.clone(), ticket.clone());
        tokio::spawn(async move { s.fetch_predicted(t, T, ok).await.map(|_| ()) })
    };
    tokio::time::sleep(MS * 20).await;
    ticket.promote(); // a request joined: demand may use the reserve
    assert!(waiting.await.unwrap().is_ok());
    drop(held);
}
