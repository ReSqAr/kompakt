//! Scheduler tests.

use super::*;

fn quick(slots: usize) -> Scheduler {
    Scheduler::for_test(
        slots,
        1,
        Limits {
            breaker_failures: 1000,
            store_time_counts: MS * 50,
            breaker_open: MS * 100,
            demand_first_byte: MS * 200,
            demand_stall: MS * 200,
            retry_after: MS * 5,
            retry_needs: MS * 50,
            hedge_min: MS * 20,
            hedge_max: MS * 20,
            prefetch_first_byte: MS * 200,
        },
    )
}

/// A body of `chunks` chunks, `every` apart.
fn body(chunks: usize, every: Duration) -> Download {
    let s = futures::stream::unfold(0usize, move |i| async move {
        if i == chunks {
            return None;
        }
        tokio::time::sleep(every).await;
        Some((Ok(Bytes::from_static(b"x")), i + 1))
    });
    (u64::try_from(chunks).unwrap(), s.boxed())
}

async fn drain(d: Download) -> Result<usize, Error> {
    let mut s = d.1;
    let mut n = 0;
    while let Some(c) = s.next().await {
        c?;
        n += 1;
    }
    Ok(n)
}

#[tokio::test]
async fn slots_bound_requests_until_their_bodies_end() {
    let s = quick(2);
    let (now, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let fetches = (0..6).map(|_| {
        let (s, now, peak) = (s.clone(), now.clone(), peak.clone());
        tokio::spawn(async move {
            let d = s
                .fetch(Urgency::Background, T, || async { Ok(body(3, MS * 10)) })
                .await
                .unwrap();
            let n = now.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(n, Ordering::SeqCst);
            drain(d).await.unwrap();
            now.fetch_sub(1, Ordering::SeqCst);
        })
    });
    futures::future::join_all(fetches).await;
    assert_eq!(
        peak.load(Ordering::SeqCst),
        2,
        "a body holds its slot until read"
    );
}

#[tokio::test]
async fn an_unread_body_keeps_its_slot_and_demand_gives_up_waiting() {
    let s = quick(1);
    let held = s
        .fetch(Urgency::Demand, T, || async { Ok(body(1, MS)) })
        .await
        .unwrap();
    let waited = s
        .fetch(Urgency::Demand, T, || async { Ok(body(1, MS)) })
        .await;
    assert!(
        matches!(waited, Err(Error::Deadline("a free slot"))),
        "{:?}",
        waited.as_ref().err()
    );
    drop(held);
    assert!(
        s.fetch(Urgency::Demand, T, || async { Ok(body(1, MS)) })
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn demand_gives_up_at_its_first_byte_deadline_background_does_not() {
    let s = quick(4);
    let slow = || async {
        tokio::time::sleep(MS * 400).await;
        Ok(body(1, MS))
    };
    let started = Instant::now();
    let d = s.fetch(Urgency::Demand, T, slow).await;
    assert!(
        matches!(d, Err(Error::Deadline("the first byte"))),
        "{:?}",
        d.as_ref().err()
    );
    assert!(started.elapsed() < MS * 350);
    assert!(s.fetch(Urgency::Background, T, slow).await.is_ok());
}

#[tokio::test]
async fn demand_retries_a_failure_once_but_not_a_missing_object() {
    let s = quick(4);
    let tries = Arc::new(AtomicUsize::new(0));
    let flaky = || {
        let tries = tries.clone();
        async move {
            if tries.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Error::Store(object_store::Error::Generic {
                    store: "test",
                    source: "503".into(),
                }));
            }
            Ok(body(1, MS))
        }
    };
    assert!(s.fetch(Urgency::Demand, T, flaky).await.is_ok());
    assert_eq!(tries.load(Ordering::SeqCst), 2);

    let missing = Arc::new(AtomicUsize::new(0));
    let gone = || {
        let missing = missing.clone();
        async move {
            missing.fetch_add(1, Ordering::SeqCst);
            Err::<Download, _>(Error::NotFound("x".into()))
        }
    };
    assert!(matches!(
        s.fetch(Urgency::Demand, T, gone).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        missing.load(Ordering::SeqCst),
        1,
        "a missing object is not retried"
    );
}

#[tokio::test]
async fn a_stalled_body_fails_for_demand() {
    let s = quick(4);
    // The first chunk comes at once, the second after 400 ms.
    let d = s
        .fetch(Urgency::Demand, T, || async {
            let first = futures::stream::iter([Ok(Bytes::from_static(b"x"))]);
            let late = futures::stream::once(async {
                tokio::time::sleep(MS * 400).await;
                Ok(Bytes::from_static(b"y"))
            });
            Ok((2u64, first.chain(late).boxed()))
        })
        .await
        .unwrap();
    assert!(matches!(
        drain(d).await,
        Err(Error::Deadline("the next chunk"))
    ));
}
