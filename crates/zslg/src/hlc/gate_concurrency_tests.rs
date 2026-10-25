//! `HlcGate` under a real thread pool.
//!
//! The tests beside `HlcGate` itself are `#[tokio::test(start_paused = true)]`
//! driving spawned tasks with hand-placed `yield_now()` calls: they pin one
//! interleaving deterministically, which is what makes them readable, but no
//! two operations ever genuinely race. This one runs writers and a cutoff
//! advancer on four worker threads and checks the invariants hold under real
//! contention.
//!
//! **What this does not prove.** It cannot establish the absence of a
//! lost-wakeup window in `set_cutoff`'s `notified_owned` + `enable` + swap
//! ordering. A stress test samples interleavings; it does not enumerate them.
//! That needs exhaustive exploration (loom), which is deferred — see
//! `docs/TESTING.md`. Do not restate this test's guarantee more strongly than
//! this paragraph does.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "test code"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::hlc::{Hlc, HlcGate};

/// Writers admit at `hlc` well above every cutoff the advancer installs, so a
/// rejection means the gate admitted against a cutoff it should not have.
const WRITER_HLC: u64 = 1_000_000;

/// Concurrent writer tasks in the admission race below.
const WRITERS: usize = 8;
/// Admission attempts per writer task.
const ITERATIONS: usize = 500;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_and_cutoff_advances_hold_the_invariants() {
    let gate = Arc::new(HlcGate::new());
    let admitted = Arc::new(AtomicU64::new(0));

    let writers: Vec<_> = (0..WRITERS)
        .map(|_| {
            let gate = Arc::clone(&gate);
            let admitted = Arc::clone(&admitted);
            tokio::spawn(async move {
                for _ in 0..ITERATIONS {
                    if let Ok(guard) = gate.check_hlc(Hlc(WRITER_HLC)) {
                        admitted.fetch_add(1, Ordering::Relaxed);
                        // Hold the guard across a yield so a concurrent
                        // set_cutoff genuinely has to wait for this drop.
                        tokio::task::yield_now().await;
                        drop(guard);
                    }
                    tokio::task::yield_now().await;
                }
            })
        })
        .collect();

    let advancer = {
        let gate = Arc::clone(&gate);
        tokio::spawn(async move {
            let mut highest = 0u64;
            for step in 1..=50u64 {
                if gate.set_cutoff(Hlc(step)).await {
                    assert!(
                        step > highest,
                        "set_cutoff reported an advance to {step} at or below {highest}"
                    );
                    highest = step;
                }
            }
            highest
        })
    };

    for w in writers {
        w.await.expect("writer task panicked");
    }
    let highest = advancer.await.expect("advancer task panicked");

    assert_eq!(
        highest, 50,
        "the advancer should have installed every cutoff"
    );
    let expected = u64::try_from(WRITERS.saturating_mul(ITERATIONS))
        .expect("WRITERS * ITERATIONS fits in u64");
    assert_eq!(
        admitted.load(Ordering::Relaxed),
        expected,
        "the gate rejected a write it was required to admit: writer HLC \
         {WRITER_HLC} is above every cutoff the advancer ever installs, so \
         every one of {WRITERS} writers' {ITERATIONS} attempts must succeed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_at_or_below_the_cutoff_is_never_admitted_under_contention() {
    let gate = Arc::new(HlcGate::new());
    assert!(gate.set_cutoff(Hlc(100)).await, "advance from None");

    let attempts: Vec<_> = (0..8)
        .map(|_| {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                let mut rejected = 0u32;
                for _ in 0..500 {
                    // Exactly at the cutoff must be refused: a write admitted
                    // at `cutoff` would be ACKed into no snapshot at all.
                    if gate.check_hlc(Hlc(100)).is_err() {
                        rejected += 1;
                    }
                    tokio::task::yield_now().await;
                }
                rejected
            })
        })
        .collect();

    // Keep advancing underneath them; the cutoff only ever rises, so a write
    // at 100 can never become admissible.
    let gate_advancer = Arc::clone(&gate);
    let advancer = tokio::spawn(async move {
        for step in 101..=150u64 {
            let _ = gate_advancer.set_cutoff(Hlc(step)).await;
        }
    });

    for a in attempts {
        assert_eq!(
            a.await.expect("attempt task panicked"),
            500,
            "a write at the cutoff was admitted"
        );
    }
    advancer.await.expect("advancer task panicked");
}
