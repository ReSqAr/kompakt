#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

/// Most tests care about item flow, not the verdict: wrap a receiver in a
/// channel whose producer has already declared completion.
pub(super) fn closed<T: Send + 'static>(rx: Receiver<T>) -> Channel<T, Finite> {
    let (ch, closer) = Channel::from(rx, "test");
    closer.close();
    ch
}
#[test]
fn from_and_recv_roundtrip() {
    let (tx, rx) = crossbeam_channel::bounded(4);
    let mut ch = closed(rx);
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    drop(tx);
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn filter_map_is_lazy_and_runs_at_pull() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = crossbeam_channel::bounded(4);
    let c = calls.clone();
    let mut ch = closed(rx).filter_map(move |x: u32| {
        c.fetch_add(1, Ordering::SeqCst);
        x.is_multiple_of(2).then_some(x * 10)
    });
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(ch.recv(), Ok(20));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

mod combinators;
mod deferred_cancel;
mod routing;
mod verdicts;
