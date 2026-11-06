//! Explicit close and the verdict each combinator reports.

#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use super::*;

// ── explicit close ────────────────────────────────────────────────

/// The consumer forms its verdict when the *data* channel disconnects, so a
/// producer that drops its sender before closing is briefly indistinguishable
/// from one that died. The transient answer must not be memoised, or a
/// correct close arriving a moment later would be ignored.
#[test]
fn verdict_is_not_memoised_while_the_closer_is_still_alive() {
    let (tx, rx) = crossbeam_channel::bounded::<u32>(4);
    let (mut ch, closer) = Channel::from(rx, "racy producer");
    drop(tx); // sender gone first — the ordering hazard
    assert_eq!(
        ch.end_reason(),
        End::Aborted {
            reason: AbortReason::ProducerGone {
                what: "racy producer"
            }
        },
        "not closed yet, so fail closed"
    );
    closer.close();
    assert_eq!(
        ch.end_reason(),
        End::Complete,
        "a close arriving after the disconnect must still be honoured"
    );
}

/// An infinite stream has no `Closer`: ending is always an error, whatever
/// the underlying reason. That is what drives a consumer's reconnect
/// backoff rather than letting it look like a clean finish.
#[test]
fn infinite_stream_end_is_always_aborted() {
    let (tx, rx) = crossbeam_channel::bounded(4);
    let mut ch = Channel::from_infinite(rx, "socket");
    tx.send(1).unwrap();
    drop(tx); // peer closed politely — still an error to the app
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Err(AbortReason::ProducerGone { what: "socket" }));
    // Idempotent: a drain loop may ask again after the loop breaks.
    assert_eq!(
        ch.end_reason(),
        AbortReason::ProducerGone { what: "socket" }
    );
}

/// An infinite branch ending condemns the merge, so a connection-scoped
/// stream cannot be mistaken for a completed one.
#[test]
fn merge_with_infinite_branch_reports_aborted() {
    let (tx_f, rx_f) = crossbeam_channel::bounded::<u32>(4);
    let (tx_i, rx_i) = crossbeam_channel::bounded::<u32>(4);
    let (ch_f, closer_f) = Channel::from(rx_f, "finite");
    drop(tx_f);
    closer_f.close();
    let ch_i = Channel::from_infinite(rx_i, "socket");
    drop(tx_i);
    let mut ch = ch_f.merge(ch_i);
    assert_eq!(ch.recv(), Err(AbortReason::ProducerGone { what: "socket" }));
}

#[test]
fn closed_producer_yields_closed() {
    let (tx, rx) = crossbeam_channel::bounded(4);
    let (mut ch, closer) = Channel::from(rx, "finite scan");
    tx.send(1).unwrap();
    drop(tx);
    closer.close();
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Err(End::Complete));
}

/// The whole point: a producer that vanished is not a producer that
/// finished, even though the data channel looks identical.
#[test]
fn unclosed_producer_yields_aborted() {
    let (tx, rx) = crossbeam_channel::bounded(4);
    let (mut ch, closer) = Channel::from(rx, "finite scan");
    tx.send(1).unwrap();
    drop(tx);
    drop(closer); // dropped without close(): panicked, or forgot
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::ProducerGone {
                what: "finite scan"
            }
        })
    );
}

/// A `spawn_blocking` body that panics drops its `Closer` while unwinding —
/// the case that motivated all of this.
#[test]
fn panicking_producer_yields_aborted() {
    let (tx, rx) = crossbeam_channel::bounded(4);
    let (mut ch, closer) = Channel::from(rx, "scan");
    let handle = std::thread::spawn(move || {
        let _closer = closer;
        tx.send(1).unwrap();
        panic!("scan blew up");
    });
    assert!(handle.join().is_err());
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::ProducerGone { what: "scan" }
        })
    );
}

/// A task discarded before its body ever runs still drops the `Closer`, so
/// the tokio-shutdown gap is covered too.
#[test]
fn producer_dropped_before_running_is_aborted() {
    let (tx, rx) = crossbeam_channel::bounded::<u32>(4);
    let (mut ch, closer) = Channel::from(rx, "never ran");
    let never_ran = move || {
        let _closer = closer;
        let _tx = tx;
    };
    drop(never_ran); // the closure is discarded, never called
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::ProducerGone { what: "never ran" }
        })
    );
}

/// Fail closed across a merge: one aborted branch condemns the stream even
/// though the other closed cleanly. An abort now condemns the merge
/// *immediately* — the sibling's buffered items may or may not have been
/// drained first, depending on which branch `Select::ready` polled. Both
/// outcomes are valid: the contract is "items may be missing" for `Aborted`.
#[test]
fn merge_reports_aborted_if_either_branch_aborted() {
    let (tx_a, rx_a) = crossbeam_channel::bounded(4);
    let (tx_b, rx_b) = crossbeam_channel::bounded(4);
    let (ch_a, closer_a) = Channel::from(rx_a, "good");
    let (ch_b, closer_b) = Channel::from(rx_b, "bad");
    tx_a.send(1).unwrap();
    tx_b.send(2).unwrap();
    drop(tx_a);
    drop(tx_b);
    closer_a.close();
    drop(closer_b); // this branch died

    let mut ch = ch_a.merge(ch_b);
    let mut got: Vec<u32> = Vec::new();
    loop {
        match ch.recv() {
            Ok(v) => {
                assert!(v == 1 || v == 2, "unexpected item {v}");
                got.push(v);
            }
            Err(End::Aborted {
                reason: AbortReason::ProducerGone { what: "bad" },
            }) => break,
            Err(e) => panic!("unexpected end: {e:?}"),
        }
    }
    // The aborted branch may surface before the sibling's buffered item is
    // drained, so 1 or 2 items is valid (never zero — both are buffered and
    // `try_recv` returns items before `Disconnected`).
    assert!((1..=2).contains(&got.len()), "got {got:?}");
}

#[test]
fn merge_reports_closed_when_both_branches_closed() {
    let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(4);
    let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(4);
    let (ch_a, closer_a) = Channel::from(rx_a, "a");
    let (ch_b, closer_b) = Channel::from(rx_b, "b");
    drop(tx_a);
    drop(tx_b);
    closer_a.close();
    closer_b.close();
    let mut ch = ch_a.merge(ch_b);
    assert_eq!(ch.recv(), Err(End::Complete));
}

/// A phase that died mid-chain condemns it immediately — the
/// historical→live handoff must not launder a truncated replay behind live
/// items that would make the stream look complete. The live phase's items
/// are discarded the moment the historical phase reports `Aborted`.
#[test]
fn chain_reports_aborted_from_the_phase_that_died() {
    let (tx_h, rx_h) = crossbeam_channel::bounded(4);
    let (tx_l, rx_l) = crossbeam_channel::bounded(4);
    let (ch_h, closer_h) = Channel::from(rx_h, "historical");
    let (ch_l, closer_l) = Channel::from(rx_l, "live");
    tx_h.send(1).unwrap();
    drop(tx_h);
    drop(closer_h); // historical replay died
    tx_l.send(2).unwrap();
    drop(tx_l);
    closer_l.close();

    let mut ch = ch_h.chain(ch_l);
    assert_eq!(ch.recv(), Ok(1));
    // The historical phase aborts before the live phase is ever reached.
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::ProducerGone { what: "historical" }
        })
    );
}

/// The verdict survives a `filter_map` — the snapshot pipeline shape.
#[test]
fn abort_survives_filter_map() {
    let (tx, rx) = crossbeam_channel::bounded(4);
    let (ch, closer) = Channel::from(rx, "window");
    tx.send(2u32).unwrap();
    drop(tx);
    drop(closer);
    let mut ch = ch.filter_map(|v: u32| Some(v.saturating_mul(10)));
    assert_eq!(ch.recv(), Ok(20));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::ProducerGone { what: "window" }
        })
    );
}

#[test]
fn aborted_is_producer_gone() {
    let mut ch = Channel::<u32, Infinite>::aborted("test");
    assert_eq!(ch.recv(), Err(AbortReason::ProducerGone { what: "test" }));
}
