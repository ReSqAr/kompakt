//! The pull-time combinators and constructors.

#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use super::*;

#[test]
fn empty_is_complete() {
    let mut ch = Channel::<u32, Finite>::empty();
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn once_yields_one_then_complete() {
    let mut ch = Channel::once(7u32);
    assert_eq!(ch.recv(), Ok(7));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn take_until_stops_before_the_match() {
    let mut ch = Channel::from_vec(vec![1u32, 2, 3, 4]).take_until(|v| *v == 3);
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Err(End::Complete));
}

/// Keyed by the tens digit: (10, 11) is one group. `n` counts items, so the
/// group that reaches it is finished, and the first item of the next group
/// ends the stream (and is dropped).
#[test]
fn take_at_least_finishes_the_group_that_reaches_n() {
    let n = std::num::NonZeroUsize::new(2).unwrap();
    let items = vec![
        Some(1u64),
        None,
        Some(10),
        Some(10),
        Some(10),
        Some(20),
        Some(30),
    ];
    let mut ch = Channel::from_vec(items).take_at_least::<u64>(n);
    let got: Vec<_> = std::iter::from_fn(|| ch.recv().ok()).collect();
    assert_eq!(got, vec![Some(1), None, Some(10), Some(10), Some(10)]);
    assert_eq!(ch.end_reason(), End::Complete);
}

#[test]
fn take_at_least_below_n_keeps_the_source_verdict() {
    let n = std::num::NonZeroUsize::new(5).unwrap();
    let (tx, rx) = crossbeam_channel::bounded(4);
    tx.send(Some(1u64)).unwrap();
    drop(tx);
    let mut ch = Channel::from_infinite(rx, "short").take_at_least::<u64>(n);
    assert_eq!(ch.recv(), Ok(Some(1)));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::ProducerGone { what: "short" }
        })
    );
}

#[test]
fn filter_above_drops_keys_at_or_below_the_cut() {
    let items = vec![Some(1u64), Some(2), None, Some(3), Some(4)];
    let mut ch = Channel::from_vec(items.clone()).filter_above::<u64>(Some(2));
    let got: Vec<_> = std::iter::from_fn(|| ch.recv().ok()).collect();
    assert_eq!(got, vec![None, Some(3), Some(4)]);
    let mut all = Channel::from_vec(items.clone()).filter_above::<u64>(None);
    let got: Vec<_> = std::iter::from_fn(|| all.recv().ok()).collect();
    assert_eq!(got, items);
}

#[test]
fn filter_map_may_hold_state() {
    let mut seen = 0u32;
    let mut ch = Channel::from_vec(vec![5u32, 6, 7]).map(move |v| {
        seen += 1;
        (seen, v)
    });
    assert_eq!(ch.recv(), Ok((1, 5)));
    assert_eq!(ch.recv(), Ok((2, 6)));
}

#[test]
fn ready_batch_returns_the_verdict() {
    let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
    drop(tx);
    let mut ch = Channel::from_infinite(rx, "gone");
    assert!(matches!(
        ch.ready_batch(4),
        Err(AbortReason::ProducerGone { what: "gone" })
    ));
}

#[test]
fn dedup_by_key_drops_consecutive_duplicates() {
    let mut ch = Channel::from_vec(vec![Some(1u64), Some(1), None, Some(2), Some(2), Some(1)])
        .dedup_by_key::<u64>();
    assert_eq!(ch.recv(), Ok(Some(1)));
    assert_eq!(ch.recv(), Ok(None));
    assert_eq!(ch.recv(), Ok(Some(2)));
    assert_eq!(ch.recv(), Ok(Some(1)));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn from_vec_is_always_closed() {
    let mut ch = Channel::from_vec(vec![1u32]);
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn take_while_stops_at_the_bound_with_complete() {
    let mut ch = Channel::from_vec(vec![1u32, 2, 3, 4]).take_while(|v| *v < 3);
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Err(End::Complete));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn take_while_preserves_an_abort() {
    let (tx, rx) = crossbeam_channel::bounded(4);
    tx.send(1u32).unwrap();
    drop(tx);
    let mut ch = Channel::from_infinite(rx, "bounded").take_while(|_| true);
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::ProducerGone { what: "bounded" },
        })
    );
}

#[test]
fn take_while_advances_a_chain_phase() {
    let bounded = Channel::from_vec(vec![1u32, 2, 3]).take_while(|v| *v < 2);
    let mut ch = bounded.chain(Channel::from_vec(vec![9]));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(9));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn closed_constructor_drains_then_reports_closed() {
    let (tx, rx) = crossbeam_channel::bounded(2);
    tx.send(1u32).unwrap();
    tx.send(2).unwrap();
    drop(tx);
    let mut ch = Channel::closed(rx);
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn chain_three_phases_keeps_order() {
    let mut ch = Channel::from_vec(vec![1u32])
        .chain(Channel::from_vec(vec![2]))
        .chain(Channel::from_vec(vec![3]));
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Ok(3));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn chain_all_empty_is_a_closed_stream() {
    let mut ch: Channel<u32, Finite> = Channel::chain_all([]);
    assert_eq!(ch.recv(), Err(End::Complete));
    assert_eq!(ch.end_reason(), End::Complete);
}

#[test]
fn chain_all_single_phase_is_that_stream() {
    let mut ch = Channel::chain_all([Channel::from_vec(vec![1u32, 2])]);
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn chain_all_runs_phases_in_order_and_skips_empty_ones() {
    let mut ch = Channel::chain_all([
        Channel::from_vec(vec![1u32]),
        Channel::from_vec(Vec::new()),
        Channel::from_vec(vec![2]),
    ]);
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(ch.recv(), Err(End::Complete));
}

/// Like `chain`, `chain_all` condemns the rest of the phases on an abort:
/// the third phase's item must never reach the consumer.
#[test]
fn chain_all_aborts_and_drops_the_remaining_phases() {
    let (tx_c, rx_c) = crossbeam_channel::bounded::<u32>(4);
    drop(tx_c);
    let mut ch = Channel::chain_all([
        Channel::from_vec(vec![1]),
        aborted_of(vec![2u32], "middle"),
        closed(rx_c),
    ]);
    assert_eq!(ch.recv(), Ok(1));
    assert_eq!(ch.recv(), Ok(2));
    assert_eq!(
        ch.recv(),
        Err(End::Aborted {
            reason: AbortReason::ProducerGone { what: "middle" }
        })
    );
    assert_eq!(
        ch.end_reason(),
        End::Aborted {
            reason: AbortReason::ProducerGone { what: "middle" }
        }
    );
}

/// `drain_first` must not starve one side: `ready_batch` takes only its head
/// through the fair `recv`, so a fixed bias would drain the whole left branch
/// before ever touching the right.
#[test]
fn merge_drain_first_alternates_between_branches() {
    let (tx_l, rx_l) = crossbeam_channel::bounded::<u32>(16);
    let (tx_r, rx_r) = crossbeam_channel::bounded::<u32>(16);
    for _ in 0..6 {
        tx_l.send(1).unwrap();
        tx_r.send(2).unwrap();
    }
    let mut ch = closed(rx_l).merge(closed(rx_r));

    let batch: Vec<u32> = ch.ready_batch(8).unwrap().collect();
    assert!(
        batch.contains(&1) && batch.contains(&2),
        "both branches must appear within one batch, got {batch:?}"
    );
}

#[test]
fn chain_try_recv_transitions() {
    let (htx, hrx) = crossbeam_channel::bounded(4);
    let (ltx, lrx) = crossbeam_channel::bounded(4);
    let mut ch = closed(hrx).chain(closed(lrx));

    htx.send("h").unwrap();
    drop(htx);

    assert_eq!(ch.try_recv(), Some("h"));
    ltx.send("L").unwrap();
    assert_eq!(ch.try_recv(), Some("L"));
}

#[test]
fn ready_batch_drains_max_items() {
    let (tx, rx) = crossbeam_channel::bounded(16);
    let mut ch = closed(rx);
    for i in 0..10u32 {
        tx.send(i).unwrap();
    }
    let batch: Vec<u32> = ch.ready_batch(3).unwrap().collect();
    assert_eq!(batch.len(), 3);
    assert_eq!(batch, vec![0, 1, 2]);
    let rest: Vec<u32> = ch.ready_batch(100).unwrap().collect();
    assert_eq!(rest.len(), 7);
}

#[test]
fn ready_batch_yields_everything_when_max_exceeds() {
    let (tx, rx) = crossbeam_channel::bounded(4);
    let mut ch = closed(rx);
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    drop(tx);
    let batch: Vec<u32> = ch.ready_batch(10).unwrap().collect();
    assert_eq!(batch, vec![1, 2]);
}

#[test]
fn from_vec_drains_all_items() {
    let mut ch = Channel::from_vec(vec![10, 20, 30]);
    assert_eq!(ch.recv(), Ok(10));
    assert_eq!(ch.recv(), Ok(20));
    assert_eq!(ch.recv(), Ok(30));
    assert_eq!(ch.recv(), Err(End::Complete));
}

#[test]
fn from_vec_empty_disconnects_immediately() {
    let mut ch: Channel<u32, Finite> = Channel::from_vec(vec![]);
    assert_eq!(ch.recv(), Err(End::Complete));
}
