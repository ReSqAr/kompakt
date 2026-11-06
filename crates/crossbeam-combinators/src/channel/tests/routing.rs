//! Ordinal routing through merge, chain and guard; leaf poll contracts.

#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]

use std::sync::atomic::AtomicBool;

use super::*;

#[test]
fn merge_selects_across_sources_then_survivor() {
    let (tx_a, rx_a) = crossbeam_channel::bounded(4);
    let (tx_b, rx_b) = crossbeam_channel::bounded(4);
    let mut ch = closed(rx_a).merge(closed(rx_b));
    tx_a.send(1).unwrap();
    tx_b.send(2).unwrap();
    let mut got = vec![ch.recv().unwrap(), ch.recv().unwrap()];
    got.sort_unstable();
    assert_eq!(got, vec![1, 2]);
    drop(tx_a);
    tx_b.send(3).unwrap();
    assert_eq!(ch.recv(), Ok(3));
    drop(tx_b);
    assert_eq!(ch.recv(), Err(End::Complete));
}

/// Routing must survive nesting: a `Merge` whose left child is itself a
/// `Merge` flattens to three select operations, and each ordinal must reach
/// its own leaf. A wrong `leaf_count` split misroutes the third.
#[test]
fn nested_merge_reaches_every_leaf() {
    let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
    let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
    let (tx_c, rx_c) = crossbeam_channel::bounded::<u32>(1);
    tx_a.send(1).unwrap();
    tx_b.send(2).unwrap();
    tx_c.send(3).unwrap();

    let mut ch = closed(rx_a).merge(closed(rx_b)).merge(closed(rx_c));

    let mut got = vec![ch.recv().unwrap(), ch.recv().unwrap(), ch.recv().unwrap()];
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3]);
}

/// A transposed ordinal split still delivers every item when all three
/// leaves are primed — a set-membership check cannot tell which leaf each
/// ordinal reached. Prime only the middle leaf (`b`, ordinal 1) and leave
/// `a`/`c` alive but empty, so a misroute lands on an empty leaf and fails
/// fast instead of hanging.
#[test]
fn nested_merge_routes_each_ordinal_to_its_own_leaf() {
    let (_tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
    let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
    let (_tx_c, rx_c) = crossbeam_channel::bounded::<u32>(1);
    tx_b.send(2).unwrap();

    let mut ch = closed(rx_a).merge(closed(rx_b)).merge(closed(rx_c));

    let mut sel = Select::new();
    ch.source.register(&mut sel);
    let idx = sel.ready();
    match ch.source.poll(idx) {
        Poll::Got(v) => assert_eq!(v, 2),
        _ => panic!("ready index routed to the wrong leaf"),
    }
}

/// When one branch of a `Merge` closes cleanly the root collapses to
/// `MergeState::One` holding the surviving subtree. Here the survivor is
/// itself a `Merge` (leaves 0 and 1) and the dead branch `c` was ordinal 2,
/// so the survivor's ordinals must keep routing through `One` to `a` and
/// `b`. Polling directly by ordinal is the point: the `Select` is not
/// rebuilt between polls.
#[test]
fn merge_one_routes_ordinals_into_a_multi_leaf_survivor() {
    let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
    let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
    let (tx_c, rx_c) = crossbeam_channel::bounded::<u32>(1);
    let inner = closed(rx_a).merge(closed(rx_b));
    let (ch_c, closer_c) = Channel::from(rx_c, "c");
    closer_c.close();
    drop(tx_c);
    let mut ch = inner.merge(ch_c);

    tx_a.send(1).unwrap();
    tx_b.send(2).unwrap();

    let mut sel = Select::new();
    ch.source.register(&mut sel);

    // c's ordinal (2): the branch died cleanly, so the root collapses to
    // `One` and reports `Filtered` rather than an item.
    assert!(matches!(ch.source.poll(2), Poll::Filtered));
    match ch.source.poll(0) {
        Poll::Got(v) => assert_eq!(v, 1),
        _ => panic!("ordinal 0 did not route through One to leaf a"),
    }
    match ch.source.poll(1) {
        Poll::Got(v) => assert_eq!(v, 2),
        _ => panic!("ordinal 1 did not route through One to leaf b"),
    }
}

/// The [`Source`] contract, checked per leaf: every ordinal must reach
/// *its own* leaf.
///
/// Fail-fast by construction — plant an item in exactly one leaf and leave
/// the rest alive-but-empty, so a misroute finds nothing and the case
/// fails instead of passing on a coincidence. "Did every item come out?"
/// cannot distinguish a correct split from a transposed one.
fn assert_ordinals_reach(
    parts: usize,
    ordinal_to_part: &[usize],
    build: impl Fn(Vec<Channel<u32, Finite>>) -> Channel<u32, Finite>,
) {
    for (ordinal, &part) in ordinal_to_part.iter().enumerate() {
        let planted = u32::try_from(part).unwrap().saturating_add(100);
        let mut keepalive = Vec::new();
        let mut leaves = Vec::new();
        for i in 0..parts {
            let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
            if i == part {
                tx.send(planted).unwrap();
            }
            keepalive.push(tx);
            leaves.push(closed(rx));
        }
        let mut ch = build(leaves);

        let mut sel = Select::new();
        ch.source.register(&mut sel);
        let idx = sel.ready();
        assert_eq!(idx, ordinal, "part {part} did not register at {ordinal}");
        match ch.source.poll(idx) {
            Poll::Got(v) => assert_eq!(v, planted, "ordinal {ordinal} hit the wrong leaf"),
            _ => panic!("ordinal {ordinal} did not reach part {part}"),
        }
        drop(keepalive);
    }
}

/// Depth-first ordinals across a nested merge: `(a|b)|c` flattens to three
/// registrations and each must round-trip to the leaf that owns it.
#[test]
fn every_ordinal_reaches_its_own_leaf_across_a_nested_merge() {
    assert_ordinals_reach(3, &[0, 1, 2], |mut leaves| {
        let c = leaves.pop().unwrap();
        let b = leaves.pop().unwrap();
        let a = leaves.pop().unwrap();
        a.merge(b).merge(c)
    });
}

/// A `Chain` registers only its **front** phase, and counts only that
/// phase — so the queued phase must consume no ordinal at all. Get that
/// wrong and everything to the chain's right shifts by one: here `c` would
/// stop being reachable at ordinal 1.
#[test]
fn a_queued_chain_phase_consumes_no_ordinal() {
    assert_ordinals_reach(3, &[0, 2], |mut leaves| {
        let c = leaves.pop().unwrap();
        let b = leaves.pop().unwrap();
        let a = leaves.pop().unwrap();
        a.chain(b).merge(c)
    });
}

/// `Guarded` registers inner-then-guard, so the guard takes the *highest*
/// ordinal and the inner source's own ordinals stay put. The guard carries
/// no items by construction, so the observable claim is the inner half:
/// its leaves are still reachable at the ordinals they had alone.
#[test]
fn guarded_leaves_the_inner_ordinals_alone() {
    for target in 0..2usize {
        let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(1);
        let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(1);
        let planted = u32::try_from(target).unwrap().saturating_add(100);
        if target == 0 {
            tx_a.send(planted).unwrap();
        } else {
            tx_b.send(planted).unwrap();
        }
        // The guard's sender must outlive the poll: a disconnected guard
        // is *ready*, and would win the select on its own.
        let (alive_tx, alive_rx) = crossbeam_channel::bounded::<()>(1);
        let inner = closed(rx_a).merge(closed(rx_b));
        let mut ch: Channel<u32, Finite> = Channel::wrap(Box::new(Guarded {
            inner: inner.source,
            alive: LeafState::Alive {
                rx: alive_rx,
                close_rx: None,
                what: "test guard",
            },
            fired: Arc::new(AtomicBool::new(false)),
        }));

        let mut sel = Select::new();
        ch.source.register(&mut sel);
        let idx = sel.ready();
        assert_eq!(idx, target, "the guard displaced inner leaf {target}");
        match ch.source.poll(idx) {
            Poll::Got(v) => assert_eq!(v, planted),
            _ => panic!("ordinal {idx} did not reach inner leaf {target}"),
        }
        drop((tx_a, tx_b, alive_tx));
    }
}

/// A `Source` that claims one leaf but registers none — the
/// under-registration the [`Source`] contract forbids. It otherwise
/// presents as a hang, so the debug assert must catch it.
struct UnderRegistering;

impl Source<u32> for UnderRegistering {
    fn register<'a>(&'a mut self, _sel: &mut Select<'a>) -> usize {
        0
    }
    fn poll(&mut self, _ordinal: usize) -> Poll<u32> {
        Poll::Empty
    }
    fn drain_first(&mut self) -> Option<u32> {
        None
    }
    fn leaf_count(&self) -> usize {
        1
    }
    fn end_reason(&mut self) -> End {
        End::Complete
    }
}

#[test]
#[should_panic(expected = "register pushed 0 ops but leaf_count() is 1")]
fn register_leaf_count_mismatch_is_caught() {
    let mut ch: Channel<u32, Finite> = Channel::wrap(Box::new(UnderRegistering));
    let _ = ch.recv();
}

/// A leaf is only ever reached at relative ordinal 0; in a debug build a
/// nonzero ordinal aborts, pinning the bug to its source.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "a leaf is always polled at relative ordinal 0")]
fn leaf_poll_rejects_a_nonzero_ordinal() {
    let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
    let mut ch = closed(rx);
    let _ = ch.source.poll(1);
}

/// In a release build the same bad ordinal must fail closed: the leaf
/// resolves `Aborted` and reports `Dead`, so an enclosing tree condemns the
/// stream instead of hanging or serving a sibling's item.
#[cfg(not(debug_assertions))]
#[test]
fn leaf_poll_fails_closed_in_release() {
    let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
    let mut ch = closed(rx);
    assert!(matches!(ch.source.poll(1), Poll::Dead));
    assert_eq!(
        ch.end_reason(),
        End::Aborted {
            reason: AbortReason::ProducerGone { what: "test" }
        }
    );
}

/// The release side of the leaf ordinal check: a bad ordinal must resolve
/// the leaf `Aborted`, not silently poll it, so an enclosing tree can
/// condemn the stream. Exercised directly because the debug assert fires
/// first in a debug build.
#[test]
fn leaf_poison_fails_closed_on_a_bad_ordinal() {
    let (_tx, rx) = crossbeam_channel::bounded::<u32>(1);
    let mut leaf = LeafState::Alive {
        rx,
        close_rx: None,
        what: "routing test",
    };
    leaf.poison(7);
    assert_eq!(leaf.leaf_count(), 0);
    assert_eq!(
        leaf.end_reason(),
        End::Aborted {
            reason: AbortReason::RoutingViolation {
                what: "routing test"
            }
        }
    );
}

#[test]
fn merge_of_heterogeneous_origin_types() {
    let (tx_n, rx_n) = crossbeam_channel::bounded(4);
    let (tx_s, rx_s) = crossbeam_channel::bounded(4);
    let numbers = closed(rx_n).map(|n: u32| format!("n{n}"));
    let strings = closed(rx_s);
    let mut ch = numbers.merge(strings);
    tx_n.send(7).unwrap();
    tx_s.send("s".to_owned()).unwrap();
    let mut got = vec![ch.recv().unwrap(), ch.recv().unwrap()];
    got.sort();
    assert_eq!(got, vec!["n7".to_owned(), "s".to_owned()]);
}

/// A clean death in one merge branch must not make `try_recv` report an
/// empty batch while the survivor still holds items: it retries the
/// survivor after collapsing to `One`.
#[test]
fn try_recv_retries_the_survivor_after_a_clean_branch_death() {
    let mut ch = Channel::from_vec(Vec::<u32>::new()).merge(Channel::from_vec(vec![1, 2]));
    assert_eq!(ch.try_recv(), Some(1));
    assert_eq!(ch.try_recv(), Some(2));
    assert_eq!(ch.try_recv(), None);
    assert_eq!(ch.end_reason(), End::Complete);
}

#[test]
fn try_recv_none_when_empty_or_disconnected() {
    let (tx_a, rx_a) = crossbeam_channel::bounded(4);
    let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(4);
    let mut ch = closed(rx_a).merge(closed(rx_b));
    assert_eq!(ch.try_recv(), None);
    tx_a.send(1).unwrap();
    assert_eq!(ch.try_recv(), Some(1));
    assert_eq!(ch.try_recv(), None);
    drop(tx_a);
    drop(tx_b);
    assert_eq!(ch.try_recv(), None);
}

#[test]
fn try_recv_drains_filtered_items() {
    let (tx, rx) = crossbeam_channel::bounded(8);
    let mut ch = closed(rx).filter_map(|x: u32| x.is_multiple_of(2).then_some(x));
    tx.send(1).unwrap();
    tx.send(3).unwrap();
    tx.send(4).unwrap();
    assert_eq!(ch.try_recv(), Some(4));
}

#[test]
fn chain_historic_then_live() {
    let (htx, hrx) = crossbeam_channel::bounded(4);
    let (ltx, lrx) = crossbeam_channel::bounded(4);
    let mut ch = closed(hrx).chain(closed(lrx));

    htx.send("h0").unwrap();
    htx.send("h1").unwrap();
    drop(htx);

    ltx.send("L0").unwrap();

    assert_eq!(ch.recv(), Ok("h0"));
    assert_eq!(ch.recv(), Ok("h1"));
    assert_eq!(ch.recv(), Ok("L0"));
    drop(ltx);
    assert_eq!(ch.recv(), Err(End::Complete));
}

/// A `Merge` inside a `Chain`: one leaf dying must not discard the items the
/// sibling still holds, nor pop the whole phase. Before `Merge` reported
/// `Filtered` (rather than `Dead`) while one side was alive, this yielded
/// `[1, 99]` — item `2` was silently lost.
#[test]
fn chain_of_merge_survives_one_leaf_dying() {
    let (tx_a, rx_a) = crossbeam_channel::bounded::<u32>(4);
    let (tx_b, rx_b) = crossbeam_channel::bounded::<u32>(4);
    let (tx_c, rx_c) = crossbeam_channel::bounded::<u32>(4);

    let mut ch = closed(rx_a).merge(closed(rx_b)).chain(closed(rx_c));

    tx_b.send(1).unwrap();
    tx_b.send(2).unwrap();
    drop(tx_a); // left leaf dies; right still holds buffered items
    tx_c.send(99).unwrap();
    drop(tx_b);
    drop(tx_c);

    let mut got: Vec<u32> = Vec::new();
    while let Ok(v) = ch.recv() {
        got.push(v);
    }
    assert_eq!(got, vec![1, 2, 99]);
}

/// A request/response topology: the *left* branch's closure owns the
/// `Sender` for the *right* branch. Dropping the exhausted left branch
/// releases that sender and lets the merged output disconnect — without it
/// `recv()` blocks forever and the writer loop leaks its thread.
#[test]
fn merge_drops_dead_branch_releasing_its_captured_senders() {
    let (in_tx, in_rx) = crossbeam_channel::bounded::<u32>(8);
    let (cmt_tx, cmt_rx) = crossbeam_channel::bounded::<u32>(8);
    let tx = Arc::new(cmt_tx);

    let outcomes = closed(cmt_rx);
    let mut merged = closed(in_rx)
        .filter_map(move |d: u32| {
            // "submit": hand off with a sender clone, answered out of band.
            let _ = tx.send(d.saturating_mul(10));
            None::<u32>
        })
        .merge(outcomes);

    in_tx.send(1).unwrap();
    drop(in_tx); // input closed, one write still in flight

    assert_eq!(merged.recv(), Ok(10), "the in-flight write is answered");
    assert_eq!(
        merged.recv(),
        Err(End::Complete),
        "output must disconnect once the input is gone and nothing is in flight"
    );
}
