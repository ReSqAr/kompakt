#![allow(
    unused_crate_dependencies,
    reason = "integration tests inherit all package deps"
)]

//! A model-based check of finite combinator graphs.
//!
//! This is the external oracle for the experiment: whatever happens inside
//! `Channels`, a graph of closed leaves must deliver exactly the model's
//! multiset, in the model's order when the graph is ordered (leaves, chains,
//! maps and filters; a merge makes the order unspecified). It also pins
//! `try_recv` and `recv` to the same result, which is the contract the routing
//! rewrites must preserve.

use crossbeam_combinators::{AbortReason, Channels, End, Feed, Ranged, Tail, blend_chain};
use proptest::prelude::*;

/// A segment with items already queued, then a disconnect without a close.
fn aborted(items: Vec<u64>) -> Channels<u64> {
    let (tx, rx) = crossbeam_channel::bounded(items.len().max(1));
    for item in items {
        tx.send(item).unwrap();
    }
    drop(tx);
    Channels::from_infinite(rx, "aborted")
}

#[derive(Debug, Clone)]
enum Node {
    Leaf(Vec<u64>),
    Aborted,
    Merge(Box<Node>, Box<Node>),
    Chain(Box<Node>, Box<Node>),
    Map(Box<Node>),
    Filter(Box<Node>),
}

fn arb_node() -> impl Strategy<Value = Node> {
    let leaf = prop::collection::vec(0u64..100, 0..8).prop_map(Node::Leaf);
    leaf.prop_recursive(5, 64, 4, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Node::Merge(Box::new(a), Box::new(b))),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Node::Chain(Box::new(a), Box::new(b))),
            inner.clone().prop_map(|a| Node::Map(Box::new(a))),
            inner.prop_map(|a| Node::Filter(Box::new(a))),
        ]
    })
}

impl Node {
    fn build(&self) -> Channels<u64> {
        match self {
            Self::Leaf(items) => Channels::from_vec(items.clone()),
            Self::Aborted => {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(1);
                drop(tx);
                Channels::from_infinite(rx, "aborted")
            }
            Self::Merge(a, b) => a.build().merge(b.build()),
            Self::Chain(a, b) => a.build().chain(b.build()),
            Self::Map(a) => a.build().map(|v| v.wrapping_add(1)),
            Self::Filter(a) => a.build().filter_map(|v| (v % 2 == 0).then_some(v)),
        }
    }

    /// The expected values and whether their order is meaningful.
    fn model(&self) -> (Vec<u64>, bool) {
        match self {
            Self::Leaf(items) => (items.clone(), true),
            Self::Aborted => (Vec::new(), true),
            Self::Merge(a, b) => {
                let (mut av, _) = a.model();
                let (bv, _) = b.model();
                av.extend(bv);
                (av, false)
            }
            Self::Chain(a, b) => {
                let (mut av, a_ordered) = a.model();
                let (bv, b_ordered) = b.model();
                av.extend(bv);
                (av, a_ordered && b_ordered)
            }
            Self::Map(a) => {
                let (mut values, ordered) = a.model();
                for v in &mut values {
                    *v = v.wrapping_add(1);
                }
                (values, ordered)
            }
            Self::Filter(a) => {
                let (values, ordered) = a.model();
                (values.into_iter().filter(|v| v % 2 == 0).collect(), ordered)
            }
        }
    }

    /// Replace the first leaf with an aborted one; exactly one abort per graph.
    fn with_abort(&self) -> Self {
        match self {
            Self::Leaf(_) | Self::Aborted => Self::Aborted,
            Self::Merge(a, b) => Self::Merge(Box::new(a.with_abort()), b.clone()),
            Self::Chain(a, b) => Self::Chain(Box::new(a.with_abort()), b.clone()),
            Self::Map(a) => Self::Map(Box::new(a.with_abort())),
            Self::Filter(a) => Self::Filter(Box::new(a.with_abort())),
        }
    }
}

fn is_multiset_subset(got: &[u64], want: &[u64]) -> bool {
    let mut counts = std::collections::HashMap::new();
    for v in want {
        *counts.entry(*v).or_insert(0usize) += 1;
    }
    for v in got {
        match counts.get_mut(v) {
            Some(left) if *left > 0 => *left -= 1,
            _ => return false,
        }
    }
    true
}

fn collect_try(node: &Node) -> (Vec<u64>, End) {
    let mut ch = node.build();
    let mut out = Vec::new();
    while let Some(v) = ch.try_recv() {
        out.push(v);
    }
    (out, ch.end_reason())
}

fn collect_recv(node: &Node) -> (Vec<u64>, End) {
    let mut ch = node.build();
    let mut out = Vec::new();
    let end = loop {
        match ch.recv() {
            Ok(v) => out.push(v),
            Err(end) => break end,
        }
    };
    (out, end)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn finite_graphs_match_the_model(node in arb_node()) {
        let (want, ordered) = node.model();
        let (got_try, end_try) = collect_try(&node);
        let (got_recv, end_recv) = collect_recv(&node);

        prop_assert_eq!(end_try, End::Complete);
        prop_assert_eq!(end_recv, End::Complete);

        let mut want_sorted = want.clone();
        let mut try_sorted = got_try.clone();
        let mut recv_sorted = got_recv.clone();
        want_sorted.sort_unstable();
        try_sorted.sort_unstable();
        recv_sorted.sort_unstable();
        prop_assert_eq!(&try_sorted, &want_sorted);
        prop_assert_eq!(&recv_sorted, &want_sorted);

        if ordered {
            prop_assert_eq!(&got_try, &want);
            prop_assert_eq!(&got_recv, &want);
        }
    }

    #[test]
    fn an_abort_condemns_the_graph(node in arb_node()) {
        let node = node.with_abort();
        let (want, _) = node.model();
        let (got_try, end_try) = collect_try(&node);
        let (got_recv, end_recv) = collect_recv(&node);

        prop_assert_eq!(end_try, End::Aborted { reason: AbortReason::ProducerGone { what: "aborted" } });
        prop_assert_eq!(end_recv, End::Aborted { reason: AbortReason::ProducerGone { what: "aborted" } });
        prop_assert!(is_multiset_subset(&got_try, &want));
        prop_assert!(is_multiset_subset(&got_recv, &want));
    }

    #[test]
    fn resume_on_abort_concatenates_aborted_segments(segments in prop::collection::vec(prop::collection::vec(0u64..100, 0..5), 0..6)) {
        let mut ch = Channels::resume_on_abort(0usize, {
            let segments = segments.clone();
            move |next, _previous| {
                let segment = segments.get(*next)?;
                *next += 1;
                Some(aborted(segment.clone()))
            }
        });
        let got: Vec<u64> = std::iter::from_fn(|| ch.recv().ok()).collect();
        let want: Vec<u64> = segments.iter().flatten().copied().collect();
        prop_assert_eq!(got, want);
        if segments.is_empty() {
            prop_assert_eq!(ch.end_reason(), End::Complete);
        } else {
            prop_assert_eq!(
                ch.end_reason(),
                End::Aborted {
                    reason: AbortReason::ProducerGone { what: "aborted" }
                }
            );
        }
    }

    #[test]
    fn take_while_is_the_matching_prefix(
        items in prop::collection::vec(0u64..100, 0..20),
        cut in 0u64..100,
    ) {
        let mut ch = Channels::from_vec(items.clone()).take_while(move |v| *v < cut);
        let got: Vec<u64> = std::iter::from_fn(|| ch.recv().ok()).collect();
        let want: Vec<u64> = items.iter().copied().take_while(|v| *v < cut).collect();
        prop_assert_eq!(&got, &want);
        prop_assert_eq!(ch.end_reason(), End::Complete);
    }

    #[test]
    fn ready_batch_matches_the_model(node in arb_node(), batch_size in 1usize..8) {
        let (want, ordered) = node.model();
        let mut ch = node.build();
        let mut got = Vec::new();
        while let Some(batch) = ch.ready_batch(batch_size) {
            got.extend(batch);
        }
        prop_assert_eq!(ch.end_reason(), End::Complete);

        let mut want_sorted = want.clone();
        let mut got_sorted = got.clone();
        want_sorted.sort_unstable();
        got_sorted.sort_unstable();
        prop_assert_eq!(&got_sorted, &want_sorted);
        if ordered {
            prop_assert_eq!(&got, &want);
        }
    }
}

struct TestSeam {
    live: Vec<u64>,
    history: Vec<u64>,
    cut: u64,
}

impl Tail for TestSeam {
    type Item = u64;
    type Cut = u64;

    fn subscribe(&self) -> Channels<u64> {
        Channels::from_vec(self.live.clone())
    }

    fn cut(&self) -> u64 {
        self.cut
    }

    fn above(&self, live: Channels<u64>, cut: u64) -> Channels<u64> {
        live.filter_map(move |v| (v > cut).then_some(v))
    }
}

impl Ranged<u64> for TestSeam {
    type Item = u64;

    fn range(&self, cut: u64) -> Channels<u64> {
        assert_eq!(cut, self.cut);
        Channels::from_vec(self.history.clone())
    }
}

proptest! {
    #[test]
    fn seam_is_history_then_the_live_part_above_cut(
        history in prop::collection::vec(0u64..100, 0..8),
        live in prop::collection::vec(0u64..100, 0..8),
        cut in 0u64..100,
    ) {
        let seam = TestSeam {
            live: live.clone(),
            history: history.clone(),
            cut,
        };
        let got: Vec<u64> = blend_chain(&seam, &seam).into_iter().collect();
        let want: Vec<u64> = history.into_iter().chain(live.into_iter().filter(|v| *v > cut)).collect();
        prop_assert_eq!(got, want);
    }
}

/// Two producers into one merge: every item once, no loss, no duplication.
/// The model test is sequential; this is the real `Select` interleaving.
#[test]
fn concurrent_merge_loses_nothing() {
    const N: u64 = 2_000;
    let (feed_a, a) = Feed::channel(64, "producer a");
    let (feed_b, b) = Feed::channel(64, "producer b");
    let mut merged = a.merge(b);

    let producer_a = std::thread::spawn(move || {
        for i in 0..N {
            if !feed_a.send(i) {
                return;
            }
        }
        feed_a.close();
    });
    let producer_b = std::thread::spawn(move || {
        for i in 0..N {
            if !feed_b.send(N + i) {
                return;
            }
        }
        feed_b.close();
    });

    let mut got: Vec<u64> = std::iter::from_fn(|| merged.recv().ok()).collect();
    producer_a.join().expect("producer a");
    producer_b.join().expect("producer b");
    got.sort_unstable();
    assert_eq!(got, (0..2 * N).collect::<Vec<_>>());
}
