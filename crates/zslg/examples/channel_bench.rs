//! Microbenchmark for `zslg::Channels`' poll-routing cost.
//!
//! An example, not a test: nothing runs it in CI and no number is pinned. It
//! exists to put a figure on the ordinal-routing change, in the shape this repo
//! already sanctions for perf work — an example binary, no `criterion`, no
//! `benches/` (CLAUDE.md §14, `docs/TESTING.md` §Status), alongside
//! `loadgen.rs`. Being an example is also the point of it: it drives
//! `Channels` through the same public surface an embedder has, so it cannot
//! quietly measure something only the crate's insides can reach.
//!
//! ## What is being measured
//!
//! One `recv()` on a topology where **every leaf already holds items**, so
//! `Select::ready` returns immediately and the timing is the routing path
//! alone — never a block, never a producer thread:
//!
//! ```text
//!   recv()  ->  leaf_count()   walk the tree           ]
//!           ->  Select::new()  + register() each leaf  ] this is the cost
//!           ->  ready()        already-ready, no park  ] under test
//!           ->  poll(ordinal)  descend to the leaf     ]
//! ```
//!
//! Topologies are left-leaning merge stacks — `a.merge(b).merge(c)` — because
//! that is what the combinator builds. Arity 2 is the only shape in production
//! (`replica/mod.rs`, `quorum/coordinator.rs`, `network/server/stream_handler.rs`);
//! 4/8/16 are there to expose how each scheme scales, not to model real load.
//!
//! `ready_batch` is a **control**: it takes one item through `recv` and the
//! rest through `drain_first`, which this change never touched. It should move
//! by noise. If it moves, the run is noise.
//!
//! Run it (release, or the numbers are meaningless):
//!
//! ```sh
//! timeout 600 cargo run --release -p zslg --example channel_bench
//! ```

#![allow(
    unused_crate_dependencies,
    reason = "example crates inherit all package deps and use only a subset"
)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;

use zslg::Channels;

/// Items pulled per timed round, split evenly across the leaves.
const ITEMS_PER_ROUND: usize = 120_000;
/// Timed rounds after the discarded warmup. Odd, so the median is a sample.
const ROUNDS: usize = 9;

/// A built topology plus the senders keeping its leaves alive.
///
/// The senders must outlive the timed loop: a leaf whose last `Sender` dropped
/// disconnects, and the collapse would change the topology mid-measurement.
struct Rig {
    ch: Channels<u64>,
    keepalive: Vec<Sender<u64>>,
    items: usize,
}

/// `leaves` infinite leaves, each pre-filled with `per_leaf` items, merged
/// left-leaning. Pre-filling is what keeps `ready()` from ever parking.
fn merged(leaves: usize, per_leaf: usize) -> Rig {
    let mut keepalive = Vec::with_capacity(leaves);
    let mut built: Option<Channels<u64>> = None;
    for _ in 0..leaves {
        let (tx, rx) = crossbeam_channel::bounded::<u64>(per_leaf);
        for n in 0..per_leaf {
            let v = u64::try_from(n).unwrap_or(0);
            tx.send(v).unwrap_or(());
        }
        keepalive.push(tx);
        let leaf = Channels::from_infinite(rx, "bench leaf");
        built = Some(match built.take() {
            Some(acc) => acc.merge(leaf),
            None => leaf,
        });
    }
    Rig {
        ch: built.unwrap_or_else(|| Channels::from_vec(Vec::new())),
        keepalive,
        items: leaves.saturating_mul(per_leaf),
    }
}

/// Median and minimum of a set of per-round durations, in nanoseconds per item.
///
/// Both, because they answer different questions: the minimum is the cleanest
/// observation of the code (least preemption, warmest cache), the median says
/// what a typical pull costs. A gap between them is scheduler noise, and a
/// verdict that only holds on one of the two is not a verdict.
fn report(label: &str, mut rounds: Vec<Duration>, items: usize) {
    rounds.sort_unstable();
    let per_item = |d: Duration| -> u128 {
        u128::try_from(items)
            .ok()
            .and_then(|n| d.as_nanos().checked_div(n))
            .unwrap_or(0)
    };
    let min = rounds.first().copied().unwrap_or_default();
    let median = rounds.get(rounds.len().checked_div(2).unwrap_or(0));
    let median = median.copied().unwrap_or_default();
    println!(
        "{label:<34} {:>8} ns/item (min)   {:>8} ns/item (median)",
        per_item(min),
        per_item(median),
    );
}

/// Time `f` over `ROUNDS` rounds, discarding one warmup round first.
fn timed(label: &str, items: usize, mut build: impl FnMut() -> Rig) {
    let mut rounds = Vec::with_capacity(ROUNDS);
    for round in 0..ROUNDS.saturating_add(1) {
        let mut rig = build();
        let start = Instant::now();
        for _ in 0..rig.items {
            match rig.ch.recv() {
                Ok(v) => {
                    black_box(v);
                }
                Err(_) => break,
            }
        }
        let elapsed = start.elapsed();
        if round > 0 {
            rounds.push(elapsed);
        }
    }
    report(label, rounds, items);
}

fn bench_recv_routing() {
    println!("\n== Channels::recv, every leaf pre-filled ==");
    for leaves in [1usize, 2, 4, 8, 16] {
        let per_leaf = ITEMS_PER_ROUND.checked_div(leaves).unwrap_or(1);
        let items = leaves.saturating_mul(per_leaf);
        timed(&format!("recv, {leaves} leaf/leaves"), items, || {
            merged(leaves, per_leaf)
        });
    }
}

fn bench_recv_with_filtered_retry() {
    // Half the items are filtered out, so every delivered item costs two trips
    // through register/ready/poll.
    println!("\n== Channels::recv through a filter_map dropping half ==");
    let per_leaf = ITEMS_PER_ROUND.checked_div(2).unwrap_or(1);
    let mut rounds = Vec::with_capacity(ROUNDS);
    let delivered = per_leaf; // 2 leaves x per_leaf items, half survive
    for round in 0..ROUNDS.saturating_add(1) {
        let rig = merged(2, per_leaf);
        let keepalive = rig.keepalive;
        let mut ch = rig.ch.filter_map(|v| (v % 2 == 0).then_some(v));
        let start = Instant::now();
        for _ in 0..delivered {
            match ch.recv() {
                Ok(v) => {
                    black_box(v);
                }
                Err(_) => break,
            }
        }
        let elapsed = start.elapsed();
        drop(keepalive);
        if round > 0 {
            rounds.push(elapsed);
        }
    }
    report("recv, 2 leaves, 1 retry/item", rounds, delivered);
}

fn bench_ready_batch_control() {
    // Control. `ready_batch` takes its head through `recv` and everything else
    // through `drain_first`, which this change did not touch — so this number
    // must not move. If it does, the run is noise and the others mean nothing.
    println!("\n== control: ready_batch(256), 2 leaves ==");
    let per_leaf = ITEMS_PER_ROUND.checked_div(2).unwrap_or(1);
    let mut rounds = Vec::with_capacity(ROUNDS);
    let items = per_leaf.saturating_mul(2);
    for round in 0..ROUNDS.saturating_add(1) {
        let mut rig = merged(2, per_leaf);
        let start = Instant::now();
        let mut seen = 0usize;
        while seen < rig.items {
            let Some(batch) = rig.ch.ready_batch(256) else {
                break;
            };
            for v in batch {
                black_box(v);
                seen = seen.saturating_add(1);
            }
        }
        let elapsed = start.elapsed();
        if round > 0 {
            rounds.push(elapsed);
        }
    }
    report("ready_batch(256), 2 leaves", rounds, items);
}

fn main() {
    bench_recv_routing();
    bench_recv_with_filtered_retry();
    bench_ready_batch_control();
}
