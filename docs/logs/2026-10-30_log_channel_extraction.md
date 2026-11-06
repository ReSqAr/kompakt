# Log — extracting `channel.rs` into `crossbeam-combinators`

Intent (from the user):

- Extract `zslg::channel` into a workspace crate `crossbeam-combinators`.
- Move more data-flow patterns out of `zslg` into it — blending (archive replay +
  live), merge, etc. Goal: `zslg`'s data flow reads as intent; the reusable
  building blocks live and are exhaustively tested in the new crate.
- Be aggressive: extract broadly, and refine at least one pattern into a proper
  abstraction.
- The crate is generic, so it needs a performance pass; a full reimplementation
  is on the table if it preserves behaviour.
- Scope called out by the user: `zslg` → `channel.rs` (and its consumers,
  including `nvzn`).

This log is append-only: settled questions and the reasons, including rejected
alternatives.

---

## Q1 — Where is the seam between the generic crate and `zslg`?

Options put to the user:

- **A (recommended).** `crossbeam-combinators` owns the whole pipeline runtime:
  `Channels<T>`, the `Source` graph, `End { Closed, Aborted{what}, Cancelled }`,
  `Closer`, `Cancellation`, `into_stream`. `zslg` re-exports it and becomes graph
  construction. Rationale: the hard part is the verdict algebra — merge/chain/
  cancel/guard semantics are inseparable from it — and it is generic.
- **B.** Crate owns only structural combinators; a policy trait supplies
  termination semantics. Keeps `zslg`'s `End` at home, at the cost of a policy
  parameter on every node.
- **C.** Crate is a pure combinator layer over crossbeam receivers (Iterator-
  like); `zslg` keeps all lifecycle. Rejected in advance: duplicated seam and
  verdict logic stay in `zslg`, which defeats the extraction.

**Answer: A.** The crate owns the whole pipeline runtime — `Channels<T>`,
`Source` graph, `End`, `Closer`, `Cancellation`, `into_stream`, fan-out. `zslg`
re-exports (`Channels`, `ChannelEnd`, `Cancellation`, `CancelHandle`) and keeps
only its domain sources, seams and `WatermarkGate`.

Consequence: the verdict tests move with the core, and `zslg`'s public surface
is unchanged for `nvzn`/`btrb`.

## Q2 — Is the current routing core disposable after the move?

Options:

- **A (recommended).** Lift-and-shift first, green with the existing core and
  all 53 tests; then replace the internals behind the same `Channels` API,
  bench-gated against `channel_bench`.
- **B.** Write the new core now, existing tests as the spec, skip the
  intermediate lift.
- **C.** Keep the core; add only the `try_recv` fast path and micro-optimize.

Context: every `recv` today is `leaf_count()` walk + `Select::new()` +
`register()` walk + `poll(ordinal)` walk, through `Box<dyn Source<T>>`.

**Answer: A.** Extraction is a behaviour-preserving checkpoint; the routing
core is then replaced behind the same public API. The bench is the performance
contract, the moved tests are the semantics contract. This makes the
reimplementation a separate, reviewable step instead of a rewrite tangled with
the move.

## Q3 — Shape of the seam abstraction

Under exploration after Q2. The user wants traits instead of closures for the
sources, and is not convinced the seam is the right decomposition.

Decomposition offered:

| Concern | Candidate home |
| --- | --- |
| live subscription | `Handoff::live` |
| cut snapshot | `Handoff::cut` |
| bounded replay | `Handoff::replay` |
| overlap / watermark admission | separate `Admit` gate (monotone cursor, fixed cut, domain gate) |
| one vs two consumers | provided `sequential` / `split` |
| resync cycles | existing `Unfold` |
| composition | existing `Chain` / `Merge` |

Three trait shapes:

- **H1 (recommended).** One `Handoff` trait with `live`/`cut`/`replay`; provided
  `sequential` and `split` call them in that order, so the ordering defect is
  crate-owned. Admission stays out of the seam.
- **H2.** Capability traits `Live<T>`, `CutPoint`, `Replay<T, C>` composed by a
  `Handoff` type/free function. More reusable pieces, more ceremony, and the
  order exists only at the composition site.
- **H3.** One `Snapshot::snapshot() -> (Channels<T>, Cut)` plus `Replay`.
  Strongest atomicity inside the domain, but the crate cannot enforce the
  subscribe-before-cut order.

Key observation: the four sites' overlap rules are not one predicate. Site 1 is
a shared monotone cursor over the whole output; site 2 is a stateful watermark
gate that also emits events; site 3 is a fixed cutover filter; nvzn is a keyed
map (`Held`). That argues for admission as its own abstraction, not a
`keep(item, cut)` closure on the seam.

Status: **settled**.

**Q3 settled.** The crate owns the seam contract, not the boundary. Two traits,
one domain-chosen `Item` type, two thin schedulers:

- `Tail`: `subscribe()` first, `cut()` second, `above(live, cut)` is a pure
  filter (`Channels<Item> -> Channels<Item>`); provided `open()` fixes the
  order and returns raw `(Channels<Item>, Cut)`. `Cut: Copy`.
- `Ranged<C>`: `range(cut) -> Channels<Item>`; raw, bounded at `cut`, closing
  only after the cut is served.
- `blend_chain(tail, ranged) = range(cut).chain(live)`.
- `blend_merge(tail, ranged) = range(cut).merge(live)`.
- Neither trait nor either blend changes the item type. The domain picks it:
  nvzn uses `Item { Data(..), ReplayDone }` (unit `ReplayDone`, emitted inside
  its own `Ranged::range`); zslg uses its ordinary event type and has no done
  variant.
- The boundary is a domain item when the domain needs one, absent otherwise.
  `cut` is only the range bound; `above` only filters live against it.

Rejected on the way: `split`, `blend_merged`, `MergeItem`, `RangeItem`,
`Covered(cut)`, `.verify`, and a public `Admit` gate. `verify` was circular
(a token appended by the same layer that checks it); a real coverage check
would need a keyed assertion inside `Ranged::range`, which is domain work.

Refinement after discussion: `Replay` is renamed `Ranged` (it returns a range,
it does not re-run); its output carries an in-band `RangeItem::Covered(cut)`
token, so the cut is a verified coverage claim rather than a convention.
`blend_sequential` is the only blend; `split` and `blend_merged` were dropped.

Second refinement: live filtering moves into the subscription, not a separate
`Admit` parameter. `Subscription` (rename: `Tail`) grows `above(live, cut)`,
called by the provided `open()` after `subscribe()` and `cut()`. `open()`
returns the live half already filtered. `cut: Copy + PartialEq`. `Admit`
survives only as an implementation detail of a domain's `above`.

Third refinement: two schedulers over the same raw halves —
`blend_chain` (history then live; zslg) and `blend_merge` (interleaved; nvzn).

`Tail::above` is a pure filter: `Channels<Item> -> Channels<Item>`. It does not
change the type. `Tail::open` returns raw `Channels<Item>` plus the cut;
`Ranged::range(cut)` returns raw `Channels<Item>`, closing at the cut.

Final shape: **one `Item` type chosen by the domain, unchanged by the crate.**
Neither `Tail` nor `Ranged` converts it. `blend_chain`/`blend_merge` are
`chain`/`merge` over `Channels<Item>` and know nothing about a boundary.

A consumer that needs an in-band "range ended" signal puts it in its own item
type: nvzn defines `Item { Data(..), ReplayDone }` and its `Ranged::range`
appends `ReplayDone` itself (using `then`). zslg's item is its ordinary event
type; it needs no done item, so it has none. No `BlendItem`, no `MergeItem`, no
`verify`, no crate-side token.

## Q4 — Extraction scope: consumer half only, or producer half too?

Options:

- **S1 (recommended).** Both halves: core + seam + `Fanout`/`Wakes` + a
  feed/scan adapter + cancellation and async bridges.
- **S2.** Consumer half only; `Fanout`/`Wakes` and the feed pattern stay in
  zslg.
- **S3.** Also lift domain-adjacent pieces (`WatermarkGate`, reconnect loops);
  rejected as not generic.

**Answer: S1, aggressively.** Even one-off generic patterns should be
extracted. The crate owns the full subscription protocol: how a live stream is
created, how it hangs up on lag, how a finite range is fed and closed. zslg
keeps only domain sources (archive replay, `WatermarkGate`, reconnect loops).

## Q5 — Dependency boundary

- **D1 (recommended).** Core is `crossbeam-channel` only; `Closer` becomes a
  dependency-free state; `feed` is synchronous (the caller spawns);
  `into_stream`, `from_token`, `spawn_feed` behind `tokio`/`futures` features.
- **D2.** Tokio in the core; lift as-is.
- **D3.** No async in the crate; zslg keeps the bridges.

**Answer: D1.** The core stays usable from sync code and from any runtime.
`Closer` rewrites to an open/closed/aborted state (or a capacity-1 crossbeam
channel); `Drop` marks aborted only while still open.

## Q6 — `Feed` API shape

Options: a factory that takes the producer closure and spawns it (with a
`Spawn` strategy type), versus `Feed::channel(cap, what)` returning
`(Feed<T>, Channels<T>)` and the caller spawning.

**Answer: the latter.** Thread handling belongs to the caller. The caller owns
the loop, the spawn (`thread`, `spawn_blocking`, or anything), and calls
`feed.close()` on the normal exit. `Feed` is just the write half: `send` /
`try_send` / `close`; dropping it without `close` yields `Aborted`. No `Spawn`
trait, no factory closure.

```rust
let (feed, ch) = Feed::channel(cap, "replica historic tail");
tokio::task::spawn_blocking(move || {
    let scan = for_each_entry(&storage, from, to, &mut |idx, data_id, payload| {
        feed.send(Ok((idx, Data { data_id, blob: copy(payload) })))
    });
    if let Err(e) = scan { let _ = feed.send(Err(e)); }
    feed.close();
});
ch
```

## Q7 — Performance / wait model

Options: P1 (keep `Select`, flatten routing, fast path), P2 (producer-side
wake), P3 (micro-optimize only).

**Decision: none now.** Do not change the current `Source`/`Select` core.
The extraction is strictly behaviour-preserving; only the dependency boundary
changes (`Closer` crossbeam-backed, `Feed`, async bridges behind features).
Performance is a later experiment on a stable foundation: around ten
strategies, creatively chosen, measured against the existing bench, then keep
what sticks. The current core is the baseline, not the target.

## Q8 — Public surface and the §13 items

- Feature name: `tokio` (one feature; core has no tokio).
- Module: `blend.rs`.
- `nvzn` uses `crossbeam-combinators` directly.
- `zslg` has **no channel re-exports**. It imports the crate directly (88
  `crate::channel::…` references in 38 files become `crossbeam_combinators::…`
  imports); no alias module. Its public traits name the crate's types
  directly. Consumers import `Channels`, `End`, `Cancellation`, `CancelHandle`
  from the crate. `Feed` is crate-internal; `zslg` does not re-export it.
- `StableCut` is a named struct `{ frontier, watermark }`; the trailing
  `Watermark` is the last item of `zslg`'s stable `Ranged::range` (domain
  behavior, not crate support), ordered after the replay and before the chain
  switches to live.

## Q9 — implementation notes (Checkpoint B)

- nvzn needs the cut to set `Coverage::Scanning` before its consumer starts, so
  `begin` uses `seam.open()` + `seam.range(until).merge(tail)` rather than
  `blend_merge`. The ordering is still `open`; `blend_merge` is the same
  composition where the cut is not needed.
- `FoldItem` is `{ Live(Data), Scan(Data), ReplayDone, Failed(FoldSource,
  Error), ScanEnded(End) }`. The Live/Scan split keeps the `live_scan` metric;
  `Failed`/`ScanEnded` keep the two stop reasons (`LiveScanEnded` vs
  `UnstableTailEnded`).
- `Ranged::range` is a `Feed` + `spawn_blocking` adapter. It stops on
  `hlc > cut` or `Watermark >= cut` (the stable read emits the trailing
  watermark; waiting for an entry above the cut would block forever), emits
  `ReplayDone` only on a clean scan, and closes the history half on
  cancellation so the live `Cancelled` wins.
- Checkpoints: the extraction is one squashed commit on
  `channel-extraction`; the review fixes are amended into it.

## Q10 — review fixes

- `run` gets a `finish_scan` drop guard, so `Cancelled`/aborts/panics all wake
  `scanned`; a cancel-mid-scan test pins it.
- `Tail::above` filters the live overlap in every seam (stable frontier,
  unstable cut watermark, nvzn cut watermark); the outer cursor/gate/`Held`
  remain as extra nets, not the only correctness.
- `blend_merge` deleted; `Tail`/`Ranged` kept (seam bugs justify the structure).
- Seam docs moved above `resyncing_*` so the functions keep them; resync
  warning + metric live together in the cycle.
- `FoldItem` split into `TailFailed`/`ScanFailed`/`ScanEnded`; overflow comment
  restored; README and `Channels::from` docs updated.
- `OrderedChannel` deferred: a checked ordered seam at the API boundary, later.

## Branch context — `channel-extraction`

Work is on `channel-extraction`, branched from `origin/main` (3202bcf) and
fast-forwarded to `live-started` (e69b361). `live-started` includes:
- `wait_for` returns only `Seen` or `TimedOut`; it waits through
  `NotStarted`/`Overflowed`, waking on `changed`/`retired`.
- `Live::started` was added and removed again; nothing depends on it.
- owned event subsets gained `TryFrom<Event>` (unrelated to the channel work).

Impact on the design: the seam decisions are unchanged. The nvzn migration
must keep `finish_scan` on `ReplayDone` bumping `changed` and notifying
`scanned`, and the error path must release waiters the same way, so a
`wait_for` caller does not sit to its deadline.

## Facts gathered during exploration

- `zslg/src/channel.rs`: 2,434 lines, 53 tests. Non-test core ≈ 1,450 lines.
  Public through `zslg::{Channels, End as ChannelEnd, Cancellation, CancelHandle}`;
  the module itself is `pub(crate)`.
- `End` has three verdicts and a producer-side `Closer` (explicit close vs
  abort), plus `Cancellation` because a tokio token cannot wake a crossbeam
  `Select`.
- Graph: `LeafState`, `MergeState`, `ChainState`, `Guarded`, `Unfold`,
  `FilterMap`. `recv` rebuilds a `Select` per item; `try_recv` drains by
  `drain_first`; `ready_batch` takes one through `recv` + the rest through
  `drain_first`.
- `learner/fanout.rs` (265 lines) is a producer-side primitive (bounded
  per-subscriber queues, hang up on lag, `Wakes` level variant). Also generic.
- Seam pattern ("blending") appears at least four times, each hand-rolled:
  `learner/read.rs::resyncing_tail`, `resyncing_unstable_tail`,
  `replica/log.rs::subscribe_from`, `nvzn/live.rs::begin`.
- Performance baseline: `zslg/examples/channel_bench.rs` measures
  recv-routing per leaf count (1/2/4/8/16), filtered retry, and `ready_batch`
  as control. No numbers pinned; nothing runs it in CI.

## Rejected / settled

(none yet)
