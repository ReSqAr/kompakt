# Internal model — `crossbeam-combinators`

Not a decision record. My current model, open questions, and what I expect the
user to answer. Check it to correct me cheaply.

## Model of the target

```text
  crossbeam-combinators        zslg                     nvzn
  ─────────────────────        ────                     ────
  Channels<T> graph            domain items + seams     folds, live views
  verdict algebra              WatermarkGate            subscribers
  fan-out / scan adapters      domain sources           (crate direct)
  cancellation                 direct imports
```

Two independent axes, often conflated:

1. **Mechanics** — leaves, select, routing, batching, scan adapters, fan-out.
   Obviously generic.
2. **Verdict policy** — what "the producer finished" means, whether an abort
   condemns a merge/chain, and how cancellation is distinguished. This is where
   the subtle tests live (silent-item-loss bugs); it is *also* generic, but it
   is the real product of the crate.

My bet: the user wants 1 and 2 extracted together; `zslg` should only name
domain producers and the seam policy. If 2 stays in `zslg`, every extractable
combinator needs a policy parameter and the tests split along the wrong line.

## Pattern inventory (candidates to extract)

| Pattern | Where today | Generic? |
| --- | --- | --- |
| leaf + `Closer` | `channel.rs` | yes |
| fair binary merge | `channel.rs` | yes |
| chain with abort condemnation | `channel.rs` | yes |
| unfold/resync segments | `channel.rs` | yes |
| guard: race a source against a disconnect | `channel.rs` | yes |
| cancellation (`CancelHandle`) | `channel.rs` | yes |
| map / filter_map | `channel.rs` | yes |
| batch drain | `channel.rs` | yes |
| async bridge (`into_stream`) | `channel.rs` | yes (tokio feature) |
| fan-out with lag hang-up | `learner/fanout.rs` | yes — moves (S1) |
| level wake (`Wakes`) | `learner/fanout.rs` | yes — moves (S1) |
| **seam: subscribe → cut → replay → live, dedup above cut** | 4 sites | yes — `Tail`/`Ranged` |
| scan/range → channel + closer | `pump_range`, `range`, archive | yes — feed/scan adapter moves (S1) |
| batch fold loop | `nvzn/dispatcher/fold.rs` | partly (consumer side) |
| reconnect loop | `quorum/coordinator.rs`, subscriber | no (domain) |
| watermark gate | `learner/read.rs` | no (domain) |

S1 chosen: the crate owns both halves. `Fanout`/`Wakes`, the feed/scan
adapter, cancellation and the async bridges move; domain sources, the watermark
gate and reconnect loops stay in zslg.

The seam is the candidate for "abstract and refine". Its invariant:

```text
  t0  subscribe live            (must be first)
  t1  cut = frontier/watermark  (snapshot after subscribing)
  t2  replay [cursor, cut]      (gap-free)
  t3  live, keep > cut          (overlap = duplicates, never loss)
```

The same shape serves index order (replica), HLC order (unstable), and HLC
order with a watermark event (stable). The cut type and the keep predicate
differ; the ordering hazard does not.

Hard case: `nvzn::live::begin` drains the live tail during the replay,
because the tail queue is bounded and must not overrun. It composes `open` +
`range` + `merge` directly (it needs the cut for `Coverage`); `blend_chain`
covers the zslg seams. `blend_merge` was removed after review: it had no users.

### Current seam model (settled)

The seam is two traits and one scheduler:

- `Tail` — `subscribe()` + `cut()`, with provided `open()` enforcing
  subscribe-before-cut and calling `above(live, cut)`. `above` is a *pure
  filter*: `Channels<Item> -> Channels<Item>`. `open` returns raw
  `(Channels<Item>, Cut)`. No type change.
- `Ranged<C>` — `range(cut) -> Channels<Item>`; raw, bounded at `cut`, closing
  only after the cut is served.
- `blend_chain` — `open`, `range(cut).chain(live)`; output `Channels<Item>`.
- The `Item` type is the domain's. A domain that needs an in-band range-end
  signal puts it in that type (nvzn: `FoldItem::ReplayDone`, emitted inside its
  `Ranged::range`). A domain that does not (zslg) has no such variant.
- Every seam now uses `above` for the live overlap with its cut (stable
  frontier, replica index, unstable/nvzn watermark); the outer cursor,
  `WatermarkGate` and `Held` remain extra nets, not the only correctness.
- No `BlendItem`, `MergeItem`, `verify`, or crate-side token.

`Admit` is not part of the blend API; it is an implementation detail of a
domain's `above` (cursor, fixed cut, watermark gate). nvzn's `Ranged::range`
appends its unit `ReplayDone`; the consumer maps it to `finish_scan`. zslg has
no done item. No `split`, no `MergeItem`, no `verify`.

Consequence for nvzn: the `range` + `merge` composition keeps the tail drained
during the scan, so the cold-start overrun does not occur; `Held` dedups, and
`ReplayDone` releases `finish_scan` waiters. The scan shares `recv` with the
tail; if tail load slows the scan, that is a latency cost, not a correctness
one. On `live-started`, `finish_scan` is also what bumps `changed` for
`wait_for` (Seen/TimedOut, which waits through NotStarted/Overflowed); `run`'s
drop guard releases waiters on every exit.

## Producer half (S1, D1)

- `Feed<T>` — the write half. `Feed::channel(cap, what) -> (Feed<T>,
  Channels<T>)`; `send` (blocking, false when the consumer is gone),
  `try_send`, `close` (idempotent/consuming). Drop without `close` yields
  `Aborted`. The caller writes the loop and chooses the spawn; the crate never
  spawns. `Closer` is crossbeam-backed, no tokio oneshot.
- `Fanout<T>` + `Wakes` — unchanged, crossbeam-only: bounded per-subscriber
  queues, hang up on lag; level wake as a separate primitive.

## Performance model

**Frozen for the extraction.** The current `Source`/`Select` core is lifted
as-is; the extraction is behaviour-preserving. Performance is a later
experiment on the stable foundation: around ten strategies, measured against
`channel_bench`, then keep what sticks. The current core is the baseline.

Current per-item `recv` cost is roughly:

```text
  leaf_count() walk  +  Select::new  +  register walk  +  ready()  +  poll() walk
```

Two tree walks plus select setup per item, all through `Box<dyn Source<T>>`.
Ideas, to be narrowed later:

1. Fast path: `try_recv` over leaves before building a `Select`; only block when
   every live leaf is empty. The bench's "every leaf prefilled" case then skips
   `Select` entirely.
2. Flatten the static topology once into a leaf vector + routing table; only
   `Unfold` invalidates it. Removes recursive `leaf_count`/`poll` walks.
3. Enum/generic nodes instead of `Box<dyn Source>`; hard with re-typed streams
   (`filter_map` changes `T`), so likely keep dynamic dispatch and shrink what
   is dispatched.
4. Batch-first API (`ready_batch`, callback drain) as the throughput path;
   reserve `recv` for latency.
5. Avoid `mem::replace(Done(Closed))` churn in merge poll if measurable.

Open: is the target (a) no regression, (b) a specific ns/item, (c) beat the
current bench by a stated factor? Need the user's number or shape.

## Settled

- Crate owns the verdict protocol (`Closed`/`Aborted`/`Cancelled`, `Closer`).
- Lift-and-shift first; the current `Source`/`Select` core is frozen.
- Performance is a later experiment (~10 strategies) on a stable foundation.
- Seam: `Tail`/`Ranged`, one domain-chosen `Item`, `blend_chain`;
  `above` is a pure filter; no `verify`, no `split`, no crate-side token.
- S1: the crate owns both halves (core + seam + `Fanout`/`Wakes` + `Feed`).
- D1: crossbeam-only core; `Closer` crossbeam-backed; `tokio` feature for
  `into_stream`/`from_token`.
- `Feed::channel(cap, what) -> (Feed, Channels)`; the caller spawns and closes.
- F2: extraction (Checkpoint A) then migration (Checkpoint B).
- No `zslg` channel re-exports; `nvzn` uses the crate directly; `Feed` stays
  crate-internal. Feature `tokio`; module `blend.rs`.
- Base branch `channel-extraction`, from `origin/main` and `live-started`.

## Remaining risks / open

- Stable `range` coverage is a per-domain test; no generic `verify`.
- `WatermarkGate` stays outside `blend_chain`; its state spans `unfold` cycles.
- `StableCut`'s trailing watermark is the last item of `zslg`'s stable
  `Ranged::range`; the crate has no watermark concept.
- nvzn `ReplayDone` must call `finish_scan`, which bumps `changed`/notifies
  `scanned`; the error path must release waiters for `wait_for`.
- The later performance experiment: wait model vs graph representation; ~10
  strategies, benchmark-driven.
