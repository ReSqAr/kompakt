# Channel seams — internal model

Working document for the IAIC interview. Not a design doc.

## How I read the feedback

The previous round moved the *plumbing* into the crate but left three kinds of
residue:

1. **Mechanical duplication** — producer spawn, fan-out owner, `empty_historic`,
   the flaky test. These are safe to fix and should not change the architecture.
2. **Wrong seam shape** — `blend_chain` does not return the cut, so every caller
   that needs the cut rebuilds the seam; `above` is repeated because the cut is
   not connected to the item key.
3. **Missing type-level invariants** — finite vs infinite is a constructor
   choice; `resume_on_abort` cannot require an infinite source; `Ranged::range`
   cannot promise a finite one. The previous `Channel::empty()` bug is the proof.

The ranking in the feedback is by code removed and invariants checked. My
reading is that A (producer constructor) and E (fan-out owner) are independent
and low-risk; B/C (seam/resync) depend on the shape chosen for the cut and the
item key; D (finite/infinite) is foundational and invasive.

## Recommended architecture (to test against the user's answers)

### Kind marker

Decision: **no default**. Every value names `Finite` or `Infinite`, including
combinators, signatures, and test doubles. This is deliberately more verbose
than `Channel<T>`; the verbosity is the reminder.

```rust
pub struct Finite;
pub struct Infinite;

pub struct Channel<T, K> { .. }        // no default: the kind is explicit

impl<T> Channel<T, Finite> {
    fn recv(&mut self) -> Result<T, End>;          // Complete possible
}
impl<T> Channel<T, Infinite> {
    fn recv(&mut self) -> Result<T, AbortReason>;  // termination is an error
}
```

Combinators preserve the kind. `chain(Finite, Infinite)` is `Infinite`;
`merge` is `Infinite` if either side is; `filter_map`/`map` preserve. A
`Finite` source may also be read as `Infinite` only through an explicit
`assume_infinite`? No: that is the bug. Instead `resume_on_abort` starts from an
infinite source only, and a finite phase is chained into it.

The risk is churn plus verbosity: every consumer that matches `End::Complete` on
a source that is actually infinite must switch to the abort-only surface, and
every signature spells the kind. The benefit is that `Channel::empty()` cannot
be returned for `subscribe_*`, and `Ranged::range` can return
`Channel<T, Finite>`. Possible aliases (`Finite<T>`, `Infinite<T>`) are a
convenience only; they must not introduce a default.

### Seam

```rust
trait Seam {
    type Item;
    type Cut: Copy;
    fn subscribe(&self) -> Channel<Self::Item, Infinite>;
    fn cut(&self) -> Self::Cut;     // only reachable through open()
}

struct Segment<T, C> { live: Channel<T, Infinite>, cut: C }

impl Segment {
    fn chain(self, history: Channel<T, Finite>) -> Channel<T, Infinite>;
    fn merge(self, other: Channel<T, Infinite>) -> Channel<T, Infinite>;
}
```

`open()` is a free function:
`subscribe()` then `cut(&live)` then `above()` then `Segment`. `Live` is the
proof that the subscription exists, so `cut` cannot be called on its own.
**Decision D2:** one trait, `above` required. The crate provides
`filter_above::<K>()` for seams whose cut is `Option<K>`; unstable and nvzn
pass a closure because their cut is `Hlc` and their key is `DataId`. No
`KeyedSeam` second trait.

### Resync / Resumable

```rust
resync(make_seam) -> Channel<T, Infinite>
```

does the `resume_on_abort` loop, weak upgrades, and segment assembly. The
factory is `FnMut(Cursor, End) -> Option<Seam>`; it owns the resume position
(D3) and may log (D4). A `Resumable` wrapper for a position keeps the cursor (no
`AtomicU64`); it does **not** dedup. `FnMut` combinators are needed for the
cursor to live in the closure state.

### Recv and pump

Two layers:

```text
Source (private)  select/poll leaf used by Channel
Recv  (public)   recv / try_recv / ready_batch / end_reason
Channel          Recv + Source-backed; the only type that can merge/chain
Resumable        Recv, wraps one Channel/OrderedChannel per segment + cursor
```

`Resumable` is **not** a `Source`; it becomes a `Channel` only through an
explicit pump. A pump runs a blocking `Recv` on a caller-chosen spawn policy and
feeds a `Channel`; the tokio feature adds `TokioBlocking`. `produce_blocking`
is the write-side dual. This removes the receiver+producer-task shape from
`HistoricalReplay` and the scans.

`block_on` exists in `HistoricalReplay` only because the archive `stream` is
`async` while `stitch`'s fallback is sync. **Decision D9:** `produce_async` is
the async primitive; the archive `stream` becomes a sync `fn -> ArchiveStream`
that spawns the opens and carries open errors in-band. `HistoricalReplay` drops
`block_on`. The only requirement is a runtime handle for the task; the current
path already needs one. `futures::Stream -> Recv` is a thin adapter over
`produce_async`.

**Decision D10:** `Recv` carries the lazy single-source combinators; `merge` is
the multi-source operation that needs `Source`/a pump. `Resumable` composes
through nvzn's scan without a thread; only `merge` needs a `Channel`.

**Decision D11-D13:** keep the split `(fanout, registry)`; the producer-side
`Fanout` owns `tx` + `Shutdown`, the registry stays independent. Caps may have
defaults; `OverflowPolicy` is explicit. The earlier "no Fanout wrapper" was
about hiding the parts, not about forbidding an owner forever; this is the
producer half, analogous to `Feed`.

### Producer and fan-out

`Channel::produce_blocking(cap, what, |feed| ..)` under the `tokio` feature:
closes on normal return, aborts on panic, removes the `Closer` hand-off. A
`Fanout` owner type reverses the previous "no wrapper" decision; it owns
`tx`/`registry`/`shutdown`, `Drop` cancels, `send` returns consumer-gone, and a
config struct carries the three caps.

## Predictions about the user's answers

- Q1 (kind marker): answered **yes, no default**; explicit at every site.
  The open part is only whether aliases are acceptable to keep signatures
  readable.
- Q2 (seam): answered **yes, single trait**; `above` required plus helpers.
- Q3 (resync): answered **no generic dedup**, `next(cursor)` owns the position;
  **no metric hook** beyond the factory's `(cursor, previous)`; **`Recv` +
  pump** rather than a forced channel/thread.
- Q4 (fan-out owner): likely **yes**; the earlier "no Fanout" was about the
  wrapper obscuring the three parts, not about forbidding an owner forever.
- Q5 (async archive): answered **yes**, `produce_async` is the primitive; the
  archive `stream` becomes sync and `block_on` disappears.
- Q5 (scope): likely wants one coherent round if the kind marker is accepted;
  otherwise split.

## Open risks

- The kind marker touches the whole crate and every test double. A partial
  rollout would leave two channel types.
- `block_on` in the archive fallback must move to a sync archive read or run on
  the consumer thread. If the consumer is a tokio worker, that is a bug.
- The seam `above` default depends on the cut being the item key; nvzn is the
  counterexample.
- `FnMut` combinators affect `Channel`'s object-safety/`Box<dyn Source>` design.
  A `Source` is polled by one consumer, so `FnMut` should fit, but the current
  `filter_map`/`take_until`/`resume_on_abort` bounds must be checked.

## Review round — model

Review restarted the interview. Settled: D14 (`Segment::open`), D15 (`Result`
sends). My ranking of the open review points by leverage:

1. R1 `Recv`/pump — SETTLED D16 (dropped; add later if needed).
   problem fall out of it. Expected answer: drop `Recv`, `Resumable` is a
   private `Source`, combinators take `FnMut`.
2. R8 scope — SETTLED D17 (all four slices, in order; more if found).
3. R2 cancel-vs-resume — SETTLED D18 (resume only on ProducerGone).
   `CancelHandleDropped`; node `until` sits outside.
4. R4 cursor — expected: always "next expected" via `Successor`.
5. R3 `Live` — expected: drop it; `Segment::open` is the guarantee.
6. R5 archive — expected: `defer` + `chain_all` + two ordered checks.
7. R6/R7 — doc structure; no decision needed beyond agreeing the order.
