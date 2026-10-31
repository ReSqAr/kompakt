# Channel end verdicts — decision log

Process: `/home/agent/nlnr/docs/iaic.md`. Branch `channel-extraction`.
Append-only: settled questions and decisions, with rejected alternatives.

## Context

`End` is the verdict returned when a `Channels<T>` stops yielding items:

```text
  Closed              producer called Closer::close
  Aborted { what }    producer vanished; items may be missing
  Cancelled           the Cancellation bound with until() fired
```

`take_while` needs a verdict for "the consumer's bound was reached." A first
attempt added `End::Complete`; it was reverted because the concept was not
convincing. The branch is back at `29615fd` with no `take_while` and no
`End::Complete`.

The current variants answer different questions:

```text
  producer integrity:  Closed, Aborted
  consumer action:     Cancelled
```

That is why `Complete` felt wrong: it is a third kind of cause bolted onto a
type that reads like a producer verdict.

## Q1 — What question should `End` answer?

Status: open.

Options:

1. **Producer integrity.** `End` says what happened to the producer:
   `Closed`/`Aborted`. `Cancelled` moves out (or becomes a separate stop
   request). A consumer bound is not an `End`.
2. **Why this stream stopped.** `End` is the consumer-visible stop reason.
   `Cancelled` already fits; a consumer-side `Bounded`/`Complete` does too.
3. **Integrity only.** `End` says whether the stream is whole or truncated:
   `Closed`/`Aborted`. `Cancelled` is a stop, not an integrity verdict. A
   bounded range is whole for the requested range, so it needs another
   representation.
4. **Two axes.** `End { outcome, cause }`, e.g.
   `outcome: Clean | Fault | Stopped`, `cause: Producer | Consumer | Bound`.

Recommendation to discuss: choose between 2 and 3 first. Option 1 forces
`Cancelled` out and leaves `take_while` without a clean answer; option 4 is
probably over-modelled unless a third consumer appears.

## Q2 — Where does bounded completion live, if not in `End`?

Status: blocked on Q1.

The domain already carries range completion in-band:

```text
  nvzn::FoldItem::ReplayDone
  zslg::WindowItem terminals
```

If `End` stays producer/integrity-only, `take_while` needs one of:

- a separate `Stop`/`Completion` result from the bounded combinator;
- an in-band terminal item chosen by the domain;
- no `take_while` at all, keeping the loop at the call site.

This is deliberately not decided before Q1.

## Q1 — Provisional resolution

Status: `Cancelled` is the exception. `End` should be read as a
producer/integrity verdict first; cancellation is a consumer-side stop that
happens to live in the same return type.

This does not yet say whether that exception is acceptable or should move.

## Q2 — Can `Cancelled` be removed from `End`?

Status: open.

The direct answer is: it can move, but it cannot disappear. Cancellation is a
consumer action, not a producer outcome. Something still has to carry it.

Options:

### S1 — Split the return type

```text
  End  = Closed | Aborted { what }          producer verdict
  Stop = End(End) | Cancelled | Bounded     consumer-visible stop
  recv() -> Result<T, Stop>
```

- `End` becomes producer-only; `Cancelled` and `take_while`'s bound both live
  in `Stop`.
- The exception is gone from `End`, but the same information is one level up.
- Wide diff: every `recv`/`end_reason` caller and every composite source
  (`chain`, `merge`, `Guarded`, `resume_on_fault`) must learn `Stop`.
- Composition still needs to distinguish `Cancelled` (hard stop) from `Bounded`
  (clean end of a bounded source) and `End::Closed` (clean producer close).

### S2 — Outermost-only `Cancellable`

```text
  End stays { Closed, Aborted, Cancelled }
  until() returns Cancellable<T>, not Channels<T>
  Cancellable::recv() -> Result<T, CancelledOr<End>>
```

- Cancellation is type-level "outermost only"; it cannot be merged or chained.
- `End` is untouched internally, but `Cancelled` still exists in the wrapper.
- Breaks `until` as a graph combinator; every `until` call site is a boundary
  change.

### S3 — Map cancellation to `Closed`; caller checks the handle

- `until` drops the source and reports `Closed`; the caller already owns the
  `Cancellation` and can ask `is_cancelled()`.
- Smallest type change, but `Closed` no longer distinguishes "producer
  completed" from "consumer cancelled"; composites would treat cancellation as
  a clean branch and keep survivors.
- Only safe if `until` is enforced as the outermost layer.

### S4 — Keep the exception

- `Cancelled` stays in `End`; `take_while` gets no new `End` variant.
- The bound is either handled at the call site or carried in-band by the domain
  (`nvzn::FoldItem::ReplayDone`, `zslg::WindowItem` terminals).

Recommendation to discuss: S1 is the only option that actually removes the
exception from `End` without lying about the producer. S2/S3 move or hide it.
S4 is the status quo, and the domain already has in-band terminals for bounded
completion.

## Q3 — If `End` stays producer/integrity, does `take_while` belong in `Channels`?

Status: blocked on Q2.

A bounded read may be a consumer loop that returns its own completion, not a
channel combinator. That would keep `End` small and put "did I reach the cut?"
next to the predicate that defines the cut.

## Q2 — Direction under consideration

Status: leaning, not settled.

The flat form is:

```text
  End = Closed
      | Aborted { reason: AbortReason }

  AbortReason = Producer { what }
              | Cancelled

  take_while bound  -> Closed
  cancellation      -> Aborted { reason: Cancelled }
```

This is the S3 direction, but with the reason retained. `End` becomes a
clean/unclean bit plus a cause; composition only needs two arms, and callers
that care about shutdown can still match `AbortReason::Cancelled`.

The open consequence: `Closed` no longer distinguishes "the producer closed"
from "a `take_while` bound cut the stream short." A full-load consumer cannot
tell the difference from `End` alone. The caller that installed the bound knows;
a generic consumer does not.

## Q3 — Type-level invariants

Status: leaning, not settled.

The type-level direction names invariants:

```text
  Channels<T>                 no extra promise
  Cancellable<T>              until() bound
  OrderedChannel<T>           monotone position
  GaplessOrderedChannel<T>    contiguous position
```

The appeal is that misuse becomes a type error. The cost is combinatorial:
`Cancellable<OrderedChannel<T>>`, `Cancellable<GaplessOrderedChannel<T>>`, and
every combinator must either be duplicated or generic over the wrapper.

Working recommendation: keep the **core** as one `Channels<T>` with the flat
`End`, and use at most one boundary type, `OrderedChannel<T>` with an
`OrderPolicy` (monotone vs gapless). Do not make cancellation a channel type;
`AbortReason::Cancelled` already carries it, and `until` must still compose
with merge/chain. This keeps the invariant naming without the matrix.

## Q4 — Does `take_while` need to report a distinct bound outcome?

Status: blocked on Q2.

If `take_while -> Closed` is acceptable, no new verdict is needed. If callers
must distinguish "bound reached" from "source closed early," the distinction
belongs in the operator's result or an in-band domain terminal, not in `End`.

## Q4 — Resolution: `take_while` maps to `Closed`

Status: settled.

The earlier objection was wrong. `take_while` is a consumer-chosen bound:
once the predicate declines, the derived stream is complete, and the source is
dropped immediately. A later source error is unreachable — the consumer cannot
receive it, because the source is no longer read.

The "full-load consumer" case is not a channel-verdict problem. If a consumer's
contract is "load everything" but it installs a `take_while`, its predicate is
wrong. `End::Complete` would only report that the predicate fired — something
the caller already knows — and would not prove the cut was the correct
completeness boundary.

Consequences:

- `Channels::take_while` ends `End::Closed`; no `Complete`/`Bound` variant.
- `Closed` means "this stream ended cleanly", not "the producer called close".
  A producer close and a consumer bound are both clean ends.
- Domain completeness remains a domain check or an in-band terminal
  (`nvzn::FoldItem::ReplayDone`, `zslg::WindowItem`).

## Q5 — What is the `AbortReason` shape?

Status: open.

With `End = Closed | Aborted { reason }`, the next question is what `reason`
must distinguish:

```text
  Producer { what }     data channel disconnected without close
  Cancelled             a Cancellation fired
```

Open edge: an unfired `CancelHandle` dropped yields `Aborted` today. Is that
`Cancelled` (wrong: nothing was requested), `Producer { what: "cancellation" }`
(misleading: no producer), or a third reason such as `CancelHandleDropped`?

## Q5 — Resolution: top-level ok/not-ok, fine-grained reason

Status: settled direction.

`Closed` is renamed because it names the producer's action, not the stream's
outcome. The top-level type becomes:

```text
  End = Complete
      | Aborted { reason: AbortReason }
```

- `Complete` = the stream ended cleanly, at whatever boundary owns it: producer
  close, all phases/branches complete, or a `take_while` bound.
- `Aborted` = the stream did not complete cleanly. The detail lives in
  `AbortReason`.

This is different from the rejected `End::Complete`: there it was a third
variant beside `Closed` and `Aborted`, so "complete" competed with "closed".
Here it replaces `Closed` and is the only clean end.

Proposed reason set:

```text
  ProducerGone { what }      data channel disconnected without close
  Cancelled                  Cancellation fired
  CancelHandleDropped        handle dropped without firing
  ConsumerGone { what }      liveness guard lost (e.g. into_stream consumer)
  RoutingViolation { what }  internal ordinal invariant broken
```

`take_while` ends `Complete`; `until` fired ends `Aborted(Cancelled)`; an
unfired handle dropped ends `Aborted(CancelHandleDropped)`. The top-level match
in `chain`/`merge` only needs `Complete` vs `Aborted`.

## Q6 — What exactly belongs in `AbortReason`?

Status: open.

The proposed set above is a first cut. The open question is granularity:
should the public enum expose only reasons a caller can act on
(`ProducerGone`, `Cancelled`, `CancelHandleDropped`), with the internal
`ConsumerGone`/`RoutingViolation` folded into `ProducerGone { what }`; or should
every current abort source have its own variant?

## Q5/Q6 — Resolution

Status: settled.

```text
  End = Complete
      | Aborted { reason: AbortReason }

  AbortReason =
      ProducerGone { what }       data channel disconnected without close
    | Cancelled                   Cancellation fired
    | CancelHandleDropped         handle dropped without firing
    | ConsumerGone { what }       liveness guard lost
    | RoutingViolation { what }   internal ordinal invariant broken
```

All five reasons are public. `what` names the producer/guard/leaf. `Complete`
replaces `Closed` as the only clean end; `take_while` ends `Complete`.
`chain`/`merge` only need `Complete` vs `Aborted`; code that cares about
shutdown matches `AbortReason::Cancelled`.

## Q7 — What is `OrderedChannel` at the trait boundary?

Status: open.

`OrderedChannel` is the next topic. The boundary candidates are the traits that
already promise order:

```text
  Replica::subscribe_from         index-ordered, resumable
  Reader::subscribe_stable_log    gapless, index-ordered
  StableLogSnapshot::range        index-ordered range
```

The contract choice:

1. **Checked view.** `OrderedChannel<T>` yields items plus an `OrderEvent`
   (duplicate, gap, regression); the consumer decides what to do. It replaces
   assertions such as `emit_checked`, but not `detect_purge_gap` /
   `classify_order`.
2. **Policy-enforcing.** `OrderedChannel<T>` owns a policy (gapless vs
   monotone), drops duplicates, tolerates or rejects gaps, and stops on
   regression; an observer callback feeds metrics. It replaces
   `detect_purge_gap` and `classify_order`, but cannot replace
   `HistoricalReplay`'s gap-fill (there a gap is control flow, not an error).
3. **Type-level policy.** `OrderedChannel<T, Gapless>` vs
   `OrderedChannel<T, Monotone>`; stronger naming, more combinations and
   generic plumbing.

Working recommendation: one `OrderedChannel<T>` with a policy value and an
observer, used only at the subscription boundaries. Keep `HistoricalReplay`'s
gap-fill producer-side. Do not multiply channel types for every combination.

## Q7 — Resolution: enforce, policy owns duplicates

Status: settled direction.

`OrderedChannel` enforces a policy. It does **not** silently drop duplicates;
whether a duplicate is allowed or a violation is part of the policy.

Closed policy set (no user-defined policies for now):

```text
  Contiguous       key == next(expected)   gaps, duplicates = violation
  StrictMonotone   key > previous          gaps allowed, duplicates/regression = violation
  NonDecreasing    key >= previous         gaps allowed, duplicates allowed, regression = violation
```

"Enforce" means the channel aborts with an order-violation reason; it never
filters items on its own. A caller that wants to drop duplicates does so
explicitly after the channel.

## Q8 — Order violation reason and typed position

Status: open.

Add an abort reason:

```text
  OrderViolation { what, kind }
  kind = Gap | Duplicate | Regression
```

`End` is not generic, so the typed `expected`/`got` cannot live in
`AbortReason` without making the whole end type generic. The proposed hook is
an observer:

```text
  OrderEvent<K> =
      Gap { expected, got }
    | Duplicate { expected, got }
    | Regression { expected, got }
```

The observer sees the typed event; `End` carries only the kind. The domain uses
the observer for metrics and for the replay cursor.

## Q8b — HistoricalReplay decomposition

The replay should be expressible with existing primitives:

```text
  stable-log scan with Contiguous policy
      -> gap => End::Aborted(OrderViolation { kind: Gap })
      -> resume_on_fault inspects the reason
      -> builds archive.range(expected, got).chain(scan-from-got)
      -> repeats until the scan reaches its bound
```

The domain supplies the key, the archive range, and the cursor; the crate
supplies ordering enforcement and `resume_on_fault`. This removes the bespoke
`HistoricalReplay` loop if the typed gap position is available to the resumer
(observer/state), and leaves `blend_chain` for the live handoff.

## Q7b — No default policy

Status: settled.

`OrderedChannel::new` requires an explicit `OrderPolicy`. There is no
`Default` and no implicit "usually strict monotone" choice. The caller names the
concept:

```text
  Contiguous
  StrictMonotone
  NonDecreasing
```

## Q8 — Typed order detail: options

Status: open.

The callback is not convincing. It hides the cursor in shared state and makes
the dependency between `OrderedChannel` and `resume_on_fault` implicit. The real
question is: where does `OrderedEnd<K> = Complete | OrderedAborted<K>` live, and
how does it compose with `Channels`?

### D — Detail parameter on `End`/`Channels`

```text
  End<D = ()> = Complete | Aborted { reason: AbortReason<D> }

  AbortReason<D> =
      ProducerGone { what }
    | Cancelled
    | CancelHandleDropped
    | ConsumerGone { what }
    | RoutingViolation { what }
    | OrderViolation { what, detail: D }

  Channels<T, D = ()>

  OrderDetail<K> = { kind: Gap | Duplicate | Regression, expected: K, got: K }
  OrderedEnd<K>  = End<OrderDetail<K>>
```

- `OrderedChannel<T,K>` is `Channels<T, OrderDetail<K>>` plus an order source.
- `resume_on_fault` becomes generic over `D`, so it receives the typed detail.
- All existing combinators get a `D` parameter (mostly mechanical).
- Projection to ordinary `Channels<T>` is `erase_detail`.
- One primitive family; typed replay works directly.

### E — End type parameter on `Channels`

```text
  Channels<T, E = End>
  OrderedEnd<K> = Complete | OrderedAborted<K>
  trait StreamEnd { fn complete() -> Self; fn from_end(End) -> Self; ... }
```

- Closest to the proposed `OrderedEnd<T>`; each end type can have its own shape.
- Requires a `StreamEnd` trait, conversions for guards/cancellation, and every
  combinator generic over `E`.
- More general than D, but more machinery for the same current need.

### B — Boundary type plus explicit fault handle

```text
  OrderedChannel<T,K> { channels: Channels<T>, faults: Faults<K> }
  recv -> OrderedEnd<K> at the boundary
  into_channels() -> (Channels<T>, Faults<K>)
```

- No generic core. The typed event is an explicit one-shot channel, not a
  callback. `resume_on_fault` reads the fault handle from its state.
- Chaining ordered segments means combining fault handles; the typed detail is
  not in the `Channels` end, so it is easy to lose.

### C — Separate ordered channel family

- `OrderedChannel<T,K>` with its own `map`, `filter_map`, `chain`,
  `resume_on_fault`, returning `OrderedEnd<K>`.
- Strongest typing and no generic core, but duplicates combinators, loses
  `merge`, and grows a type lattice.

Recommendation to discuss: D. It gives `OrderedEnd<K>` as a type alias, keeps
`resume_on_fault` one primitive, and makes the typed gap first-class for the
`HistoricalReplay` decomposition. The cost is one generic parameter through the
source tree; default `D = ()` keeps ordinary call sites unchanged. E is the
full version of the same idea; B is the fallback if the generic parameter is too
invasive.

## Q9 — HistoricalReplay interface refactor

Status: open detail.

```text
  stable-log scan with Contiguous
      -> OrderViolation { kind: Gap, expected, got }
      -> resume_on_fault sees the typed detail
      -> archive.range(expected, got).chain(scan_from(got))
      -> live via blend_chain
```

With D, the gap detail is in `End<OrderDetail<K>>`; no side channel. The
interface work is mostly making `chain`/`resume_on_fault`/`Ranged` generic over
D, and projecting to plain `Channels` where the seam only needs an end.

## Q10 — Gap as control flow, not as an abort

Status: direction under consideration.

A gap is not the same kind of event as a producer disappearing:

```text
  Aborted        the stream failed; items may be missing
  Gap            the stream is ordered, but this range was not present
```

For `HistoricalReplay`, a gap is expected and recoverable: the archive is the
fallback for a purged range. Expressing it as `Aborted` + `resume_on_fault`
works mechanically, but makes a normal repair look like a fault and conflates
the gap with producer failure.

The better expression is a dedicated adapter:

```text
  OrderedChannel<T,K>::fill_gaps(fallback)
      -> Channels<T>

  fallback: FnMut(Gap<K>) -> Channels<T>
  Gap<K> = { expected: K, got: K }
```

It consumes the ordered stream; on a gap it reads the fallback for
`[expected, got)`, emits those items, then continues with the primary item at
`got`. The result is a normal `Channels<T>` and can be `chain`ed or blended with
the live stream.

This also removes the need to put typed order detail into `Channels<T>`:
`OrderedEnd<K>` stays on the ordered boundary type, and `fill_gaps` is the
projection that consumes it. `resume_on_fault` remains for actual aborts, not
for gaps.

Open details:

- Name: `fill_gaps`, `stitch`, `splice`, `repair`.
- Does `OrderedChannel` buffer the gap-triggering item so `fill_gaps` can emit
  it after the fallback, or does the fallback contract include `got`?
- Does `fill_gaps` verify that the fallback covered exactly `[expected, got)`,
  or does it trust the fallback plus the next primary item?
- Can the fallback itself abort? It should propagate that as an abort, not
  silently drop the gap.
- Does `fill_gaps` return `Channels<T>` with the normal `End`, or retain order
  detail for a later composer?

## Q11 — `stitch` returns a stronger ordered channel

Status: settled direction.

`stitch` (the operation; `fill_gaps` describes it) takes an ordered channel
whose policy permits gaps and returns an ordered channel whose policy is
gapless:

```text
  OrderedChannel<T,K, StrictlyMonotone>
      .stitch(fallback)
      -> OrderedChannel<T,K, Gapless>
```

The policy is strengthened, not carried over unchanged. `NonDecreasing` cannot
become `Gapless` without deciding what happens to duplicates; that is a
separate policy step, not part of `stitch`.

## Q12 — Reuse `End`; no typed `OrderedEnd`

Status: settled direction.

`OrderedChannel::recv` returns the normal `End`. Add an abort reason:

```text
  AbortReason::OrderViolation { what: &'static str }
```

The typed `expected`/`got` does not enter `End`; it stays inside
`OrderedChannel`/`stitch`. This avoids a generic `End` or `Channels<T,D>` just
for the replay path.

## Q13 — Gap-triggering item and fallback contract

Status: leaning.

Recommended: buffer the gap-triggering primary item, and let the fallback cover
the gap **exclusive** of `got`:

```text
  fallback: [expected, got)
  buffered: primary item at got
  output:   fallback items, then buffered item
```

This matches the existing `HistoricalReplay` shape and does not require the
archive to provide `got`. `stitch` verifies that the fallback covers exactly
`[expected, got)`; a short/overlong fallback aborts. A fallback abort
propagates.

Alternative: fallback covers `[expected, got]`; no buffer, but the fallback must
provide `got`, which may duplicate the primary item and is not guaranteed by
the archive.

## Q14 — Rename `resume_on_fault`

Status: open naming.

`retry_on_fault` is better than bare `retry` (the trigger is the point).
`resume_on_abort` is the most precise with the new `Complete | Aborted`
vocabulary, but `retry_on_fault` reads better for reconnect/rebuild sites.
Gaps do not use this combinator; it is for actual aborts.

## Q15 — `stitch` vs `fill_gaps`

Status: leaning.

Use `stitch` as the method name; it names the composition of primary and
fallback. `fill_gaps` is the behavior in the docs. A separate `fill_gaps`
alias is unnecessary unless the call sites read better with it.

## Q16 — Policy in the type or a runtime value?

Status: open.

```text
  type-level:  OrderedChannel<T, K, P>
               stitch: OrderedChannel<T,K,StrictlyMonotone>
                         -> OrderedChannel<T,K,Gapless>
  runtime:     OrderedChannel<T, K> { policy: OrderPolicy }
               stitch checks the policy and returns a Gapless wrapper
```

Type-level makes the one-way strengthening a compile-time guarantee and puts
the invariant in the type, at the cost of one marker parameter on the ordered
boundary type (not on `Channels`). Runtime is less code but the guarantee is an
assertion.

Recommendation: type-level, because the policy is exactly the invariant we
want the type to name, and `stitch` is the one place where it changes.

## Q16 — Resolution: policy is in the type

Status: settled.

```text
  OrderedChannel<T, K, StrictlyMonotone>
      .stitch(fallback)
      -> OrderedChannel<T, K, Gapless>
```

The policy marker is part of the ordered boundary type; `Channels<T>` stays
non-generic. `stitch` only exists where the input policy allows gaps and the
key has a successor, so the `Gapless` output is a compile-time guarantee. There
is no default policy.

## Q17 — Is `what: &'static str` usable?

Status: leaning.

`&'static str` is a **label**, not a formatted message. All current producers
already pass static labels (`Channels::from(rx, "stable log")`,
`Feed::channel(..., "live scan")`), so there is no leak. The string names the
source; it does not carry `expected`/`got`.

If we want dynamic text, the options are:

```text
  &'static str          no allocation, no leak, static label only
  Cow<'static, str>     static labels free, dynamic text allocates; End loses Copy
  String                simplest dynamic, always owned; End loses Copy
```

Recommendation: keep `&'static str` for `AbortReason`. Put typed/dynamic order
detail in the `Gap<K>` event and logs, not in `End`. `End` remains a compact,
copyable verdict. If a formatted message is needed, `Cow<'static, str>` is the
compromise when we are ready to lose `Copy`.

## Q18 — What is `K`?

Status: open detail.

`K` is the **position key**, not the item:

```text
  T = item type
  K = key/position type, e.g. LogIndex, Hlc, (Hlc, DataId)
  key: Fn(&T) -> Option<K>
```

The extractor returns `None` for items that do not participate in the order
(stable-log `Watermark`, error items); those pass through without advancing the
position. `StrictlyMonotone`/`NonDecreasing` need `K: Ord`. `Gapless` and
`stitch` need a successor/step (`K::next`), because they must verify
`[expected, got)` and advance the expected key. `Hlc` may be ordered but is not
gapless; it would not use `stitch`.

Open: is `K` a raw key with a `Successor` bound only where needed, or should
the item implement a `Position` trait that supplies both extraction and `next`?

## Q19 — Key extractor: construction parameter, not a trait on `T`

Status: settled direction.

The item type and the key are independent. `Data` may be the item in several
sites while the key differs by context (`Hlc`, `(Hlc, DataId)`, `LogIndex`), so
the extractor cannot live on `T`.

```text
  key: Fn(&T) -> Option<K>     construction parameter
  K                            named in the ordered type
```

A named extractor trait is possible, but not needed: a closure can be a named
function, and it can capture context. `OrderedChannel` stores the extractor
type-erased and keeps `T`, `K`, and the policy marker in the type.

## Q20 — Successor: trait or construction parameter?

Status: open.

`Gapless` and `stitch` need `K::next` to verify `[expected, got)`. Two options:

```text
  trait:        pub trait Successor: Copy { fn next(self) -> Self; }
                Gapless/stitch add K: Successor
  parameter:    next: Fn(K) -> K at construction (or at stitch)
```

Recommendation: **trait**. The successor is a property of the key, not of the
use site; `LogIndex` has exactly one natural next. The bound is visible in the
type and only appears on `Gapless`/`stitch`; `StrictlyMonotone` and
`NonDecreasing` only need `K: Ord`, so `Hlc` works there but is not gapless.

Proposed structure:

```text
  OrderedChannel<T, K, P> {
      inner: Channels<T>,
      key: Arc<dyn Fn(&T) -> Option<K> + Send + Sync>,
      policy: PhantomData<P>,
  }

  P = Gapless | StrictlyMonotone | NonDecreasing
  private trait Policy<K> { check; advance; }
  Gapless: K: Successor
  StrictlyMonotone / NonDecreasing: K: Ord

  impl OrderedChannel<T, K, StrictlyMonotone> where K: Successor {
      fn stitch<F>(self, fallback: F) -> OrderedChannel<T, K, Gapless>
  }
```

If a key ever needs context-dependent successor semantics, wrap the key in a
newtype and implement `Successor` for that; do not add a second closure to the
common case.
