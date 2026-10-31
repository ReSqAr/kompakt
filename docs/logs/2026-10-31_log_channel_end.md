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
