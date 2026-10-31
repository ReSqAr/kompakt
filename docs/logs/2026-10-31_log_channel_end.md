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
