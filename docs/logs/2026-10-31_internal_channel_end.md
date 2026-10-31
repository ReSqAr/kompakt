# Internal model — channel end verdicts

Working document for the IAIC interview. Not a design doc.

## What I currently believe

- `End` is trying to answer one question but its variants answer two. `Closed`
  and `Aborted` are producer/integrity facts; `Cancelled` is a consumer action.
- `End::Complete` failed because "complete" does not name *whose* completion it
  is. The producer did not complete; the consumer's requested range did.
- The underlying distinction `take_while` needs is real: "the bound was
  reached" must not be confused with "the source closed before the bound"
  (a short read) or "the source aborted."
- The domain already solves range completion in-band: `nvzn::FoldItem::ReplayDone`
  and `zslg::WindowItem` terminals. That is evidence that bounded completion
  may not belong in `End` at all.
- But `take_while` is generic and cannot introduce a domain terminal. Whatever
  we choose has to work for an arbitrary `T`.
- `End` is matched exhaustively in enough places that a new variant is cheap to
  add but not free. A structural change (`End { outcome, cause }`) is a much
  wider diff and needs a strong reason.
- `Cancelled` is already a consumer-side stop. Any claim that `End` is
  producer-only has to explain what happens to it.

## Predicted answer to Q1

I expect the user to reject "producer integrity only" if it means moving
`Cancelled` out, because that is a large API break for little gain. I expect
the choice to be either:

- `End` is the stream's stop reason (option 2), with a consumer-side variant
  named after the action (`Bounded`/`Bound`) rather than `Complete`; or
- `End` is an integrity verdict (option 3), and `take_while` returns a separate
  outcome rather than pretending the producer ended.

The user's "maybe the whole of `End` needs a little overhaul" suggests they are
open to option 3 or 4, so the next question should test whether `Cancelled` is
part of `End`'s identity or an exception.

## Next likely questions

1. If `End` is the stream stop reason, is `Complete` the right name, or is the
   concept `Bound`/`Bounded`?
2. If `End` is integrity-only, does `Cancelled` stay in `End` as a special
   case, or move to a separate `Stop`?
3. Does `take_while` need to expose the bound outcome to the caller, or is it
   enough that the bounded stream reports a clean end?
4. How do `chain`, `merge`, `Guarded` and `resume_on_fault` compose a consumer
   stop? (They already compose `Cancelled`; that is the precedent.)
5. Only after this: `OrderedChannel` at trait boundaries.

## Update after Q1

- The user treats `Cancelled` as the exception, so the working model is:
  `End` is producer/integrity-first; cancellation is a consumer-side stop that
  currently shares the type.
- My stronger belief now: removing `Cancelled` from `End` is possible, but only
  by introducing a second consumer-visible terminal type (`Stop`/`Recv`) or an
  outermost `Cancellable`. It does not reduce the number of concepts; it moves
  the consumer stop out of `End`.
- `take_while` is a different case from cancellation. Its bound is a **clean
  completion of the derived stream**, whereas cancellation is a **stop request
  with possible missing items**. That distinction is why `take_while` might use
  `Closed` while `Cancelled` cannot.
- If the caller must distinguish "bound reached" from "source closed early",
  that is a domain completeness check, not necessarily a channel verdict. The
  domain already models this with in-band terminals.

## Predicted answer to Q2

I expect the user to reject S3 (mapping cancellation to `Closed`) because it
loses the stop contract and changes merge/chain behavior. I expect either:

- keep `Cancelled` as the exception (S4), and keep `take_while` out of the
  channel verdict or handle it at the call site; or
- choose S1 if they want the clean two-level model and are willing to pay the
  wide diff.

My recommendation will be S4 unless the user explicitly wants to remove the
exception as a design goal: S1 is coherent but expensive, and the exception is
semantically real. The cheaper win is to stop trying to put the `take_while`
bound into `End`.

## Next likely questions

1. Does `take_while` need to be a `Channels` method, or a consumer-side loop
   that returns `Bound`/`ShortRead`?
2. If it stays a method, does it report `Closed` for the bound, and leave
   short-read detection to the domain?
3. If `Cancelled` stays, does `End` need a small doc/name overhaul even without
   a new variant?
4. Only after this: `OrderedChannel` at trait boundaries.

## Update after Q2 direction

- The flat `End = Closed | Aborted { reason }` is now the leading core model.
  It makes composition cheap and keeps the cancellation distinction in
  `AbortReason::Cancelled`; no `Stop` type is needed.
- The type-level channel family is attractive for invariants but I expect it
  to lose on composition and API surface. The likely compromise is:
  - one `Channels<T>` core;
  - one `OrderedChannel<T>` at trait boundaries, with an `OrderPolicy`;
  - no `Cancellable<T>` type, because `AbortReason::Cancelled` is enough and
    `until` must compose.
- The unresolved consequence is `Closed` ambiguity: producer close vs
  `take_while` bound. I expect the user to accept this because the caller that
  installed the bound already knows, and domain completeness is already an
  in-band terminal. If not, `take_while` must expose its own result.

## Predicted answer to Q4

The user will accept `take_while -> Closed` for the generic channel, because
they explicitly said "map completed to closed." The interesting follow-up is
then whether `AbortReason` is a flat enum or a richer reason type.

## Next likely questions

1. Is `AbortReason { Producer { what }, Cancelled }` enough, or does a dropped
   unfired cancellation handle need its own reason?
2. Is `OrderedChannel` a boundary newtype (trait returns) or a method on
   `Channels`? The earlier boundary discussion says trait returns.
3. Does `OrderedChannel` use a policy value or a type parameter for
   gapless/monotone?
4. Then `OrderedChannel` implementation planning.

## Update after Q4

- I was wrong about the full-load `take_while` objection. A bounded stream is
  complete by construction; later source errors are unreachable. The only
  failure is a caller using a bound when its contract was "everything", which
  is a predicate/contract bug that `End::Complete` cannot detect either.
- So `take_while -> Closed` is the right generic channel behavior. `Closed`
  becomes "clean end of this stream", not "producer closed".
- The remaining detail is `AbortReason` and specifically the dropped-unfired
  cancellation handle. That is a reason-shape question, not an `End` taxonomy
  question.
- Then `OrderedChannel` at trait boundaries.

## Update after Q5

- The user wants the top level to be only ok/not-ok, with a fine-grained reason
  inside `Aborted`. That settles the shape:
  `End = Complete | Aborted { reason }`.
- `Complete` now replaces `Closed`; it is not the rejected extra variant. The
  earlier failure was that `Complete` competed with `Closed`; now it is the only
  clean end.
- `AbortReason` is where cancellation, producer loss, dropped handles and
  internal invariants live. `take_while` does not appear there.
- Composition is simple again: `Complete` is clean, any `Aborted` is terminal.

## Predicted answer to Q6

The user will want enough reasons to explain the stop without pretending the
library can distinguish a panic from a dropped producer. I expect a small set:
producer gone, cancelled, cancel-handle dropped, and perhaps liveness guard
lost. Internal routing violations may stay as a `what` label rather than a
public variant. I should not over-model indistinguishable cases (panic vs
forget vs discarded task).

## Next likely questions

1. Exact public `AbortReason` variants and whether `what` stays on
   `ProducerGone`.
2. Whether `End::Complete` is the final name, or `Finished`/`Done`.
3. Then `OrderedChannel` at trait boundaries.
