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
