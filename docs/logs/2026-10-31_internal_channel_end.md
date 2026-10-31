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

## Update after Q6

- `End` is settled: `Complete | Aborted { reason }`, with the five public
  `AbortReason` variants. The top level is ok/not-ok; the reason carries the
  detail.
- `take_while -> Complete`; `Cancelled` and `CancelHandleDropped` are aborts,
  not clean ends.
- The next topic is `OrderedChannel`. The key question is whether it is a
  checked view (reports order events) or a policy-enforcing channel (drops
  duplicates, handles gaps, stops on regression). The latter is what could
  replace `detect_purge_gap` and `classify_order`.
- I expect the user to prefer naming the invariant in the type, but the
  enforcement policy still needs runtime configuration because the learner and
  the server writer disagree about backwards indices. A single
  `OrderedChannel<T>` with `OrderPolicy` is the likely compromise.

## Predicted answer to Q7

The user will want the policy-enforcing form at the boundary, with the invariant
visible in the name. They may accept one type plus a policy, or ask for a
marker type parameter for gapless. I should ask which contract they want before
designing the API.

## Next likely questions

1. Which boundaries get `OrderedChannel` first: `Replica::subscribe_from`,
   `Reader::subscribe_stable_log`, or both?
2. What is the policy split: gapless vs monotone/purge-tolerant?
3. Does a violation become `End::Aborted(RoutingViolation)` or an
   `OrderEvent`?
4. How do metrics/logging attach without pushing domain policy into the crate?

## Update after Q7

- `OrderedChannel` enforces; duplicates are policy, not an implicit filter.
  The closed policy set is `Contiguous`, `StrictMonotone`, `NonDecreasing`.
- The order violation becomes an `AbortReason` so `resume_on_fault` can inspect
  it. The typed `expected`/`got` needs a home outside `End` because `End` is not
  generic: an `OrderEvent<K>` observer/shared state is the likely answer.
- This decomposition is promising for `HistoricalReplay`: `Contiguous` scan
  reports a gap, `resume_on_fault` fills from the archive, then resumes the
  scan. That is a real reuse win, not a bespoke loop.
- Open: does the observer/shared state make the cursor coupling too implicit?
  Could `resume_on_fault` accept a richer fault object instead of `End`? That
  would be a different generic design.

## Predicted answer to Q8

The user will accept `AbortReason::OrderViolation` plus an observer for typed
positions, because they asked for reusable primitives rather than the bespoke
replay loop. They may want the gap event to be part of the resume state rather
than a shared observer. The next question should test that coupling.

## Next likely questions

1. Does `resume_on_fault` need a richer fault type than `End` so the gap
   position is first-class, or is an observer/state enough?
2. Is `Contiguous` the policy for the stable-log scan, with `StrictMonotone`
   for the live/replica streams?
3. Where does the HLC monotonicity check live: a second key, a policy hook, or
   a domain filter?
4. Then implementation.

## Update after Q8

- The user rejects a default policy: `OrderPolicy` must be explicit.
- The callback is not acceptable. The typed `expected`/`got` must be first-class
  in the end/resume path.
- My leading option is D: a generic detail parameter on `End`/`Channels`.
  `OrderedEnd<K> = End<OrderDetail<K>>`; `resume_on_fault` stays one primitive
  and sees the typed gap. Projection is `erase_detail`.
- E (generic end type) is more faithful to `OrderedEnd<T>` but needs a
  `StreamEnd` trait and conversions; likely overkill for now.
- B (boundary type + fault handle) avoids the generic cascade but makes typed
  detail out-of-band again, just explicit. Chaining faults is awkward.
- C (separate ordered family) duplicates combinators and loses merge.
- The HistoricalReplay decomposition is the test case that decides this:
  can `resume_on_fault` see the typed gap without a side channel? D says yes.

## Predicted answer to Q9

The user will likely accept D's generic parameter if the alternative is a
callback or a duplicated combinator family, because they explicitly proposed
`OrderedEnd<T>` and want the replay to use existing primitives. The main risk is
the `D` cascade through `blend`/`Ranged` and every `Source`. I should quantify
that before committing.

## Next likely questions

1. D vs E: generic detail vs generic end type.
2. Does `OrderedChannel` get `D = OrderDetail<K>` at the trait boundary, or is
   the detail erased before `Reader`/`Replica` return?
3. How do `chain`/`merge`/`Guard` lift a plain `Channels<T>` into a typed D
   when composing with an ordered one?
4. Then implementation sequencing.

## Update after Q10

- This is a better decomposition: a gap is a **repair signal**, not an abort.
  `OrderedChannel` detects it; `fill_gaps` consumes it and inserts the fallback
  range; `resume_on_fault` stays for real aborts.
- It also reduces the typed-end problem: `OrderedEnd<K>` can stay on the ordered
  boundary type, and `fill_gaps` is the projection to `Channels<T>`. We may not
  need `Channels<T,D>` or a generic `End` at all for the replay use case.
- My recommendation shifts: prefer `fill_gaps` over "gap -> Aborted ->
  resume_on_fault". It expresses the normal path and keeps `Aborted` for
  faults.
- The generic D/E options remain possible for strict ordered boundaries, but
  they are no longer required to make HistoricalReplay work.

## Predicted answer to Q10

The user will like `fill_gaps` / `stitch` as the named concept, because it
separates "the stream is ordered" from "the stream has a hole". The remaining
question is the name and the fallback contract, not whether the pattern exists.

## Next likely questions

1. `fill_gaps` vs `stitch` vs `splice`: which concept name?
2. Does `fill_gaps` return `Channels<T>` or keep a typed order end until the
   caller explicitly erases it?
3. How does `fill_gaps` handle the gap-triggering item and fallback coverage?
4. Then implementation sequencing.

## Update after Q11-Q15

- The user wants `stitch` to return an ordered channel whose policy is
  strengthened to `Gapless`. `StrictlyMonotone -> Gapless` is the normal path.
- Reuse `End`; add `AbortReason::OrderViolation { what }` carrying a string, not
  a typed key object. The typed gap stays internal to ordered/stitch.
- Buffer the gap-triggering item and make the fallback cover `[expected, got)`.
  Verify exact coverage; propagate fallback aborts. This matches
  `HistoricalReplay` and keeps the archive contract narrow.
- Rename `resume_on_fault` likely to `retry_on_fault`; `resume_on_abort` is more
  precise but longer. Gaps no longer use it.
- `stitch` is the method name; `fill_gaps` is the behavior/doc phrase.
- The remaining structural question is whether the policy is a type parameter
  on `OrderedChannel` or a runtime value. Type-level gives the nice
  `StrictlyMonotone -> Gapless` guarantee.

## Predicted answer to Q16

The user will choose type-level policy because they explicitly asked to name
the invariant and "upsert" the policy. The cost is confined to the ordered
boundary type, not the channel core, so it is much cheaper than the earlier
type-family idea.

## Next likely questions

1. Policy markers: `Gapless`, `StrictlyMonotone`, `NonDecreasing`; are these
   the final names?
2. `stitch` signature and the fallback type (`Channels<T>` vs ordered).
3. How `stitch` verifies coverage and reports a bad fallback.
4. Final rename of `resume_on_fault`.
5. Implementation sequencing.

## Update after Q16-Q18

- The policy is type-level and explicit: `OrderedChannel<T,K,P>`.
  `stitch: StrictlyMonotone -> Gapless`. No default.
- `what: &'static str` is fine as a static label; no leak. If we need dynamic
  text, `Cow`/`String` makes `End` non-Copy, so keep it out of the verdict.
- `K` is the position key, with `Option<K>` extraction for non-positional items
  (watermarks/errors). Gapless needs a successor; HLC is ordered but not
  gapless.
- The next design detail is whether K carries a `Successor` bound or the item
  implements a `Position` trait.

## Predicted answer to Q18

The user will likely prefer a raw `K` plus a policy-specific `Successor` bound,
because `Position` on the item couples extraction to the ordered wrapper. But
`Gapless` needs a successor, so `K` cannot be purely `Ord`; the trait has to
exist somewhere.

## Next likely questions

1. `K: Successor` bound vs `Position` trait.
2. `Gap<K>` shape and fallback signature.
3. `stitch` verification details.
4. Final `retry_on_fault` naming.
5. Implementation sequencing.

## Update after Q19-Q20

- The key extractor is a construction parameter. The item is often `Data`, but
  the key varies by use, so a trait on `T` is wrong.
- The successor is naturally a trait on `K`, bounded only for `Gapless`/`stitch`.
  `LogIndex` implements it; `Hlc` stays orderable but not gapless.
- The trait structure is now small:
  - `T`, `K`, policy marker `P` on the ordered boundary type;
  - key closure stored type-erased;
  - private `Policy<K>` for checking/advancing;
  - public `Successor` for `K`.
- The remaining uncertainty is whether the user prefers the successor as a
  construction parameter instead of a trait. I expect the trait, because it
  keeps the type honest and avoids a second closure.

## Predicted answer to Q20

The user may push back that "parameters at construction time" is simpler and
more flexible. If so, the alternative is `next: Fn(K)->K` stored alongside the
key closure, with `stitch` requiring it. That avoids implementing a trait for
domain keys but makes the successor a runtime choice.

## Next likely questions

1. Successor trait vs constructor closure.
2. `Gap<K>` and the fallback signature.
3. `stitch` coverage verification.
4. `retry_on_fault` final name.
5. Implementation sequencing.

## Update after Q21

- The user's `From<&T>` idea is close but not sufficient: no `Option`, and the
  contract is wrong for non-positional items. `Key<T>` with
  `fn key(item: &T) -> Option<Self>` is the right trait.
- This removes the key closure from `OrderedChannel`: `K: Key<T>` names the key
  and its extraction. Same `T`, different keys still works because each `K`
  implements `Key<T>`.
- `Successor` stays a trait on `K`, only for `Gapless`/`stitch`.
- Trait structure is now: `Key<T>` (public), `Successor` (public), policy
  markers + private `Policy<K>`.
- The main risk is context-dependent extraction; a newtype key covers it
  without polluting the common API.

## Predicted answer to Q22

The user will accept `K: Key<T>` because they proposed a trait bound and dislike
adding the key to the item. They may want a closure escape hatch for
context-dependent keys; I should mention it as a later option, not the default.

## Next likely questions

1. Final `Key<T>` signature (`&T` vs `&mut T`, `Option<K>`).
2. `Gap<K>` and fallback signature.
3. `stitch` coverage verification.
4. `retry_on_fault` final name.
5. Implementation sequencing.

## Update after Q22

- `K: Key<T>` is settled; no closure. This makes `OrderedChannel<T,K,P>`
  self-contained and puts the key concept in the type.
- The remaining practical question is whether `OrderedChannel` is a drop-in
  read surface. The learner and server writer use `ready_batch`; a `recv`-only
  type would force a rewrite at exactly the sites we want to simplify.
- I expect full delegation, with `ReadyBatch` made generic or reused. The order
  check is in the source, so batching and `try_recv` see the same policy.

## Predicted answer to Q23

The user will choose the full read surface, because the goal is replacing
existing enforcement code, not adding a parallel API. If they choose minimal,
the ordered channel is limited to recv-driven boundaries and the `ready_batch`
sites stay hand-checked.

## Next likely questions

1. Full surface vs minimal.
2. `Gap<K>` and fallback signature.
3. `stitch` coverage verification.
4. `retry_on_fault` final name.
5. Implementation sequencing.

## Update after Q24-Q25

- K likely moves into the policy: `OrderedChannel<T, StrictlyMonotone<K>>`.
  This names one concept and prevents K/P mismatch.
- Full read surface is accepted: delegate `recv`, `try_recv`, `ready_batch`,
  `end_reason` to the inner channel. The order check is in the source, so
  batching sees it.
- Projection is accepted and cheap because `End` is reused: `into_channels`
  returns the inner `Channels<T>` with no end mapping.
- The next concrete API is `Gap<K>` and the fallback type. I expect
  `Channels<T>` with verification, because archive ranges are ordinary
  channels and `stitch` must verify coverage anyway.

## Predicted answer to Q26

The user will choose `Channels<T>` for the fallback: fewer wrappers, and
`stitch` has to verify the range regardless. Returning an ordered channel
would still require endpoint/coverage checks, so it buys little.

## Next likely questions

1. Fallback `Channels<T>` vs ordered channel.
2. `Gap<K>` fields and whether the buffered item is exposed.
3. `stitch` verification and abort reason.
4. `retry_on_fault` final name.
5. Implementation sequencing.

## Update after Q26

- Fallback is `Channels<T>`; `stitch` buffers the `got` item, verifies
  `[expected, got)`, propagates fallback aborts, and returns
  `OrderedChannel<T, Gapless<K>>`.
- The design is nearly complete. The only public-name decision left is
  `retry_on_fault` vs `resume_on_abort`/`resume_on_fault`.
- After that we can write the design doc: `End`, `OrderedChannel`, `Key`,
  `Successor`, policy markers, `stitch`, projection, full read surface, and the
  zslg replay migration.

## Predicted answer to Q27

The user suggested `retry_on_fault`/`retry`, and the new `Aborted` vocabulary
makes `resume_on_abort` attractive. I expect `retry_on_fault` as the compromise:
it says what triggers the retry without tying the public name to an enum
variant.

## Next likely questions

1. Final public name.
2. Then design doc sign-off; implementation sequence after that.

## Update after Q27-Q28

- `resume_on_abort` is settled. `stitch` is settled.
- The remaining wide-reaching decision is the trait boundary for
  `OrderedChannel`. The learner and server writer disagree on backwards items:
  learner ignores them, server treats them as corruption.
- A single return policy is a lie unless the trait contract is strict. My
  recommendation is to return plain `Channels<T>` from `Replica::subscribe_from`
  and wrap at each consumer with the policy it needs. That preserves behavior
  and still replaces `detect_purge_gap`/`classify_order` with the same type.
- If the user says the trait is actually strict (no duplicates/regressions), the
  boundary can return `OrderedChannel<..., StrictlyMonotone<...>>` and the
  learner becomes stricter.

## Predicted answer to Q28

The user may prefer the trait boundary to carry the invariant, but the
learner/server disagreement makes a single policy hard. I expect either a
consumer-side wrapper or a generic trait policy. The domain answer to "are
duplicates expected?" decides it.

## Next likely questions

1. Are backwards/duplicate items expected or corruption?
2. Then finalize the trait boundary list.
3. Then design doc.

## Update after Q28

- The learner/server disagreement is resolved by the seam: `subscribe_from` is
  strictly increasing with forward gaps only. The learner's duplicate tolerance
  is stale. The correct policy is `StrictlyMonotone<LogIndex>` at the trait
  boundary; both consumers share it.
- This is the “unrepresentable” outcome: no consumer-specific policy, no
  `detect_purge_gap`, no `classify_order`. Duplicates/backwards abort; forward
  gaps are allowed.
- `Reader::subscribe_stable_log` can be `Gapless`, because `HistoricalReplay`
  fills the gaps. The raw stable-log range is `StrictlyMonotone`; HLC/DataId
  windows are strictly monotone on the composite key.
- The only remaining detail is how accepted gaps feed metrics. An observer for
  gaps is acceptable because it is observability, not typed end/control.

## Predicted answer to Q29

The user will accept a gap observer or a simple count handle; they rejected the
callback as the carrier of typed end, not as a metrics hook.

## Next likely questions

1. Gap observer shape.
2. Then design doc sign-off.
