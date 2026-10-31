# Design — channel ends and ordered channels

Status: draft for sign-off. Branch `channel-extraction`.

## 1. Purpose

Two related changes:

1. Make `End` say only whether a stream ended cleanly, with a fine-grained
   reason inside the failure case.
2. Introduce `OrderedChannel` as a boundary type for streams that promise a
   monotone position, and `stitch` as the generic gap-repair adapter.

The target is fewer ad-hoc checks: the source trait names the invariant, the
consumer does not rediscover it.

## 2. Goals

- `End` is a two-level verdict: `Complete` or `Aborted { reason }`.
- `take_while` ends `Complete`; it does not add a verdict.
- `Cancelled` and `CancelHandleDropped` are abort reasons, not top-level
  variants.
- `OrderedChannel` enforces a closed policy set; the policy is in the type.
- `OrderedChannel` is a drop-in read surface for the existing consumer loops.
- `stitch` repairs gaps from a fallback `Channels<T>` without pretending a gap
  is an abort.
- Existing zslg/nvzn enforcement code is replaced where the invariant is
  already true.

## 3. Non-goals

- No user-defined order policies. The three markers are the set.
- No metrics layer in this change; `on_gap`/counters are deferred.
- No typed `expected`/`got` in `End`; the ordered type and `stitch` own that.
- No callback/observer to carry order detail.
- No ordered `merge`; merging independent sources has no order contract.
- No `map`/`filter_map` on `OrderedChannel`; project to `Channels` first.
- No new normal dependencies, no unsafe.

## 4. Current state

`End` mixes two questions:

```text
  Closed              producer called Closer::close
  Aborted { what }    producer vanished; items may be missing
  Cancelled           the Cancellation bound with until() fired
```

`take_while` has no clean end that distinguishes a consumer bound from a
producer close. `HistoricalReplay` is a bespoke loop that detects index gaps,
fills them from the archive, and emits the gap-triggering entry. Two replica
consumers re-check order separately (`detect_purge_gap`, `classify_order`).

## 5. `End` model

```rust
pub enum End {
    /// The stream ended cleanly: producer close, all phases/branches complete,
    /// or a consumer bound was reached.
    Complete,
    /// The stream did not complete cleanly.
    Aborted { reason: AbortReason },
}

pub enum AbortReason {
    ProducerGone { what: &'static str },
    Cancelled,
    CancelHandleDropped,
    ConsumerGone { what: &'static str },
    RoutingViolation { what: &'static str },
    OrderViolation { what: &'static str },
}
```

`what` is a static label naming the producer/guard/ordered source, not a
formatted diagnostic. `End` stays `Copy`; dynamic detail belongs in logs or in
the ordered gap event.

`Complete` replaces `Closed` because it names the stream outcome rather than
the producer's action. It is the only clean end.

### 5.1 Composition

```text
  chain: Complete -> next phase; Aborted -> condemn the rest
  merge: Complete -> drop that branch; Aborted -> condemn the merge
  guard: Complete -> preserve; Aborted(Cancelled) -> cancellation
  resume_on_abort: Complete -> stop; Aborted -> let the closure decide
```

`End::Cancelled` and `End::Closed` disappear. All existing matches update.

### 5.2 `take_while`

`Channels::take_while(pred)` ends `Complete` when `pred` declines an item. The
declined item is consumed and the inner source is dropped. A source that ends
before the bound also reports its own `Complete`/`Aborted`; the channel does not
try to distinguish a short read from a bound. That is a domain completeness
question, not a transport verdict.

### 5.3 `resume_on_abort`

Renamed from `resume_on_fault`. It is called at start with `None`, and after an
`Aborted` segment with `Some(end)`. It may return a new segment or `None`.
`Complete` ends the stream; it is not resumable. Gaps use `stitch`, not this.

## 6. Ordered channel model

```rust
pub trait Key<T>: Copy + Ord + Send + 'static {
    fn key(item: &T) -> Option<Self>;
}

pub trait Successor: Copy {
    fn next(self) -> Self;
}

pub struct Gapless<K>(/* marker */);
pub struct StrictlyMonotone<K>(/* marker */);
pub struct NonDecreasing<K>(/* marker */);

pub struct OrderedChannel<T, P> {
    inner: Channels<T>,
    start: Option<<P as OrderPolicy>::Key>,
    // policy state
}
```

`P` is a sealed `OrderPolicy`; its associated `Key` is the position type.
`K: Key<T>` extracts the key. `None` means the item does not participate in
the order (e.g. `Watermark`, error items) and passes through unchanged.

### 6.1 Policies

```text
  Gapless<K>          first == start, then every key == previous.next()
                      gap, duplicate, regression = violation
  StrictlyMonotone<K> first >= start, then every key > previous
                      forward gaps allowed; duplicate/regression = violation
  NonDecreasing<K>    first >= start, then every key >= previous
                      gaps and duplicates allowed; regression = violation
```

No policy drops items. Duplicates are either allowed (`NonDecreasing`) or a
violation; they are never silently filtered.

`start: Option<K>` is explicit. `None` means “no expectation”; `Some(k)` is the
first expected/lower-bound key. For `Replica::subscribe_from(from)`, the call
site passes `Some(LogIndex::next_after(from))`; for a reader replay from `from`,
`Some(from)`.

### 6.2 Read surface and projection

`OrderedChannel` delegates the consumer read surface:

```text
  recv, try_recv, ready_batch, end_reason
```

The order check lives in the wrapped source, so batching sees the same policy.
`into_channels()` returns the inner `Channels<T>`; because `End` is reused, the
projection needs no end mapping. The caller explicitly gives up the typed order
guarantee.

`map`/`filter_map` are not methods on `OrderedChannel`; project first. `merge`
is not defined on ordered channels.

## 7. `stitch`

`stitch` is the gap-repair adapter:

```rust
pub struct Gap<K> { pub expected: K, pub got: K }

impl<T, K> OrderedChannel<T, StrictlyMonotone<K>>
where
    K: Key<T> + Successor,
{
    pub fn stitch<F>(self, fallback: F) -> OrderedChannel<T, Gapless<K>>
    where
        F: FnMut(Gap<K>) -> Channels<T> + Send + 'static;
}
```

```text
  primary item at got is buffered
  fallback covers [expected, got) exclusive
  stitch emits fallback items, then the buffered item
  stitch verifies the fallback is exactly contiguous over [expected, got)
  short/overlong/gapped fallback -> Aborted(OrderViolation)
  fallback abort propagates
  output policy is Gapless
```

The fallback is an ordinary `Channels<T>`; it does not have to be an
`OrderedChannel`. `stitch` owns the verification.

### 7.1 Async fallback adaptation

`stitch` is synchronous over `Channels<T>`. In zslg the replay consumer already
runs on a blocking thread, so the archive fallback uses
`tokio::runtime::Handle::block_on` to turn the async `archive.stream(...)` into
a `Channels<T>`. The crate stays runtime-agnostic; the adapter lives in the
domain call site.

## 8. Trait boundaries

The ordered type is applied only where the boundary really promises order:

| Boundary | Policy | Start |
| --- | --- | --- |
| `Replica::subscribe_from(from)` | `StrictlyMonotone<LogIndex>` | `next_after(from)` |
| `Reader::subscribe_stable_log(from)` | `Gapless<LogIndex>` | `from` |
| `StableLogSnapshot::range(from, until)` | `StrictlyMonotone<LogIndex>` if wrapped | `from` |
| `LocalStoreView::stream_above_hlc(threshold)` | deferred; still `Channels` | — |
| `LocalStoreView::stream_window_ordered` | deferred; still `Channels` | — |
| `Reader::subscribe_unstable_tail` | none | arrival order |
| merge output | none | unordered |

`Replica::subscribe_from` is strictly monotone because the seam serves
`[next_after(from), cut]` and then live `> cut`. There are no duplicates; only
forward purge gaps. The learner's `detect_purge_gap` duplicate tolerance is
removed. The server's “backwards is corruption” is the contract. Both consumers
use the same ordered type.

## 9. HistoricalReplay

The bespoke replay becomes:

```text
  stable-log range (StrictlyMonotone, start = expected)
      .stitch(|gap| archive range [expected, got))
      -> Gapless
      -> into_channels
      -> consumer
```

`resume_on_abort` still handles actual aborts (scan died, archive unavailable,
producer gone). Gaps are not aborts and do not use it.

## 10. Edge cases

- **No-key item**: `Key::key` returns `None`; pass through and do not advance.
- **Leading gap**: `start = Some(k)` and first key `> k` yields a gap.
- **Empty stream**: `Complete`, no item.
- **Duplicate/regression**: policy violation -> `Aborted(OrderViolation)`.
- **Forward gap**: allowed by `StrictlyMonotone`; not an abort.
- **Stitch fallback short/overlong/gapped**: `OrderViolation` abort.
- **Stitch fallback abort**: propagate.
- **Stitch no-key item in the fallback**: passes through without advancing
  coverage; the endpoint check must still pass. This carries archive error items.
- **Projection without stitching**: `into_channels`; order checks are already
  in the source, but the caller loses the type-level guarantee.
- **`take_while` short read**: source ending before the predicate is `Complete`
  too; domain completeness check if it matters.
- **`resume_on_abort` with `Complete`**: stop, do not call the factory.
- **`resume_on_abort` with `Aborted(Cancelled)`**: the closure may decline or
  resume; cancellation is not treated specially by the combinator.
- **Ordered `merge`**: unsupported; project to `Channels` first.

## 11. Alternatives considered

| Alternative | Why rejected |
| --- | --- |
| `End::Complete` as a third variant | `Complete` competed with `Closed`; better to replace `Closed` |
| Keep `End::Cancelled` | Cancellation is a consumer action; it belongs under `Aborted` |
| Separate `Stop`/`Recv` type | Moves the exception, does not remove it; wide churn |
| `Channels<T, D>` generic detail | Typed order detail can stay in `OrderedChannel`/`stitch` |
| Typed `OrderedEnd<K>` through `Channels` | Projection/combinator complexity, no need for replay |
| Callback/observer for typed gap | Hidden coupling; rejected |
| Type family `Cancellable`, `Ordered`, `GaplessOrdered` | Combinatorial; one ordered boundary type with policy markers |
| Runtime `OrderPolicy` | Policy in the type is the point; `stitch` strengthening is compile-time |
| Key extractor closure | Same `T` needs different keys; `Key<T>` on `K` names it |
| `K: From<&T>` | Infallible; cannot express `None` for watermarks/errors |
| Gap as `Aborted` + `resume_on_abort` | A gap is a repair signal, not a fault; `stitch` is the right expression |
| Fallback includes `got` | Wider fallback contract; buffering one item is simpler |
| Metrics hook now | Orthogonal; deferred to optional feature |

## 12. Testing strategy

Crate:

- `End` reason mapping and composition (`chain`, `merge`, `Guarded`).
- `take_while -> Complete`; source `Complete`/`Aborted` before the bound.
- `resume_on_abort` only resumes on `Aborted`.
- Each order policy: contiguous, forward gap, duplicate, regression, `None`
  key, leading gap with `start`.
- `OrderedChannel` full read surface (`recv`, `try_recv`, `ready_batch`) and
  `into_channels` projection.
- `stitch`: exact fill, buffered `got`, short/overlong/gapped fallback,
  fallback abort, no-key fallback item, `Gapless` output.
- Model/proptest for ordered prefixes and stitch coverage.

zslg/nvzn:

- The existing 657/154 suites.
- Remove `detect_purge_gap`/`classify_order` tests and replace with ordered
  policy tests at the boundary.
- Add a replica subscription test that a duplicate/backwards index aborts.
- Add a replay test where the scan has a purge gap and the archive fills it.
- Add a leading-gap metric/behavior test (metric deferred; behavior still
  asserted).
- Confirm `ready_batch` consumers still work.

## 13. Implementation sequence

1. **`End` overhaul.** Rename `Closed` -> `Complete`; add `AbortReason`;
   move cancellation under `Aborted`; update all matches and tests. Green.
2. **`take_while`.** Add to `Channels`; ends `Complete`; tests.
3. **`resume_on_abort`.** Rename and change semantics to only resume on
   `Aborted`; update zslg call sites and tests.
4. **Ordered core.** `Key<T>`, `Successor`, policy markers, `OrderPolicy`,
   `OrderedChannel<T,P>`, full read surface, `into_channels`; tests.
5. **`stitch`.** Gap detection, buffer, fallback verification, async adapter
   pattern; tests.
6. **zslg replica boundary.** `Replica::subscribe_from` returns
   `OrderedChannel<..., StrictlyMonotone<LogIndex>>`; delete
   `detect_purge_gap`/`classify_order`; tests.
7. **zslg stable reader.** `Reader::subscribe_stable_log` returns
   `Gapless<LogIndex>`; refactor `HistoricalReplay` onto ordered + `stitch`;
   tests.
8. **Other boundaries.** HLC/window scans and nvzn `LiveSeam`; project where a
   transform is needed; tests.
9. **Cleanup.** Docs, clippy/fmt, full zslg/nvzn suites; record deferred
   metrics work.

## 14. Operational concerns

- **Public API break.** `End::Closed` becomes `End::Complete`; `Cancelled` moves
  under `AbortReason`; `resume_on_fault` becomes `resume_on_abort`. Downstream
  crates in this workspace update in the same change.
- **Metrics.** `subscription_purge_gaps` is temporarily not emitted at the
  ordered boundaries. Correctness is unaffected. A later optional `metrics`
  feature can add a counter/observer additively.
- **Performance.** Key extraction and policy checks are per keyed item, static
  dispatch. `stitch` buffers one item. No extra copies otherwise.
- **Dependencies/unsafe.** None added.

## 15. Implementation status

The first implementation covers:

- `End`/`AbortReason`, `take_while`, and `resume_on_abort`.
- `OrderedChannel<T,P>`, `Key<T>`, `Successor`, the three policy markers,
  full read surface, and `into_channels`.
- `stitch` with exact fallback coverage.
- `Replica::subscribe_from` as `StrictlyMonotone<LogIndex>`.
- `Reader::subscribe_stable_log` as `Gapless<LogIndex>`, with zslg's
  `HistoricalReplay` rebuilt on `OrderedChannel` + `stitch`.
- nvzn's zslg reader stubs and live seam updated.

The HLC/window `LocalStoreView` boundaries remain plain `Channels` for now;
`subscription_purge_gaps` is retained but unused until the optional metrics
feature.

## 16. Deferred

- Optional `metrics` feature / gap counter.
- User-defined order policies.
- Async-native `stitch` fallback.
- Ordered `map`/`filter_map`.
- `OrderedChannel` composition with `merge` (not meaningful without an order
  contract).
