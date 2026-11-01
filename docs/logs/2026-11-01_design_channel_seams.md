# Channel seams — design

Process: IAIC. Decision log: `2026-11-01_log_channel_seams.md`. Internal model:
`2026-11-01_internal_channel_seams.md`. Branch `channel-extraction`, base
`08a1dd3`.

## 1. Goals, non-goals, success criteria

**Goal.** The channel crate owns transport: completion vs abort, subscribe
failure, the history/live seam, resync, producer/pump lifecycles, and fan-out
wiring. `zslg` and `nvzn` declare the pipeline — source, cut, key, metric — and
nothing else.

**Non-goals.** No user-defined order policies, no fusion or cached `Select`, no
public `Closer::abort`/`Feed::abort`, and no forced thread for a lazy
single-source pipeline. `channel-experiment` stays local.

**Success criteria.**

- No hand-built `bounded + Channel::from + Closer::close` producers in
  `zslg`/`nvzn`; no hand-written fan-out `Drop`/cap wiring; no
  `AtomicU64`/`Mutex` purely to hold channel state.
- `HistoricalReplay` is a lazy source with no `block_on`; the archive `stream`
  is synchronous.
- A finite source cannot be used where an infinite one is required, and vice
  versa; the previous `Channel::empty()`-for-a-subscription bug does not
  compile.
- The full crate/zslg/nvzn suites, clippy, and fmt are green; the split-HLC
  watermark test is deterministic under load.

## 2. Current state

The crate already has `Channel<T>`, `OrderedChannel<T,P>`, typed order
boundaries, a threaded fan-out returning `(tx, registry, shutdown)`, `Feed`,
`Tail`/`Ranged`/`blend_chain`, and `resume_on_abort`. The residue this round
removes:

```text
seam assembled three ways: blend_chain / open().chain / range().merge
above written four times
producer spawn written nine times; Feed has no production user
fan-out owner tuple + Drop repeated four times
HistoricalReplay: tokio::spawn + spawn_blocking + hand-closed channel + block_on
finite/infinite is a constructor choice, not a type
resync cursor is AtomicU64; WatermarkGate holds a Mutex
```

## 3. Architecture

```text
                 ┌─────────────────────────────┐
                 │  Finite / Infinite marker    │
                 └──────────────┬──────────────┘
                                │
        ┌───────────────────────┴───────────────────────┐
        │                                               │
   Recv<T,K>                                      Channel<T,K>
   public read surface                            Source-backed
   single-source combinators                      merge / chain / batches
        │                                               │
   Resumable<T,P>                                OrderedChannel<T,P,K>
   HistoricalReplay                              (order policy)
        │
   pump(policy) ────────────────────────────────> Channel<T,K>

   Seam ── open() ──> Segment { live, cut } ── chain/merge ──> Channel
   produce_blocking / produce_async ──> Channel (write side)
   Fanout (producer) + Registry (readers)
```

The split is deliberate: `Recv` is lazy and single-consumer; `Channel` is
source-backed and can merge multiple sources; a `pump` is the explicit bridge
when a lazy source must run on a chosen thread. `Seam` and `Resumable` are
domain-facing; `Source` remains crate-private.

## 4. Kind marker: `Channel<T, K>`

`Channel` has **no default kind**. Every value names `Finite` or `Infinite`:

```rust
pub struct Finite;
pub struct Infinite;

Channel<T, Finite>    recv -> Result<T, End>           // Complete possible
Channel<T, Infinite>  recv -> Result<T, AbortReason>   // termination is an error
```

`End` stays the finite verdict (`Complete` or `Aborted`); `AbortReason` is the
infinite verdict. `end_reason` follows the same split, so a consumer of an
infinite stream cannot accidentally match `Complete`.

### 4.1 Constructors

| constructor | kind |
| --- | --- |
| `from(rx, what) -> (Channel, Closer)` | `Finite` |
| `closed(rx)` | `Finite` |
| `empty()`, `once(item)`, `from_vec(items)` | `Finite` |
| `Feed::channel(cap, what)` | `Finite` |
| `produce_blocking(..)`, `produce_async(..)` | `Finite` |
| `from_infinite(rx, what)` | `Infinite` |
| `aborted(what)` | `Infinite` |
| `Registry::subscribe(..)`, `subscribe_infinite(..)` | `Infinite` |
| `Wakes::subscribe()` | `Infinite` |

### 4.2 Combinator kinds

| combinator | result |
| --- | --- |
| `map`, `filter_map`, `dedup_by_key`, `until` | same kind |
| `take_while`, `take_until` | `Finite` (a consumer bound is completion) |
| `chain` | `Finite.chain(K) -> K`; `Infinite` has no `chain` |
| `merge` | `Infinite` if either side is; `Finite` only for two finite sides |
| `resume_on_abort` | `Infinite` only -> `Infinite` |
| `stitch` | preserves finite; the fallback is finite |
| `into_channel` on `OrderedChannel` | same kind |

`take_until` turning `Infinite` into `Finite` is what makes nvzn's live scan a
finite window without a separate “scan” type. `chain` starts only from a finite
phase; an infinite phase never reaches its second argument, so the API does not
offer it.

### 4.3 Boundaries

```rust
StableStream = Resumable<Result<StableLogEvent, Error>, Contiguous<LogIndex>>
ReplicaStream = OrderedChannel<Result<(LogIndex, Data), Error>,
                               StrictlyIncreasing<LogIndex>, Infinite>
LocalStoreStream = OrderedChannel<Result<Data, Error>,
                                  StrictlyIncreasing<DataId>, Finite>
WindowStream = OrderedChannel<Result<WindowItem, Error>,
                             StrictlyIncreasing<DataId>, Finite>
ArchiveStream = OrderedChannel<Result<SnapshotEntry, Error>,
                              Contiguous<LogIndex>, Finite>
```

`Ranged::range` returns `Finite`; `Seam::subscribe` returns `Infinite`.
`subscribe_from` returns an infinite replica stream; a historic scan is finite.

## 5. `Recv`: the lazy read surface

```rust
pub trait Recv<T, K> {
    type End;
    fn recv(&mut self) -> Result<T, Self::End>;
    fn try_recv(&mut self) -> Option<T>;
    fn end_reason(&mut self) -> Self::End;
    fn ready_batch(&mut self, max: usize) -> Option<Batch<'_, Self, T>>;
}
```

`Channel`, `OrderedChannel`, `Resumable`, and `HistoricalReplay` implement
`Recv`. `Source` stays private; a `Recv` is not necessarily selectable or
mergeable.

### 5.1 Single-source combinators

`Recv` carries `map`, `filter_map`, `take_until`, `take_while`, `chain`,
`until`, and `dedup_by_key` as lazy adapters. They return `Recv` values, so a
`Resumable` composes through a scan without a pump. The adapters take `FnMut`
state: a channel has one consumer, so a cursor or watermark gate is plain
mutable state, not an `AtomicU64` or `Mutex`.

`merge` stays on `Channel` (or an explicit pump): a blocking `recv` cannot wait
on two sources without a thread, and the selectable `Source` already solves it.

### 5.2 `pump` and spawn policy

A `pump` moves a blocking `Recv` onto a caller-chosen thread and feeds a
`Channel`:

```rust
pub trait SpawnBlocking {
    fn spawn<F: FnOnce() + Send + 'static>(&self, f: F);
}
pub trait SpawnTask { /* tokio::spawn for produce_async */ }

pump(source, cap, what, policy) -> Channel<T, K>
```

Policies are zero-sized marker types (`Tokio`, `StdThread`, `CurrentThread` for
tests); the tokio one is feature-gated. The pump reads `end_reason` and forwards
the exact verdict (see §8.2). `pump` is only needed at a thread/async/multi-source
boundary; a `Resumable` consumed directly by `recv` never needs it.

## 6. Seam: subscribe, then cut

One trait replaces `Tail` + `Ranged` + `blend_chain`:

```rust
pub trait Seam {
    type Item;
    type Cut: Copy;

    fn subscribe(&self) -> Live<Self::Item>;              // Infinite channel
    fn cut(&self, live: &Live<Self::Item>) -> Self::Cut;  // proof required
    fn above(&self, live: Live<Self::Item>, cut: Self::Cut) -> Live<Self::Item>;
    fn range(&self, cut: Self::Cut) -> Channel<Self::Item, Finite>;
}

pub fn open<S: Seam>(seam: &S) -> Segment<'_, S>;

pub struct Segment<'a, S: Seam> { /* live, cut, seam */ }

impl<'a, S: Seam> Segment<'a, S> {
    pub fn cut(&self) -> S::Cut;
    pub fn chain(self) -> Channel<S::Item, Infinite>;   // range(cut) then live
    pub fn merge(self) -> Channel<S::Item, Infinite>;   // range(cut) ∥ live
}
```

`Live<T>` is the proof that `subscribe` ran; `cut` cannot be called without it.
`open` is a free function, so no implementation can reorder subscribe and cut.
`Segment` is the only assembly point; the three shapes disappear:

```text
replica        : open(seam).chain()
stable resync  : open(seam).chain()  (inside Resumable)
unstable       : open(seam).chain()
nvzn live view : open(seam).merge()
```

`above` stays required (D2). `filter_above::<K>(live, cut: Option<K>)` covers
stable/replica; `filter_above_by(live, cut, key_fn)` covers unstable/nvzn,
whose cut is `Hlc` and whose key is `DataId`. Four implementations become four
one-line calls.

## 7. Resumable and resync

### 7.1 `Resumable`

```rust
pub struct Resumable<T, P> { /* inner segment state, cursor: P::Key */ }

impl<T, P> Resumable<T, P> {
    pub fn new<S>(start: P::Key, make: impl FnMut(P::Key, Option<AbortReason>) -> Option<S>) -> Self
    where
        S: Seam<Item = T>;
}
```

`Resumable` implements `Recv<T, Infinite>`; it is not a `Source`. It advances
the cursor on every emitted key and passes the cursor to `make` on a resync.
It does **not** dedup: the factory starts after the cursor (its inclusive or
exclusive rule). The ordered policy is the safety net for a bad factory.

`make` receives `(cursor, previous)` so zslg can log or measure for free; there
is no separate resync hook. The factory's weak upgrades stay domain-owned.

### 7.2 `resync` for streams without a position

The unstable tail has no cursor. It uses the same skeleton without the cursor:

```rust
resync<S>(make: impl FnMut(Option<AbortReason>) -> Option<S>) -> Channel<T, Infinite>
where
    S: Seam<Item = T>;
```

`WatermarkGate` remains domain policy, now as `FnMut` state in a `filter_map`
rather than a `Mutex`.

### 7.3 Stable boundary

`Reader::subscribe_stable_log` returns `StableStream = Resumable<...,
Contiguous<LogIndex>>`. The old `stable_stream` wrapper disappears because the
resumable type is the order check. `AtomicU64` and the outer cursor filter are
gone; the cursor is the `Resumable` state.

## 8. Producers, pumps, and the archive

### 8.1 `produce_blocking` / `produce_async`

```rust
Channel::produce_blocking(cap, what, policy, |feed| { ... })
Channel::produce_async(cap, what, policy, |feed| async move { ... })
```

The body owns a `Feed<T>`, which pairs the sender with the close proof. Normal
return closes; panic drops the feed and aborts. The async form is the dual,
behind the `tokio` feature. Both return immediately and are `Finite`.

### 8.2 Exact `End`, crate-internal

```rust
Closer::close()            // None          -> Complete
Closer::abort(reason)      // Some(reason)  -> Aborted { reason }   (pub(crate))
drop(Closer)               // disconnected  -> ProducerGone { what }
```

`Feed::abort(reason)` is `pub(crate)`. External users cannot fabricate an
`OrderViolation` or `Cancelled`; the crate's pumps/adapters can forward a
source's exact verdict. This is what keeps a stitched archive's order violation
visible through a pump.

### 8.3 Archive and `HistoricalReplay`

`ObjectStoreArchiveReader::stream` becomes:

```rust
fn stream(&self, bounds: ArchiveBounds) -> ArchiveStream
```

It does the sync manifest lookup, then `produce_async` for the snapshot opens.
Open errors are the first `Err` item; the item type already carries them. No
`block_on` remains.

`HistoricalReplay` is one lazy expression:

```rust
let scan = stable_log.range(start, until).map(..);
let ordered = OrderedChannel::<_, StrictlyIncreasing<LogIndex>, Finite>::named(scan, ..);
let stitched = ordered.stitch(move |gap| archive.stream(bounds).map(..));
let hlc_checked = OrderedChannel::<_, NonDecreasing<Hlc>, Finite>::named(
    stitched.into_channel(), None, "historical replay HLC");
hlc_checked.into_channel()
```

No `tokio::spawn`, no `spawn_blocking` consumer, no second hand-closed channel.
The HLC regression is now an `OrderViolation`, not an `assert!` panic.

## 9. Fan-out owner

```rust
pub struct FanoutConfig { data_cap, register_cap, sub_cap }
// OverflowPolicy is a required argument, not a config default.

let (fanout, registry) = broadcast(what, config, overflow);
let (fanout, registry) = keyed(what, route, config, overflow);

impl<T> Fanout<T> {
    pub fn publish(&self, item: T) -> bool;      // false: consumer gone
    pub fn try_publish(&self, item: T) -> ...;
    pub fn shutdown(self);                       // cancel + join
}
```

`Fanout` owns `tx` + `Shutdown`; `Drop` cancels (no join). `Registry`/
`KeyedRegistry` stays independent and cloneable; `subscribe` maps a failed
registration through `Channel::aborted(what)`, and `subscribe_infinite` returns
the always-infinite convenience. Caps can have defaults; `Block` vs `Shed`
cannot.

This reverses the earlier “no `Fanout` wrapper” decision on purpose: the
wrapper is producer-side and the reader side stays visible.

## 10. Edge cases and invariants

- **Subscribe before cut.** `open` is the only public assembly; `cut` needs a
  `Live`. A caller cannot read the cut before subscribing.
- **No generic dedup.** The resumable factory owns the resume position; the
  ordered policy catches a factory that starts too early.
- **Completion.** An infinite stream never returns `Complete`; `aborted` is the
  only empty infinite constructor. `take_until`/`take_while` are the only
  infinite→finite transitions.
- **Panic.** A panicking producer body drops its `Feed`/`Closer`, so the
  consumer sees `ProducerGone`; a `produce_async` task panicking is the same.
- **Runtime.** `produce_async` and `Tokio` need a runtime handle; the archive
  caller already has one. `HistoricalReplay` no longer needs a non-worker
  thread.
- **Cancellation.** `until` preserves the kind; on an infinite stream it is an
  abort, so `resume_on_abort` sees it as a resumable segment.
- **Fan-out send.** `publish` returns consumer-gone; producers may log or stop
  instead of silently ignoring a stopped fan-out.
- **Test timing.** The split-HLC watermark test receives its expected count
  with a timeout; `try_recv` is kept only for “nothing else arrives” assertions
  where order makes it valid.

## 11. Alternatives considered

| rejected | why |
| --- | --- |
| Default `Finite` kind | hides the decision; the previous bug remains representable |
| Runtime `End` only (no marker) | `resume_on_abort`/`range` cannot state their contract |
| `Infinite<T>` newtype only at boundaries | conversions remain an escape hatch |
| Second `KeyedSeam` trait | one extra line saved, two seam traits to learn |
| Read-only `Recv` | forces a pump/thread for lazy `take_until`/`filter_map`/`chain` |
| `Resumable` as `Source` | pulls the private select model into the public surface |
| Keep the `(tx, registry, shutdown)` tuple | repeats wiring, `Drop`, caps, and error handling |
| Keep the archive async + `block_on` | thread-affinity hazard with no benefit |
| Public `Closer::abort` | lets callers fabricate order/cancel verdicts |
| `pump` takes a closure | a marker policy makes the thread choice a type, not a value |
| Resumable dedup filter | bakes in “<=” semantics that are wrong for HLC groups |

## 12. Testing strategy

**Crate.**

- Kind compile tests: `Finite` cannot be passed where `Infinite` is required;
  `take_until` on `Infinite` yields `Finite`; `chain` starts only from
  `Finite`; `resume_on_abort` rejects `Finite`.
- `Recv` combinators are lazy and preserve the end; `pump` forwards `Complete`
  and each `AbortReason` (including `OrderViolation`).
- Spawn policies: `Tokio`, `StdThread`, `CurrentThread`; a panicking body
  aborts.
- Seam: `cut` cannot be called without `Live`; `open` order; `Segment::chain`
  vs `merge`; `filter_above` covers the four seams.
- `Resumable`: cursor advances, no duplicate filtering, factory receives the
  cursor and previous reason, abort mid-stream resumes.
- Fan-out owner: `publish` consumer-gone, `Drop` cancels, `shutdown` joins,
  caps/config, explicit overflow.

**zslg/nvzn.**

- Migrate the four fan-out owners; remove the three-field structs and `Drop`.
- Migrate the nine producer sites to `produce_blocking`/`produce_async`; remove
  `Feed`-less closers.
- `HistoricalReplay` lazy and HLC `NonDecreasing`; archive `stream` sync.
- Split-HLC watermark test uses timeout; full suites under parallel load.
- Stable/unstable resync tests: cursor, no dedup, WatermarkGate, shutdown.
- nvzn live view: `Segment::merge`, cancellation, fold stop.

## 13. Implementation order

Each slice is green before the next; the first is a thin vertical cut.

1. **Kind + `Recv` core.** `Channel<T, Finite|Infinite>`, `Recv`, constructors,
   kind rules, `Ordinary`/`Ordered` boundaries. Update crate tests and the
   public examples. This is the widest churn; keep the old names as aliases only
   where a migration needs them.
2. **Producer/pump.** `produce_blocking`, `produce_async`, spawn policies,
   internal `Closer::abort`/`Feed::abort`, `pump`. Migrate the nine scan
   producers; remove `empty_historic`.
3. **Seam + Resumable.** `Seam`, `Live`, `Segment`, `filter_above`;
   `Resumable`, `resync`; migrate replica/stable/unstable; delete
   `Tail`/`Ranged`/`blend_chain`; remove `AtomicU64`/`Mutex` state.
4. **Archive + HistoricalReplay.** Sync `stream` with `produce_async`; lazy
   `HistoricalReplay`; HLC `NonDecreasing`; no `block_on`.
5. **Fan-out owner.** `Fanout`/`FanoutConfig`; migrate zslg/nvzn; `publish`
   consumer-gone.
6. **Test + cleanup.** Split-HLC timeout; remaining `try_recv` audit; cap
   helper; full workspace suite, clippy, fmt.
