# Channel seams — design

Process: IAIC. Decision log: `2026-11-01_log_channel_seams.md` (D1–D32).
Internal model: `2026-11-01_internal_channel_seams.md`. Branch
`channel-extraction`. The last polish round of the extraction (D17).

## 1. Intent

### 1.1 Goal

The crate owns every transport decision; `zslg` and `nvzn` declare pipelines.

```text
crate owns      verdicts (Complete/Aborted), finite vs infinite, order checks,
                completeness, history/live seams, resumption, producer and
                fan-out lifecycles, watched values
domain declares sources, keys, cuts, fallbacks, policies (overflow, stall),
                metrics and log lines
```

### 1.2 Non-goals

- No public `Recv` trait, `pump`, or spawn-policy type (D16). Addable later
  without a break if a caller appears.
- No user-defined order policies; no fusion or cached `Select`.
- No public `Closer::abort` / `Feed::abort`: callers cannot fabricate a reason.
- No non-tokio producer (`produce_thread`) until a caller needs it.

### 1.3 Success criteria

- No hand-built producer in `zslg`/`nvzn` (`bounded` + `Channel::from` +
  spawn + close). Remaining `from_infinite` sites are multi-sender receivers
  only (§3).
- No hand-written fan-out owner (`tx`/`registry`/`shutdown` + `Drop`), no
  `watch` + `Wakes` pair, no `AtomicU64`/`Mutex` holding channel state, no
  in-band terminal item (`WindowItem`).
- No read path that drops a verdict: `Channel` has no `IntoIterator` (D25),
  and `ready_batch` returns `Result<ReadyBatch, K::End>` (D28); the lossy
  `into_stream` bridge is removed (D29).
- No `block_on`; `HistoricalReplay` and the archive are lazy declarations.
- Returning `Channel::empty()` for a subscription does not compile.
- A window that stops short of its bound aborts (no trailing-gap hole).
- Crate/zslg/nvzn suites, clippy, fmt green; the split-HLC watermark test is
  deterministic under parallel load.

## 2. Current state

The crate has `Channel<T>`, `OrderedChannel<T, P>`, `stitch`, a threaded
fan-out returning `(tx, registry, shutdown)`, `Feed`, `Wakes`,
`Tail`/`Ranged`/`blend_chain`, `resume_on_abort`. Residue this round removes:

```text
seam assembled three ways (blend_chain / blend / open+merge); above ×4
producer wiring hand-written per site (scans, snapshot reads, archive, replay,
  merge feeder, network bridges)
fan-out owner tuple + Drop ×4; watermark watch + Wakes moved together ×3
HistoricalReplay: tokio::spawn + spawn_blocking + second channel + block_on
archive: hand-tracked expected / last_hlc / open_failed, HLC assert!, tiling check
finite vs infinite is a constructor choice, not a type
resume cursor AtomicU64; WatermarkGate Mutex; resume on any abort
nvzn live scan: sentinel error chained after an infinite stream
window scan: cap + in-band terminal (WindowItem) smuggled out via Arc<Mutex>/atomics
for x in channel drops the verdict (compaction merge feeder)
```

## 3. Constraints and assumptions

- **One consumer per channel.** A `Channel` is pulled by one thread at a time;
  closures may hold mutable state (`FnMut + Send`).
- **Tokio is the runtime** for every producer site. Producer constructors and
  `defer_async` live behind the `tokio` feature. Producers spawn at the call,
  so they need a runtime context there; `defer_async` spawns at its first poll
  and therefore takes the `Handle` explicitly (D26.7).
- **`Source` stays crate-private.** New lazy types (`Resumable`, deferred
  phases) are private `Source`s; nothing public exposes the select model.
- **Multi-sender receivers stay `from_infinite`.** Coordinator fan, write-stream
  ingress/drift, replica commit outcomes end by sender refcount, not by a
  producer body; they are not producer sites.
- **Windows are open at both ends** `(after, until)`: every start is an
  exclusive `after: Option<K>` (D19, D26.3) — stable `range`, archive lower
  bound, `Gap`, resume cursor; every end is an exclusive `until`.
- Public API breaks are fine: zslg/nvzn migrate in the same change.

## 4. Architecture

```text
            ┌──────────────────────── read side ────────────────────────┐
            │ Channel<T, K>          K = Finite | Infinite (no default)  │
            │ OrderedChannel<T,P,K>  P = Contiguous | StrictlyIncreasing │
            │                            | NonDecreasing                 │
            │ combinators (FnMut) · ordered · covers · stitch · until    │
            └──────▲───────────────────────▲───────────────────▲─────────┘
                   │                       │                   │
   write side      │        composition    │                   │
   produce_blocking│        Segment::open(&seam)  ─ chain/merge│
   produce_async   │        Resumable::new(what, after, make)  │
   defer_async ────┘        resync(what, make)                 │
   (each ::<T, K>)          (one private resume Source)        │
   Fanout ── Registry::subscribe_infinite ─────────────────────┘
   Level  ── level.changes() / changed().await / get()
```

Three layers: the **read side** is one type family with the kind and the order
in the type; the **write side** turns a producer body, a fan-out, or a watched
value into read-side channels; **composition** builds read-side channels from
other read-side channels, lazily, on the consumer's thread.

## 5. Kind: `Channel<T, K>`

### 5.1 The two kinds

```rust
pub struct Finite;   pub struct Infinite;
pub trait Kind: sealed::Sealed { type End; type Proof; }
// Finite:   End = End,         Proof = Closed   (a body proves it closed)
// Infinite: End = AbortReason, Proof = ()       (nothing to prove: no close)

Channel<T, Finite>   recv -> Result<T, End>          // End = Complete | Aborted{reason}
Channel<T, Infinite> recv -> Result<T, AbortReason>  // ending is always an error
```

No default kind (D1): every signature, constructor and test double names it.
Kind-generic code (combinators, `integrate_loop`, which takes an infinite
subscription in production and finite vectors in tests) is generic over
`K: Kind` and reads `K::End`. Every read path returns the verdict (D28):
`recv -> Result<T, K::End>`, `ready_batch(max) -> Result<ReadyBatch<'_, T>,
K::End>`; `try_recv -> Option<T>` is the non-blocking poll and `end_reason`
remains for a stream already known to have stopped. There is no async
`Stream` bridge (D29): an async caller drains with `recv` on
`spawn_blocking`. Its dead surface goes with it (D31): `into_stream` on
`Channel` and `OrderedChannel`, `STREAM_BRIDGE_CAP`, `drain_into`,
`ChannelIter` and its export, the `futures` and `tokio-stream` dependencies
(the `tokio` feature becomes `["dep:tokio", "dep:tokio-util"]`), and
`tokio-stream` in zslg and the workspace once `write_tracker` is migrated.
`Guarded` stays: `until` uses it. `OrderedChannel<T, P, K>` and `Feed<T, K>` carry
the kind too. `Channel` has no `IntoIterator` (D25).

**Internal contract.** `Source` still reports `End`; an infinite wrapper maps
an inner `Complete` to `Aborted(ProducerGone)` behind a `debug_assert!`, so the
public type never lies (D26.8).

### 5.2 Constructors

| constructor | kind |
| --- | --- |
| `from(rx, what) -> (Channel, Closer)`, `closed(rx)`, `Feed::channel` | `Finite` |
| `empty()`, `once(item)`, `from_vec(items)` | `Finite` |
| `produce_blocking`, `produce_async`, `defer_async` | either: `::<T, K>` (D32) |
| `from_infinite(rx, what)`, `aborted(what)` | `Infinite` |
| `Level::changes` | `Infinite` |
| `Registry::subscribe` (in `Ok`), `Registry::subscribe_infinite` | `Infinite` |

`aborted` is the only empty infinite channel; `empty` is finite. The previous
`Channel::empty()`-as-subscription bug is a type error.

### 5.3 Combinators

| combinator | kind rule |
| --- | --- |
| `map`, `filter_map`, `dedup_by_key`, `until` | preserves |
| `take_while`, `take_until`, `take_at_least` | → `Finite` (a consumer bound is completion) |
| `chain` | only on `Finite`: `Finite.chain(K) -> K`; `chain_all` of finite phases |
| `merge` | `Infinite` if either side is |
| `stitch` | preserves; the fallback is `Finite` |
| `ordered::<P>(after, what)` | on `Channel` only; preserves |
| `covers`, `also` | on `OrderedChannel` (§9.1) |

An infinite stream never reaches a `chain` argument, so the API does not offer
it. `take_at_least::<K: Key<T>>(n: NonZeroUsize)` yields at least `n` keyed
items, then finishes the current key
group and stops at the first item with a different key (that item is dropped,
as in `take_while`). `n` counts keyed items, not groups, so the last group may
push the total past `n` — that is the intent (D24). A `None`-key item (an
`Err`) passes through and does not count. `NonZeroUsize` leaves no zero rule
to define.

A position is always named by a `Key<T>` type, never by a closure (D32):
`ordered::<P>`, `covers`, `take_at_least::<K>`, `filter_above::<K>` and
`Resumable` all read the key the same way, and the key type is visible in the
signature. `take_until` and `take_at_least` are thin layers over `take_while`,
whose `FnMut` predicate carries their state — one private `Source` (D32).

`OrderedChannel` carries only order-preserving operations (`until`, `covers`,
`also`, `stitch`); anything else goes through `into_channel()`.

Closures are `FnMut + Send` (D16): `FilterMap`/`TakeWhile`/`DedupByKey` store
`Box<dyn FnMut + Send>` instead of `Arc<dyn Fn + Send + Sync>`.

### 5.4 Domain boundaries

```rust
StableStream     = OrderedChannel<Result<StableLogEvent, Error>, Contiguous<LogIndex>,         Infinite>
ReplicaStream    = OrderedChannel<Result<(LogIndex, Data), Error>, StrictlyIncreasing<LogIndex>, Infinite>
LocalStoreStream = OrderedChannel<Result<Data, Error>, StrictlyIncreasing<DataId>, Finite>   // scans and windows
ArchiveStream    = OrderedChannel<Result<SnapshotEntry, Error>, Contiguous<LogIndex>,  Finite>
```

`subscribe_*` return `Infinite`; scans, ranges and archive reads `Finite`.
`WindowStream` and `WindowItem` are gone: a window is a plain scan (D24).

## 6. Write side

### 6.1 Producers

```rust
Channel::<T, K>::produce_blocking(cap, what, |feed: Feed<T, K>| -> K::Proof { .. })
Channel::<T, K>::produce_async(cap, what, |feed: Feed<T, K>| async move { .. /* -> K::Proof */ })
```

Both return at once and are generic over the kind (D32). A finite body must
return `Closed`, which only `Feed::close(self)` produces (D26.6): a normal
return is provably a close, and the close precedes the sender drop. An
infinite `Feed` has no `close()` and its body returns `()`: return and panic
both end `ProducerGone` (D22). A panic always drops the feed:
`Aborted(ProducerGone)`. Sites:

```text
produce_blocking        stream_above_hlc, stream_window, StableLog::range,
                        replica pump_range, read_snapshot's decode (the GET
                        stays in read_snapshot's async body)
produce_async::<_, Infinite>  client subscribe_stream, client write_stream
declarations instead    archive stream (§9.3), compaction merge feeder (§9.5),
                        HistoricalReplay (§9.4)
```

Finite `produce_async` has no site today but costs nothing: the constructor
is generic (D32). The window scan becomes
`stream_window(above, upto)`: no cap, no terminal, the same plain scan as
`stream_above_hlc` with an upper bound. Slice 1 audits every remaining
`Channel::from` / `from_infinite` in production: each becomes a producer or is
a §3 multi-sender receiver. Before slice 3 (no kinds yet), the network
bridges use a pre-kind `produce_async` returning a plain `Channel`.

### 6.2 `Feed<T, K>`

`send(item) -> Result<(), Disconnected<T>>` (consumer gone), `try_send`,
`send_timeout(item, d) -> Result<(), SendTimeoutError<T>>`, and for `Finite`
`close(self) -> Closed`. `send_timeout` keeps the client's
`CONSUMER_STALL_TIMEOUT` policy in zslg (D22). `Closer::abort`/`Feed::abort`
stay `pub(crate)` (D8) and exist only if an implementation needs to forward an
exact reason.

### 6.3 Fan-out owner (D11–D13, D15)

```rust
let (fanout, registry) = broadcast(what, FanoutConfig { data_cap, register_cap, sub_cap }, overflow);
let (fanout, registry) = keyed(what, route, config, overflow);

impl<T> Fanout<T> {
    pub fn publish(&self, item: T) -> Result<(), Stopped<T>>;            // thread gone
    pub fn try_publish(&self, item: T) -> Result<(), TryPublishError<T>>; // Full(T) | Stopped(T)
    pub fn shutdown(self);                                               // cancel + join
}
```

`Drop` cancels without joining. `OverflowPolicy` is required; caps may
default. The reader side keeps both forms (D26.4):
`subscribe() -> Result<Channel<T, Infinite>, RegistryError>` — nvzn maps
`Full` to `TooManyPendingSubscriptions` — and
`subscribe_infinite() -> Channel<T, Infinite>`, where a failed registration is
`aborted(what)`. A departed subscriber never surfaces at the producer.

### 6.4 `Level<T>` (D23)

```rust
let (setter, level) = Level::new(initial);
setter.set(v);                              // value, then wake — order owned here
level.get() -> T;
level.changes() -> Channel<(), Infinite>;   // blocking / selectable wake-ups
level.changed().await;                      // async, tokio feature
```

The core is synchronous (a mutex plus the private `Wakes`); only
`changed().await` is behind the `tokio` feature. Replaces `StableLog.watermark`
+ `watermark_wakes` and the test double's pair: `StableLogSnapshot` exposes
`watermark() -> Level<Option<Hlc>>` instead of `watch_watermark` and
`watermark_moves`. `Level` (the reader) is `Clone`; the setter is single-owner.
Readers: `WatermarkGate` (`get`), unstable seam (`changes`), `purge.rs` and
`quorum.rs` (`watch.changed().await` → `level.changed().await`,
`*watch.borrow()` → `level.get()`).

Scope: `Level` replaces only the watermark's `watch` + `Wakes` pair this round.
Other value-plus-notification sites that use tokio `watch` alone —
`LocalStoreView::subscribe_integrated`, nvzn live changed/retired counters,
the dispatcher watermark, the replica effective cutoff — stay as they are; they
are candidates for a later round, not residue.

## 7. Seam

### 7.1 Trait and `Segment::open` (D2, D14, D20)

```rust
pub trait Seam {
    type Item;
    type Cut: Copy;
    fn subscribe(&self) -> Channel<Self::Item, Infinite>;
    fn cut(&self) -> Self::Cut;
    fn above(&self, live: Channel<Self::Item, Infinite>, cut: Self::Cut)
        -> Channel<Self::Item, Infinite>;
    fn range(&self, cut: Self::Cut) -> Channel<Self::Item, Finite>;
}

impl<S: Seam> Segment<'_, S> {
    pub fn open(seam: &S) -> Self;              // subscribe → cut → above
    pub fn cut(&self) -> S::Cut;
    pub fn chain(self) -> Channel<S::Item, Infinite>;   // range(cut), then live
    pub fn merge(self) -> Channel<S::Item, Infinite>;   // range(cut) ∥ live
}
```

The methods are hooks only `Segment::open` calls; the module docs say so. That
`cut` stays callable on its own is an accepted trade-off (D20): every seam is
used from one place, and sealing would stop zslg implementing the trait. The
order (subscribe before cut) is the seam's whole correctness argument:
anything at or below the cut is in `range`, anything above it is already on
the subscription.

### 7.2 `above` helpers

One helper (D32): `filter_above::<K: Key<Item>>(live, cut: Option<K>)` drops
live items whose key is `<= cut`. Stable and replica use `K = LogIndex`;
unstable and nvzn use `K = Hlc`, via `Key` impls that read `data_id.hlc()`
from `UnstableStep` and `FoldItem`. Each seam's `above` is one line.

### 7.3 The four uses

```text
replica          Segment::open(&seam).chain()              one cycle; the learner reconnects
stable           Segment::open(&seam).chain()              inside Resumable (§8.2)
unstable         Segment::open(&seam).chain()              inside resync (§8.3)
nvzn live view   Segment::open(&seam) → cut → .merge()     (§10.1)
```

`Tail`, `Ranged`, `blend_chain`, `Tail::blend` are deleted.

## 8. Resumption

### 8.1 The rule (D18)

A resumer reopens only after `Aborted(ProducerGone)` — the fan-out hang-up or
a dead producer that a fresh cycle repairs. Every other reason ends the resumed
stream unchanged; `until` is therefore safe inside or outside a segment, and a
corrupt log cannot spin a resync loop.

```rust
enum Opening { First, AfterProducerGone { what: &'static str } }
```

The factory receives the `Opening` (for its metric and warning) and may decline
with `None` (its `Weak` upgrades failed). A decline after a `ProducerGone` ends
the stream with that reason; a decline at the first open ends it
`Aborted(ProducerGone { what })` under the resumer's label, never `Complete`
(D26.8).

### 8.2 `Resumable` (D19)

```rust
Resumable::<T, P>::new(what: &'static str, after: Option<P::Key>,
                       make: impl FnMut(Option<P::Key>, Opening) -> Option<S>)
    -> OrderedChannel<T, P, Infinite>          where S: Seam<Item = T>
```

```text
        ┌───────── make(after, First) ─────────┐
        ▼                                      │
   [segment open] ── item k ──> emit, after := k
        │
        ├─ Aborted(ProducerGone) ──> make(after, AfterProducerGone) ──┐
        │                                      ▲                      │
        │                                      └──── Some(seam) ──────┘
        ├─ make → None ──────────────> end: previous reason
        └─ any other abort ──────────> end: that reason
```

Each segment is `Segment::open(&seam).chain()`; `what` labels the order check
and a first-open decline. `after` is always exclusive: `None` = from the beginning; first open passes
the caller's, a resync the last emitted key. No dedup (D3): the factory starts
after `after`; the policy check (`first > after` for `StrictlyIncreasing`,
`== Successor::next_after(after)` for `Contiguous`) catches a factory that
starts wrong; `NonDecreasing` also requires the first key `> after`.
`Successor` becomes `fn next_after(prev: Option<Self>) -> Self`.

### 8.3 `resync` for streams without a position

```rust
resync(what: &'static str, make: impl FnMut(Opening) -> Option<S>) -> Channel<T, Infinite>
```

Same loop, no cursor: `resync` and `Resumable` share one private resume
`Source` whose cursor is optional, so the D18 rule exists once (D32). The two
public constructors stay because their signatures differ. The unstable tail
uses it; `WatermarkGate` becomes plain
`FnMut` state in the trailing `filter_map`, reading `Level::get`.

### 8.4 Stable boundary

```rust
Reader::subscribe_stable_log(after: Option<LogIndex>) -> StableStream
```

Aligned with `Replica::subscribe_from(Option<LogIndex>)` and with
`StableLogSnapshot::range(after: Option<LogIndex>, until)` (D26.3). `resyncing_tail`
becomes `Resumable::new("stable log", after, |after, opening| { upgrade; log; Some(StableSeam{..}) })`
plus `.until(&shutdown)`. The `AtomicU64` cursor, the outer cursor filter and
`stable_stream` disappear.

## 9. Completeness and the archive

### 9.1 `covers(..until)` (D21)

```rust
impl<T, K: Successor + Key<T>> OrderedChannel<T, Contiguous<K>, Finite> {
    pub fn covers(self, range: RangeTo<K>, what: &'static str) -> Self;
}
impl<T, P: OrderPolicy, K: Kind> OrderedChannel<T, P, K> {
    pub fn also<Q: OrderPolicy>(self, after: Option<Q::Key>, what: &'static str) -> Self
        where Q::Key: Key<T>;
}
```

`covers` ends `Complete` only if the stream reached `until`: last key's
successor `== until`, or, for an empty stream, `next_after(after) == until`
(the channel holds its `after`). Otherwise
`Aborted(OrderViolation { what })`. An `Aborted` source keeps its own reason.
Order policies check steps between items; `covers` checks the end. There is no
unbounded form: a caller with an open upper bound does not call it.

`also` enforces a second policy at runtime and keeps `P` in the type (D26.1):
the primary order is what consumers rely on; the secondary is a guard (HLC
monotonicity).

### 9.2 `defer_async` (D21)

```rust
Channel::<T, K>::defer_async(handle: &Handle, || async move { /* -> Channel<T, K> */ })
    -> Channel<T, K>
```

On first poll: spawn the future on `handle`, wait (selectably) on the
hand-off, then delegate to the resolved channel, verdict included. The handle
is explicit (D26.7): the construction site may be a blocking-pool or test
thread; the archive reader and the snapshot store hold the handle they were
built with. Dropping a deferred phase aborts its task (reads have no side
effects). In `chain_all`, only the front phase is polled, so phases open one at
a time.

### 9.3 The archive

```rust
fn stream(&self, bounds: ArchiveBounds) -> ArchiveStream {   // no longer async
    let covering = covering_snapshots(&manifest, bounds);    // sync read
    let ch = Channel::chain_all(covering.into_iter().map(|m| {
            let store = self.snapshot_store.clone();
            Channel::defer_async(&self.handle, move || async move {
                store.read_snapshot(m.snapshot_id, ArchiveRead).await
                     .unwrap_or_else(|e| Channel::once(Err(e)))
            })
        }))
        .take_while(not_above(bounds))     // Err items pass; the first entry ≥ until ends
        .filter_map(skip_below(bounds))    // only drops entries ≤ bounds.after
        .ordered::<Contiguous<LogIndex>>(bounds.after, "archive tiling");
    let ch = match bounds.until {
        IndexExclusive(until) => ch.covers(..until, "archive window"),
        Unbounded => ch,
    };
    ch.also::<NonDecreasing<Hlc>>(None, "archive HLC")   // replaces the assert!
}
```

`ArchiveStream` stays `OrderedChannel<Result<SnapshotEntry, Error>,
Contiguous<LogIndex>, Finite>`. `read_snapshot` stays async (11 callers);
`StableArchiveReader::stream` becomes synchronous. `ArchiveBounds` becomes
`{ after: Option<LogIndex>, until: ArchiveUpperBound }`; `ArchiveLowerBound` is
deleted (a one-variant enum would add nothing, D30).

### 9.4 `HistoricalReplay`

```rust
stable_log.range(after, until)                               // (after, until), may have gaps
    .ordered::<StrictlyIncreasing<LogIndex>>(after, "stable scan")
    .stitch(move |gap: Gap<LogIndex>| {                      // fill (gap.after, gap.got)
        archive.stream(ArchiveBounds { after: gap.after, until: IndexExclusive(gap.got) })
            .into_channel()
            .map(|r| r.map(|e| (e.global_index, e.data)))
    })                                                       // → Contiguous
    .covers(..until, "historical replay")                    // trailing gap now aborts
    .also::<NonDecreasing<Hlc>>(None, "historical replay HLC")
    .into_channel()                                          // Seam::range returns a Channel
```

One lazy expression on the consumer's thread: no task, no second channel, no
`block_on`, no `assert!`. `Gap` becomes `{ after: Option<K>, got: K }`. The
archive stream already `covers` its window, which replaces the fallback's hand
endpoint check.

### 9.5 Compaction merge feeder (D25)

```rust
Channel::chain_all(ids.map(|id| Channel::defer_async(&handle, move || async move {
        store.read_snapshot(id, Compaction).await.unwrap_or_else(|e| Channel::once(Err(e)))
    })))
    .until(&Cancellation::from_token(&cancel))
```

Replaces `feed_sources`, its `Feed` enum and the `for item in ch` loop that
swallowed an aborted source. An abort now reaches `write_snapshot`, which
refuses to finalize (`StreamTruncated`).

### 9.6 Snapshot window (D24)

```rust
let mut window = local_store.stream_window(lower, cutoff)   // plain scan (lower, cutoff], no cap
    .into_channel()
    .take_at_least::<Hlc>(cap);                              // Key<Result<Data, Error>> for Hlc
let first = match window.recv() { .. };                     // the peek: empty → commit cutoff, no object
let entries = Channel::once(Ok(first)).chain(window)
    .map(index_from(next_index_start));                     // FnMut counter
// write_snapshot(..) -> Result<SnapshotWrite, Error>
// watermark = if w.count < cap.get() { cutoff } else { w.last.hlc }
```

```rust
pub struct SnapshotWrite {
    pub count: usize,                  // ≥ 1: the peek guarantees a first entry
    pub last: EntryMeta,               // { index, hlc }
    pub stored_bytes: u64,             // metrics, as today's (count, stored, uncompressed)
    pub uncompressed_bytes: u64,
}
```

The peek stays: it runs before the multipart upload starts, so an empty window
creates no object, and it is where `StableLogBehindManifest` is checked. The
first entry's metadata comes from the peek; `SnapshotWrite` carries the rest
as a named struct, so the metrics sizes survive (D30 refines D27).

`WindowItem`, the `terminal` mutex, the `end_hlc` and `index` atomics, and the
scan's `finishing`/`capped` state go.

## 10. nvzn

### 10.1 Live view

```rust
fn range(&self, cut: Option<Hlc>) -> Channel<FoldItem, Finite> {
    let Some(until) = cut else { return Channel::empty() };
    self.reader.subscribe_stable_log(self.after).into_channel()
        .until(&self.cancelled)
        .take_until(move |item| matches!(item, Ok(ev) if at_or_after_cut(until, ev)))
        .filter_map(scan_item)
        .chain(Channel::once(FoldItem::ReplayDone))
}
```

The sentinel error is gone: an infinite source can only end aborted, so an
early end is an `Aborted` finite range and `ReplayDone` is not emitted.
`begin` uses `Segment::open(&seam)`, reads `cut()` for `Coverage`, then
`.merge()`.

### 10.2 Fold and subscribers

The fold subscribes with `after = log_index_watermark()` directly. The
subscriber fan-out uses `keyed(..) -> (Fanout, KeyedRegistry)`. On
`Stopped`, `Subscribers::publish` warns once and continues: subscribers are
observers, and the fold's correctness does not depend on them.

## 11. Complexity budget

**Essential, and where it lives.**

- Seam order (subscribe → cut) — `Segment::open`, one place.
- Resume rule and cursor — `Resumable`, one state machine (§8.2).
- Order and completeness — policies + `covers`, per item and at the end.
- Fan-out pacing (`Block`/`Shed`) — the distribution thread.

**Accepted accidental complexity.**

- The kind parameter on every signature (D1): verbose by design.
- `defer_async`'s hand-off wait: one private `Source` state.
- `also` as a runtime-only second policy, so the type names one order.
- `Kind::Proof` so one constructor serves both kinds; two `Feed` kinds.
- `Level` holds a mutex plus private `Wakes`, with a tokio-gated async wait:
  two notification paths behind one setter.

## 12. Edge cases

- **Empty window.** `covers` on an empty stream compares `next_after(after)`
  with `until`; `range(cut)` for `cut = None` is `empty()`.
- **Cancellation.** `until` preserves the kind and ends `Cancelled`; resumers
  never resume it (D18).
- **Order violation in a resumed stream** ends the stream; no loop.
- **Archive open error** is the first item of its phase; the consumer stops on
  `Err` as today.
- **`defer_async`** runs on the handle it is given; a dropped phase aborts
  its task.
- **Exact-cap snapshot window**: the watermark lands on the last HLC; the next
  iteration finds `(h, cutoff]` empty and commits `cutoff`.
- **Panic** in any producer body or deferred future: `ProducerGone`.
- **Fan-out stopped**: `publish` returns `Stopped(item)`; the producer logs and
  stops or drops, per site.
- **HLC groups** across a resync: `after` is an index, not an HLC, so a group
  split by a resync is not re-filtered (D3).

## 13. Operations

- **Threads**: one per fan-out (zslg: replica live, local-store unstable,
  stable log; nvzn: one per dispatcher), unchanged. `defer_async` and producers
  use the tokio pools.
- **Metrics/logs**: resync metrics and warnings move into `make(.., Opening)`;
  unchanged names. New `OrderViolation` labels — `archive tiling`,
  `archive window`, `archive HLC`, `historical replay`,
  `historical replay HLC` — surface as
  `StreamTruncated { what }` at the zslg boundary, as today.
- **Behaviour changes**: an HLC regression is an abort, not a panic; a short
  replay window aborts instead of ending early; a compaction source that aborts
  fails the merge instead of shortening it; an exact-cap snapshot chunk costs
  one empty iteration; every start bound is `after`.

## 14. Alternatives considered

| rejected | why |
| --- | --- |
| Default `Finite` kind | the `empty()` bug stays representable (D1) |
| Public `Recv` + `pump` + spawn policy | `until` cannot race cancel lazily; nvzn merge would need a thread (D16) |
| `Resumable` outside `Source` | false premise: `Source` stays private either way (D16) |
| `Live` proof token | forgeable; `Segment::open` is the guarantee (D20) |
| Factory decides what to resume | every factory repeats one match; forgetting it loops (D18) |
| Inclusive / mixed cursor, contiguous-only `Resumable` | off-by-one between first open and resync (D19) |
| `bool` publish | names the wrong event; drops the item (D15) |
| `through(last)` inclusive | every zslg window is exclusive (D21) |
| Sync `defer` | no caller (D21) |
| Archive async + `block_on` | thread-affinity hazard (D9) |
| Keep `watch` + `Wakes` | the value-then-wake order repeated at each writer (D23) |
| Typed end value `Finite<R>` | its only use was the window terminal (D24) |
| Chunk splitter (one scan → many snapshots) | Raft commits sit between chunks (D24) |
| `IntoIterator` for `Channel` | drops the verdict; hid a compaction data-loss path (D25) |
| Unbounded `covers` | an open bound has no end to check (D26.5) |
| Keep `into_stream` | drops the verdict; no production user (D29) |
| Kind-specific producers (`produce_async_infinite`) | a `Kind::Proof` makes one constructor cover both (D32) |
| Closure keys (`filter_above_by`, `take_at_least(n, f)`) | two ways to name a position; `Key<T>` is the one (D32) |

## 15. Testing

**Crate.**

- Kind: `compile_fail` doctests — finite as infinite, `chain` on infinite,
  `empty()` as a subscription, `covers` on infinite.
- Combinators: `FnMut` state; kind transitions; `take_until` on infinite ends
  `Complete`, source abort preserved.
- Producers: a finite body must return `Closed`; close/panic verdicts for
  both constructors in both kinds; `send_timeout`. `defer_async` in both kinds.
- `take_while`/`take_until`/`take_at_least` share one `Source`; `filter_above`
  for `LogIndex` and `Hlc` keys.
- `defer_async`: lazy start (no spawn before first poll), verdict delegation,
  phases open sequentially in `chain_all`.
- `covers`: reached, short, empty-window both ways, aborted source keeps reason.
  `also`: secondary violation aborts with its label; type unchanged.
- `take_at_least`: below cap, exact cap, group finished past cap, `Err` items.
- `compile_fail`: `for x in channel` on a `Channel` (a `ReadyBatch` stays an
  iterator).
- Test modules and `examples/write_tracker` move from `into_stream` to
  `spawn_blocking` + `recv`; the `into_stream` and `drain_into` tests go.
- `Segment`: open order, `chain` vs `merge`, `filter_above*`.
- `Resumable`/`resync`: resume only on `ProducerGone`; `Cancelled` and
  `OrderViolation` end; `after` passed exactly; decline ends with previous
  reason.
- `Fanout`: `publish`→`Stopped`, `Drop` cancels, `shutdown` joins. `Level`:
  set-then-wake ordering, both readers.

**zslg/nvzn.**

- Split-HLC watermark test receives its count with a timeout.
- Archive: tiled, gap, short, unbounded, HLC regression, open error.
- Compaction merge: an aborted source fails the merge.
- Snapshot window: below cap, exact cap (one empty follow-up), capped.
- `HistoricalReplay`: mid gap stitched, trailing gap aborts, HLC regression
  aborts.
- Stable/unstable resync: cursor continuity, shutdown ends (no resync).
- nvzn live view: early stable end aborts, cancellation, `ReplayDone` order.
- Full suites under parallel load, clippy, fmt.

## 16. Implementation order (D17)

Each slice is green (tests, clippy, fmt) before the next.

1. **Mechanical.** Split-HLC test fix; `FnMut` combinators; remove
   `IntoIterator` and `into_stream` with their dead surface and dependencies
   (D31); `ready_batch` returns the verdict;
   `take_until`/`take_at_least` over `FnMut` `take_while`; `produce_blocking`
   with `Closed`, pre-kind `produce_async` for the network bridges,
   `Feed::send_timeout` (pre-kind: plain `Channel`); migrate producer sites and
   audit remaining `from`/`from_infinite`; snapshot window via
   `take_at_least` and `write_snapshot` metadata, `WindowItem` deleted;
   `Fanout`/`FanoutConfig`/`Stopped`; `Level`, private `Wakes`.
2. **Completeness + archive.** Exclusive starts everywhere (`after`, `Gap`,
   `range`, `ArchiveBounds.after`), `Successor::next_after`; `covers`, `also`;
   `defer_async`; archive declaration; compaction merge feeder; lazy
   `HistoricalReplay`.
3. **Kinds + seam.** `Kind`, `Channel<T, K>`, `Feed<T, K>`,
   `OrderedChannel<T, P, K>`, kind rules, `Kind::Proof` (producers and
   `defer_async` become kind-generic), `Key<T>` for `Hlc` on the unstable and
   nvzn items, `compile_fail` tests; `Seam` +
   `Segment::open` + `filter_above*`, end to end on the replica path; then all
   signatures in zslg/nvzn.
4. **Resumption.** `Opening`, one resume `Source` behind `Resumable` and
   `resync`;
   `subscribe_stable_log(after)`; stable/unstable/nvzn migrated; delete
   `Tail`/`Ranged`/`blend_chain`, `resume_on_abort`'s public form if unused,
   `stable_stream`.

