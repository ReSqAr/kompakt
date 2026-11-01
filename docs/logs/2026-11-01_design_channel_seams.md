# Channel seams — design

Process: IAIC. Decision log: `2026-11-01_log_channel_seams.md` (D1–D23).
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
  `watch` + `Wakes` pair, no `AtomicU64`/`Mutex` holding channel state.
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
producer wiring hand-written ×11 (9 finite scans/reads, 2 network bridges)
fan-out owner tuple + Drop ×4; watermark watch + Wakes moved together ×3
HistoricalReplay: tokio::spawn + spawn_blocking + second channel + block_on
archive: hand-tracked expected / last_hlc / open_failed, HLC assert!, tiling check
finite vs infinite is a constructor choice, not a type
resume cursor AtomicU64; WatermarkGate Mutex; resume on any abort
nvzn live scan: sentinel error chained after an infinite stream
```

## 3. Constraints and assumptions

- **One consumer per channel.** A `Channel` is pulled by one thread at a time;
  closures may hold mutable state (`FnMut + Send`).
- **Tokio is the runtime** for every producer site. Producer constructors and
  `defer_async` live behind the `tokio` feature and capture a runtime handle at
  construction, so a first poll from a blocking thread works.
- **`Source` stays crate-private.** New lazy types (`Resumable`, deferred
  phases) are private `Source`s; nothing public exposes the select model.
- **Multi-sender receivers stay `from_infinite`.** Coordinator fan, write-stream
  ingress/drift, replica commit outcomes end by sender refcount, not by a
  producer body; they are not producer sites.
- **Windows are half-open** `(after, until)` in zslg: starts exclusive (D19),
  ends exclusive (`range(from, until)`, `IndexExclusive`).
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
   produce_async   │        Resumable::new(after, make)        │
   produce_async_infinite   resync(make)                       │
   defer_async ────┘        (all private Sources)              │
   Fanout ── Registry::subscribe ──────────────────────────────┘
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

Channel<T, Finite>   recv -> Result<T, End>          // End = Complete | Aborted{reason}
Channel<T, Infinite> recv -> Result<T, AbortReason>  // ending is always an error
```

No default kind (D1): every signature, constructor and test double names it.
`end_reason` follows the same split. `ready_batch`, `try_recv`, `into_stream`
exist for both. `OrderedChannel<T, P, K>` and `Feed<T, K>` carry the kind too.

### 5.2 Constructors

| constructor | kind |
| --- | --- |
| `from(rx, what) -> (Channel, Closer)`, `closed(rx)` | `Finite` |
| `empty()`, `once(item)`, `from_vec(items)` | `Finite` |
| `produce_blocking`, `produce_async`, `defer_async` | `Finite` |
| `from_infinite(rx, what)`, `aborted(what)` | `Infinite` |
| `produce_async_infinite`, `Registry::subscribe`, `Level::changes` | `Infinite` |

`aborted` is the only empty infinite channel; `empty` is finite. The previous
`Channel::empty()`-as-subscription bug is a type error.

### 5.3 Combinators

| combinator | kind rule |
| --- | --- |
| `map`, `filter_map`, `dedup_by_key`, `until` | preserves |
| `take_while`, `take_until` | → `Finite` (a consumer bound is completion) |
| `chain` | only on `Finite`: `Finite.chain(K) -> K`; `chain_all` of finite phases |
| `merge` | `Infinite` if either side is |
| `stitch` | preserves; the fallback is `Finite` |
| `covers` | `Finite` only (§9.1) |

An infinite stream never reaches a `chain` argument, so the API does not offer
it. Closures are `FnMut + Send` (D16): `FilterMap`/`TakeWhile`/`DedupByKey`
store `Box<dyn FnMut + Send>` instead of `Arc<dyn Fn + Send + Sync>`.

### 5.4 Domain boundaries

```rust
StableStream     = OrderedChannel<Result<StableLogEvent, Error>, Contiguous<LogIndex>,         Infinite>
ReplicaStream    = OrderedChannel<Result<(LogIndex, Data), Error>, StrictlyIncreasing<LogIndex>, Infinite>
LocalStoreStream = OrderedChannel<Result<Data, Error>,      StrictlyIncreasing<DataId>, Finite>
WindowStream     = OrderedChannel<Result<WindowItem, Error>, StrictlyIncreasing<DataId>, Finite>
ArchiveStream    = OrderedChannel<Result<SnapshotEntry, Error>, Contiguous<LogIndex>,  Finite>
```

`subscribe_*` return `Infinite`; scans, ranges and archive reads `Finite`.

## 6. Write side

### 6.1 Producers

```rust
Channel::produce_blocking(cap, what, |feed: Feed<T, Finite>| { .. })            // spawn_blocking
Channel::produce_async(cap, what, |feed: Feed<T, Finite>| async move { .. })    // tokio::spawn
Channel::produce_async_infinite(cap, what, |feed: Feed<T, Infinite>| async move { .. })
```

All return at once. A finite body that returns closes (`Complete`); a panic
drops the feed (`Aborted(ProducerGone)`). An infinite `Feed` has no `close()`:
return and panic both end `ProducerGone` (D22). Sites:

```text
produce_blocking        stream_above_hlc, stream_window_ordered, StableLog::range,
                        replica pump_range, read_snapshot (decode after the GET)
produce_async           compaction merge feeder — or, if `feed_sources` is only
                        "read each snapshot in order", the archive's shape:
                        chain_all(defer_async(read_snapshot)) with no feeder
produce_async_infinite  client subscribe_stream, client write_stream
gone                    HistoricalReplay (§9.4), archive stream (§9.3)
```

The exact list is audited in slice 1; every remaining `Channel::from` /
`from_infinite` in production either becomes a producer or is a §3
multi-sender receiver.

### 6.2 `Feed<T, K>`

`send(item) -> Result<(), Disconnected<T>>` (consumer gone), `try_send`,
`send_timeout(item, d) -> Result<(), SendTimeoutError<T>>`. `send_timeout`
keeps the client's `CONSUMER_STALL_TIMEOUT` policy in zslg (D22).
`Closer::abort`/`Feed::abort` stay `pub(crate)` (D8) and exist only if an
implementation needs to forward an exact reason.

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
default. `Registry::subscribe` returns `Channel<T, Infinite>` and maps a failed
registration to `aborted(what)` (the `Result` form goes; `subscribe_infinite`
becomes `subscribe`). A departed subscriber never surfaces at the producer.

### 6.4 `Level<T>` (D23)

```rust
let (setter, level) = Level::new(initial);
setter.set(v);                              // value, then wake — order owned here
level.get() -> T;
level.changes() -> Channel<(), Infinite>;   // blocking / selectable wake-ups
level.changed().await;                      // async, tokio feature
```

Replaces `StableLog.watermark` + `watermark_wakes` and the test double's pair.
`Wakes` becomes private. Readers: `WatermarkGate`, unstable seam, `purge.rs`,
`quorum.rs`.

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

The methods are hooks only `Segment::open` calls; the module docs say so. The
order (subscribe before cut) is the seam's whole correctness argument:
anything at or below the cut is in `range`, anything above it is already on
the subscription.

### 7.2 `above` helpers

`filter_above::<K>(live, cut: Option<K>)` (stable, replica: key and cut are
`LogIndex`) and `filter_above_by(live, cut, key_fn)` (unstable, nvzn: cut
`Hlc`, key `DataId`). Each seam's `above` is one line.

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
with `None` (its `Weak` upgrades failed); a decline ends the stream with the
previous segment's reason.

### 8.2 `Resumable` (D19)

```rust
Resumable::<T, P>::new(after: Option<P::Key>,
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

`after` is always exclusive: `None` = from the beginning; first open passes
the caller's, a resync the last emitted key. No dedup (D3): the factory starts
after `after`; the policy check (`first > after` for `StrictlyIncreasing`,
`== Successor::next_after(after)` for `Contiguous`) catches a factory that
starts wrong. `Successor` becomes `fn next_after(prev: Option<Self>) -> Self`.

### 8.3 `resync` for streams without a position

```rust
resync(make: impl FnMut(Opening) -> Option<S>) -> Channel<T, Infinite>
```

Same loop, no cursor. The unstable tail uses it; `WatermarkGate` becomes plain
`FnMut` state in the trailing `filter_map`, reading `Level::get`.

### 8.4 Stable boundary

```rust
Reader::subscribe_stable_log(after: Option<LogIndex>) -> StableStream
```

Aligned with `Replica::subscribe_from(Option<LogIndex>)`. `resyncing_tail`
becomes `Resumable::new(after, |after, opening| { upgrade; log; Some(StableSeam{..}) })`
plus `.until(&shutdown)`. The `AtomicU64` cursor, the outer cursor filter and
`stable_stream` disappear.

## 9. Completeness and the archive

### 9.1 `covers(..until)` (D21)

```rust
impl<T, K: Successor> OrderedChannel<T, Contiguous<K>, Finite> {
    pub fn covers(self, range: RangeTo<K>) -> Self;
}
```

Ends `Complete` only if the stream reached `until`: last key's successor
`== until`, or, for an empty stream, `next_after(after) == until`. Otherwise
`Aborted(OrderViolation("short"))`. An `Aborted` source keeps its own reason.
Order policies check steps between items; `covers` checks the end.

### 9.2 `defer_async` (D21)

```rust
Channel::defer_async(|| async move { /* -> Channel<T, Finite> */ }) -> Channel<T, Finite>
```

On first poll: spawn the future, wait (selectably) on the hand-off, then
delegate to the resolved channel, verdict included. In `chain_all`, only the
front phase is polled, so phases open one at a time.

### 9.3 The archive

```rust
fn stream(&self, bounds: ArchiveBounds) -> ArchiveStream {   // no longer async
    let covering = covering_snapshots(&manifest, bounds);    // sync read
    Channel::chain_all(covering.into_iter().map(|m| {
        let store = self.snapshot_store.clone();
        Channel::defer_async(move || async move {
            store.read_snapshot(m.snapshot_id, ArchiveRead).await
                 .unwrap_or_else(|e| Channel::once(Err(e)))
        })
    }))
    .take_while(not_above(bounds)).filter_map(within(bounds))
    .ordered::<Contiguous<LogIndex>>(after(bounds), "archive tiling")
    .ordered::<NonDecreasing<Hlc>>(None, "archive HLC")      // replaces assert!
    .covers(..until(bounds))                                  // replaces "not fully tiled"
}
```

`read_snapshot` stays async (11 callers). `StableArchiveReader::stream`
becomes synchronous.

### 9.4 `HistoricalReplay`

```rust
stable_log.range(after, until)                               // Finite scan, may have gaps
    .ordered::<StrictlyIncreasing<LogIndex>>(after, "stable scan")
    .stitch(|gap| archive.stream(gap.bounds()).into_channel())
    .ordered::<NonDecreasing<Hlc>>(None, "historical replay HLC")
    .covers(..until)                                          // trailing gap now aborts
```

One lazy expression on the consumer's thread: no task, no second channel, no
`block_on`, no `assert!`. The stitch fallback's hand endpoint check is replaced
by `covers` on the fallback.

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
subscriber fan-out uses `keyed(..) -> (Fanout, KeyedRegistry)`;
`Subscribers::publish` returns `Stopped` instead of ignoring it.

## 11. Complexity budget

**Essential, and where it lives.**

- Seam order (subscribe → cut) — `Segment::open`, one place.
- Resume rule and cursor — `Resumable`, one state machine (§8.2).
- Order and completeness — policies + `covers`, per item and at the end.
- Fan-out pacing (`Block`/`Shed`) — the distribution thread.

**Accepted accidental complexity.**

- The kind parameter on every signature (D1): verbose by design.
- `defer_async`'s hand-off wait: one private `Source` state.
- Two `Feed` kinds; three producer constructors.
- `Level` wraps tokio `watch` plus private `Wakes`: two notification paths
  behind one setter.

## 12. Edge cases

- **Empty window.** `covers` on an empty stream compares `next_after(after)`
  with `until`; `range(cut)` for `cut = None` is `empty()`.
- **Cancellation.** `until` preserves the kind and ends `Cancelled`; resumers
  never resume it (D18).
- **Order violation in a resumed stream** ends the stream; no loop.
- **Archive open error** is the first item of its phase; the consumer stops on
  `Err` as today.
- **`defer_async` without a runtime**: construction captures the handle;
  constructing outside a runtime panics at construction, not at first poll.
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
  `archive HLC`, `historical replay HLC`, `short` — surface as
  `StreamTruncated { what }` at the zslg boundary, as today.
- **Behaviour changes**: an HLC regression is an abort, not a panic; a short
  replay window aborts instead of ending early; `subscribe_stable_log` takes
  `after`.

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

## 15. Testing

**Crate.**

- Kind: `compile_fail` doctests — finite as infinite, `chain` on infinite,
  `empty()` as a subscription, `covers` on infinite.
- Combinators: `FnMut` state; kind transitions; `take_until` on infinite ends
  `Complete`, source abort preserved.
- Producers: close/panic verdicts for all three; `send_timeout`.
- `defer_async`: lazy start (no spawn before first poll), verdict delegation,
  phases open sequentially in `chain_all`.
- `covers`: reached, short, empty-window both ways, aborted source keeps reason.
- `Segment`: open order, `chain` vs `merge`, `filter_above*`.
- `Resumable`/`resync`: resume only on `ProducerGone`; `Cancelled` and
  `OrderViolation` end; `after` passed exactly; decline ends with previous
  reason.
- `Fanout`: `publish`→`Stopped`, `Drop` cancels, `shutdown` joins. `Level`:
  set-then-wake ordering, both readers.

**zslg/nvzn.**

- Split-HLC watermark test receives its count with a timeout.
- Archive: tiled, gap, short, HLC regression, open error.
- `HistoricalReplay`: mid gap stitched, trailing gap aborts, HLC regression
  aborts.
- Stable/unstable resync: cursor continuity, shutdown ends (no resync).
- nvzn live view: early stable end aborts, cancellation, `ReplayDone` order.
- Full suites under parallel load, clippy, fmt.

## 16. Implementation order (D17)

Each slice is green (tests, clippy, fmt) before the next.

1. **Mechanical.** Split-HLC test fix; `FnMut` combinators; `produce_*` and
   `Feed::send_timeout` (pre-kind: plain `Channel`); migrate producer sites and
   audit remaining `from`/`from_infinite`; `Fanout`/`FanoutConfig`/`Stopped`;
   `Level`, private `Wakes`.
2. **Completeness + archive.** `OrderedChannel` start → `after`,
   `Successor::next_after`; `covers`; `defer_async`; archive declaration;
   lazy `HistoricalReplay`; stitch fallback via `covers`.
3. **Kinds + seam.** `Channel<T, K>`, `Feed<T, K>`, `OrderedChannel<T, P, K>`,
   kind rules, `compile_fail` tests; `Seam` + `Segment::open` +
   `filter_above*`, end to end on the replica path; then all signatures in
   zslg/nvzn.
4. **Resumption.** `Opening`, `Resumable`, `resync`; `subscribe_stable_log(after)`;
   stable/unstable/nvzn migrated; delete `Tail`/`Ranged`/`blend_chain`,
   `resume_on_abort`'s public form if unused, `stable_stream`.
